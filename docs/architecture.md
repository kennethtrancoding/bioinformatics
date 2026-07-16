# Architecture

Five figures summarize the deployed data paths. Each figure cites the repository
files that define it, so the diagram can be checked against the implementation.

## Figure 1. Upload and import

Both upload routes register one or more paired FASTQ files under a 12-character
job ID. Their disk behavior differs: a paired upload is copied to disk while S3
consumes the request stream, whereas a folder upload is staged first and then
offloaded to S3 one verified pair at a time. In an S3-backed deployment, a local
read is removed only after S3 confirms its copy; without S3, the local copy is
retained.

```mermaid
flowchart TB
    USER["Browser user<br/>uploads sequencing reads"]

    subgraph ROUTES["frontend.py routes"]
        direction TB
        PAIR["POST /submit<br/>single R1/R2 pair"]
        FOLDER["POST /import<br/>folder or cloud import"]
    end

    subgraph IMPORT["Import helpers"]
        direction TB
        PAIRSTREAM["import_samples.py<br/>stream pair to local temp file<br/>and optional S3 raw object"]
        FOLDERSTAGE["import_service.py<br/>stage uploaded folder first"]
        PAIRFASTQ["import_samples.py<br/>find R1/R2 pairs<br/>derive isolate IDs"]
        CHECKSUM["Verify supplied MD5s<br/>or workbook checksums<br/>when present"]
        ACCEPT["Accept sample<br/>write manifest rows"]
        REJECT["Reject bad pair<br/>delete local and S3 copies"]

        PAIRSTREAM --> CHECKSUM
        FOLDERSTAGE --> PAIRFASTQ --> CHECKSUM
        CHECKSUM -- valid or unchecked --> ACCEPT
        CHECKSUM -- mismatch --> REJECT
    end

    subgraph STATE["Persistent job state on disk"]
        direction TB
        JOBID["jobs.py<br/>12-character job ID"]
        SAMPLECSV[["config/jobs/&lt;job&gt;/samples.csv<br/>pipeline input manifest"]]
        UPLOADJSON[["config/jobs/&lt;job&gt;/uploads.json<br/>upload history"]]
        CHECKSUMJSON[["config/jobs/&lt;job&gt;/checksums.json<br/>observed checksums"]]
    end

    RAW[("S3 raw/&lt;job&gt;/&lt;file&gt;<br/>tag: raw-state=unrun")]
    RESPONSE["JSON response<br/>job ID + accepted isolates<br/>optional auto-run result"]

    USER --> PAIR
    USER --> FOLDER
    PAIR --> PAIRSTREAM
    FOLDER --> FOLDERSTAGE
    JOBID --> ACCEPT
    PAIRSTREAM --> RAW
    ACCEPT --> SAMPLECSV
    ACCEPT --> UPLOADJSON
    ACCEPT --> CHECKSUMJSON
    ACCEPT --> RESPONSE

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class USER,RESPONSE client
    class PAIR,FOLDER,PAIRSTREAM,FOLDERSTAGE,PAIRFASTQ,CHECKSUM,ACCEPT,REJECT app
    class RAW store
    class JOBID,SAMPLECSV,UPLOADJSON,CHECKSUMJSON state
```

> The job ID is the only per-batch access credential. It is generated with
> `secrets` from an ambiguity-free alphabet; anyone holding it can read that
> batch's status and results. Pair uploads with no supplied MD5, and folder files
> with no matching workbook checksum, are accepted without checksum verification.
> `frontend.py` · `workflow/helpers/import_service.py` ·
> `workflow/helpers/import_samples.py` · `workflow/helpers/jobs.py`

## Figure 2. Run admission and scheduling

The app runs at most two Snakemake processes by default and persists excess work
in a FIFO queue. Each process receives its own CPU, BV-BRC, and raw-read-on-disk
budgets; those pools are per run, not shared across the two concurrent processes.

```mermaid
flowchart TB
    USER["Browser user<br/>starts analysis for a job ID"]

    subgraph ROUTES["frontend.py"]
        direction TB
        RUNREQ["POST /run<br/>or upload auto-run"]
        STATUS["GET /status<br/>shows queued/running/done"]
        HEALTH["GET /api/health<br/>reports running and queued counts"]
    end

    subgraph MANAGER["workflow/helpers/pipeline_manager.py"]
        direction TB
        AUTH{"BV-BRC token<br/>is authenticated?"}
        ADMIT["Record admitted time<br/>clear old status markers"]
        CLAIM["s3_storage.py<br/>retag raw reads in-use"]
        SLOT{"Drain flag absent<br/>and process slot free?"}
        START["Start one Snakemake<br/>subprocess for this job"]
        ENQUEUE["Append job ID to<br/>persistent FIFO queue"]
        DRAIN["When a slot opens<br/>promote next queued job"]
        DENY["HTTP 401<br/>login required"]

        AUTH -- no --> DENY
        AUTH -- yes --> ADMIT --> CLAIM --> SLOT
        SLOT -- yes --> START
        SLOT -- no --> ENQUEUE
        ENQUEUE --> DRAIN --> START
    end

    subgraph LIMITS["Per Snakemake subprocess resource budgets"]
        direction TB
        CPU["cpu<br/>local rule work"]
        BVBRC["bvbrc<br/>remote BV-BRC jobs in flight"]
        UPLOADS["uploads<br/>local FASTQs materialized at once"]
        CPU -. constrains .-> START
        BVBRC -. constrains .-> START
        UPLOADS -. constrains .-> START
    end

    QUEUE[["config/jobs/.pipeline_queue.json<br/>survives restart"]]
    HIST[["config/jobs/.run_history.json<br/>successful durations"]]
    ETA["Status response<br/>queue position + wait/run estimate"]

    USER --> RUNREQ
    RUNREQ --> AUTH
    ENQUEUE --> QUEUE
    STATUS --> ETA
    QUEUE --> ETA
    HIST --> ETA
    START -- success --> HIST
    HEALTH -. read by refresh script .-> START

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class USER,ETA client
    class RUNREQ,STATUS,HEALTH,ADMIT,CLAIM,START,ENQUEUE,DRAIN,DENY,CPU,BVBRC,UPLOADS app
    class AUTH,SLOT,QUEUE,HIST state
```

> Admission retags reads `in-use` whether the run starts or queues. The S3 raw
> lifecycle matches only `unrun` objects, so queued inputs are protected from
> expiry. Snakemake's `--cores` value is deliberately a non-binding job-slot
> budget; the three named resource pools do the actual limiting.
> `frontend.py` · `workflow/helpers/pipeline_manager.py` ·
> `workflow/helpers/run_estimates.py` · `workflow/helpers/s3_storage.py`

## Figure 3. Per-sample analysis DAG

Snakemake fetches each read from S3 only when a sample is ready. Validation and
BV-BRC upload both consume the temporary reads; after both finish, Snakemake's
`temp()` cleanup removes them. The assembled contigs then fan out into independent
local and remote-assisted analyses.

```mermaid
flowchart TB
    MANIFEST[["config/jobs/&lt;job&gt;/samples.csv<br/>one row per isolate"]]
    RAW[("S3 raw FASTQs<br/>one R1/R2 pair per sample")]

    subgraph SNAKE["workflow/Snakefile includes rule files"]
        direction TB
        RAWRULES["rules/raw.smk<br/>fetch temporary reads from S3"]
        QCRULES["rules/qc.smk<br/>validate FASTQ structure"]
        BVRULES["rules/bvbrc.smk<br/>upload reads, infer genus,<br/>run assembly and annotation"]
        RESRULES["rules/resistance.smk<br/>CARD RGI and resistance hit processing"]
        MLSTRULES["rules/mlst.smk<br/>local MLST plus PubMLST rMLST"]
        MGERULES["rules/mobile_elements.smk<br/>MobileElementFinder calls"]
        BLASTRULES["rules/blast.smk<br/>local AMRFinderPlus BLAST,<br/>optional remote NCBI nr"]
        SUMMARYRULES["rules/summary.smk<br/>sample HTML and batch CSV reports"]
    end

    subgraph TEMPREADS["Temporary read lifecycle"]
        direction TB
        FETCH["Download R1/R2 only<br/>when this sample is ready"]
        VALIDATE["QC script consumes reads"]
        UPLOAD["BV-BRC upload consumes reads"]
        DROP["Snakemake temp() cleanup<br/>removes local FASTQs"]
        FETCH --> VALIDATE
        FETCH --> UPLOAD
        VALIDATE --> DROP
        UPLOAD --> DROP
    end

    subgraph REMOTE["Remote services called by rules"]
        direction TB
        BVBRC["BV-BRC<br/>Similar Genome Finder<br/>Comprehensive Genome Analysis"]
        PUBMLST["PubMLST rMLST API"]
        NCBI["NCBI nr BLASTP<br/>only for unresolved proteins"]
    end

    subgraph LOCAL["Local analysis after contigs exist"]
        direction TB
        CONTIGS["BV-BRC contigs and annotation<br/>become local inputs"]
        METRICS["Assembly metrics"]
        RGI["CARD RGI resistance genes"]
        MLST["MLST typing"]
        MEF["MobileElementFinder<br/>known mobile elements"]
        NOVELTY["Evaluate RGI<br/>coverage and identity"]
        PROTEINS["Export RGI hit<br/>proteins"]
        LOCALBLAST["Local BLAST against<br/>AMRFinderPlus catalog"]
        COLOC["ARG-MGE colocation<br/>overlap or nearby coordinates"]
        CONTIGS --> METRICS
        CONTIGS --> RGI
        CONTIGS --> MLST
        CONTIGS --> MEF
        RGI --> NOVELTY
        RGI --> PROTEINS --> LOCALBLAST
        RGI --> COLOC
        MEF --> COLOC
    end

    subgraph OUTPUT["Durable outputs"]
        direction TB
        REPORT["results/&lt;sample&gt;/summary/report.html"]
        MASTER["results/master_report.csv"]
        ZIP["prebuilt sample and job ZIPs"]
        S3RESULTS[("S3 results prefix<br/>HTML, CSV, ZIP")]
    end

    MANIFEST --> RAWRULES
    RAW --> RAWRULES --> FETCH
    QCRULES --> VALIDATE
    BVRULES --> UPLOAD
    UPLOAD --> BVBRC --> CONTIGS
    MLST --> PUBMLST
    LOCALBLAST --> NCBI
    METRICS --> REPORT
    NOVELTY --> REPORT
    MLST --> REPORT
    PUBMLST --> REPORT
    MEF --> REPORT
    COLOC --> MASTER
    REPORT --> ZIP --> S3RESULTS
    MASTER --> S3RESULTS


    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef local fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef remote fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef out fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    class MANIFEST,RAW,S3RESULTS store
    class RAWRULES,QCRULES,BVRULES,RESRULES,MLSTRULES,MGERULES,BLASTRULES,SUMMARYRULES,FETCH,VALIDATE,DROP,CONTIGS,METRICS,RGI,MLST,MEF,NOVELTY,PROTEINS,LOCALBLAST,COLOC local
    class UPLOAD,BVBRC,PUBMLST,NCBI remote
    class REPORT,MASTER,ZIP out
```

> Purple nodes call remote services; blue nodes execute locally. The report uses
> the compact mobile-element summary, while the master report uses RGI rows,
> species results, the full MGE call table, and ARG–MGE colocation. Protein export
> and QC are retained final artifacts even though neither feeds a report.
> `workflow/Snakefile` · `workflow/rules/raw.smk` ·
> `workflow/rules/bvbrc.smk` · `workflow/rules/summary.smk`

## Figure 4. Results, aborts, and retention

```mermaid
flowchart TB
    USER["Browser user<br/>knows only the job ID"]

    subgraph ROUTES["frontend.py result and control routes"]
        direction TB
        STATUS["GET /status<br/>live process or persisted status"]
        JOBPAGE["GET /job/&lt;job&gt;<br/>status page and report links"]
        VIEW["GET report HTML<br/>per-sample summary"]
        DOWNLOAD["Download sample ZIP,<br/>job ZIP, or master CSV"]
        ABORT["POST /abort"]
    end

    subgraph LOCAL["Local persistent volumes"]
        direction TB
        RESULTS[["data/results/&lt;job&gt;/<br/>working outputs and reports"]]
        CONFIG[["config/jobs/&lt;job&gt;/<br/>manifests, status, tokens"]]
        FIRST[[".first_viewed<br/>created once after terminal lookup"]]
        QUEUE[[".pipeline_queue.json<br/>queued jobs"]]
    end

    subgraph S3["S3-backed deployment"]
        direction TB
        S3RESULTS[("S3 results<br/>HTML, CSV, prebuilt ZIPs")]
        PRESIGN["5-minute presigned URL<br/>browser downloads from S3"]
    end

    subgraph CONTROL["workflow/helpers"]
        direction TB
        MANAGER["pipeline_manager.py<br/>queued: remove from FIFO<br/>running: TERM then KILL"]
        RETENTION["retention.py<br/>15-minute local sweep"]
        LIFECYCLE["deploy/s3-lifecycle.json<br/>S3 expiration policy"]
    end

    USER --> STATUS
    USER --> JOBPAGE
    USER --> VIEW
    USER --> DOWNLOAD
    USER --> ABORT
    STATUS --> CONFIG
    JOBPAGE --> CONFIG
    STATUS -- job done or failed --> FIRST
    VIEW -- prefer local copy --> RESULTS
    VIEW -. if local copy expired .-> S3RESULTS
    DOWNLOAD -- if S3 object exists --> PRESIGN --> S3RESULTS
    DOWNLOAD -. local fallback .-> RESULTS
    ABORT --> MANAGER
    MANAGER --> QUEUE
    MANAGER --> CONFIG
    FIRST -. starts local retention clock .-> RETENTION
    RETENTION -. deletes eligible local job data .-> RESULTS
    RETENTION -. keeps queued/running jobs .-> QUEUE
    LIFECYCLE -. expires old S3 objects .-> S3RESULTS

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class USER,PRESIGN client
    class STATUS,JOBPAGE,VIEW,DOWNLOAD,ABORT,MANAGER,RETENTION app
    class S3RESULTS store
    class RESULTS,CONFIG,FIRST,QUEUE,LIFECYCLE state

```

## Figure 5. Weekly database refresh

The production cron runs Sunday at 03:00. CARD/RGI and MobileElementFinder/MGEdb
live in Snakemake conda environments baked into the image, and the local
AMRFinderPlus BLAST catalog is built during the Docker build. Refreshing those
three resources therefore means building a candidate image and restarting onto
it without interrupting an active pipeline.

```mermaid
flowchart TB
    CRON["Host cron<br/>Sunday 03:00"]

    subgraph SCRIPT["deploy/refresh-databases.sh"]
        direction TB
        CHECK["docker inspect<br/>is bioinformatics container running?"]
        COLD["Cold path<br/>build image and restart service<br/>nothing active to drain"]
        DRAIN["Hot path<br/>docker exec touch<br/>/app/config/jobs/.drain"]
        BUILD["docker build<br/>candidate image with refreshed databases"]
        POLL["Poll /api/health<br/>pipelines.running count"]
        WAIT{"running == 0<br/>before 4-hour timeout?"}
        RESTART["sudo systemctl restart<br/>bioinformatics.service"]
        SKIP["Skip restart<br/>remove drain flag<br/>keep current container"]
    end

    subgraph WHY["What the rebuild refreshes"]
        direction TB
        RGI["workflow/envs/rgi.yml<br/>RGI plus CARD database"]
        MEF["workflow/envs/mefinder.yml<br/>MobileElementFinder plus MGEdb"]
        AMR["Dockerfile build step<br/>local AMRFinderPlus BLAST catalog"]
    end

    subgraph APP["Running Flask app behavior"]
        direction TB
        QUEUE["While .drain exists<br/>new runs persist to FIFO queue"]
        CURRENT["Current Snakemake runs<br/>continue in old container"]
        BOOT["Startup reconciliation<br/>clears drain and starts queue"]
        RESUME["Queued work resumes<br/>on current image if skipped"]
    end

    CRON --> CHECK
    CHECK -- no --> COLD --> BUILD --> RESTART
    CHECK -- yes --> DRAIN --> QUEUE
    DRAIN --> BUILD
    BUILD --> RGI
    BUILD --> MEF
    BUILD --> AMR
    BUILD --> CURRENT
    CURRENT --> POLL --> WAIT
    WAIT -- yes --> RESTART --> BOOT
    WAIT -- no or health unknown --> SKIP --> RESUME

    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    classDef stop fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    class CRON,CHECK,COLD,DRAIN,BUILD,POLL,RESTART,QUEUE,CURRENT,BOOT,RESUME app
    class WAIT,RGI,MEF,AMR state
    class SKIP stop
```

> The four-hour deadline begins after the image build finishes. If the health
> endpoint is unreachable, the script treats the running count as unknown rather
> than assuming the host is idle. BV-BRC, PubMLST, and NCBI `nr` remain remote
> services and are not refreshed by this job.
> `deploy/refresh-databases.sh` · `Dockerfile` ·
> `workflow/envs/rgi.yml` · `workflow/envs/mefinder.yml`
