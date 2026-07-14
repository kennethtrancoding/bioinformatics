# Architecture

Five figures, each one flow. Sources of truth in the repo are cited under each
so a figure can be checked against the code rather than trusted.

## Figure 1. Upload

`POST /submit` accepts one pair of sequencing files; `POST /import` accepts a
folder of pairs. The files are large, so the app streams them to S3 *as the
request body arrives*. A temporary local copy exists only long enough to verify
the supplied checksum. Between upload and execution, S3 holds the only copy.

```mermaid
flowchart TB
    B["Browser<br/>paired sequencing files + checksums"]

    subgraph APP["App server — Flask behind Caddy (TLS)"]
        direction TB
        S1["1 · Stream files to S3<br/>while the request arrives"]
        S2["2 · Verify each checksum<br/>using a temporary local copy"]
        MATCH{"Checksums match?"}
        S3["3 · Record the sample<br/>and delete the local copy"]
        FAIL["Upload rejected<br/>delete local + S3 copies · HTTP 400"]
        S1 --> S2 --> MATCH
        MATCH -- Yes --> S3
        MATCH -- No --> FAIL
    end

    RAW[("S3 input files<br/>state: not yet run")]
    VOL[["Persistent volume<br/>samples.csv · BV-BRC token"]]
    OK["Browser receives job ID"]

    B --> S1
    S1 --> RAW
    S3 --> VOL
    S3 --> OK

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class B,OK client
    class S1,S2,S3,FAIL app
    class MATCH state
    class RAW store
    class VOL state
```

> The job ID is the only credential for a batch: 12 unguessable characters, and
> anyone holding it can read the results.
> `frontend.py:733` · `workflow/lib/s3_storage.py` · `workflow/lib/jobs.py`

## Figure 2. Run admission and scheduling

The service admits at most two workflow processes at a time. Each process has
separate limits for local CPU, calls to the external analysis provider, and
temporary disk use. This keeps one batch from exhausting the host or flooding a
remote service.

```mermaid
flowchart TB
    RUNREQ["POST /run<br/>external analysis credentials"]
    ADMIT["Authenticate · clear old markers<br/>protect inputs from expiry"]
    SLOT{"Run slot free<br/>and not draining?"}
    QUEUE[["Persisted FIFO queue"]]

    subgraph WORK["At most 2 runs at once"]
        direction TB
        POOLS["Per-run resource limits<br/>CPU · 12 external tasks<br/>temporary input disk"]
        RUN["Workflow engine<br/>one process per job"]
        POOLS -. constrains .-> RUN
    end

    HIST[["Run history<br/>past durations"]]
    ETA["Queue position + ETA"]

    RUNREQ --> ADMIT --> SLOT
    SLOT -- Yes --> RUN
    SLOT -- No --> QUEUE
    QUEUE -- Next free slot --> RUN
    QUEUE --> ETA
    HIST --> ETA
    RUN -- On success --> HIST

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class RUNREQ client
    class ADMIT,RUN,POOLS,ETA app
    class SLOT state
    class QUEUE,HIST state
```

> Admission retags a job's reads `in-use` whether the run *starts* or merely
> *queues*: the S3 lifecycle rule only expires `unrun` objects, so a job can sit
> in the queue for a week without its inputs being deleted underneath it.
> `workflow/lib/pipeline_manager.py`

## Figure 3. Per-sample analysis DAG

Sequencing produces millions of short text fragments rather than a finished
genome. An external service first reconstructs and labels a genome from those
fragments. Only then can the workflow run several independent analyses and
combine their findings into reports.

```mermaid
flowchart TB
    subgraph INPUT["1 · Prepare the input files"]
        direction TB
        RAW[("S3<br/>paired sequencing files")] --> FETCH["Download a temporary<br/>local copy"]
        FETCH --> VALIDATE["Check file structure<br/>(FASTQ validation)"]
    end

    subgraph ASSEMBLY["2 · External service: reconstruct a genome from read fragments"]
        direction TB
        UPLOAD["Upload files<br/>to BV-BRC"] --> GENUS["Identify broad<br/>organism group"] --> CGA["Build and label<br/>the genome · 40–60 min"]
    end

    subgraph ANALYSIS["3 · Run independent analyses on the reconstructed genome"]
        direction TB
        METRICS["Assembly quality<br/>and size metrics"]
        RGI["Find antibiotic<br/>resistance genes"]
        MLST["Classify the strain<br/>(MLST + rMLST)"]
        MEF["Find mobile DNA<br/>that can move between organisms"]
    end

    subgraph DERIVED["4 · Cross-check and summarize the findings"]
        direction TB
        NOVELTY["Flag unusual<br/>resistance matches"]
        PROTEINS["Export resistance<br/>protein files"]
        BLAST["Compare proteins with<br/>local catalog, then NCBI"]
        MESUM["Summarize<br/>mobile DNA"]
        COLOC["Find resistance genes<br/>on or near mobile DNA"]
    end

    subgraph OUTPUT["5 · Publish human- and machine-readable results"]
        direction TB
        REPORT["Per-sample<br/>report.html"] --> MASTER["Batch<br/>master_report.csv"]
    end

    FETCH --> UPLOAD
    CGA --> METRICS & RGI & MLST & MEF
    RGI --> NOVELTY & PROTEINS & BLAST
    MEF --> MESUM
    RGI & MEF --> COLOC
    METRICS & NOVELTY & BLAST & MLST & MESUM --> REPORT
    COLOC --> MASTER

    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef local fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef remote fill:#EEEDFE,stroke:#534AB7,color:#26215C
    classDef out fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    class RAW store
    class FETCH,VALIDATE,METRICS,RGI,MEF,NOVELTY,PROTEINS,MESUM,COLOC local
    class UPLOAD,GENUS,CGA,MLST,BLAST remote
    class REPORT,MASTER out
```

> Purple steps wait on an external service; blue steps run on this server.
> Uploading to BV-BRC is the only external step that reads the temporary FASTQ
> files. The long assembly consumes remote capacity, not local CPU.
> `workflow/rules/*.smk`

## Figure 4. Job management and retention

```mermaid
flowchart LR
    C["Browser + job ID"]

    STATUS["Check status<br/>queued · running · done · failed"]
    VIEW["View report"]
    DL["Download results"]
    ABORT["Abort job"]

    LOCAL[["Local result cache"]]
    S3R[("Durable S3 results<br/>reports + archives")]
    STOP["Queued: remove from queue<br/>Running: TERM, then KILL"]
    URL["5-minute presigned URL<br/>browser downloads from S3"]

    C --> STATUS & VIEW & DL & ABORT
    VIEW -- First choice --> LOCAL
    VIEW -. If local copy is gone .-> S3R
    DL --> URL --> S3R
    ABORT --> STOP --> STATUS

    subgraph RETENTION["Automatic retention (every 15 min)"]
        SWEEP["Apply age, first-view<br/>and pinned rules"]
    end
    SWEEP -. deletes eligible copies .-> LOCAL & S3R

    classDef client fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef store fill:#E1F5EE,stroke:#0F6E56,color:#04342C
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    class C,URL client
    class STATUS,VIEW,DL,ABORT,STOP,SWEEP app
    class S3R store
    class LOCAL state
```

What the sweep deletes, and when:

| Data | Deleted |
| --- | --- |
| Results | 3 h after the **first** view (the clock starts once and does not reset), or 7 days after completion, whichever comes first |
| Results marked `.pinned` | Never |
| Raw reads, run succeeded | Immediately — Snakemake drops them mid-run, the app deletes the S3 copies at the end |
| Raw reads, run failed or aborted | Retagged `unrun`, back on the 7-day lifecycle rule |
| Raw reads, never run | 7 days after upload |

> `workflow/lib/retention.py` · `workflow/lib/job_store.py:126` · `deploy/s3-lifecycle.json`

## Figure 5. Weekly database refresh

Cron, Sunday 03:00. The databases live *in the image* — RGI/CARD and
MobileElementFinder are installed into Snakemake's own per-rule conda
environments, and NCBI's AMRFinderPlus catalog is baked in at build time — so a
refresh is an image rebuild, and a rebuild is a restart. The restart would kill a
multi-hour assembly, hence the drain.

```mermaid
flowchart TB
    CRON["Sunday 03:00"] --> DRAIN["Drain<br/>new runs join the queue"]
    DRAIN --> BUILD["Build candidate image<br/>refresh 3 genomic reference catalogs"]
    BUILD --> WAIT{"Running jobs finished<br/>within 4 hours?"}
    WAIT -- Yes --> RESTART["Restart with new image"]
    RESTART --> BOOT["Clear drain<br/>resume persisted queue"]
    WAIT -- No --> SKIP["Keep current image<br/>clear drain · resume queue"]

    classDef app fill:#E6F1FB,stroke:#185FA5,color:#042C53
    classDef state fill:#FAEEDA,stroke:#854F0B,color:#412402
    classDef stop fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    class CRON,BUILD,RESTART,BOOT app
    class DRAIN,WAIT state
    class SKIP stop
```

> The build runs *during* the drain, not after it, so the conda solve overlaps
> with the wait rather than extending it. BV-BRC, PubMLST and NCBI `nr` are
> remote services — nothing here refreshes them.
> `deploy/refresh-databases.sh`
