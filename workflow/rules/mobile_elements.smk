"""
MobileElementFinder Rules: Step 7 (mobile genetic element catalog)

Detects KNOWN mobile genetic elements -- insertion sequences, unit/composite
transposons, MITEs, ICEs, IMEs, CIMEs -- in each assembly by aligning it against
MGEdb (DTU CGE MobileElementFinder / `mefinder`).

It needs NO reference genome and catalogs the FIXED mobile-element content of each
genome directly from its assembly, which is the biologically informative result for
these mixed-species isolates. Runs unconditionally, like the other analysis stages.

Tool: MobileElementFinder 1.1.2, db: MGEdb (from PyPI; see envs/mefinder.yml).
"""

rule mobile_element_finder:
    """
    Run `mefinder find` on a sample's assembly. Writes <sample>.csv (the MGE table),
    <sample>_result.txt, and <sample>_mge_sequences.fna into 06_mobile_elements/.
    Uses a per-sample --temp-dir: mefinder's default temp path is derived from the
    (identical) input filename, so parallel samples would otherwise collide.
    """
    input:
        assembly = f"{config['results_dir']}/{{sample}}/02_assembly/assembly_contigs.fasta",
        db_fresh = f"{config['results_dir']}/.databases_fresh"
    params:
        outdir = f"{config['results_dir']}/{{sample}}/06_mobile_elements",
        sample_id = lambda wildcards: wildcards.sample
    output:
        csv = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}.csv",
        result_txt = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_result.txt",
        mge_sequences = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_mge_sequences.fna"
    threads: min(config['mobile_elements']['threads'], PIPELINE_CORES)
    resources:
        # See card_rgi_analysis: --cores is a job-slot budget now, so a CPU-bound
        # rule has to charge its threads to the `cpu` pool to stay bounded by the
        # cores the box actually has.
        cpu = lambda wildcards, threads: threads
    conda:
        "../envs/mefinder.yml"
    log:
        f"{config['results_dir']}/logs/{{sample}}_mefinder.log"
    shell:
        """
        ASM="$(cd "$(dirname {input.assembly})" && pwd)/$(basename {input.assembly})"
        LOG="$(pwd)/{log}"
        mkdir -p {params.outdir}
        cd {params.outdir}
        rm -rf mef_tmp && mkdir -p mef_tmp
        mefinder find -c "$ASM" -t {threads} --temp-dir mef_tmp {params.sample_id} > "$LOG" 2>&1
        rm -rf mef_tmp
        """


rule mobile_element_summary:
    """
    Parse the mefinder CSV into a compact per-sample summary
    (total MGEs + counts by element type).
    """
    input:
        csv = rules.mobile_element_finder.output.csv
    output:
        summary_csv = f"{config['results_dir']}/{{sample}}/06_mobile_elements/me_summary.csv",
        summary_json = f"{config['results_dir']}/{{sample}}/06_mobile_elements/me_summary.json"
    params:
        sample_id = lambda wildcards: wildcards.sample
    log:
        f"{config['results_dir']}/logs/{{sample}}_mefinder_summary.log"
    script:
        "../scripts/parse_mefinder.py"


rule mobile_element_colocation:
    """
    Cross-reference RGI resistance-gene coordinates with the MGE coordinates to
    answer the tutorial's key question: are the antibiotic resistance genes on a
    mobile element? A gene counts as linked if it overlaps an MGE or sits within
    colocation_proximity_bp of one on the same contig.
    """
    input:
        mge_csv = rules.mobile_element_finder.output.csv,
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json"
    output:
        colocation_csv = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.csv",
        colocation_json = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.json"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        proximity_bp = config['mobile_elements']['colocation_proximity_bp']
    log:
        f"{config['results_dir']}/logs/{{sample}}_mge_colocation.log"
    shell:
        """
        python3 workflow/scripts/mge_colocation.py \
            --mge-csv {input.mge_csv} \
            --rgi-json {input.rgi_json} \
            --out-csv {output.colocation_csv} \
            --out-json {output.colocation_json} \
            --sample-id {params.sample_id} \
            --proximity-bp {params.proximity_bp} \
            2>&1 | tee {log}
        """
