"""
Resistance Gene Analysis Rules: Steps 10-11
Use CARD RGI to identify antibiotic resistance genes and evaluate novelty
"""

rule card_rgi_analysis:
    """
    Step 10: Run CARD RGI on assembled contigs
    Identifies antibiotic resistance genes, coverage %, identity %
    """
    input:
        assembly = f"{config['results_dir']}/{{sample}}/02_assembly/assembly_contigs.fasta",
        db_fresh = f"{config['results_dir']}/.databases_fresh"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        output_prefix = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results",
        output_format = config['card']['output_format']
    # Declared, not passed as a param. RGI's -n used to come from a params entry,
    # so Snakemake booked this rule at one core and would start `--cores` of them
    # at once, each spawning that many RGI threads: four samples on a four-core box
    # meant sixteen threads fighting over four cores, and RGI's 5-10 minutes
    # stretched to match. As a directive it is a budget Snakemake honours -- it
    # scales {threads} down to what is free, and never oversubscribes.
    #
    # Capped by PIPELINE_CORES (see Snakefile), never by config.yaml alone: RGI
    # validates -n against the CPUs it can see and exits non-zero if it is higher,
    # so a config asking for more cores than the box has does not merely oversubscribe
    # -- it fails the run.
    threads: min(config['resources']['max_parallel_samples'], PIPELINE_CORES)
    resources:
        # RGI is the heaviest local step, and --cores is no longer the CPU budget:
        # it is a job-slot budget, sized to let dozens of samples wait on BV-BRC at
        # once. `cpu` is the real budget, so the rule has to charge its threads to
        # it or nothing would hold it back.
        cpu = lambda wildcards, threads: threads
    output:
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json",
        rgi_csv = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.csv",
        # RGI writes this tab-delimited report itself alongside the JSON
        # (same --output_file prefix, .txt instead of .json).
        rgi_txt = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.txt"
    conda:
        "../envs/rgi.yml"
    log:
        f"{config['results_dir']}/logs/{{sample}}_rgi.log"
    shell:
        """
        rgi main \
            --input_sequence {input.assembly} \
            --output_file {output.rgi_json} \
            --input_type contig \
            -n {threads} \
            --clean

        python workflow/scripts/rgi_json_to_csv.py \
            {output.rgi_json} \
            {output.rgi_csv}
        """

rule evaluate_novelty:
    """
    Step 11: Evaluate novelty of resistance genes
    Flags genes with coverage < 100% or identity < 95% as potential novel variants
    """
    input:
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json",
        # RGI records each hit's coverage only here, never in the JSON.
        rgi_txt = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.txt"
    params:
        coverage_min = config['report']['novelty_thresholds']['coverage_min_pct'],
        identity_min = config['report']['novelty_thresholds']['identity_min_pct']
    output:
        novelty_report = f"{config['results_dir']}/{{sample}}/03_resistance/novelty_report.txt"
    log:
        f"{config['results_dir']}/logs/{{sample}}_novelty.log"
    script:
        "../scripts/evaluate_novelty.py"
