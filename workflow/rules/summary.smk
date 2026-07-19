rule generate_sample_report:
    """
    Generate HTML summary report for individual sample
    Integrates: assembly stats, resistance genes, species, BLAST hits, mobile elements

    Each tab renders one service's results in that service's own vocabulary, so it
    takes the per-service output that still carries it. The summarised forms alone
    cannot: novelty_report.txt paraphrases RGI's columns, me_summary.csv counts
    elements by type without naming them, and mlst_results.json keeps only the
    winning species from the rMLST response. All four additions are declared
    outputs of rules that already run, so this costs no extra work.
    """
    input:
        assembly_metrics = f"{config['results_dir']}/{{sample}}/02_assembly/genome_metrics.csv",
        card = f"{config['results_dir']}/{{sample}}/03_resistance/novelty_report.txt",
        rgi = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.csv",
        rgi_json = f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.json",
        blast = f"{config['results_dir']}/{{sample}}/04_blast/blast_results.csv",
        mlst = f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.json",
        rmlst_raw = f"{config['results_dir']}/{{sample}}/05_mlst/rmlst_raw.json",
        mobile_element_finder = f"{config['results_dir']}/{{sample}}/06_mobile_elements/me_summary.csv",
        mge_calls = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}.csv",
        colocation = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.json",
        # Per-gene, and already joined to a contig: mge_colocation.py normalises
        # RGI's ORF header down to a contig token the same way it normalises
        # mefinder's defline, which is what makes a per-contig resistance column
        # possible without re-deriving that join here.
        colocation_calls = f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.csv"
    output:
        html_report = f"{config['results_dir']}/{{sample}}/summary/report.html"
    log:
        f"{config['results_dir']}/logs/{{sample}}_report.log"
    script:
        "../scripts/generate_sample_report.py"

rule generate_master_report:
    """
    Generate CSV summary report for whole batch
    Integrates: isolate, species and percent confidence, mobile elements, beta lactamase proteins, antibiotic inactivation genes

    Takes mefinder's call table ({sample}.csv), not me_summary.csv: the report names
    the mobile elements, and the summary only counts them by type.
    """
    input:
        card = expand(f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.csv", sample=SAMPLE_IDS),
        mlst = expand(f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.json", sample=SAMPLE_IDS),
        mobile_element_finder = expand(f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}.csv", sample=SAMPLE_IDS),
        colocation = expand(f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.json", sample=SAMPLE_IDS)
    params:
        sample_ids = SAMPLE_IDS
    output:
        csv_report = f"{config['results_dir']}/master_report.csv"
    log:
        f"{config['results_dir']}/logs/master_report.log"
    script:
        "../scripts/generate_master_report.py"
