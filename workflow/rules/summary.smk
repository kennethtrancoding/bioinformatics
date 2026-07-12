rule generate_sample_report:
    """
    Generate HTML summary report for individual sample
    Integrates: assembly stats, resistance genes, species, BLAST hits, mobile elements
    """
    input:
        assembly_metrics = f"{config['results_dir']}/{{sample}}/02_assembly/genome_metrics.csv",
        card = f"{config['results_dir']}/{{sample}}/03_resistance/novelty_report.txt",
        blast = f"{config['results_dir']}/{{sample}}/04_blast/blast_results.csv",
        mlst = f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.json",
        mobile_element_finder = f"{config['results_dir']}/{{sample}}/06_mobile_elements/me_summary.csv"
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
    """
    input:
        card = expand(f"{config['results_dir']}/{{sample}}/03_resistance/rgi_results.csv", sample=SAMPLE_IDS),
        mlst = expand(f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.json", sample=SAMPLE_IDS),
        mobile_element_finder = expand(f"{config['results_dir']}/{{sample}}/06_mobile_elements/me_summary.csv", sample=SAMPLE_IDS),
        colocation = expand(f"{config['results_dir']}/{{sample}}/06_mobile_elements/{{sample}}_arg_mge_colocation.json", sample=SAMPLE_IDS)
    params:
        sample_ids = SAMPLE_IDS
    output:
        csv_report = f"{config['results_dir']}/master_report.csv"
    log:
        f"{config['results_dir']}/logs/master_report.log"
    script:
        "../scripts/generate_master_report.py"
