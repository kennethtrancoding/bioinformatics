"""
MLST Rules: Steps 14
Multi-locus sequence typing and species identification
"""

rule mlst_analysis:
    """
    Step 14: MLST / Species Identification
    Determines species and sequence type (ST) using PubMLST schemes
    """
    input:
        assembly = f"{config['results_dir']}/{{sample}}/02_assembly/assembly_contigs.fasta"
    params:
        sample_id = lambda wildcards: wildcards.sample,
        scheme = config['mlst']['scheme']
    output:
        mlst_results = f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.txt",
        mlst_json = f"{config['results_dir']}/{{sample}}/05_mlst/mlst_results.json",
        # Full PubMLST rMLST API response (all rank predictions + allele match
        # detail), not just the single best-species string parsed into
        # mlst_json. Always written (an {"error": ...} placeholder if the API
        # call fails), since it's a declared output.
        rmlst_raw = f"{config['results_dir']}/{{sample}}/05_mlst/rmlst_raw.json"
    conda:
        "../envs/mlst_arm64.yml"
    log:
        f"{config['results_dir']}/logs/{{sample}}_mlst.log"
    shell:
        """
        mlst {input.assembly} > {output.mlst_results} 2> {log}

        # Parse MLST output and resolve species (rMLST species-ID + scheme fallback)
        python3 workflow/scripts/parse_mlst.py \
            {output.mlst_results} {output.mlst_json} {params.sample_id} {input.assembly} \
            {output.rmlst_raw} \
            2>&1 | tee -a {log}
        """



  