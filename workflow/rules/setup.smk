"""
Database Setup Rules: ordering dependency for the database-backed rules

CARD (RGI) and MGEdb (MobileElementFinder) live in Snakemake's per-rule conda
environments, so they refresh only when the image is rebuilt -- see
DATABASE_UPDATES.md. Nothing updates them at run time. This rule performs no
update; it exists to give those rules a common ordering dependency.
"""

rule setup_fresh_databases:
    """
    Create the marker flag the database-backed rules depend on.

    Databases are image content, refreshed by deploy/refresh-databases.sh.
    """
    output:
        flag = f"{config['results_dir']}/.databases_fresh"
    log:
        f"{config['results_dir']}/logs/setup_fresh_databases.log"
    shell:
        """
        echo "Databases are image content; refreshed by rebuild (see DATABASE_UPDATES.md)" | tee {log}
        touch {output.flag}
        """
