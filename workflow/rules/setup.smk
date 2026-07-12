"""
Database Setup Rules: Daily updates via scheduled job

Databases (RGI/CARD and MobileElementFinder/MGEdb) are updated once per day
via the external scheduled job daily_update_databases.py. This rule creates
a marker flag that other rules depend on.
"""

rule setup_fresh_databases:
    """
    Create marker flag for database availability.

    Actual database updates are managed by an external scheduled job
    (daily_update_databases.py, run daily via cron or Claude Code scheduler).
    """
    output:
        flag = f"{config['results_dir']}/.databases_fresh"
    log:
        f"{config['results_dir']}/logs/setup_fresh_databases.log"
    shell:
        """
        echo "Databases updated daily via scheduled job (daily_update_databases.py)" | tee {log}
        touch {output.flag}
        """
