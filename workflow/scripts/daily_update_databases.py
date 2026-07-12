#!/usr/bin/env python3
"""
Daily database update script for RGI (CARD) and MobileElementFinder (MGEdb).

Updates packages from PyPI and records the timestamp. Run once per day via:
  - Cron: 0 2 * * * python3 workflow/scripts/daily_update_databases.py
  - Claude Code: /schedule "daily_update_databases.py" --cron "0 2 * * *"

Output: JSON with last update timestamp, used by frontend to display DB freshness.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def get_db_info_path():
    """Path to database update info JSON."""
    return Path("results/.db_update_info.json")


def update_databases():
    """Update RGI and MobileElementFinder from PyPI."""
    print(f"[{datetime.utcnow().isoformat()}] Starting daily database update...")

    success = True
    for package in ["rgi", "MobileElementFinder"]:
        try:
            print(f"  Updating {package}...")
            result = subprocess.run(
                ["pip", "install", "--upgrade", package],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode != 0:
                print(f"    ERROR: {result.stderr}")
                success = False
            else:
                print(f"    ✓ {package} updated")
        except Exception as e:
            print(f"    ERROR: {e}")
            success = False

    return success


def record_update():
    """Record update timestamp to JSON file read by frontend."""
    db_info_path = get_db_info_path()
    db_info_path.parent.mkdir(parents=True, exist_ok=True)

    info = {
        "last_updated_utc": datetime.utcnow().isoformat(),
        "strategy": "daily_check",
        "packages": ["rgi", "MobileElementFinder"]
    }

    with open(db_info_path, "w") as f:
        json.dump(info, f, indent=2)

    print(f"  Recorded update timestamp: {info['last_updated_utc']}")
    return info


def main():
    """Update databases and record timestamp."""
    try:
        success = update_databases()
        info = record_update()

        if success:
            print(f"\n✓ Database update complete at {info['last_updated_utc']}")
            return 0
        else:
            print(f"\n⚠ Database update completed with errors; check logs")
            return 1
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
