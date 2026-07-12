#!/bin/bash
# EC2 user-data for Amazon Linux 2023: installs Docker and git at boot so the
# instance is ready to `git clone` and `docker build` as soon as it's up.
set -euo pipefail

dnf update -y
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
