#!/bin/bash
# EC2 user-data for Ubuntu: installs Docker and git at boot so the instance is
# ready to `git clone` and `docker build` as soon as it's up.
#
# Written for the AMI this project actually runs on -- Ubuntu 26.04 LTS, whose
# login user is `ubuntu`, not `ec2-user`. Pick an Ubuntu AMI to match: on an
# Amazon Linux instance none of this runs (there is no apt-get), the instance
# comes up without Docker, and the systemd unit fails on boot.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# Ubuntu's cloud images run unattended-upgrades on first boot, which holds the
# dpkg lock for a minute or two. Without a lock timeout apt exits immediately,
# and `set -e` then aborts user-data -- leaving an instance with no Docker and no
# obvious reason why. `apt-get` waits for the lock rather than failing.
APT="apt-get -o DPkg::Lock::Timeout=600"

$APT update
# docker.io + containerd from the Ubuntu archive, which is what this host runs.
# Deliberately not Docker's own CE repo: nothing here needs a newer engine, and
# adding a third-party apt source is one more thing that can break a boot.
$APT install -y docker.io git

systemctl enable --now docker

# So the `ubuntu` user can run docker without sudo. Takes effect on next login,
# which is fine: user-data itself runs as root.
usermod -aG docker ubuntu
