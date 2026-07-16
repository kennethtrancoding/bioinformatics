# State durability and backups

Finished results are already durable. With `RESULTS_S3_BUCKET` set, a completed
job's reports are pushed to S3 and survive both the local retention sweep and the
loss of the instance. This document is about the state that is **not** in S3 — the
in-flight job state on the instance's disk — and how to make it survive a disk or
instance failure.

## What is at risk

The app keeps per-job working state in four Docker named volumes
(`deploy/bioinformatics.service`):

| Volume                   | Holds                                                             |
| ------------------------ | ---------------------------------------------------------------- |
| `bioinformatics-config`  | sample manifests, BV-BRC tokens, the persisted run queue, run history for estimates |
| `bioinformatics-data`    | raw FASTQ staged for in-flight uploads                           |
| `bioinformatics-results` | a run's working outputs before it finishes and uploads to S3     |
| `bioinformatics-logs`    | per-run Snakemake logs                                           |

By default Docker stores these under `/var/lib/docker/volumes/` on the instance's
**root EBS volume**. Nothing outside S3 replicates them, so a lost EBS volume or a
terminated instance loses every job that has not yet finished — manifests, tokens,
the queue, and any run in progress. `systemd` restarts the container on crash and
the app reconciles the persisted queue on boot (README, *Runs and restarts*), but
only if the volume survived. A snapshot is what makes the volume survivable.

## The fix: scheduled EBS snapshots (DLM)

`deploy/dlm-snapshot-policy.json` is an AWS Data Lifecycle Manager policy that
snapshots the tagged volume every 6 hours and keeps a week of them (28 snapshots).
It is zero application code, captures the volume atomically at the block level, and
includes the BV-BRC tokens without ever copying credentials into object storage.

### Recommended layout: a dedicated data volume

Put the four Docker volumes on their own EBS volume, separate from the OS root. The
instance itself is disposable — it is rebuilt from `git clone` + `docker build` — so
the only thing worth restoring is this data volume, and keeping it separate makes a
restore "attach a volume" instead of "rebuild an instance." One-time, with the
container stopped:

```bash
INSTANCE_ID=<your-instance-id>
AZ=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text)

# Create the data volume in the instance's AZ and tag it so DLM will snapshot it.
VOL=$(aws ec2 create-volume --availability-zone "$AZ" --size 100 --volume-type gp3 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=backup,Value=bioinformatics-state},{Key=Name,Value=bioinformatics-state}]' \
  --query VolumeId --output text)
aws ec2 attach-volume --volume-id "$VOL" --instance-id "$INSTANCE_ID" --device /dev/sdf
```

Then on the host — **substitute the real device name** (`lsblk`; on Nitro
instances the volume above usually appears as `/dev/nvme1n1`, not `/dev/sdf`):

```bash
sudo systemctl stop bioinformatics
sudo mkfs.ext4 /dev/nvme1n1                      # ONLY on a brand-new, empty volume
sudo mkdir -p /mnt/docker-volumes
echo '/dev/nvme1n1 /mnt/docker-volumes ext4 defaults,nofail 0 2' | sudo tee -a /etc/fstab
sudo mount /mnt/docker-volumes

# Move existing Docker volume data onto it, then point Docker at the new location.
sudo systemctl stop docker
sudo rsync -aAX /var/lib/docker/volumes/ /mnt/docker-volumes/
sudo mv /var/lib/docker/volumes /var/lib/docker/volumes.bak
sudo ln -s /mnt/docker-volumes /var/lib/docker/volumes
sudo systemctl start docker
sudo systemctl start bioinformatics
```

Confirm the app came back with its jobs, then remove `/var/lib/docker/volumes.bak`.

**No dedicated volume?** The policy still works — tag the **root** volume with
`backup=bioinformatics-state` instead. Snapshots then include the whole OS, and
recovery means launching a replacement instance from the snapshot rather than
re-attaching a volume (see *Restore*).

### Turn the policy on

```bash
# 1. Create the DLM service role (once per account).
aws dlm create-default-role --resource-type snapshot
ROLE_ARN=$(aws iam get-role --role-name AWSDataLifecycleManagerDefaultRole \
  --query 'Role.Arn' --output text)

# 2. Create the lifecycle policy from the committed JSON.
aws dlm create-lifecycle-policy \
  --description "bioinformatics job-state EBS snapshots" \
  --state ENABLED \
  --execution-role-arn "$ROLE_ARN" \
  --policy-details file://deploy/dlm-snapshot-policy.json
```

DLM uses its own service role and does not touch the instance's IAM role
(`deploy/iam-policy-s3-results.json`) — nothing there changes. Verify the first
snapshot appears within a schedule interval:

```bash
aws ec2 describe-snapshots --owner-ids self \
  --filters Name=tag:snapshot-of,Values=bioinformatics-state \
  --query 'reverse(sort_by(Snapshots,&StartTime))[].{Id:SnapshotId,Started:StartTime,State:State}' \
  --output table
```

### Restore

**Dedicated data volume.** Create a volume from the newest snapshot, attach it to
the (new or existing) instance, and remount it where the Docker volumes live:

```bash
SNAP=$(aws ec2 describe-snapshots --owner-ids self \
  --filters Name=tag:snapshot-of,Values=bioinformatics-state \
  --query 'sort_by(Snapshots,&StartTime)[-1].SnapshotId' --output text)

NEW_VOL=$(aws ec2 create-volume --snapshot-id "$SNAP" --availability-zone "$AZ" \
  --volume-type gp3 \
  --tag-specifications 'ResourceType=volume,Tags=[{Key=backup,Value=bioinformatics-state}]' \
  --query VolumeId --output text)
aws ec2 attach-volume --volume-id "$NEW_VOL" --instance-id "$INSTANCE_ID" --device /dev/sdf
# then: mount it at /mnt/docker-volumes and `systemctl start bioinformatics`
```

**Root-volume snapshots.** Register the snapshot as (or launch it via) a new
instance — the snapshot is a full disk image, so a replacement instance comes up
with the OS, Docker, and every job volume intact. Retag its data volume
`backup=bioinformatics-state` so DLM keeps protecting the replacement.

## Recovery point

Up to 6 hours of job state can be lost — the snapshot interval. That is
proportionate here: a run is hours long, and re-running is cheap and safe because
Snakemake skips samples that already completed (README, *What a batch costs*). The
worst case is re-submitting the jobs created since the last snapshot, not redoing
finished work. Shorten `Interval` in the policy for a tighter window; snapshots are
incremental, so more frequent ones cost little.

## Alternative considered: mirroring metadata to S3

Mirroring `config/jobs/` to S3 on every write would give a near-zero recovery
point, but it puts BV-BRC bearer tokens in object storage, adds a write path to
every job mutation, and duplicates what one atomic volume snapshot already
captures. At two-active-runs scale the snapshot interval is the better trade.
Revisit this only alongside the move to a shared metadata store (README, *Scaling
beyond one host*), where the metadata leaves local disk anyway and the tokens can
live in a secrets manager rather than in a file.
