# HTTPS with Caddy

Puts the app behind a real certificate at `bioinformatics.kennethtrancoding.com`.
Before this, the app was reachable only over plaintext HTTP on port 5001, which
meant its Basic Auth password crossed the network base64-encoded — readable by
anything on the path.

Afterwards:

```
internet ──▶ :443  Caddy (TLS terminated, cert auto-renewed)
             :80   Caddy (ACME challenge + redirect to 443)
                     │
                     └──▶ 127.0.0.1:5001  gunicorn (not internet-reachable)
```

## Order matters

Each step below leaves the app reachable. **Do not close 5001 until HTTPS is
confirmed working** — closing it first, then hitting a snag with the
certificate, locks you out of your own instance's web UI.

## 1. DNS — done

```
bioinformatics.kennethtrancoding.com.  300  IN  A  13.57.78.169
```

`13.57.78.169` is an **Elastic** IP, which is what makes this durable: the
instance's auto-assigned public address changes on every stop/start, which would
break both the record and the certificate renewal that depends on it.

Allocating an Elastic IP does **not** attach it — "Allocate" and "Associate" are
separate actions in the console. Confirm the Elastic IPs page shows this address
with an associated instance ID, not blank. An allocated-but-unassociated EIP
routes nowhere: every port times out, and (unlike a closed firewall port) so does
SSH.

Associating also *releases* the instance's old auto-assigned IP, so update DNS to
the EIP at the same time.

## 2. Security group: open 80 and 443

Do this **before** starting Caddy. Right now only 5001 is open, so an ACME
challenge would be unreachable and fail.

| Port | Action                | Why                                                                        |
| ---- | --------------------- | -------------------------------------------------------------------------- |
| 443  | open                  | HTTPS                                                                      |
| 80   | open                  | Let's Encrypt fetches `/.well-known/acme-challenge/` here to issue and renew |
| 5001 | leave open **for now** | Your only way in until HTTPS works. Closed in step 5.                      |

Port 80 stays open permanently — Caddy serves nothing on it but a redirect, and
renewals every ~60 days need it.

## 3. Caddy

The host is **Ubuntu** (despite deploy/ec2-user-data.sh, which is written for Amazon
Linux and is not what this instance booted from). Caddy is not in Ubuntu's default
repos at a usable version, so install from Caddy's own:

```bash
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
  | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install -y caddy
```

Then point it at this repo's config:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo mkdir -p /var/log/caddy && sudo chown caddy:caddy /var/log/caddy

# Catch typos before a bad config burns a Let's Encrypt attempt. Validate *as the
# caddy user*: the config opens /var/log/caddy/access.log, which only that user
# can write, so validating as yourself reports a permission error on a config
# that is in fact fine.
sudo -u caddy caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile

sudo systemctl enable --now caddy
sudo journalctl -u caddy -f     # watch the first cert issuance
```

The Caddyfile already names `bioinformatics.kennethtrancoding.com`. Caddy asks
for a certificate for exactly that string, so it must keep matching the A record.

Wait for a log line reporting the certificate obtained. If it errors and retries,
**stop and fix the cause before restarting it** — repeated failures count against
a Let's Encrypt rate limit that can lock you out of retries for an hour.

## 4. App: loopback-only, one trusted proxy hop

```bash
sudo cp deploy/bioinformatics.service /etc/systemd/system/
echo 'TRUSTED_PROXY_HOPS=1' | sudo tee -a /etc/bioinformatics/app.env
sudo systemctl daemon-reload
sudo systemctl restart bioinformatics
```

`TRUSTED_PROXY_HOPS=1` is not optional cosmetics. Behind the proxy every request
reaches Flask from `127.0.0.1`; without it, the per-IP rate limit on job-ID
lookups collapses into a single bucket shared by every user.

## 5. Verify, then close 5001

```bash
curl -sI  https://bioinformatics.kennethtrancoding.com/api/health | head -1  # 200
curl -sI  http://bioinformatics.kennethtrancoding.com | head -1              # 308 → https
```

Only once those pass, **remove the 5001 inbound rule from the security group**.
Then confirm it's actually gone:

```bash
curl -sS --max-time 5 http://13.57.78.169:5001/api/health   # must fail to connect
```

That last check is the one people skip. If 5001 still answers from outside, the
container is still publishing to `0.0.0.0` and plaintext Basic Auth is still
exposed — at which point the security group is the only thing protecting it.

Curl the **Elastic IP**, not the hostname: the hostname's port 5001 could stop
answering merely because DNS moved, which proves nothing about the listener. And
run it *before* pulling the security-group rule — with the rule still in place, a
refused connection is a real result (the port is reachable, nothing is bound to
it). Afterwards the rule blocks the packet regardless, so the check passes even
if the container never stopped publishing to `0.0.0.0`.

## Notes

- **Certs survive restarts.** Caddy stores them under
  `/var/lib/caddy/.local/share/caddy`. Rebuilding the _app_ image never touches
  them. Replacing the _instance_ loses them, and Caddy re-issues on first boot.
- **The app is unchanged by TLS.** It still speaks plain HTTP on 5001 and still
  runs locally with no proxy at all. The only coupling is `TRUSTED_PROXY_HOPS`,
  which must be `1` when Caddy is in front and unset otherwise.
- **Uploads work because Caddy has no default request-body size limit.** nginx
  would have rejected FASTQ uploads at 1 MB until reconfigured; there is no
  equivalent cap to raise here.
