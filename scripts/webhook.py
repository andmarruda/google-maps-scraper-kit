#!/usr/bin/env python3
"""Webhook relay: POSTs each finished job's CSV to WEBHOOK_URL.

The engine has no webhooks, so this polls GET /api/v1/jobs every WEBHOOK_INTERVAL seconds. When a
job reaches "ok", it downloads the CSV and POSTs it once. That covers every job, whoever created it:
the scripts, the web UI, or another app on the same machine.

Runs as the "webhook" compose service (see docker-compose.yml) or straight on the host:
    python3 scripts/webhook.py           # loop forever
    python3 scripts/webhook.py --once    # one pass, then exit

The request:
- Body: the raw CSV (Content-Type: text/csv), exactly what /api/v1/jobs/{id}/download returns.
- Headers: X-Job-Id, X-Job-Name (URL-encoded), and X-Signature: sha256=<hex HMAC of the body>
  when WEBHOOK_SECRET is set. The receiver should check the signature.
- Any 2xx marks the job as sent. Anything else is retried on the next pass, up to
  WEBHOOK_MAX_ATTEMPTS times.

Sent job IDs are kept in WEBHOOK_STATE so a restart does not resend. On the very first run (no state
file), jobs that are already finished are recorded as sent, not posted.

Uses only the Python standard library.
"""
import argparse, hashlib, hmac, json, os, sys, time, urllib.error, urllib.parse, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scrape  # noqa: E402  (shares the engine client and the .env loader)

scrape.load_env_file(("WEBHOOK_URL", "WEBHOOK_SECRET", "WEBHOOK_INTERVAL", "WEBHOOK_STATE",
                      "WEBHOOK_MAX_ATTEMPTS"))
URL = os.environ.get("WEBHOOK_URL", "")
SECRET = os.environ.get("WEBHOOK_SECRET", "")
INTERVAL = int(os.environ.get("WEBHOOK_INTERVAL") or 15)
MAX_ATTEMPTS = int(os.environ.get("WEBHOOK_MAX_ATTEMPTS") or 5)
STATE = os.environ.get("WEBHOOK_STATE") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, ".webhook-state.json")


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def load_state(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def save_state(path, state):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def post(job, body):
    headers = {"Content-Type": "text/csv; charset=utf-8", "User-Agent": scrape.UA,
               "X-Job-Id": job["ID"], "X-Job-Name": urllib.parse.quote(job.get("Name") or "")}
    if SECRET:
        headers["X-Signature"] = "sha256=" + hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    r = urllib.request.Request(URL, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(r, timeout=30) as resp:
        return resp.status


def tick(state):
    """One pass over the engine's jobs. Returns True when the state changed."""
    _, raw = scrape.req("GET", "/api/v1/jobs")
    jobs = json.loads(raw) or []
    ids = {j.get("ID") for j in jobs}
    # Forget jobs that were deleted from the engine, so the state file stays small.
    sent = [i for i in state["sent"] if i in ids]
    failed = {i: n for i, n in state["failed"].items() if i in ids}
    changed = len(sent) != len(state["sent"]) or len(failed) != len(state["failed"])
    state["sent"], state["failed"] = sent, failed

    for job in jobs:
        jid = job.get("ID")
        if job.get("Status") != "ok" or jid in sent or failed.get(jid, 0) >= MAX_ATTEMPTS:
            continue
        try:
            _, body = scrape.req("GET", f"/api/v1/jobs/{jid}/download")
            code = post(job, body)
            sent.append(jid)
            failed.pop(jid, None)
            log(f"sent {jid} ({len(body)} bytes) -> HTTP {code}")
        except (urllib.error.URLError, OSError) as e:
            failed[jid] = failed.get(jid, 0) + 1
            log(f"✗ {jid} attempt {failed[jid]}/{MAX_ATTEMPTS}: {e}")
        changed = True
    return changed


def main():
    ap = argparse.ArgumentParser(description="POST each finished job's CSV to WEBHOOK_URL.")
    ap.add_argument("--once", action="store_true", help="run one pass and exit")
    a = ap.parse_args()
    if not URL:
        sys.exit("✗ WEBHOOK_URL is not set. Add it to .env (see .env.example).")

    state = load_state(STATE)
    first_run = state is None
    state = state or {"sent": [], "failed": {}}
    log(f"webhook relay: {scrape.BASE} -> {URL} every {INTERVAL}s")

    while True:
        try:
            if first_run:
                _, raw = scrape.req("GET", "/api/v1/jobs")
                state["sent"] = [j["ID"] for j in json.loads(raw) or [] if j.get("Status") == "ok"]
                save_state(STATE, state)
                log(f"first run: {len(state['sent'])} finished job(s) recorded as sent, not posted")
                first_run = False
            elif tick(state):
                save_state(STATE, state)
        except (urllib.error.URLError, OSError, ValueError) as e:
            log(f"✗ engine unreachable at {scrape.BASE}: {e}")
        if a.once:
            break
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
