#!/usr/bin/env python3
"""Offline tests for scripts/webhook.py (standard library only, no network).
Run: python3 scripts/test_webhook.py"""
import hashlib, hmac, importlib.util, io, json, os, sys, tempfile, threading, unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "webhook", os.path.join(os.path.dirname(os.path.abspath(__file__)), "webhook.py"))
webhook = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(webhook)

CSV = "title,phone\nAcme,123\n"
JOBS = []        # what the mock engine lists
RECEIVED = []    # (headers, body) the mock receiver got
REPLY = [200]    # status code the receiver answers with


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path == "/api/v1/jobs":
            self._send(200, json.dumps(JOBS))
        elif self.path.endswith("/download"):
            self._send(200, CSV, "text/csv")
        else:
            self._send(404, "{}")

    def do_POST(self):  # the webhook receiver
        body = self.rfile.read(int(self.headers["Content-Length"]))
        RECEIVED.append((dict(self.headers), body))
        self._send(REPLY[0], "")


class Relay(unittest.TestCase):
    def setUp(self):
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{self.srv.server_port}"
        JOBS[:] = [{"ID": "old", "Name": "old", "Status": "ok"}]
        RECEIVED.clear()
        REPLY[0] = 200
        self.state = os.path.join(tempfile.mkdtemp(), "state.json")
        self.patches = [mock.patch.object(webhook.scrape, "BASE", base),
                        mock.patch.object(webhook, "URL", base + "/hook"),
                        mock.patch.object(webhook, "SECRET", "s3cret"),
                        mock.patch.object(webhook, "STATE", self.state),
                        mock.patch.object(sys, "argv", ["webhook.py", "--once"])]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        self.srv.shutdown()

    def run_once(self):
        with redirect_stdout(io.StringIO()):
            webhook.main()

    def test_first_run_baselines_then_sends_new_once(self):
        self.run_once()
        self.assertEqual(RECEIVED, [])  # already-finished jobs are not posted

        JOBS.extend([{"ID": "new", "Name": "café SP", "Status": "ok"},
                     {"ID": "busy", "Name": "x", "Status": "working"}])
        self.run_once()
        self.run_once()
        self.assertEqual(len(RECEIVED), 1)  # only "new", and only once
        headers, body = RECEIVED[0]
        self.assertEqual(body.decode(), CSV)
        self.assertEqual(headers["X-Job-Id"], "new")
        self.assertEqual(headers["X-Job-Name"], "caf%C3%A9%20SP")
        sig = "sha256=" + hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
        self.assertEqual(headers["X-Signature"], sig)

    def test_retries_then_gives_up(self):
        self.run_once()
        JOBS.append({"ID": "new", "Name": "n", "Status": "ok"})
        REPLY[0] = 500
        for _ in range(webhook.MAX_ATTEMPTS + 2):
            self.run_once()
        self.assertEqual(len(RECEIVED), webhook.MAX_ATTEMPTS)
        with open(self.state) as f:
            self.assertEqual(json.load(f)["failed"], {"new": webhook.MAX_ATTEMPTS})

    def test_deleted_jobs_are_forgotten(self):
        self.run_once()
        JOBS.clear()
        self.run_once()
        with open(self.state) as f:
            self.assertEqual(json.load(f)["sent"], [])


if __name__ == "__main__":
    unittest.main()
