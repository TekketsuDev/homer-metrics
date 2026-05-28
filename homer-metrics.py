#!/usr/bin/env python3
"""Homer metrics API — multithreaded: online, bandwidth, RAM, CPU, meta each on independent loops."""

import json
import os
import re
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request, urlopen

PVE_API   = os.environ["HOMER_PVE_API"]    # required — set in /etc/conf.d/homer-metrics
PVE_TOKEN = os.environ["HOMER_PVE_TOKEN"]
PVE_NODE  = os.environ.get("HOMER_PVE_NODE", "pve")

_services_file = os.environ.get("HOMER_SERVICES", "/opt/homer-services.json")
with open(_services_file) as _f:
    SERVICES = json.load(_f)

# Poll intervals — unchanged from original
T_ONLINE    = 2
T_BANDWIDTH = 0.5
T_META      = 30
T_PVE       = 1

cache      = {}
cache_lock = threading.Lock()
prev_net   = {}   # keyed by (exporter, sid)
prev_cpu   = {}   # keyed by (exporter, sid)
prev_lock  = threading.Lock()

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode    = ssl.CERT_NONE


# ── helpers ──────────────────────────────────────────────────────────────────

def _default_entry():
    return {"online": False, "latency_ms": 0,
            "cpu_pct": None, "ram_pct": None, "disk_pct": None,
            "net_rx": None, "net_tx": None,
            "net_rx_fmt": None, "net_tx_fmt": None,
            "version": None, "uptime_str": None}


def _patch(sid, **fields):
    with cache_lock:
        if sid not in cache:
            cache[sid] = _default_entry()
        cache[sid].update(fields)


def fetch_url(url, timeout=5):
    try:
        t0  = time.time()
        req = Request(url, headers={"User-Agent": "homer-metrics/1.0"})
        kw  = {"timeout": timeout}
        if url.startswith("https"):
            kw["context"] = _ssl_ctx  # reuse — was creating a new context every call
        urlopen(req, **kw)
        return True, int((time.time() - t0) * 1000)
    except Exception:
        return False, 0


def fetch_url_wget(url, timeout=6):
    try:
        t0 = time.time()
        r  = subprocess.run(
            ["wget", "-qO", "/dev/null", f"--timeout={timeout}", url],
            capture_output=True, timeout=timeout + 2
        )
        return (r.returncode == 0), int((time.time() - t0) * 1000)
    except Exception:
        return False, 0


def fetch_exporter(host_port, timeout=3):
    try:
        req = Request(f"http://{host_port}/metrics",
                      headers={"User-Agent": "homer-metrics/1.0"})
        with urlopen(req, timeout=timeout) as r:
            return _parse_exp(r.read().decode())
    except Exception:
        return None


def fetch_exporter_wget(host_port, timeout=6):
    try:
        r = subprocess.run(
            ["wget", "-qO", "-", f"--timeout={timeout}", f"http://{host_port}/metrics"],
            capture_output=True, timeout=timeout + 2
        )
        if r.returncode == 0:
            return _parse_exp(r.stdout.decode())
    except Exception:
        pass
    return None


def _parse_exp(text):
    vals = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = re.match(r'^(\w+)(\{[^}]*\})?\s+([\d.e+\-]+)', line)
        if m:
            vals.setdefault(m.group(1), {})[m.group(2) or ""] = float(m.group(3))
    return vals


def fmt_uptime(seconds):
    if seconds is None:
        return None
    d = int(seconds) // 86400
    h = (int(seconds) % 86400) // 3600
    m = (int(seconds) % 3600) // 60
    if d:   return f"{d}d {h}h"
    if h:   return f"{h}h {m}m"
    return f"{m}m"


def fmt_bytes(b):
    if b is None:
        return None
    if b < 1024:          return f"{b} B/s"
    if b < 1024 * 1024:   return f"{b/1024:.1f} KB/s"
    return f"{b/1024/1024:.1f} MB/s"


# ── per-metric thread functions ───────────────────────────────────────────────

def loop_online(svc):
    sid      = svc["id"]
    url      = svc["url"]
    use_wget = svc.get("wget", False)
    while True:
        try:
            if url:
                ok, ms = fetch_url_wget(url) if use_wget else fetch_url(url)
            else:
                ok, ms = False, 0
            _patch(sid, online=ok, latency_ms=ms)
        except Exception:
            pass
        time.sleep(T_ONLINE)


def loop_stats(exp, svcs, wget):
    """Fetch exporter once per cycle; update all services that share this exporter."""
    while True:
        try:
            vals = fetch_exporter_wget(exp) if wget else fetch_exporter(exp)
            if vals:
                now = time.time()

                rx_raw = {k: v for k, v in vals.get("node_network_receive_bytes_total",  {}).items() if '"lo"' not in k}
                tx_raw = {k: v for k, v in vals.get("node_network_transmit_bytes_total", {}).items() if '"lo"' not in k}
                rx = sum(rx_raw.values())
                tx = sum(tx_raw.values())

                mem   = vals.get("node_memory_MemTotal_bytes",     {})
                avail = vals.get("node_memory_MemAvailable_bytes", {})
                cpu_data = vals.get("node_cpu_seconds_total", {})
                idle  = sum(v for k, v in cpu_data.items() if 'mode="idle"' in k)
                cpu_total = sum(cpu_data.values())

                for svc in svcs:
                    sid = svc["id"]
                    key = (exp, sid)

                    with prev_lock:
                        prev = prev_net.get(key, {})
                        if prev.get("ts"):
                            dt = now - prev["ts"]
                            if dt > 0:
                                rx_bps = max(0, round((rx - prev["rx"]) / dt))
                                tx_bps = max(0, round((tx - prev["tx"]) / dt))
                                _patch(sid,
                                       net_rx=rx_bps, net_tx=tx_bps,
                                       net_rx_fmt=fmt_bytes(rx_bps),
                                       net_tx_fmt=fmt_bytes(tx_bps))
                        prev_net[key] = {"rx": rx, "tx": tx, "ts": now}

                    if mem and avail:
                        total = list(mem.values())[0]
                        free  = list(avail.values())[0]
                        if total:
                            if free > total:
                                mem_free = vals.get("node_memory_MemFree_bytes", {})
                                if mem_free:
                                    free = list(mem_free.values())[0]
                            _patch(sid, ram_pct=round((total - free) / total * 100, 1))

                    with prev_lock:
                        prev = prev_cpu.get(key, {})
                        if prev.get("ts"):
                            dt      = now - prev["ts"]
                            d_idle  = idle      - prev["idle"]
                            d_total = cpu_total - prev["total"]
                            if dt > 0 and d_total > 0:
                                _patch(sid, cpu_pct=round((1 - d_idle / d_total) * 100, 1))
                        prev_cpu[key] = {"idle": idle, "total": cpu_total, "ts": now}

        except Exception:
            pass
        time.sleep(T_BANDWIDTH)


def loop_meta(svc):
    sid = svc["id"]
    url = svc["url"]
    while True:
        try:
            version  = None
            uptime_s = None
            exp      = svc["exporter"]
            wget     = svc.get("wget", False)
            if exp:
                vals = fetch_exporter_wget(exp) if wget else fetch_exporter(exp)
                if vals:
                    boot = vals.get("node_boot_time_seconds", {})
                    if boot:
                        uptime_s = fmt_uptime(time.time() - list(boot.values())[0])
            if url:
                try:
                    if sid == "grafana":
                        d = json.loads(urlopen(Request(f"{url}/api/health",
                                               headers={"User-Agent": "homer-metrics/1.0"}), timeout=3).read())
                        version = d.get("version")
                    elif sid == "prometheus":
                        d = json.loads(urlopen(Request(f"{url}/api/v1/status/buildinfo",
                                               headers={"User-Agent": "homer-metrics/1.0"}), timeout=3).read())
                        v = d.get("data", {}).get("version", "")
                        version = v.split("+")[0] if v else None
                    elif sid == "gitea":
                        d = json.loads(urlopen(Request(f"{url}/api/v1/version",
                                               headers={"User-Agent": "homer-metrics/1.0"}), timeout=3).read())
                        version = d.get("version")
                except Exception:
                    pass
            _patch(sid, version=version, uptime_str=uptime_s)
        except Exception:
            pass
        time.sleep(T_META)


def loop_pve():
    sid = "proxmox"
    while True:
        try:
            req  = Request(f"{PVE_API}/nodes/{PVE_NODE}/status",
                           headers={"Authorization": PVE_TOKEN})
            data = json.loads(urlopen(req, context=_ssl_ctx, timeout=4).read())["data"]
            cpu  = round(data["cpu"] * 100, 1)
            ram  = round(data["memory"]["used"] / data["memory"]["total"] * 100, 1)
            disk = round(data["rootfs"]["used"]  / data["rootfs"]["total"] * 100, 1)
            upt  = fmt_uptime(data.get("uptime"))
            try:
                vreq  = Request(f"{PVE_API}/version", headers={"Authorization": PVE_TOKEN})
                vdata = json.loads(urlopen(vreq, context=_ssl_ctx, timeout=3).read())["data"]
                ver   = f"PVE {vdata.get('version', '')}"
            except Exception:
                ver = None
            _patch(sid, cpu_pct=cpu, ram_pct=ram, disk_pct=disk,
                   version=ver, uptime_str=upt)
        except Exception:
            pass
        time.sleep(T_PVE)


# ── HTTP handler ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/metrics"):
            with cache_lock:
                body = json.dumps(cache, ensure_ascii=False).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *a):
        pass


# ── bootstrap ─────────────────────────────────────────────────────────────────

def start_threads():
    exporter_groups = {}
    for svc in SERVICES:
        with cache_lock:
            cache[svc["id"]] = _default_entry()
        threading.Thread(target=loop_online, args=(svc,), daemon=True).start()
        threading.Thread(target=loop_meta,   args=(svc,), daemon=True).start()
        if svc.get("pve"):
            threading.Thread(target=loop_pve, daemon=True).start()
        exp = svc.get("exporter")
        if exp:
            exporter_groups.setdefault(exp, []).append(svc)

    for exp, svcs in exporter_groups.items():
        wget = any(s.get("wget") for s in svcs)
        threading.Thread(target=loop_stats, args=(exp, svcs, wget), daemon=True).start()


if __name__ == "__main__":
    start_threads()
    print("Metrics API listening on :8081")
    HTTPServer(("0.0.0.0", 8081), Handler).serve_forever()
