#!/usr/bin/env python3
"""report.py RESULTS_DATE_DIR [MORE_RESULTS_DIRS...] [--out report/index.html] [--json report/data.json]

Several results directories may be given: for each broker, runs are taken from the FIRST directory that has
runs/<broker>/ (so `report.py results/2026-10-05 results/2026-10-04` takes KubeMQ from the 5th and Kafka from
the 4th when the 5th has no Kafka runs). Baselines and digests are merged the same way, first directory wins.

Builds the results page from saved data only: every run under RESULTS_DATE_DIR/runs/<broker>/<cell>/<run>/
(result.json, workload.yaml, driver.yaml, omb.log, prom/*.json.gz, gate.json, kill/*), plus the cluster
baselines under RESULTS_DATE_DIR/infra/*/baseline/summary.json and digests.tsv.

Every status badge and headline value is computed here (gate rules = scripts/gate.sh, which this script
calls when gate.json is missing). Nothing is typed by hand. The page's <head> and CSS come from
report/template.html; the body is generated. Metric arithmetic: measured window from omb.log, counter rates over the window, md0 device, broker containers.
"""
import calendar, glob, gzip, html, json, os, re, subprocess, sys, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BROKER_NAME = {"kubemq": "KubeMQ", "kafka": "Kafka"}
BROKER_PRICE_PER_H = 3 * 0.3885  # 3 x n2-standard-8 on-demand list price, us-east1, USD/h (stated in Method)
TIE_PCT = 10.0


# ----------------------------------------------------------------------------- reading one run
def measured_window(run):
    start_iso = open(f"{run}/start.txt").read().strip()
    day_epoch = calendar.timegm(time.strptime(start_iso[:10], "%Y-%m-%d"))
    start_epoch = calendar.timegm(time.strptime(start_iso, "%Y-%m-%dT%H:%M:%SZ"))

    def log_epoch(hms):
        h, m, s = hms.split(":")
        t = day_epoch + int(h) * 3600 + int(m) * 60 + float(s)
        return t + 86400 if t < start_epoch - 3600 else t

    t0 = t1 = None
    for line in open(f"{run}/omb.log", errors="replace"):
        m = re.match(r"(\d\d:\d\d:\d\d\.\d+) ", line)
        if not m:
            continue
        if "Starting benchmark traffic" in line:
            t0 = log_epoch(m.group(1))
        elif t0 and "Aggregated Pub Latency" in line:
            t1 = log_epoch(m.group(1))
    return (t0 + 10, t1 - 10) if t0 and t1 else (None, None)


def series(run, name):
    p = f"{run}/prom/{name}.json.gz"
    return json.load(gzip.open(p))["data"]["result"] if os.path.exists(p) else []


def rate(vals, w0, w1):
    pts = [(float(t), float(v)) for t, v in vals if w0 <= float(t) <= w1]
    return (pts[-1][1] - pts[0][1]) / (pts[-1][0] - pts[0][0]) if len(pts) >= 2 else None


def gauge(vals, w0, w1, fn=max):
    pts = [float(v) for t, v in vals if w0 <= float(t) <= w1]
    return fn(pts) if pts else None


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def gate(run):
    if not os.path.exists(f"{run}/gate.json"):
        subprocess.run([f"{ROOT}/scripts/gate.sh", run], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return json.load(open(f"{run}/gate.json")) if os.path.exists(f"{run}/gate.json") else None


def resources(run, broker, payload_bps):
    w0, w1 = measured_window(run)
    r = {"disk_mbps": None, "disk_busy_pct": None, "cpu_cores": None, "worker_cpu_cores": None, "working_set_gib": None,
         "page_cache_gib": None, "nic_mbps": None, "bytes_per_payload_byte": None}
    if not w0:
        return r
    md0 = [rate(s["values"], w0, w1) for s in series(run, "node_disk_written_bytes_total") if s["metric"].get("device") == "md0"]
    md0 = [x for x in md0 if x]
    broker_nodes = {s["metric"]["instance"] for s in series(run, "node_disk_written_bytes_total") if s["metric"].get("device") == "md0"}
    if md0:
        r["disk_mbps"] = mean(md0) / 1e6
        if payload_bps:
            r["bytes_per_payload_byte"] = sum(md0) / (3 * payload_bps)
    busy = [rate(s["values"], w0, w1) for s in series(run, "node_disk_io_time_seconds_total") if s["metric"].get("device") == "md0"]
    busy = [x for x in busy if x is not None]
    if busy:
        r["disk_busy_pct"] = 100 * mean(busy)
    cont = "kafka" if broker == "kafka" else "bench"
    ns = "kafka" if broker == "kafka" else "kubemq"
    cpu = [rate(s["values"], w0, w1) for s in series(run, "container_cpu_usage_seconds_total")
           if s["metric"].get("container") == cont and s["metric"].get("namespace") == ns]
    cpu = [x for x in cpu if x is not None]
    if cpu:
        r["cpu_cores"] = mean(cpu); r["cpu_cores_max"] = max(cpu)
    wcpu = [rate(s["values"], w0, w1) for s in series(run, "container_cpu_usage_seconds_total")
            if s["metric"].get("container") == "worker" and s["metric"].get("namespace") == "omb"]
    wcpu = [x for x in wcpu if x is not None]
    if wcpu:
        r["worker_cpu_cores"] = max(wcpu)
    ws = [gauge(s["values"], w0, w1) for s in series(run, "container_memory_working_set_bytes")
          if s["metric"].get("container") == cont and s["metric"].get("namespace") == ns]
    ws = [x for x in ws if x]
    if ws:
        r["working_set_gib"] = mean(ws) / 2 ** 30
    pc = [gauge(s["values"], w0, w1) for s in series(run, "node_memory_Cached_bytes") if s["metric"].get("instance") in broker_nodes]
    pc = [x for x in pc if x]
    if pc:
        r["page_cache_gib"] = mean(pc) / 2 ** 30
    nic = {}
    for nm in ("node_network_transmit_bytes_total", "node_network_receive_bytes_total"):
        for s in series(run, nm):
            if s["metric"].get("device") == "eth0" and s["metric"].get("instance") in broker_nodes:
                x = rate(s["values"], w0, w1)
                if x is not None:
                    nic[s["metric"]["instance"]] = nic.get(s["metric"]["instance"], 0) + x
    if nic:
        r["nic_mbps"] = mean(list(nic.values())) / 1e6
    return r


def saturated(res, g, fio_mbps, iperf_gbps):
    if res["disk_mbps"] and fio_mbps and res["disk_mbps"] >= 0.9 * fio_mbps:
        return "disk"
    if res.get("cpu_cores_max") and res["cpu_cores_max"] >= 6.5:
        return "CPU"
    if res["nic_mbps"] and iperf_gbps and res["nic_mbps"] >= 0.9 * iperf_gbps * 125:
        return "network"
    if res["worker_cpu_cores"] and res["worker_cpu_cores"] >= 6.5:
        return "driver"
    if g and g["status"] != "pass" and (g["publish_delay_p99_ms"] >= 100 or g["publish_p99_ms"] > 50):
        return "client window"
    return "none"


def kill_facts(run):
    k = f"{run}/kill"
    out = {"ready_s": None, "in_sync_s": None, "zero_ack_s": None, "acked": None, "missing": None, "verdict": None,
           "proof": None, "tie_break": os.path.exists(f"{k}/tie-break.txt"), "licence_s": None, "controller": None, "note": None}
    if not os.path.isdir(k):
        return out
    if os.path.exists(f"{k}/verifier-verdict.json"):
        v = json.load(open(f"{k}/verifier-verdict.json"))
        out.update(acked=v.get("acked"), missing=v.get("missing"), verdict=v.get("verdict"),
                   zero_ack_s=(v.get("publish_unavailable_ms") or 0) / 1000)
    if os.path.exists(f"{k}/timeline.txt"):
        tl = open(f"{k}/timeline.txt").read()
        m = re.search(r"Ready again after (\d+) s", tl); out["ready_s"] = int(m.group(1)) if m else None
        m = re.search(r"no under-replicated partition .* (\d+) s after the kill", tl); out["in_sync_s"] = int(m.group(1)) if m else None
        m = re.search(r"longest zero-ack run=(\d+) s", tl)
        if m: out["zero_ack_s"] = int(m.group(1))
        m = re.search(r"kill proof: (\w+)", tl); out["proof"] = m.group(1) if m else None
        m = re.search(r"licen[cs]e .*?(\d+(?:\.\d+)?) ?(ms|s)\b", tl, re.I)
        if m: out["licence_s"] = float(m.group(1)) / (1000 if m.group(2) == "ms" else 1)
    out["note"] = open(f"{k}/NOTE.txt").read().strip() if os.path.exists(f"{k}/NOTE.txt") else None
    for f in ("controller-before.txt", "control-shard-leader-before.txt"):
        if os.path.exists(f"{k}/{f}"):
            out["controller"] = open(f"{k}/{f}").read().strip()[:200]
    return out


def read_run(run, fio, iperf):
    broker, cell, label = run.split("/")[-3:]
    res = json.load(open(f"{run}/result.json"))
    wl = open(f"{run}/workload.yaml").read()
    drv = open(f"{run}/driver.yaml").read() if os.path.exists(f"{run}/driver.yaml") else ""
    target = int(re.search(r"^producerRate:\s*(-?\d+)", wl, re.M).group(1))
    size = res["messageSize"]
    bs = re.search(r"batch\.size=(\d+)", drv)
    profile = ("b512" if bs.group(1) == "524288" else "std") if bs else ("b512" if label.endswith("-b512") else "std")
    carried = sum(res["publishRate"]) / len(res["publishRate"])
    g = gate(run) if target > 0 else None
    r = resources(run, broker, carried * size)
    kind = "kill" if os.path.isdir(f"{run}/kill") else ("100kb" if size >= 50000 else "1kb")
    return {
        "broker": broker, "cell": cell, "label": label, "profile": profile, "kind": kind, "target": target, "size": size,
        "carried": carried, "payload_mbps": carried * size / 1e6, "minutes": int(re.search(r"^testDurationMinutes:\s*(\d+)", wl, re.M).group(1)),
        "gate": g, "res": r, "saturated": saturated(r, g, fio.get(broker), iperf.get(broker)),
        "void": os.path.exists(f"{run}/VOID"), "start": open(f"{run}/start.txt").read().strip(),
        "extra": open(f"{run}/EXTRA").read().strip() if os.path.exists(f"{run}/EXTRA") else None,
        "kill": kill_facts(run) if kind == "kill" else None,
        "lat": {k: res[f"aggregated{k}"] for k in ("PublishLatency50pct", "PublishLatency99pct", "PublishLatency999pct",
                                                  "EndToEndLatency50pct", "EndToEndLatency99pct", "EndToEndLatency999pct")},
        "pd99": res["aggregatedPublishDelayLatency99pct"] / 1000, "bl0": (res["backlog"] or [0])[0], "bl1": (res["backlog"] or [0])[-1],
    }


def read_infra(root):
    fio, iperf, digests, versions = {}, {}, {}, {}
    for d in sorted(glob.glob(f"{root}/infra/*-*-*")):
        broker = os.path.basename(d).split("-")[1]
        sj = f"{d}/baseline/summary.json"
        if os.path.exists(sj):
            s = json.load(open(sj))
            fio[broker] = mean(s.get("fio_seqwrite_MBps", [])); iperf[broker] = mean(s.get("iperf_Gbps", []))
            versions.setdefault(broker, {})["fio_sync_iops"] = mean(s.get("fio_syncwrite_iops", []))
            versions[broker]["cpu"] = (s.get("cpu_models") or [""])[0].split(":")[-1].strip()
        if os.path.exists(f"{d}/digests.tsv"):
            for line in open(f"{d}/digests.tsv"):
                f = line.rstrip("\n").split("\t")
                if len(f) >= 4 and f[2] in ("kafka", "bench"):
                    digests[broker] = f[3]
    return fio, iperf, digests, versions


# ----------------------------------------------------------------------------- rendering
def esc(x):
    return html.escape(str(x))


def fmt(x, d=0, unit=""):
    if x is None:
        return "—"
    if isinstance(x, str):
        return esc(x)
    return f"{x:,.{d}f}{unit}"


def krate(x):
    return "—" if x is None else (f"{x / 1000:,.0f}k" if x >= 10000 else f"{x:,.0f}")


def extra_tag(r):
    return f' <span class="st" style="color:var(--text-3);border-color:var(--border)" title="{esc(r["extra"])}">extra</span>' if r.get("extra") else ""


def sustained(r):
    """achieved >= 99 % of target over the window and the last minute, producers on schedule (publish-delay p99 < 100 ms).
    This is how the ladder decided when to stop; it is a procedure, not a verdict on the broker."""
    g = r["gate"]
    return bool(g and g["criteria"]["rate"] and g["criteria"]["schedule"]) and not r["void"]


def win(a, b, lower_is_better=True):
    """(a_wins, b_wins) under the tie rule: no winner when the gap is under TIE_PCT"""
    if a is None or b is None:
        return False, False
    lo, hi = min(a, b), max(a, b)
    if hi == 0 or (hi - lo) / hi * 100 < TIE_PCT:
        return False, False
    a_better = a < b if lower_is_better else a > b
    return a_better, not a_better


def best(rows, broker, kind="sustained"):
    """highest target rung of broker that was sustained"""
    return max((r["target"] for r in rows if r["broker"] == broker and sustained(r)), default=None)


def at(rows, broker, target):
    return next((r for r in rows if r["broker"] == broker and r["target"] == target), None)


def achieved_note(r):
    """how the run ended: on target, or short and why"""
    if r is None:
        return ""
    g = r["gate"]
    if not g:
        return ""
    bits = []
    if not g["criteria"]["rate"]:
        bits.append(f"{100 * r['carried'] / r['target']:.1f} % of target")
    if not g["criteria"]["schedule"]:
        bits.append(f"producers {r['pd99'] / 1000:.1f} s behind schedule" if r["pd99"] >= 1000 else f"publish-delay p99 {r['pd99']:.0f} ms")
    if g["backlog_delta"] > r["target"]:
        bits.append(f"backlog grew {g['backlog_delta']:,}")
    return "; ".join(bits)


def ladder_table(rows, caption, with_100kb=False):
    hdr = ("<thead><tr><th scope=\"col\">Target</th>" + ("<th scope=\"col\">Payload MB/s</th>" if with_100kb else "") +
           "<th scope=\"col\">Broker</th><th scope=\"col\">Achieved</th><th scope=\"col\">Last 60 s</th><th scope=\"col\">Backlog Δ</th>"
           "<th scope=\"col\">Pub-delay p99</th><th scope=\"col\">Publish p50 / p99 / p99.9</th><th scope=\"col\">E2E p50 / p99 / p99.9</th>"
           "<th scope=\"col\">Disk MB/s</th><th scope=\"col\">CPU cores</th><th scope=\"col\">Limit seen</th></tr></thead>")
    out = [f'<div class="scroll"><table><caption>{esc(caption)}</caption>{hdr}<tbody>']
    for t in sorted({r["target"] for r in rows}):
        first = True
        for b in ("kubemq", "kafka"):
            r = at(rows, b, t)
            if r is None:
                if any(x["broker"] == b for x in rows):
                    out.append(f"<tr>{'<td></td>' if not first else f'<td>{krate(t)}</td>'}{'<td></td>' if with_100kb else ''}<td>{BROKER_NAME[b]}</td><td colspan=\"{10 if with_100kb else 9}\" style=\"text-align:left;color:var(--text-3)\">not run: this side's ladder had ended</td></tr>")
                    first = False
                continue
            g = r["gate"]; L = r["lat"]; R = r["res"]
            out.append("<tr>" + (f"<td>{krate(t)}</td>" + (f"<td>{fmt(t * r['size'] / 1e6)}</td>" if with_100kb else "") if first else "<td></td>" + ("<td></td>" if with_100kb else "")) +
                       f"<td>{BROKER_NAME[b]}{extra_tag(r)}</td><td>{fmt(r['carried'] / 1000, 1)}k ({100 * r['carried'] / r['target']:.1f} %)</td><td>{fmt(g['carried_last60_pct'], 1, ' %')}</td>"
                       f"<td>{fmt(g['backlog_delta'])}</td><td>{fmt(r['pd99'], 2 if r['pd99'] < 10 else 0, ' ms')}</td>"
                       f"<td>{L['PublishLatency50pct']:.1f} / {L['PublishLatency99pct']:.0f} / {L['PublishLatency999pct']:.0f} ms</td>"
                       f"<td>{L['EndToEndLatency50pct']:.0f} / {L['EndToEndLatency99pct']:.0f} / {L['EndToEndLatency999pct']:.0f} ms</td>"
                       f"<td>{fmt(R['disk_mbps'])}</td><td>{fmt(R['cpu_cores'], 1)}</td><td>{esc(r['saturated'] if r['saturated'] != 'client window' else 'client window')}{('; ' + esc(achieved_note(r))) if achieved_note(r) else ''}</td></tr>")
            first = False
    out.append("</tbody></table></div>")
    return "\n".join(out)


def ladder_svg(rows, title):
    """end-to-end p99 per target, log y, one line per broker; a cross marks the rung where that side's ladder ended"""
    import math
    targets = sorted({r["target"] for r in rows})
    if not targets:
        return ""
    W, H, L, R, T, B = 900, 340, 80, 30, 40, 60
    xs = {t: L + (W - L - R) * i / max(1, len(targets) - 1) for i, t in enumerate(targets)}
    vals = [r["lat"]["EndToEndLatency99pct"] for r in rows if r["lat"]["EndToEndLatency99pct"] > 0]
    lo = 0; hi = max(1, math.ceil(math.log10(max(vals + [100]))))
    def y(v):
        return T + (H - T - B) * (1 - (math.log10(max(v, 1)) - lo) / (hi - lo))
    s = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}: end-to-end 99th percentile latency per target rate, one line per broker; a cross marks the rung where that side stopped reaching its target">']
    s.append('<g class="axis" stroke-width="1">' + "".join(f'<line x1="{L}" y1="{y(10**d):.0f}" x2="{W-R}" y2="{y(10**d):.0f}"/>' for d in range(lo, hi + 1)) + "</g>")
    s.append('<g class="tick" text-anchor="end">' + "".join(f'<text x="{L-8}" y="{y(10**d)+4:.0f}">{10**d:,}</text>' for d in range(lo, hi + 1)) + "</g>")
    s.append('<text class="lab" x="12" y="20">end-to-end p99, ms (log scale)</text>')
    s.append('<g class="tick" text-anchor="middle">' + "".join(f'<text x="{xs[t]:.0f}" y="{H-B+25}">{krate(t)}</text>' for t in targets) + "</g>")
    s.append(f'<text class="lab" x="{(L+W-R)/2:.0f}" y="{H-8}" text-anchor="middle">target rate, msg/s</text>')
    for b, col in (("kafka", "var(--kafka)"), ("kubemq", "var(--kubemq)")):
        pts = sorted([r for r in rows if r["broker"] == b], key=lambda r: r["target"])
        if not pts:
            continue
        s.append(f'<polyline fill="none" stroke="{col}" stroke-width="2" points="' + " ".join(f"{xs[r['target']]:.0f},{y(r['lat']['EndToEndLatency99pct']):.0f}" for r in pts) + '"/>')
        for r in pts:
            cx, cy = xs[r["target"]], y(r["lat"]["EndToEndLatency99pct"])
            if sustained(r):
                s.append(f'<circle cx="{cx:.0f}" cy="{cy:.0f}" r="6" fill="{col}"/>')
            else:
                s.append(f'<g stroke="{col}" stroke-width="2.5"><line x1="{cx-6:.0f}" y1="{cy-6:.0f}" x2="{cx+6:.0f}" y2="{cy+6:.0f}"/><line x1="{cx+6:.0f}" y1="{cy-6:.0f}" x2="{cx-6:.0f}" y2="{cy+6:.0f}"/></g>')
            s.append(f'<text class="lab" x="{cx:.0f}" y="{cy-11:.0f}" text-anchor="middle" style="fill:{col}">{r["lat"]["EndToEndLatency99pct"]:.0f}</text>')
    s.append("</svg>")
    return "\n".join(s)


# ----------------------------------------------------------------------------- scorecard
def scorecard(std, b512, big, kills, fair):
    """model-card style table: one row per measure, one column per broker, best value in bold under the tie rule"""
    rows = []  # (group, label, mq_value, kf_value, mq_num, kf_num, lower_better, note)

    def add(group, label, mq, kf, lower_better=True, note="", fmt_fn=None):
        f = fmt_fn or (lambda x: x)
        rows.append((group, label, f(mq) if mq is not None else "—", f(kf) if kf is not None else "—", mq, kf, lower_better, note))

    def e2e(rows_, t):
        a, b = at(rows_, "kubemq", t), at(rows_, "kafka", t)
        return (a["lat"]["EndToEndLatency99pct"] if a else None, b["lat"]["EndToEndLatency99pct"] if b else None, a, b)

    G = "Throughput, 1 KB messages, 16 partitions, 3 replicas"
    add(G, "Highest target rate reached, 128 KB client batches", best(std, "kubemq"), best(std, "kafka"), False,
        "achieved ≥ 99 % of target and producers on schedule; 100k steps", lambda x: f"{krate(x)} msg/s")
    if b512:
        add(G, "Highest target rate reached, 512 KB client batches", best(b512, "kubemq"), best(b512, "kafka"), False, "same definition", lambda x: f"{krate(x)} msg/s")
    nxt = {}
    for b in ("kubemq", "kafka"):
        top = best(std, b)
        r = at(std, b, (top or 0) + 100000) if top else None
        nxt[b] = r
    if any(nxt.values()):
        add(G, "Next rung after the top, 128 KB client", *(f"{krate(nxt[b]['target'])} target → {fmt(nxt[b]['carried'] / 1000, 0)}k achieved; {achieved_note(nxt[b])}" if nxt[b] else None for b in ("kubemq", "kafka")),
            False, "why each ladder ended", lambda x: x)
        rows[-1] = rows[-1][:4] + (None, None) + rows[-1][6:]
    G = "End-to-end latency p50 / p99, 1 KB, 128 KB client batches"
    for t in (100000, 300000, 400000, 500000, 600000):
        m99, k99, a, b = e2e(std, t)
        if a is None and b is None:
            continue
        def cell(r):
            if r is None:
                return None
            n = achieved_note(r)
            return f"{r['lat']['EndToEndLatency50pct']:.0f} / {r['lat']['EndToEndLatency99pct']:.0f} ms" + (f" <small>({esc(n)})</small>" if n else "")
        rows.append((G, f"at {krate(t)} msg/s" + (" (15 min)" if t == 100000 else ""), cell(a) or "—", cell(b) or "—", m99 if a and sustained(a) else None, k99 if b and sustained(b) else None, True, "p99 compared; a side that did not reach the target is shown but not ranked"))
    G = "Publish latency p99 (producer ack), 1 KB, 128 KB client batches"
    for t in (100000, 300000, 400000):
        a, b = at(std, "kubemq", t), at(std, "kafka", t)
        if a is None and b is None:
            continue
        add(G, f"at {krate(t)} msg/s", a["lat"]["PublishLatency99pct"] if a else None, b["lat"]["PublishLatency99pct"] if b else None, True, "", lambda x: f"{x:.0f} ms")
    if big:
        G = "Large messages, 100 KB, 512 KB client batches"
        top = {b: best(big, b) for b in ("kubemq", "kafka")}
        sz = big[0]["size"]
        mbps = lambda x: f"{x:,.0f} msg/s = {x * sz / 1e6:.0f} MB/s"
        add(G, "Highest target rate reached", *(top[b] for b in ("kubemq", "kafka")), False, f"payload MB/s = rate × {sz / 1e6:.4f}", mbps)
        for t in (6000, 8000):
            a, b = at(big, "kubemq", t), at(big, "kafka", t)
            if a is None and b is None:
                continue
            tag = lambda r: (" <small>(extra)</small>" if r and r.get("extra") else "")
            rows.append((G, f"Achieved at {t:,} msg/s target ({t * sz / 1e6:.0f} MB/s)",
                         (mbps(a["carried"]) + tag(a)) if a else "—", (mbps(b["carried"]) + tag(b)) if b else "—",
                         a["carried"] if a else None, b["carried"] if b else None, False,
                         "disk sequential-write ceiling on this rig ≈ 820 MB/s per broker; \"extra\" = run past that side's ladder end"))
    if kills:
        G = "Resilience: partition leader killed (SIGKILL) at 50k msg/s"
        kk = {b: kills[b]["kill"] if b in kills else {} for b in ("kubemq", "kafka")}
        add(G, "Acknowledged messages lost", kk["kubemq"].get("missing"), kk["kafka"].get("missing"), True, "acked-ID ledger verifier beside the load", lambda x: f"{x:,}")
        add(G, "Longest interval with no acknowledgements", kk["kubemq"].get("zero_ack_s"), kk["kafka"].get("zero_ack_s"), True, "", lambda x: f"{x:.0f} s")
        add(G, "Killed server Ready again", kk["kubemq"].get("ready_s"), kk["kafka"].get("ready_s"), True, "", lambda x: f"{x:.0f} s")
        add(G, "All replicas back in sync", kk["kubemq"].get("in_sync_s"), kk["kafka"].get("in_sync_s"), True,
            "; ".join(f"{BROKER_NAME[b]}: {kk[b]['note'].split('.')[0]}" for b in ("kubemq", "kafka") if kk[b].get("note") and "upper bound" in kk[b]["note"]), lambda x: f"{x:.0f} s")
    if fair:
        G = f"Resources at {krate(fair)} msg/s, 1 KB, 128 KB client (highest rung both reached)"
        a, b = at(std, "kubemq", fair), at(std, "kafka", fair)
        add(G, "Broker CPU, cores of 7 (mean per broker)", a["res"]["cpu_cores"], b["res"]["cpu_cores"], True, "", lambda x: f"{x:.1f}")
        add(G, "Broker memory working set (cgroup, includes page cache)", a["res"]["working_set_gib"], b["res"]["working_set_gib"], True, "Kafka: 6 GiB heap + page cache; KubeMQ: Go heap + page cache", lambda x: f"{x:.1f} GiB")
        add(G, "Disk written per broker", a["res"]["disk_mbps"], b["res"]["disk_mbps"], True, "", lambda x: f"{x:.0f} MB/s")
        add(G, "Bytes written per payload byte", a["res"]["bytes_per_payload_byte"], b["res"]["bytes_per_payload_byte"], True, "write amplification incl. replication log", lambda x: f"{x:.2f}")
        add(G, "Network per broker (in + out)", a["res"]["nic_mbps"], b["res"]["nic_mbps"], True, "", lambda x: f"{x:.0f} MB/s")

    out = ['<div class="scroll"><table class="score"><caption>One run per cell. Bold = better by more than 10 %. No bold = within 10 % (a tie) or not comparable.</caption>'
           '<thead><tr><th scope="col">Measure</th><th scope="col"><span class="dot mq"></span>KubeMQ vKUBEMQ</th><th scope="col"><span class="dot kf"></span>Apache Kafka 4.3.1</th><th scope="col">Note</th></tr></thead><tbody>']
    last = None
    for group, label, mq, kf, mqn, kfn, lower, note in rows:
        if group != last:
            out.append(f'<tr class="grp"><td colspan="4">{esc(group)}</td></tr>'); last = group
        a, b = win(mqn, kfn, lower)
        out.append(f'<tr><td>{esc(label)}</td><td class="{"win" if a else ""}">{mq}</td><td class="{"win" if b else ""}">{kf}</td><td class="note">{esc(note)}</td></tr>')
    out.append("</tbody></table></div>")
    return "\n".join(out)


def _rcvbuf(run_root, broker):
    """The consumer receive.buffer.bytes a broker's runs used, read from a saved driver.yaml (None = client default)."""
    for f in glob.glob(f"{run_root}/runs/{broker}/*/run*/driver.yaml"):
        m = re.search(r"receive\.buffer\.bytes=(-?[0-9]+)", open(f).read())
        return m.group(1) if m else None
    return None


def consumer_note(sources, root):
    """One line for the Rig table: the consumer socket setting each side's runs used."""
    side = {b: _rcvbuf((sources or {}).get(b, root), b) for b in ("kubemq", "kafka")}
    if side["kubemq"] == side["kafka"]:
        return "consumers: " + ("client default socket receive buffer" if side["kubemq"] is None else f"receive.buffer.bytes={side['kubemq']}")
    say = lambda v: "client default (64 KB)" if v is None else f"receive.buffer.bytes={v}"
    return f"consumer socket receive buffer: KubeMQ runs {say(side['kubemq'])}, Kafka runs {say(side['kafka'])}"


def consumer_method(sources, root):
    """Method sentences on the consumer receive buffer and on how each side's runs started, true for whatever the runs used."""
    side = {b: _rcvbuf((sources or {}).get(b, root), b) for b in ("kubemq", "kafka")}
    fresh = lambda b: bool(glob.glob(f"{(sources or {}).get(b, root)}/runs/{b}/*/run*/fresh.log"))
    out = []
    if side["kubemq"] == "-1":
        out.append("Consumers run with receive.buffer.bytes=-1, so the kernel sizes the socket receive buffer; the kafka-clients "
                   "default pins it at 64 KB, which at these rates adds a stall of about 250 ms per consumer connection a few "
                   "times a minute, a property of the client and not of either broker.")
        if side["kafka"] != "-1":
            out.append("The Kafka runs predate this setting and used the default; their end-to-end latency tracks their publish "
                       "latency at every rung, so it is set by the producer side, not by the consumer buffer.")
    if fresh("kubemq") and not fresh("kafka"):
        out.append("Each KubeMQ run started on a freshly reinstalled KubeMQ with empty volumes; each Kafka run started on the "
                   "running cluster after the previous run's topic was deleted.")
    return " ".join(out)


def render(root, runs, fio, iperf, digests, versions, sources=None):
    date = os.path.basename(root.rstrip("/"))
    sources = sources or {}
    one_kb = [r for r in runs if r["kind"] == "1kb"]
    std = [r for r in one_kb if r["profile"] == "std"]; b512 = [r for r in one_kb if r["profile"] == "b512"]
    big = [r for r in runs if r["kind"] == "100kb"]
    kills = {r["broker"]: r for r in runs if r["kind"] == "kill"}
    fair = max((t for t in {r["target"] for r in std} if all(at(std, b, t) and sustained(at(std, b, t)) for b in ("kubemq", "kafka"))), default=None)

    parts = []
    day = lambda d: (re.match(r"\d{4}-\d{2}-\d{2}", d) or re.match(r".*", d)).group(0)  # a results folder's date, without its suffix
    kdate = day(os.path.basename(sources.get("kafka", root))); mdate = day(os.path.basename(sources.get("kubemq", root)))
    hero = ('<div class="context"><span class="dash"></span><span>Benchmark · <strong>KubeMQ vKUBEMQ</strong> vs <strong>Apache Kafka 4.3.1</strong> · identical GCP hardware · ' + esc(mdate if mdate == kdate else f"{mdate} / {kdate}") + '</span></div>'
            '<h1>Same hardware. Same client. Which broker carries more, faster?</h1>'
            '<p class="lede">Three n2-standard-8 servers on local NVMe per broker, one OpenMessaging Benchmark client with the same settings on both sides (one consumer setting differs by run date, see Method). Throughput ladder, latency at every rung, 100 KB messages, a leader kill with a loss verifier, and what each broker spends in CPU, memory and disk.</p>'
            '<div class="disclosure"><b>Run and published by KubeMQ</b>, the vendor of one of the two systems. Everything needed to rerun it is in the repository: cluster creation, both deployments, the frozen workloads, the runner, the raw results. The KubeMQ half needs a licence key from KubeMQ. '
            'Known weaknesses: one run per cell; Kafka configuration tuned and reviewed only by us, the vendor, not by an outside Kafka engineer; the two brokers\' acknowledgement semantics differ (see Rig)'
            + (f"; the two sides ran on different days and cluster incarnations ({', '.join(f'{BROKER_NAME[b]} {day(os.path.basename(rt))}' for b, rt in sources.items())}), same hardware shape, baselines within 10 %" if len(set(sources.values())) > 1 else "") + '.</div>'
            '<nav class="toc"><a href="#scorecard">Scorecard</a><a href="#ladder">Rate ladder</a><a href="#big">100 KB messages</a><a href="#kill">Leader kill</a><a href="#resources">Resources</a><a href="#rig">Rig and versions</a><a href="#method">Method</a><a href="#reproduce">Reproduce</a></nav>')
    parts.append('<h2 id="scorecard">Scorecard</h2>'
                 '<div class="note">Every number is <b>one run</b> (5 measured minutes; 15 for the 100k and kill cells). No repeats, so no spread is shown. There is no pass/fail verdict on this page: the numbers are reported as measured. '
                 '"Reached" means the brokers delivered at least 99 % of the target rate with the producers on schedule; that is how the rate ladder decided when to stop climbing.</div>' + scorecard(std, b512, big, kills, fair))
    parts.append('<h2 id="ladder">Rate ladder, 1 KB messages</h2>'
                 '<p>Fixed producer rate, 100k msg/s steps, 5 measured minutes per rung (15 at 100k). Each side climbed until a rung achieved less than 99 % of its target or the producers fell behind schedule (publish-delay p99 ≥ 100 ms); that rung is shown with a cross. Rungs marked "extra" were run beyond that point and are kept as data.</p>'
                 '<div class="legend"><span class="mq">KubeMQ vKUBEMQ</span><span class="kf">Kafka 4.3.1</span><span class="plain">dot = target reached, × = ladder ended here; label = e2e p99 ms</span></div>')
    for name, rows in (("128 KB client batches (cells A1 and B)", std), ("512 KB client batches (cell A2)", b512)):
        if rows:
            parts.append(f"<figure>{ladder_svg(rows, name)}<figcaption>{esc(name)}.</figcaption></figure>")
            parts.append(ladder_table(rows, f"Ladder, {name}. One run per rung."))
    parts.append("<h3>Why is Kafka's tail long at light load?</h3><div class=\"note\"><b>Not explained by this run.</b> At 100k msg/s Kafka's brokers were nearly idle (request-handler idle at least 98 %, GC pauses 18 to 46 ms per 10 s across three brokers, no old-generation collections) and Kafka's own Produce request timers showed a 99th percentile of 1 to 3 ms, yet the client measured a publish p99 above 50 ms in 51 of 90 ten-second windows, up to 252 ms. GC, log flushes and segment rolls do not line up with the slow windows. The delay therefore sits where this run has no instrument: in the client's accumulator and sender, or on the network before the broker reads the request. Kafka's exported percentiles are per request and have no maximum, so a few slow requests carrying many records could also hide under the broker's 99th. Analysis and per-window table: <code>results/2026-10-04/notes/kafka-light-load.md</code>.</div>")
    if big:
        parts.append('<h2 id="big">100 KB messages</h2><p>Bandwidth-bound workload with the 512 KB client (a 100 KB record cannot share a 128 KB batch). Disk sequential write on this rig is '
                     f'{fmt(fio.get("kubemq") or fio.get("kafka"))} MB/s per broker (fio). Both sides ran every rung; rungs past the point where a side stopped reaching its target are marked "extra".</p>' + ladder_table(big, "Cell C, one run per rung.", with_100kb=True))
    if kills:
        def kv(b, key, unit="", d=0):
            return fmt(kills[b]["kill"][key], d, unit) if b in kills and kills[b]["kill"][key] is not None else "—"
        parts.append('<h2 id="kill">Leader kill at 50k msg/s</h2><p>At minute 5 of a 15-minute run the server leading the most partitions receives SIGKILL from the node and restarts on the same volume. A separate verifier publishes with an acked-ID ledger; every acked message must be read back. One kill per side.</p>'
                     '<div class="scroll"><table><caption>Cell D.</caption><thead><tr><th scope="col"></th><th scope="col">KubeMQ</th><th scope="col">Kafka</th></tr></thead><tbody>'
                     f'<tr><td>Acked messages lost (verifier verdict)</td><td>{kv("kubemq","missing")} of {kv("kubemq","acked")} ({kv("kubemq","verdict")})</td><td>{kv("kafka","missing")} of {kv("kafka","acked")} ({kv("kafka","verdict")})</td></tr>'
                     f'<tr><td>Longest interval with zero acks</td><td>{kv("kubemq","zero_ack_s"," s")}</td><td>{kv("kafka","zero_ack_s"," s")}</td></tr>'
                     f'<tr><td>Killed server Ready</td><td>{kv("kubemq","ready_s"," s")}</td><td>{kv("kafka","ready_s"," s")}</td></tr>'
                     f'<tr><td>All replicas in sync</td><td>{kv("kubemq","in_sync_s"," s")}</td><td>{kv("kafka","in_sync_s"," s")}</td></tr>'
                     f'<tr><td>Victim was also controller / control-shard leader</td><td>{esc(kills["kubemq"]["kill"]["controller"] or "[recorded in kill/]") if "kubemq" in kills else "—"}</td><td>{esc(kills["kafka"]["kill"]["controller"] or "[recorded in kill/]") if "kafka" in kills else "—"}</td></tr>'
                     f'<tr><td>Locator tie-break used</td><td>{"yes" if "kubemq" in kills and kills["kubemq"]["kill"]["tie_break"] else "no"}</td><td>{"yes" if "kafka" in kills and kills["kafka"]["kill"]["tie_break"] else "no"}</td></tr>'
                     f'<tr><td>Kill proof (restart count +1, same volume inode, same image digest, survivors untouched)</td><td>{kv("kubemq","proof")}</td><td>{kv("kafka","proof")}</td></tr>'
                     '<tr><td>Timeouts left at default</td><td>Raft election (KubeMQ defaults)</td><td>broker.session.timeout.ms 9 s</td></tr></tbody></table></div>'
                     + "".join(f'<p style="font-size:13px"><b>{BROKER_NAME[b]} note:</b> {esc(kills[b]["kill"]["note"])}</p>' for b in ("kubemq", "kafka") if b in kills and kills[b]["kill"].get("note")))
    rr = [r for r in std if r["gate"]]
    if rr:
        out = ['<h2 id="resources">Resources</h2><p>From 1-second Prometheus samples on the broker nodes during the measured window, per rung both sides ran, 128 KB client. The highlighted row is the highest rung <b>both</b> sides reached. "Cores per 100k" = sum of the three brokers\' CPU ÷ carried rate. Memory is the container working set as the kernel accounts it, which includes page cache. Bold follows the tie rule.</p>'
               '<div class="scroll"><table><caption>Cell E, per broker unless stated.</caption><thead><tr><th scope="col">Rung</th><th scope="col">Broker</th><th scope="col">CPU cores (of 7)</th><th scope="col">Working set</th><th scope="col">Page cache (node)</th><th scope="col">Disk write MB/s</th><th scope="col">Disk busy</th><th scope="col">Bytes written / payload byte</th><th scope="col">NIC MB/s</th><th scope="col">Cores per 100k (cluster)</th><th scope="col">$ per billion msgs</th></tr></thead><tbody>']
        for t in sorted({r["target"] for r in rr}):
            pair = {b: at(rr, b, t) for b in ("kubemq", "kafka") if at(rr, b, t)}
            def c100(r):
                return 3 * r["res"]["cpu_cores"] / (r["carried"] / 1e5) if r["res"]["cpu_cores"] else None
            def usd(r):
                return BROKER_PRICE_PER_H / (r["carried"] * 3600) * 1e9 if sustained(r) else None
            wins = {"kubemq": {}, "kafka": {}}
            if len(pair) == 2:
                for key, f in (("cpu", lambda r: r["res"]["cpu_cores"]), ("ws", lambda r: r["res"]["working_set_gib"]), ("c100", c100), ("usd", usd)):
                    a, b = win(f(pair["kubemq"]), f(pair["kafka"]))
                    wins["kubemq"][key] = a; wins["kafka"][key] = b
            first = True
            for b in ("kubemq", "kafka"):
                if b not in pair:
                    continue
                r = pair[b]; R = r["res"]; cls = ' class="fair"' if t == fair else ""
                def td(v, key):
                    return f'<td class="win">{v}</td>' if wins[b].get(key) else f"<td>{v}</td>"
                out.append(f"<tr{cls}><td>{krate(t) if first else ''}</td><td>{BROKER_NAME[b]}{'' if sustained(r) else ' (target not reached)'}{extra_tag(r)}</td>"
                           + td(fmt(R["cpu_cores"], 1), "cpu") + td(fmt(R["working_set_gib"], 1, " GiB"), "ws")
                           + f"<td>{fmt(R['page_cache_gib'], 0, ' GiB')}</td><td>{fmt(R['disk_mbps'])}</td><td>{fmt(R['disk_busy_pct'], 0, ' %')}</td><td>{fmt(R['bytes_per_payload_byte'], 2)}</td><td>{fmt(R['nic_mbps'])}</td>"
                           + td(fmt(c100(r), 1), "c100") + td(("$" + fmt(usd(r), 1)) if usd(r) else "—", "usd") + "</tr>")
                first = False
        out.append(f'</tbody></table></div><p>$ per billion messages = hourly on-demand list price of that cluster\'s 3 broker nodes (${BROKER_PRICE_PER_H:.2f}/h) ÷ messages carried per hour, × 10⁹.</p>')
        parts.append("\n".join(out))
    parts.append('<h2 id="rig">Rig and versions</h2><div class="scroll"><table><caption>Both clusters identical except the broker. Baselines per cluster incarnation, from results/' + esc(date) + '/infra/.</caption><tbody>'
                 '<tr><td>Brokers</td><td>3 × n2-standard-8 (' + esc(versions.get("kafka", {}).get("cpu") or versions.get("kubemq", {}).get("cpu") or "Intel Ice Lake") + '), 7 CPU / 26 GiB per broker, one per node</td></tr>'
                 f'<tr><td>Disk</td><td>2 × 375 GB local NVMe as RAID-0; fio sequential write {fmt(fio.get("kafka"))} / {fmt(fio.get("kubemq"))} MB/s (Kafka / KubeMQ cluster), synced writes {fmt(versions.get("kafka", {}).get("fio_sync_iops"))} / {fmt(versions.get("kubemq", {}).get("fio_sync_iops"))} per s</td></tr>'
                 f'<tr><td>Network</td><td>iperf3 between brokers: {fmt(iperf.get("kafka"), 1)} / {fmt(iperf.get("kubemq"), 1)} Gbit/s</td></tr>'
                 '<tr><td>Driver</td><td>2 × n2-standard-8, OpenMessaging Benchmark (commit in omb/VERSION), stock Kafka driver, identical YAML both sides except bootstrap; 128 KB and 512 KB batch profiles; ' + esc(consumer_note(sources, root)) + '</td></tr>'
                 f'<tr><td>Kafka</td><td>4.3.1, KRaft combined broker+controller (non-recommended for production, chosen for symmetry with KubeMQ\'s in-process Raft), Strimzi 1.2.0, image <code>{esc(digests.get("kafka", "see infra/kafka/versions.env"))}</code>, heap 6 GiB, tuned config in infra/kafka/kafka.yaml, RF 3, min ISR 2</td></tr>'
                 f'<tr><td>KubeMQ</td><td>vKUBEMQ, image <code>{esc(digests.get("kubemq", "see infra/kubemq/versions.env"))}</code>, chart 3.5.0, shipped defaults (8 shards, leaders balanced, append-only log, default ack policy), GOMEMLIMIT 20 GiB</td></tr>'
                 '<tr><td><b>What an ack means</b></td><td><b>Kafka</b>: acks=all, the leader waits for every in-sync replica (normally 3 of 3) to have the write in the OS; no per-write fsync. <b>KubeMQ</b>: ack when 2 of 3 Raft members have the write in the OS; fsync in the background. These are not identical.</td></tr>'
                 '</tbody></table></div>')
    parts.append('<h2 id="method">Method</h2><p>' + esc(consumer_method(sources, root)) + ' Fixed-rate runs only (OMB\'s unthrottled mode understates one side). The ladder climbed in 100k steps until a side achieved less than 99 % of target or its producers fell behind schedule; later rungs are marked "extra". No pass/fail verdict is applied; latency and resource figures are reported as measured at every rung. One run per cell; a run with an infrastructure fault is VOID and rerun once (one Kafka kill attempt was void for a harness fault and rerun). One discarded warm-up after every deploy; topic deleted and disk hygiene checked between runs. Kafka: tuned (tuning/kafka.md); KubeMQ: shipped defaults. Not tested: fan-out, soak, backlog drain, many partitions, strict fsync, repeats. Full method: docs/METHOD.md.</p>')
    parts.append('<h2 id="reproduce">Reproduce</h2><pre>echo "$KEY" &gt; .license.txt\nterraform -chdir=infra/terraform init\nscripts/build-images.sh\nscripts/up.sh kafka ; sleep 120 ; scripts/up.sh kubemq\ncaffeinate -i scripts/matrix.sh --slot a    # terminal 1\ncaffeinate -i scripts/matrix.sh --slot b    # terminal 2\nscripts/report.py ' + ' '.join(esc(f'results/{day(os.path.basename(v))}') for v in (sources or {root: root}).values()) + '\nscripts/down.sh --slot a ; scripts/down.sh --slot b</pre>'
                 '<p>Raw OMB JSON, 1 s Prometheus exports, verifier ledgers, fio and iperf baselines, image digests: ' + ', '.join(f'<code>results/{esc(day(os.path.basename(v)))}/</code>' for v in (sources or {root: root}).values()) + '.</p>'
                 f'<p style="font-size:12px;color:var(--muted)">Generated by scripts/report.py from results/{esc(date)} on {time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())}.</p>')
    body = "\n".join(parts)
    tpl = open(f"{ROOT}/report/template.html").read()
    page = tpl.replace("<!-- HERO -->", hero).replace("<!-- BODY -->", body)
    page = page.replace('<title>KubeMQ vs Kafka Benchmark</title>', f'<title>KubeMQ vKUBEMQ vs Kafka 4.3.1 — benchmark {esc(date)}</title>')
    # README: the scorecard as markdown
    md = ["| Measure | KubeMQ vKUBEMQ | Apache Kafka 4.3.1 |", "|---|---|---|"]
    for m in re.finditer(r'<tr class="grp"><td colspan="4">(.*?)</td></tr>|<tr><td>(.*?)</td><td class="(win)?">(.*?)</td><td class="(win)?">(.*?)</td><td class="note">', scorecard(std, b512, big, kills, fair)):
        if m.group(1):
            md.append(f"| **{html.unescape(m.group(1))}** | | |")
        else:
            strip = lambda x: re.sub(r"<.*?>", "", html.unescape(x))
            a = ("**" + strip(m.group(4)) + "**") if m.group(3) else strip(m.group(4))
            b = ("**" + strip(m.group(6)) + "**") if m.group(5) else strip(m.group(6))
            md.append(f"| {strip(m.group(2))} | {a} | {b} |")
    # The KubeMQ version shown everywhere = the tag of the image the KubeMQ runs used (digests.tsv).
    m = re.search(r":(v[0-9][0-9.]*)@", digests.get("kubemq", ""))
    if m:
        page = page.replace("vKUBEMQ", m.group(1)); md = [l.replace("vKUBEMQ", m.group(1)) for l in md]
    return page, "\n".join(md)





def main():
    args = sys.argv[1:]
    if not args:
        print(__doc__); sys.exit(2)
    roots = [a.rstrip("/") for a in args if not a.startswith("--") and (args.index(a) == 0 or not args[args.index(a) - 1].startswith("--"))]
    root = roots[0]
    out_html = args[args.index("--out") + 1] if "--out" in args else f"{ROOT}/report/index.html"
    out_json = args[args.index("--json") + 1] if "--json" in args else f"{ROOT}/report/data.json"
    fio, iperf, digests, versions = {}, {}, {}, {}
    source_of = {}
    for rt in roots:
        f, i, d, v = read_infra(rt)
        for b in ("kafka", "kubemq"):
            if b not in source_of and glob.glob(f"{rt}/runs/{b}/*/*/result.json"):
                source_of[b] = rt
                if b in f: fio[b] = f[b]
                if b in i: iperf[b] = i[b]
                if b in d: digests[b] = d[b]
                if b in v: versions[b] = v[b]
    print("runs taken from: " + ", ".join(f"{b} ← {rt}" for b, rt in source_of.items()), file=sys.stderr)
    runs = []
    for rj in sorted(x for b, rt in source_of.items() for x in glob.glob(f"{rt}/runs/{b}/*/*/result.json")):
        run = os.path.dirname(rj)
        if os.path.basename(run).startswith("warmup") or os.path.exists(f"{run}/VOID"):
            continue  # VOID runs stay on disk with their reason; they never enter the tables
        try:
            runs.append(read_run(run, fio, iperf))
        except Exception as e:  # a broken run directory is reported, never silently dropped
            print(f"skip {run}: {e}", file=sys.stderr)
    # one run per (broker, cell, profile): keep the newest, warn on duplicates
    seen = {}
    for r in runs:
        k = (r["broker"], r["cell"], r["profile"])
        if k in seen:
            print(f"warning: more than one run for {k}; keeping the newest ({r['label']})", file=sys.stderr)
        seen[k] = r if k not in seen or r["start"] > seen[k]["start"] else seen[k]
    runs = list(seen.values())
    page, md = render(root, runs, fio, iperf, digests, versions, source_of)
    os.makedirs(os.path.dirname(out_html), exist_ok=True)
    open(out_html, "w").write(page)
    json.dump({"results": roots, "sources": source_of, "fio_MBps": fio, "iperf_Gbps": iperf, "digests": digests, "runs": runs}, open(out_json, "w"), indent=1, default=str)
    readme = f"{ROOT}/README.md"
    if os.path.exists(readme) and all(os.path.abspath(r).startswith(os.path.join(ROOT, "results")) for r in roots):  # never from a fixture elsewhere
        t = open(readme).read()
        if "<!-- headline:start -->" in t:
            t = re.sub(r"<!-- headline:start -->.*<!-- headline:end -->", "<!-- headline:start -->\n" + md + "\n<!-- headline:end -->", t, flags=re.S)
            open(readme, "w").write(t)
    print(f"wrote {out_html} ({len(runs)} runs), {out_json}")
    print(md)


if __name__ == "__main__":
    main()
