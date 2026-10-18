#!/usr/bin/env python3
"""tcpstat-diff.py RUN_DIR [--all] — deltas of the kernel TCP counters between the before and after snapshots
scripts/tcpstat.sh wrote into RUN_DIR/tcp/ (one line per pod), for the counters that tell a transport-level
delay apart from a broker-side one:

  RetransSegs         segments retransmitted (any reason)
  TCPLossProbes       tail-loss probes sent: the ~200 ms timer that fires when the last segment of a burst is lost
  TCPLossProbeRecovery  probes that repaired a real loss
  TCPTimeouts         full retransmission timeouts (RTO)
  TCPSpuriousRTOs     RTOs that turned out spurious
  TCPRcvQDrop         segments dropped because the socket's receive queue was full
  RcvPruned           receive queues pruned (collapsed) under buffer pressure
  TCPRcvCollapsed     skbs collapsed to make room in a full receive buffer
  TCPZeroWindowDrop   segments dropped outside a zero window
  TCPAbortOnTimeout   connections aborted after too many retransmissions
  DelayedACKLost      delayed ACKs that were lost (a quick-ack trigger)

--all prints every counter whose delta is non-zero.
"""
import glob, os, re, sys

KEYS = ["RetransSegs", "TCPLossProbes", "TCPLossProbeRecovery", "TCPTimeouts", "TCPSpuriousRTOs",
        "TCPRcvQDrop", "RcvPruned", "TCPRcvCollapsed", "TCPZeroWindowDrop", "TCPAbortOnTimeout",
        "DelayedACKLost", "TCPSACKReorder", "TCPDSACKRecv", "OutSegs", "InSegs"]


def parse(path):
    counters = {}
    lines = [l for l in open(path, errors="replace").read().splitlines() if not l.startswith("#")]
    i = 0
    while i + 1 < len(lines):
        a, b = lines[i].split(), lines[i + 1].split()
        if a and b and a[0] == b[0] and a[0].endswith(":") and len(a) == len(b):
            for k, v in zip(a[1:], b[1:]):
                try:
                    counters[k] = int(v)
                except ValueError:
                    pass
            i += 2
        else:
            i += 1
    return counters


def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    run = sys.argv[1]; show_all = "--all" in sys.argv
    d = os.path.join(run, "tcp")
    befores = sorted(glob.glob(os.path.join(d, "tcp-before-*.txt")))
    if not befores:
        print(f"no tcp-before-*.txt under {d}"); sys.exit(1)
    rows = []
    for bf in befores:
        pod = re.sub(r"^tcp-before-", "", os.path.basename(bf))[:-4]
        af = os.path.join(d, f"tcp-after-{pod}.txt")
        if not os.path.exists(af):
            print(f"{pod}: no after snapshot"); continue
        b, a = parse(bf), parse(af)
        if not b or not a:
            print(f"{pod}: snapshot unreadable (before {len(b)} counters, after {len(a)})"); continue
        delta = {k: a.get(k, 0) - b.get(k, 0) for k in (a.keys() | b.keys())}
        rows.append((pod, delta))
    keys = KEYS if not show_all else sorted({k for _, dl in rows for k, v in dl.items() if v})
    width = max(len(p) for p, _ in rows) if rows else 10
    print("pod".ljust(width), *[k.rjust(max(12, len(k))) for k in keys])
    for pod, delta in rows:
        print(pod.ljust(width), *[str(delta.get(k, 0)).rjust(max(12, len(k))) for k in keys])


if __name__ == "__main__":
    main()
