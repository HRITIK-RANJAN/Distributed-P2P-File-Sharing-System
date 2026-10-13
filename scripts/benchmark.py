#!/usr/bin/env python3
"""Download-time benchmark: one downloader, 1 / 2 / 4 seeders, real processes on loopback.

    cd tracker && make && cd ../client && make && cd ..
    python3 scripts/benchmark.py                       # 1 GB, unthrottled
    python3 scripts/benchmark.py --upload-mbps 40      # each seeder's uplink capped at 40 MB/s
    python3 scripts/benchmark.py --size-mb 256 --runs 5

All seeders point at the same source file on disk (so the disk holds one copy), and one warm-up
download is thrown away so every measured run reads from a warm page cache.

Two modes are worth running, because they answer different questions:
  * unthrottled  - how fast is the implementation on this machine? On loopback the bottleneck is
                   the downloader's own CPU (SHA-1 of every piece) and memcpy, NOT any seeder, so
                   extra seeders are not expected to help much.
  * --upload-mbps N - emulates real peers whose uplink is the bottleneck (the situation
                   multi-source download exists for). Speed-up from adding seeders shows up here.
"""
import argparse
import os
import re
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Cluster  # noqa: E402


def make_random_file(path, size):
    with open(path, "wb") as f:
        left = size
        while left > 0:
            n = min(left, 4 << 20)
            f.write(os.urandom(n))
            left -= n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-mb", type=int, default=1024)
    ap.add_argument("--seeders", default="1,2,4", help="comma separated seeder counts")
    ap.add_argument("--runs", type=int, default=3, help="measured runs per configuration")
    ap.add_argument("--workers", type=int, default=8, help="downloader worker threads (P2P_WORKERS)")
    ap.add_argument("--upload-mbps", type=float, default=0, help="cap each seeder's upload (MB/s); 0 = unlimited")
    ap.add_argument("--base-port", type=int, default=21000)
    args = ap.parse_args()

    counts = [int(x) for x in args.seeders.split(",")]
    max_seeders = max(counts)
    size = args.size_mb * 1024 * 1024

    cl = Cluster(args.base_port)
    rows = []
    try:
        src = os.path.join(cl.dir, "payload.bin")
        print("Creating %d MB random payload..." % args.size_mb, flush=True)
        make_random_file(src, size)

        cl.start_tracker(1)
        cl.start_tracker(2)
        time.sleep(1)

        env = {}
        if args.upload_mbps > 0:
            env["P2P_UPLOAD_KBPS"] = str(int(args.upload_mbps * 1024))

        seeders = []
        for i in range(max_seeders):
            c = cl.start_client(env=env, name="seeder%d" % (i + 1))
            c.run("create_user seeder%d pw" % (i + 1))
            c.run("login seeder%d pw" % (i + 1))
            seeders.append(c)
        seeders[0].run("create_group bench")
        for i, c in enumerate(seeders[1:], start=2):
            c.run("join_group bench")
            seeders[0].run("accept_request bench seeder%d" % i)

        print("Hashing + registering seeder 1...", flush=True)
        out = seeders[0].run("upload_file bench " + src, timeout=600)
        assert "uploaded successfully" in out, out
        active = 1

        dl_dir = os.path.join(cl.dir, "dl")
        os.makedirs(dl_dir)
        run_no = 0

        def one_download():
            nonlocal run_no
            run_no += 1
            name = "d%d" % run_no
            c = cl.start_client(env={"P2P_WORKERS": str(args.workers)}, name=name)
            c.run("create_user %s pw" % name)
            c.run("login %s pw" % name)
            c.run("join_group bench")
            seeders[0].run("accept_request bench " + name)
            dest = os.path.join(dl_dir, name)
            os.makedirs(dest)
            mk = c.mark()
            t0 = time.time()
            c.run("download_file bench payload.bin " + dest)
            m = c.wait_for(r"\[DOWNLOAD\] Completed: payload.bin\s+\(([\d.]+) MB in ([\d.]+) s transfer = ([\d.]+) MB/s; ([\d.]+) s incl[^\n]*\n\[DOWNLOAD\] Pieces per peer:[^\n]*\n",
                           since=mk, timeout=1800)
            wall = time.time() - t0
            per_peer = re.search(r"Pieces per peer:(.*)", m.group(0))
            dist = [int(x) for x in re.findall(r"=(\d+)", per_peer.group(1))] if per_peer else []
            transfer = float(m.group(2))
            ok = os.path.getsize(os.path.join(dest, "payload.bin")) == size
            os.remove(os.path.join(dest, "payload.bin"))
            c.kill()  # the downloader is a seeder after finishing; drop it so it never counts as one
            time.sleep(0.5)
            return transfer, float(m.group(4)), wall, dist, ok

        print("Warm-up download (discarded)...", flush=True)
        one_download()

        for k in counts:
            while active < k:
                active += 1
                print("Registering seeder %d (hash-verifies its copy)..." % active, flush=True)
                out = seeders[active - 1].run("become_seeder bench payload.bin " + src, timeout=600)
                assert "Now seeding" in out, out
            times = []
            for r in range(args.runs):
                transfer, incl, wall, dist, ok = one_download()
                assert ok, "size mismatch"
                times.append(transfer)
                print("  seeders=%d run %d: transfer %.3f s (%.1f MB/s), %.3f s with verification, pieces/peer=%s"
                      % (k, r + 1, transfer, args.size_mb / transfer, incl, dist), flush=True)
            rows.append((k, times))
    finally:
        cl.close()

    base = statistics.median(rows[0][1])
    print("\n| Seeders | Median transfer time | Throughput | Speed-up vs 1 seeder | Runs (s) |")
    print("|---|---|---|---|---|")
    for k, times in rows:
        med = statistics.median(times)
        print("| %d | %.3f s | %.0f MB/s | %.2fx | %s |" % (
            k, med, args.size_mb / med, base / med, ", ".join("%.3f" % t for t in times)))
    cap = "unlimited" if args.upload_mbps <= 0 else "%.0f MB/s per seeder" % args.upload_mbps
    print("\nFile: %d MB | downloader workers: %d | seeder upload cap: %s | %d runs/config (median shown)"
          % (args.size_mb, args.workers, cap, args.runs))


if __name__ == "__main__":
    main()
