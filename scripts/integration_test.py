#!/usr/bin/env python3
"""End-to-end tests: real tracker + client processes on loopback.

    cd tracker && make && cd ../client && make && cd .. && python3 scripts/integration_test.py

Each check prints PASS/FAIL; exit status is non-zero if any check failed.
"""
import hashlib
import os
import re
import socket
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import Cluster, PartitionCluster  # noqa: E402

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("  PASS  " if cond else "  FAIL  ") + name + ("" if cond else "   <-- " + str(detail)[-600:]))


def sha1(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def make_file(path, size):
    with open(path, "wb") as f:
        left = size
        while left > 0:
            n = min(left, 1 << 20)
            f.write(os.urandom(n))
            left -= n
    return path


def wait_download(client, pattern=r"\[DOWNLOAD\] (Completed|Failed)", since=0, timeout=120):
    return client.wait_for(pattern, since=since, timeout=timeout).group(0)


# ---- raw protocol helpers -------------------------------------------------

def raw_recv_exact(s, n):
    data = b""
    while len(data) < n:
        chunk = s.recv(n - len(data))
        if not chunk:
            raise EOFError("closed after %d/%d bytes" % (len(data), n))
        data += chunk
    return data


def raw_readline(s):
    line = b""
    while not line.endswith(b"\n"):
        c = s.recv(1)
        if not c:
            raise EOFError
        line += c
    return line[:-1].decode()


def raw_frame(s, with_status=False):
    n, code = raw_readline(s).split()
    body = raw_recv_exact(s, int(n)).decode()
    return (body, int(code)) if with_status else body


def raw_tracker(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    return s


def raw_login(port, user, pw, peer="127.0.0.1:19999"):
    s = raw_tracker(port)
    s.sendall(("login %s %s %s\n" % (user, pw, peer)).encode())
    resp = raw_frame(s)
    m = re.search(r"TOKEN (\w+)", resp)
    return s, (m.group(1) if m else None), resp


def setup_users(cl, clients_and_names, group="g1"):
    owner, owner_name = clients_and_names[0]
    for c, n in clients_and_names:
        c.run("create_user %s pw" % n)
        c.run("login %s pw" % n)
    owner.run("create_group " + group)
    for c, n in clients_and_names[1:]:
        c.run("join_group " + group)
        owner.run("accept_request %s %s" % (group, n))


def partition_test():
    """Both trackers stay up but cannot reach each other; each keeps serving clients and changing
    state (including deletions). After the network heals both must hold the union of all changes
    and no deleted record may come back."""
    print("\n[11] Network partition between two LIVE trackers, then heal")
    pc = PartitionCluster(22000)
    try:
        pc.start_trackers()
        time.sleep(2)  # sync connections up
        a1 = pc.start_client(info=pc.info1, name="alice@T1")
        b2 = pc.start_client(info=pc.info2, name="bob@T2")
        a1.run("create_user alice pw")
        a1.run("login alice pw")
        a1.run("create_group gp")
        fa = make_file(os.path.join(pc.dir, "fileA.bin"), 700000)
        fb = make_file(os.path.join(pc.dir, "fileB.bin"), 700000)
        a1.run("upload_file gp " + fa)
        a1.run("upload_file gp " + fb)
        time.sleep(1)
        b2.run("create_user bob pw")
        b2.run("login bob pw")
        b2.run("join_group gp")
        a1.run("accept_request gp bob")
        time.sleep(1)
        out = b2.run("list_files gp")
        check("(precondition) tracker 2 sees both files before the cut", "fileA.bin" in out and "fileB.bin" in out, out)

        pc.partition()
        time.sleep(0.5)

        # --- changes on tracker 1 only ---
        out = a1.run("stop_share gp fileB.bin")
        check("(T1) sole seeder stops sharing -> file deleted", "removed from group" in out, out)
        a1.run("create_group g_t1")
        a1.run("create_user only_t1 pw")
        # --- changes on tracker 2 only ---
        b2.run("create_group g_t2")
        b2.run("create_user only_t2 pw")
        out = b2.run("leave_group gp")
        check("(T2) bob leaves gp", "Left group" in out, out)
        c2 = pc.start_client(info=pc.info2, name="carol@T2")
        c2.run("create_user carol pw")
        c2.run("login carol pw")
        c2.run("join_group gp")
        time.sleep(4)  # several heartbeats: the trackers must NOT have exchanged anything

        out = a1.run("list_groups")
        check("during the cut the trackers really differ (T1 does not see g_t2)", "g_t2" not in out and "g_t1" in out, out)
        out = b2.run("list_groups")
        check("during the cut the trackers really differ (T2 does not see g_t1)", "g_t1" not in out and "g_t2" in out, out)

        pc.heal()
        time.sleep(9)  # heartbeat reconnect + state exchange

        out = a1.run("list_groups")
        check("after heal T1 has the group created on T2", "g_t2" in out and "g_t1" in out, out)
        out = b2.run("list_groups")
        check("after heal T2 has the group created on T1", "g_t1" in out and "g_t2" in out, out)
        out = a1.run("create_user only_t2 pw")
        check("after heal T1 has the user created on T2", "already exists" in out, out)
        out = b2.run("create_user only_t1 pw")
        check("after heal T2 has the user created on T1", "already exists" in out, out)
        out = a1.run("list_requests gp")
        check("join request made on T2 reached T1", "carol" in out, out)
        out = a1.run("list_files gp")
        check("T1: fileA kept, deleted fileB still gone", "fileA.bin" in out and "fileB.bin" not in out, out)

        a2 = pc.start_client(info=pc.info2, name="alice@T2")
        a2.run("login alice pw")
        out = a2.run("list_files gp")
        check("T2: deletion made on T1 was applied; T2's older copy of fileB did NOT resurrect it",
              "fileA.bin" in out and "fileB.bin" not in out, out)
        b1 = pc.start_client(info=pc.info1, name="bob@T1")
        b1.run("login bob pw")
        out = b1.run("list_files gp")
        check("T1: bob's departure (made on T2) was applied", "Not a member" in out, out)
        check("both trackers alive after the partition", pc.trackers[1].alive() and pc.trackers[2].alive())
        reports = pc.sanitizer_reports()
        check("no sanitizer reports (partition run)", not reports, reports[:3])
    finally:
        pc.close()


def main():
    cl = Cluster(18000)
    base = cl.base
    t1_port, t2_port = base + 1, base + 2
    try:
        cl.start_tracker(1)
        cl.start_tracker(2)
        time.sleep(1.5)

        # ------------------------------------------------------------------
        print("\n[1] Upload / download / auto-seeding / load balancing (spaces in file name)")
        alice = cl.start_client(name="alice")
        bob = cl.start_client(name="bob")
        carol = cl.start_client(name="carol")
        setup_users(cl, [(alice, "alice"), (bob, "bob"), (carol, "carol")])

        src = make_file(os.path.join(cl.dir, "my file.bin"), 9 * 1024 * 1024 + 4321)
        out = alice.run("upload_file g1 " + src)
        check("upload accepted", "File uploaded successfully" in out, out)

        dl_bob = os.path.join(cl.dir, "bob_dl")
        os.makedirs(dl_bob)
        mk = bob.mark()
        bob.run('download_file g1 "my file.bin" ' + dl_bob)
        res = wait_download(bob, since=mk)
        check("bob download completes", res.endswith("Completed"), bob.buf[-500:])
        check("bob's copy is byte-identical", os.path.exists(dl_bob + "/my file.bin") and sha1(dl_bob + "/my file.bin") == sha1(src))

        out = alice.run('stop_share g1 "my file.bin"')
        check("alice stop_share keeps file (bob is a seeder now)", "Stopped sharing" in out and "removed" not in out, out)
        out = carol.run("list_files g1")
        check("file still listed after alice stops", "my file.bin" in out, out)

        dl_carol = os.path.join(cl.dir, "carol_dl")
        os.makedirs(dl_carol)
        mk = carol.mark()
        carol.run('download_file g1 "my file.bin" ' + dl_carol)
        res = wait_download(carol, since=mk)
        check("carol downloads from bob alone (proves bob really serves the file after auto-seeding)",
              res.endswith("Completed") and sha1(dl_carol + "/my file.bin") == sha1(src), carol.buf[-500:])

        out = alice.run('become_seeder g1 "my file.bin" ' + src)
        check("alice re-registers with become_seeder", "Now seeding" in out, out)
        out = alice.run('become_seeder g1 "my file.bin" ' + src)
        check("become_seeder twice -> Already seeding", "Already seeding" in out, out)

        # now two seeders (alice, bob): a fresh downloader should draw from both
        dave = cl.start_client(name="dave")
        dave.run("create_user dave pw")
        dave.run("login dave pw")
        dave.run("join_group g1")
        alice.run("accept_request g1 dave")
        dl_dave = os.path.join(cl.dir, "dave_dl")
        os.makedirs(dl_dave)
        mk = dave.mark()
        dave.run('download_file g1 "my file.bin" ' + dl_dave)
        wait_download(dave, since=mk)
        m = re.search(r"Pieces per peer:(.*)", dave.buf[mk:])
        counts = [int(x) for x in re.findall(r"=(\d+)", m.group(1))] if m else []
        check("load balancing: both seeders served pieces", len(counts) >= 2 and all(c > 0 for c in counts), m.group(0) if m else dave.buf[-400:])
        check("load balancing: split is roughly even", len(counts) >= 2 and max(counts) - min(counts) <= 2, counts)

        out = dave.run("show_downloads")
        check("show_downloads lists own download", "my file.bin" in out and "[C]" in out, out)
        out = bob.run("show_downloads")
        check("show_downloads shows only the caller's downloads", "dave" not in out and out.count("my file.bin") == 1, out)

        # ------------------------------------------------------------------
        print("\n[2] Destination safety, failed upload bookkeeping, input validation")
        out = bob.run('download_file g1 "my file.bin" ' + dl_bob + "/my file.bin")
        check("download refuses to overwrite an existing file", "already exists" in out, out)
        out = bob.run('download_file g1 "my file.bin" ' + src)
        check("download cannot clobber the uploader's source file", "already exists" in out, out)

        ghost = make_file(os.path.join(cl.dir, "ghost.bin"), 1000)
        out = alice.run("upload_file nosuchgroup " + ghost)
        check("upload to unknown group rejected", "Group not found" in out, out)
        s, tok, _ = raw_login(t1_port, "bob", "pw", "127.0.0.1:19998")
        # alice's peer server must not serve a file whose upload the tracker rejected
        ps = socket.create_connection(("127.0.0.1", alice.port), timeout=10)
        ps.sendall(b"GET_PIECE\tnosuchgroup\tghost.bin\t0\tbob\t" + (tok or "x").encode() + b"\n")
        check("failed upload leaves no served file", raw_readline(ps).startswith("ERROR 404 File not found"))
        ps.close()
        s.close()

        out = alice.run('become_seeder g1 "my file.bin" ' + ghost)
        check("become_seeder with wrong file -> hash mismatch", "hash mismatch" in out.lower(), out)

        # ------------------------------------------------------------------
        print("\n[3] Peer server protocol: auth, bounds, pipelining")
        sbob, tokb, _ = raw_login(t1_port, "bob", "pw", "127.0.0.1:19997")
        # NB: bob's real client is logged in too; a second login on the same tracker is refused, so
        # use a user that has no client: carol2 (member), eve (non-member)
        sbob.close()
        for u in ("zed", "eve"):
            c = raw_tracker(t1_port)
            c.sendall(("create_user %s pw\n" % u).encode())
            raw_frame(c)
            c.close()
        sz, tokz, resp = raw_login(t1_port, "zed", "pw")
        se, toke, _ = raw_login(t1_port, "eve", "pw")
        # zed joins g1 (alice is owner)
        sz.sendall(b"join_group g1\n")
        raw_frame(sz)
        alice.run("accept_request g1 zed")

        def peer_req(port, gid, name, idx, user, token):
            p = socket.create_connection(("127.0.0.1", port), timeout=10)
            p.sendall(("GET_PIECE\t%s\t%s\t%s\t%s\t%s\n" % (gid, name, idx, user, token)).encode())
            first = raw_readline(p)
            return p, first

        p, first = peer_req(alice.port, "g1", "my file.bin", 0, "zed", tokz)
        n = int(first.split()[1]) if first.startswith("PIECE_DATA") else -1
        data = raw_recv_exact(p, n) if n > 0 else b""
        with open(src, "rb") as f:
            expect = f.read(512 * 1024)
        check("member gets piece 0, correct bytes", n == 512 * 1024 and data == expect, first)
        # second request on the SAME connection (persistent connection)
        p.sendall(("GET_PIECE\tg1\tmy file.bin\t1\tzed\t%s\n" % tokz).encode())
        first2 = raw_readline(p)
        check("same connection serves a second piece", first2.startswith("PIECE_DATA 524288"), first2)
        raw_recv_exact(p, 524288)
        # two requests sent in one TCP segment
        p.sendall(("GET_PIECE\tg1\tmy file.bin\t2\tzed\t%s\nGET_PIECE\tg1\tmy file.bin\t3\tzed\t%s\n" % (tokz, tokz)).encode())
        h1 = raw_readline(p)
        raw_recv_exact(p, int(h1.split()[1]))
        h2 = raw_readline(p)
        raw_recv_exact(p, int(h2.split()[1]))
        check("pipelined requests are both answered", h1.startswith("PIECE_DATA") and h2.startswith("PIECE_DATA"), (h1, h2))
        p.close()

        p, first = peer_req(alice.port, "g1", "my file.bin", 99999, "zed", tokz)
        check("out-of-range piece index rejected with 416", first.startswith("ERROR 416"), first)
        p.close()
        p, first = peer_req(alice.port, "g1", "my file.bin", -5, "zed", tokz)
        check("negative piece index rejected with 400", first.startswith("ERROR 400"), first)
        p.close()
        p, first = peer_req(alice.port, "g1", "my file.bin", 0, "eve", toke)
        check("non-member denied", first.startswith("ERROR 403 Access denied"), first)
        p.close()
        p, first = peer_req(alice.port, "g1", "my file.bin", 0, "zed", "deadbeef")
        check("wrong token denied", first.startswith("ERROR 403 Access denied"), first)
        p.close()

        # ------------------------------------------------------------------
        print("\n[4] Tracker framing, long responses, session restore auth")
        s = raw_tracker(t1_port)
        s.sendall(b"list_groups\nlist_groups\n")
        r1, r2 = raw_frame(s), raw_frame(s)
        check("two pipelined commands -> two framed responses", "Groups:" in r1 and "Groups:" in r2, (r1, r2))
        s.sendall(b"restore_session alice bogustoken 127.0.0.1:1\n")
        r, code = raw_frame(s, True)
        check("restore_session with wrong token refused (401)", "Error" in r and code == 401, (r, code))
        s.sendall(b"list_groups\n")
        r, code = raw_frame(s, True)
        check("success carries status 200", "Groups:" in r and code == 200, (r, code))
        for cmd, want in ((b"create_group x1\n", 401), (b"join_group nope\n", 401), (b"frobnicate\n", 400),
                          (b"create_user alice pw\n", 409), (b"login alice wrongpw\n", 401), (b"login\n", 400)):
            s.sendall(cmd)
            r, code = raw_frame(s, True)
            check("status %d for %r" % (want, cmd.decode().strip()), code == want, (r, code))
        s.sendall(b"create_user statu pw\n")
        raw_frame(s)
        s.sendall(b"login statu pw 127.0.0.1:19990\n")
        r, code = raw_frame(s, True)
        check("login over raw socket works (200)", code == 200, (r, code))
        s.sendall(b"join_group no_such_group\n")
        r, code = raw_frame(s, True)
        check("status 404 for unknown group", code == 404, (r, code))
        s.sendall(b"list_requests g1\n")
        r, code = raw_frame(s, True)
        check("status 403 for owner-only command", code == 403, (r, code))
        s.close()

        for i in range(70):
            f = make_file(os.path.join(cl.dir, "file_with_a_rather_long_name_%03d.txt" % i), 100 + i)
            alice.run("upload_file g1 " + f)
        out = alice.run("list_files g1")
        check("long list_files output is not truncated", "file_with_a_rather_long_name_069.txt" in out and len(out) > 4000, len(out))

        # ------------------------------------------------------------------
        print("\n[4b] Edge sizes and file types; two simultaneous downloads on one client")
        import gzip, io, tarfile, zipfile
        ftypes = os.path.join(cl.dir, "types")
        os.makedirs(ftypes)
        samples = {}
        samples["tiny.txt"] = b"hello world\n" * 7                                   # < 1 piece, text
        samples["exact_one_piece.bin"] = os.urandom(512 * 1024)                      # exactly 1 piece
        samples["one_piece_plus_1.bin"] = os.urandom(512 * 1024 + 1)                 # 2 pieces, last = 1 byte
        samples["exact_three_pieces.bin"] = os.urandom(3 * 512 * 1024)               # no short tail
        samples["empty.dat"] = b""                                                   # 0 bytes, 0 pieces
        samples["image.png"] = b"\x89PNG\r\n\x1a\n" + os.urandom(300000)              # binary "image"
        with open("/bin/ls", "rb") as f:
            samples["program"] = f.read()                                            # real executable
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            data = os.urandom(200000)
            info = tarfile.TarInfo("inner.bin"); info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        samples["archive.tar.gz"] = buf.getvalue()                                   # compressed archive
        zbuf = io.BytesIO()
        with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("a.txt", "zip content " * 5000)
        samples["bundle.zip"] = zbuf.getvalue()
        for name, content in samples.items():
            p = os.path.join(ftypes, name)
            with open(p, "wb") as f:
                f.write(content)
            out = alice.run("upload_file g1 " + p)
            check("upload %s (%d bytes)" % (name, len(content)), "File uploaded successfully" in out, out)
        dl_types = os.path.join(cl.dir, "types_dl")
        os.makedirs(dl_types)
        for name, content in samples.items():
            mk = bob.mark()
            bob.run("download_file g1 %s %s" % (name, dl_types))
            res = wait_download(bob, since=mk, timeout=60)
            got = os.path.join(dl_types, name)
            same = os.path.exists(got) and open(got, "rb").read() == content
            check("download %s -> byte-identical" % name, res.endswith("Completed") and same, bob.buf[-300:])

        slow_env = {"P2P_UPLOAD_KBPS": "15000"}
        sl = cl.start_client(env=slow_env, name="slowseeder")
        dd = cl.start_client(name="multi")
        for c, n in ((sl, "slowuser"), (dd, "multiuser")):
            c.run("create_user %s pw" % n)
            c.run("login %s pw" % n)
        sl.run("create_group g_conc")
        dd.run("join_group g_conc")
        sl.run("accept_request g_conc multiuser")
        f1 = make_file(os.path.join(cl.dir, "conc1.bin"), 30 * 1024 * 1024)
        f2 = make_file(os.path.join(cl.dir, "conc2.bin"), 30 * 1024 * 1024 + 77)
        sl.run("upload_file g_conc " + f1)
        sl.run("upload_file g_conc " + f2)
        dl_conc = os.path.join(cl.dir, "conc_dl")
        os.makedirs(dl_conc)
        mk = dd.mark()
        dd.run("download_file g_conc conc1.bin " + dl_conc)
        dd.run("download_file g_conc conc2.bin " + dl_conc)
        out = dd.run("show_downloads")
        check("two downloads are in flight at the same time", out.count("[D]") == 2, out)
        dd.wait_for(r"Completed: conc1.bin.*?Completed: conc2.bin|Completed: conc2.bin.*?Completed: conc1.bin", since=mk, timeout=120)
        check("both concurrent downloads byte-identical",
              sha1(dl_conc + "/conc1.bin") == sha1(f1) and sha1(dl_conc + "/conc2.bin") == sha1(f2))

        # ------------------------------------------------------------------
        print("\n[5] Tracker sync framing (messages glued together / split across reads / out of order)")
        sync = socket.create_connection(("127.0.0.1", base + 1002), timeout=10)  # tracker 2's sync port
        import secrets
        S, R = "\x1f", "\x1e"
        base_ms = int(time.time() * 1000) + 60000  # fixed once: newer than anything real, ordering = n
        stamp = lambda n: "%d.0.9" % (base_ms + n)
        def cell(key_parts, n, present=True, value=""):
            return "CELL\t%s\t%s\t%d\t%s\n" % (S.join(key_parts), stamp(n), 1 if present else 0, value)
        salt = secrets.token_hex(8)
        ph = salt + "$" + hashlib.sha256((salt + "pw").encode()).hexdigest()
        hashes = " ".join(hashlib.sha1(str(i).encode()).hexdigest() for i in range(5000))
        size = 5000 * 512 * 1024
        filemeta = R.join(["syncu", str(size), "5000", hashlib.sha1(b"x").hexdigest(), hashes])
        # file + seeder + member cells are deliberately sent BEFORE their group and user exist
        big = cell(["f", "sg", "big.bin"], 1, True, filemeta)
        glued = (cell(["s", "sg", "big.bin", "syncu"], 2) + cell(["m", "sg", "syncu"], 3) +
                 cell(["u", "syncu"], 4, True, ph) + cell(["u", "syncu2"], 5, True, "zzz") +
                 cell(["g", "sg"], 6, True, "syncu"))
        payload = (glued + big).encode()
        for i in range(0, len(payload), 1500):  # dribble everything out in small odd pieces
            sync.sendall(payload[i:i + 1500])
        sync.sendall(b"CELL\tbad key\tnot-a-stamp\t1\tx\nCELL\tm\x1fonly-two\t1.0.1\t1\t\n")  # malformed: must be ignored
        time.sleep(0.5)
        check("tracker 2 survived malformed sync cells", cl.trackers[2].alive())
        probe = cl.start_client(info=cl.info2, name="syncprobe")
        out = probe.run("create_user syncu2 pw")
        check("glued sync messages: later cells were applied", "already exists" in out, out)
        probe.run("login syncu pw")
        out = probe.run("list_files sg")
        check("200 KB split file cell reassembled; cells that arrived before their parents were attached (5000 pieces)",
              "big.bin" in out and "Pieces: 5000" in out, out[-300:])
        # an older stamp must NOT overwrite newer state, a tombstone with a newer stamp must delete
        sync.sendall(cell(["f", "sg", "big.bin"], -100000, True, filemeta.replace("5000", "1")).encode())
        time.sleep(0.3)
        out = probe.run("list_files sg")
        check("stale (older-stamp) cell ignored", "Pieces: 5000" in out, out[-200:])
        sync.sendall(cell(["f", "sg", "big.bin"], 10, False).encode())
        time.sleep(0.3)
        out = probe.run("list_files sg")
        check("newer tombstone deletes the file", "big.bin" not in out, out[-200:])
        sync.sendall(cell(["f", "sg", "big.bin"], 5, True, filemeta).encode())  # older than the tombstone
        time.sleep(0.3)
        out = probe.run("list_files sg")
        check("deleted file is not resurrected by an older cell", "big.bin" not in out, out[-200:])
        sync.close()

        # ------------------------------------------------------------------
        print("\n[6] Tracker crash: failover, SIGPIPE survival, restart catch-up")
        erin = cl.start_client(name="erin")  # tracker list order: T1 first
        erin.run("create_user erin pw")
        erin.run("login erin pw")
        erin.run("create_group g_fail")
        cl.trackers[1].kill()  # SIGKILL tracker 1
        time.sleep(0.5)
        out = erin.run("list_groups")
        check("client fails over to tracker 2", "g_fail" in out, out)
        check("session restored on tracker 2 (token proof)", "Session restored" in erin.buf, erin.buf[-400:])
        out = erin.run("create_group g_after_crash")
        check("tracker 2 survives writes to dead peer (no SIGPIPE death)", cl.trackers[2].alive() and "Group created" in out, out)
        time.sleep(4)  # several heartbeats against the dead peer
        check("tracker 2 still alive after heartbeats to dead peer", cl.trackers[2].alive())

        cl.start_tracker(1)  # restart with EMPTY state
        time.sleep(6)
        probe1 = cl.start_client(info=cl.info1, name="t1probe")
        out = probe1.run("login erin pw")
        check("restarted tracker 1 knows users from before the crash", "Login successful" in out, out)
        out = probe1.run("list_groups")
        check("restarted tracker 1 caught up on groups created while it was down",
              "g_fail" in out and "g_after_crash" in out, out)
        probe1.run("logout")
        probe1.run("login alice pw")
        out = probe1.run("list_files g1")
        check("restarted tracker 1 has file metadata (incl. 70+ files)", "my file.bin" in out and "069" in out, out[-300:])

        # ------------------------------------------------------------------
        print("\n[7] Disconnect == logout on BOTH trackers")
        fay = cl.start_client(name="fay")
        gus = cl.start_client(info=cl.info2, name="gus")  # talks to tracker 2 only
        fay.run("create_user fay pw")
        fay.run("login fay pw")
        gus.run("create_user gus pw")
        gus.run("login gus pw")
        fay.run("create_group g_fay")
        gus.run("join_group g_fay")
        fay.run("accept_request g_fay gus")
        solo = make_file(os.path.join(cl.dir, "solo.bin"), 700000)
        fay.run("upload_file g_fay " + solo)
        time.sleep(0.5)
        out = gus.run("list_files g_fay")
        check("(precondition) tracker 2 sees fay's upload", "solo.bin" in out, out)
        fay.kill()  # no logout: the connection to tracker 1 simply drops
        time.sleep(1.5)
        out = gus.run("list_files g_fay")
        check("dropped connection == logout: sole seeder removed on tracker 2 too",
              "No files shared" in out, out)
        relog = cl.start_client(info=cl.info2, name="fay2")
        out = relog.run("login fay pw")
        check("crashed user can log in again on the other tracker", "Login successful" in out, out)

        # ------------------------------------------------------------------
        print("\n[8] Retry across peers when a seeder dies mid-download")
        big_src = make_file(os.path.join(cl.dir, "big.bin"), 80 * 1024 * 1024)
        slow = {"P2P_UPLOAD_KBPS": "8000"}  # ~8 MB/s per seeder so there is time to kill one
        s1 = cl.start_client(env=slow, name="s1")
        s2 = cl.start_client(env=slow, name="s2")
        d1 = cl.start_client(env={"P2P_WORKERS": "6"}, name="d1")
        # tracker 1 was restarted: clients below use the full list; fine either way
        for c, n in ((s1, "ss1"), (s2, "ss2"), (d1, "dd1")):
            c.run("create_user %s pw" % n)
            c.run("login %s pw" % n)
        s1.run("create_group g_retry")
        for c, n in ((s2, "ss2"), (d1, "dd1")):
            c.run("join_group g_retry")
            s1.run("accept_request g_retry " + n)
        s1.run("upload_file g_retry " + big_src)
        s2.run("become_seeder g_retry big.bin " + big_src)
        dl_dir = os.path.join(cl.dir, "d1_dl")
        os.makedirs(dl_dir)
        mk = d1.mark()
        d1.run("download_file g_retry big.bin " + dl_dir)
        time.sleep(2.0)
        s1.kill()  # seeder 1 dies mid-transfer
        res = wait_download(d1, since=mk, timeout=120)
        check("download completes after a seeder died (failed pieces were retried on the other peer)",
              res.endswith("Completed") and sha1(dl_dir + "/big.bin") == sha1(big_src), d1.buf[-600:])
        m = re.search(r"Pieces per peer:(.*)", d1.buf[mk:])
        cnt = [int(x) for x in re.findall(r"=(\d+)", m.group(1))] if m else []
        check("both peers contributed before/after the failure", len(cnt) == 2 and min(cnt) > 0 and sum(cnt) == 160, cnt)

        print("\n[9] All seeders die -> clean failure, no partial file, no false 'Failed' while running")
        s3 = cl.start_client(env=slow, name="s3")
        d2 = cl.start_client(name="d2")
        for c, n in ((s3, "ss3"), (d2, "dd2")):
            c.run("create_user %s pw" % n)
            c.run("login %s pw" % n)
        s3.run("create_group g_dead")
        d2.run("join_group g_dead")
        s3.run("accept_request g_dead dd2")
        s3.run("upload_file g_dead " + big_src)
        dl_dir2 = os.path.join(cl.dir, "d2_dl")
        os.makedirs(dl_dir2)
        mk = d2.mark()
        d2.run("download_file g_dead big.bin " + dl_dir2)
        time.sleep(1.0)
        out = d2.run("show_downloads")
        check("in-flight download is reported as downloading, not Failed", "[D]" in out, out)
        s3.kill()
        res = wait_download(d2, since=mk, timeout=120)
        check("download fails cleanly when no seeder is left", res.endswith("Failed"), d2.buf[-400:])
        check("partial file removed", not os.path.exists(dl_dir2 + "/big.bin"))

        # ------------------------------------------------------------------
        print("\n[10] Process health")
        check("tracker 1 alive", cl.trackers[1].alive())
        check("tracker 2 alive", cl.trackers[2].alive())
        reports = cl.sanitizer_reports()
        check("no sanitizer reports (relevant when run against `make debug` / `make asan` binaries)", not reports, reports[:3])
        if os.environ.get("P2P_LEAKCHECK"):
            # macOS has no LeakSanitizer: inspect the live processes with the system `leaks` tool.
            import subprocess
            for p in cl.procs:
                if p.alive():
                    r = subprocess.run(["leaks", str(p.p.pid)], capture_output=True, text=True)
                    m = re.search(r"(\d+) leaks for (\d+) total leaked bytes", r.stdout)
                    check("leaks: %s pid %d -> %s" % (os.path.basename(p.p.args[0]), p.p.pid, m.group(0) if m else "unparsed"),
                          m is not None and m.group(1) == "0", r.stdout[-800:])
    finally:
        cl.close()

    partition_test()

    failed = [n for n, ok in results if not ok]
    print("\n%d checks, %d failed" % (len(results), len(failed)))
    for n in failed:
        print("  FAILED:", n)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
