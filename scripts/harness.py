"""Small process harness shared by integration_test.py and benchmark.py.

Starts real tracker / client binaries and drives the clients' stdin like a user would.
"""
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TRACKER_BIN = os.environ.get("P2P_TRACKER_BIN", os.path.join(ROOT, "tracker", "tracker"))
CLIENT_BIN = os.environ.get("P2P_CLIENT_BIN", os.path.join(ROOT, "client", "client"))


class Proc:
    """A child process whose merged stdout/stderr is captured in a buffer."""

    def __init__(self, argv, env=None, cwd=None):
        full_env = dict(os.environ)
        if env:
            full_env.update(env)
        self.p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, env=full_env, cwd=cwd)
        self.buf = ""
        self.cv = threading.Condition()
        self.t = threading.Thread(target=self._pump, daemon=True)
        self.t.start()

    def _pump(self):
        while True:
            chunk = os.read(self.p.stdout.fileno(), 65536)
            if not chunk:
                break
            with self.cv:
                self.buf += chunk.decode("utf-8", "replace")
                self.cv.notify_all()

    def alive(self):
        return self.p.poll() is None

    def kill(self, sig=signal.SIGKILL):
        if self.alive():
            self.p.send_signal(sig)
            self.p.wait()

    def wait_for(self, pattern, since=0, timeout=30):
        """Block until regex `pattern` appears in output after offset `since`; returns match."""
        rx = re.compile(pattern, re.S)
        deadline = time.time() + timeout
        with self.cv:
            while True:
                m = rx.search(self.buf, since)
                if m:
                    return m
                left = deadline - time.time()
                if left <= 0:
                    raise TimeoutError("timeout waiting for %r; output tail:\n%s" % (pattern, self.buf[-1500:]))
                self.cv.wait(min(left, 0.2))


class Tracker(Proc):
    def __init__(self, info_file, tracker_id):
        super().__init__([TRACKER_BIN, info_file, str(tracker_id)])
        self.wait_for(r"Server listening", timeout=10)


class Client(Proc):
    PROMPT = "> "

    def __init__(self, listen_port, info_file, env=None, name="client"):
        super().__init__([CLIENT_BIN, "127.0.0.1:%d" % listen_port, info_file], env=env)
        self.port = listen_port
        self.name = name
        self.wait_for(r"Type 'help'", timeout=10)
        self.wait_prompt(0)

    def wait_prompt(self, since, timeout=60):
        """Wait for the next '> ' prompt that appears after `since`."""
        deadline = time.time() + timeout
        with self.cv:
            while True:
                idx = self.buf.find(self.PROMPT, since)
                if idx >= 0:
                    # settle: make sure nothing more is arriving
                    n = len(self.buf)
                    self.cv.wait(0.05)
                    if len(self.buf) == n:
                        return
                    continue
                left = deadline - time.time()
                if left <= 0:
                    raise TimeoutError("no prompt; tail:\n" + self.buf[-1500:])
                self.cv.wait(min(left, 0.2))

    def run(self, line, timeout=60):
        """Send one command, return its output (everything printed before the next prompt)."""
        with self.cv:
            start = len(self.buf)
        self.p.stdin.write((line + "\n").encode())
        self.p.stdin.flush()
        # skip any echo of our own input, then wait for the prompt
        time.sleep(0.02)
        self.wait_prompt(start + len(line) if self.buf[start:start + len(line)] == line else start, timeout)
        with self.cv:
            out = self.buf[start:]
        return out

    def mark(self):
        with self.cv:
            return len(self.buf)


class Cluster:
    """Two trackers on loopback plus helpers; cleans everything up on exit."""

    def __init__(self, base_port=18000):
        self.dir = tempfile.mkdtemp(prefix="p2p_")
        self.base = base_port
        self.info = os.path.join(self.dir, "tracker_info.txt")
        self.info1 = os.path.join(self.dir, "tracker_info_t1_only.txt")
        self.info2 = os.path.join(self.dir, "tracker_info_t2_only.txt")
        t1 = "127.0.0.1 %d %d\n" % (base_port + 1, base_port + 1001)
        t2 = "127.0.0.1 %d %d\n" % (base_port + 2, base_port + 1002)
        open(self.info, "w").write(t1 + t2)
        open(self.info1, "w").write(t1)
        open(self.info2, "w").write(t2)
        self.procs = []
        self.trackers = {}
        self._next_client_port = base_port + 100

    def start_tracker(self, tid):
        t = Tracker(self.info, tid)
        self.trackers[tid] = t
        self.procs.append(t)
        return t

    def start_client(self, info=None, env=None, name="client"):
        port = self._next_client_port
        self._next_client_port += 1
        c = Client(port, info or self.info, env=env, name=name)
        self.procs.append(c)
        return c

    def sanitizer_reports(self):
        """Any ThreadSanitizer / AddressSanitizer reports printed by child processes."""
        out = []
        for p in self.procs:
            for m in re.finditer(r"WARNING: ThreadSanitizer[^\n]*|ERROR: AddressSanitizer[^\n]*|[^\n]*runtime error:[^\n]*", p.buf):
                out.append(m.group(0))
        return out

    def close(self):
        for p in self.procs:
            try:
                p.kill()
            except Exception:
                pass
        shutil.rmtree(self.dir, ignore_errors=True)  # payloads can be GBs


class TcpProxy:
    """Forwards 127.0.0.1:listen_port -> 127.0.0.1:target_port. partition() drops every live
    connection and refuses new ones (the peers see resets, like a network cut); heal() resumes."""

    def __init__(self, listen_port, target_port):
        self.target = target_port
        self.cut = False
        self.conns = []
        self.lock = threading.Lock()
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", listen_port))
        self.srv.listen(16)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            with self.lock:
                if self.cut:
                    c.close()
                    continue
            try:
                u = socket.create_connection(("127.0.0.1", self.target), timeout=5)
            except OSError:
                c.close()
                continue
            with self.lock:
                self.conns += [c, u]
            for a, b in ((c, u), (u, c)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    def _pipe(self, a, b):
        try:
            while True:
                d = a.recv(65536)
                if not d:
                    break
                with self.lock:
                    if self.cut:
                        break
                b.sendall(d)
        except OSError:
            pass
        for s in (a, b):
            try:
                s.close()
            except OSError:
                pass

    def partition(self):
        with self.lock:
            self.cut = True
            conns, self.conns = self.conns, []
        for s in conns:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            s.close()

    def heal(self):
        with self.lock:
            self.cut = False

    def close(self):
        self.srv.close()
        self.partition()


class PartitionCluster(Cluster):
    """Two trackers whose sync traffic goes through cuttable proxies; clients talk to each
    tracker directly (info1 = tracker 1 only, info2 = tracker 2 only)."""

    def __init__(self, base_port=22000):
        super().__init__(base_port)
        sync1, sync2 = base_port + 1001, base_port + 1002
        p1, p2 = base_port + 2001, base_port + 2002
        self.proxies = [TcpProxy(p1, sync1), TcpProxy(p2, sync2)]
        c1, c2 = base_port + 1, base_port + 2
        self.t1_info = os.path.join(self.dir, "info_for_t1.txt")
        self.t2_info = os.path.join(self.dir, "info_for_t2.txt")
        # tracker 1 reaches tracker 2's sync port via proxy p2; tracker 2 reaches tracker 1's via p1
        open(self.t1_info, "w").write("127.0.0.1 %d %d\n127.0.0.1 %d %d\n" % (c1, sync1, c2, p2))
        open(self.t2_info, "w").write("127.0.0.1 %d %d\n127.0.0.1 %d %d\n" % (c1, p1, c2, sync2))

    def start_trackers(self):
        for tid, info in ((1, self.t1_info), (2, self.t2_info)):
            t = Tracker(info, tid)
            self.trackers[tid] = t
            self.procs.append(t)

    def partition(self):
        for p in self.proxies:
            p.partition()

    def heal(self):
        for p in self.proxies:
            p.heal()

    def close(self):
        for p in self.proxies:
            p.close()
        super().close()
