# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Exercise the real inline terminal with the Python standard library only."""
import errno
import fcntl
import os
import pty
import re
import select
import signal
import struct
import sys
import tempfile
import termios
import time
from pathlib import Path


class Session:
    def __init__(self, binary, directory, options):
        self.transcript = bytearray()
        self.position = 0
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.environ["TERM"] = "xterm-256color"
            os.environ["DATAFUSION_DFT_DATA"] = str(directory)
            os.execv(binary, [binary, "--config", str(directory / "config.toml"), *options])
        try:
            self.resize(24, 100)
            self.expect("dft SQL prompt")
            self.expect("dft>")
        except BaseException:
            self.close()
            raise

    def resize(self, rows, columns):
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))
        os.kill(self.pid, signal.SIGWINCH)

    def send(self, text):
        os.write(self.fd, text.encode())

    def pump(self):
        if select.select([self.fd], [], [], 0.05)[0]:
            try:
                data = os.read(self.fd, 65536)
            except OSError as error:
                if error.errno == errno.EIO:
                    return False
                raise
            if not data:
                return False
            self.transcript.extend(data)
            # Respond to Ratatui's cursor-position query, as a real terminal does.
            if b"\x1b[6n" in data:
                self.send("\x1b[1;1R")
        return True

    def expect(self, text, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[HfG]", b" ", self.transcript)
            clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", clean)
            found = clean.find(text.encode(), self.position)
            if found >= 0:
                self.position = found + len(text.encode())
                return
            if not self.pump():
                break
        raise AssertionError(f"Missing {text!r}: {bytes(self.transcript[-12000:])!r}")

    def submit(self, sql):
        self.send("\x1b[200~" + sql + "\x1b[201~")
        time.sleep(0.05)
        self.send("\r")

    def finish(self, key):
        self.send(key)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            self.pump()
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                assert os.waitstatus_to_exitcode(status) == 0, bytes(self.transcript)
                attributes = termios.tcgetattr(self.fd)
                assert attributes[3] & termios.ICANON, "canonical mode was not restored"
                assert attributes[3] & termios.ECHO, "echo was not restored"
                assert b"\x1b[?1049h" not in self.transcript, "alternate screen was entered"
                os.close(self.fd)
                self.pid = None
                return
        raise AssertionError("REPL did not exit")

    def close(self):
        if self.pid:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
            os.close(self.fd)


def run(binary, test_default):
    with tempfile.TemporaryDirectory(prefix="dft-repl-") as path:
        directory = Path(path)
        (directory / "config.toml").write_text(f'[db]\npath = "{directory.as_uri()}/"\n')
        session = Session(binary, directory, [] if test_default else ["--repl"])
        try:
            session.submit("CREATE VIEW repl_values AS SELECT 42 AS answer;")
            session.expect("Elapsed:")
            session.expect("dft>")
            session.submit("SELECT\nanswer FROM repl_values;")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.submit("SELECT * FROM no_such_repl_table;")
            session.expect("Error:")
            session.expect("dft>")
            session.submit("SELECT 'semi;colon' AS quoted;")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.send("\x1b[A")
            time.sleep(0.05)
            session.send("\r")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.send("\x12")
            session.send("semi")
            session.expect("History search:")
            session.send("\r")
            time.sleep(0.05)
            session.send("\r")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.submit("SELECT SUM(a) FROM generate_series(1, 1000000000000) AS t(a);")
            session.expect("Executing.")
            session.send("\x03")
            session.expect("Cancelled.")
            session.expect("dft>")
            session.resize(18, 80)
            session.submit("SELECT 7;")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.send("SELECT unfinished")
            session.send("\x03")
            session.expect("^C")
            session.finish("\x04")
        finally:
            session.close()
        assert "semi;colon" in (directory / "repl-history.toml").read_text()
        # Restart and verify persistent history, then quit with the explicit command.
        session = Session(binary, directory, ["--repl"])
        try:
            session.send("\x1b[A")
            time.sleep(0.05)
            session.send("\r")
            session.expect("1 row(s)")
            session.expect("dft>")
            session.send("\\q\r")
            session.finish("")
        finally:
            session.close()
    print("REPL PTY checks passed")


if __name__ == "__main__":
    run(sys.argv[1], "--default-repl" in sys.argv[2:])
