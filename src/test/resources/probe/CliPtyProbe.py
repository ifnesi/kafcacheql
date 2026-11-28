"""Exercise the shipped CLI on a real controlling terminal, using only the Python stdlib."""
import errno
import fcntl
import json
import os
import pty
import re
import select
import signal
import struct
import subprocess
import tempfile
import sys
import termios
import time

java, tmpdir, jar, url, topic, version, bad_inputs_file, brokers, cache, *mode = sys.argv[1:]
command = [java, "-Xmx1g", "-Djava.io.tmpdir=" + tmpdir, "-jar", jar, "--mode", "json", url]

class Session:
    def __init__(self, offline=False):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.environ['TERM'] = 'xterm'
            os.execv(java, command[:-1] + [url + "&offline=true"] if offline else command)
        fcntl.ioctl(self.fd, termios.TIOCSWINSZ, struct.pack('HHHH', 40, 200, 0, 0))
        self.pending = ''
        try:
            greeting = self.read('kafcacheql> ')
            shown = '~' + cache[len(os.path.expanduser('~')):] if cache.startswith(os.path.expanduser('~') + os.sep) else cache
            connection = 'offline (cache only)' if offline else 'connected to ' + brokers
            assert greeting == f'KafCacheQL {version}, {connection}\nCache: {shown}\nType help for commands, quit to leave.\n', greeting
        except BaseException:
            self.close()
            raise

    def send(self, text):
        os.write(self.fd, text.encode())

    def read(self, expected, timeout=20):
        end = time.monotonic() + timeout
        while expected not in self.pending:
            remaining = end - time.monotonic()
            assert remaining > 0, (expected, self.pending)
            if not select.select([self.fd], [], [], remaining)[0]:
                raise AssertionError(('timeout', expected, self.pending))
            try:
                data = os.read(self.fd, 65536)
            except OSError as error:
                if error.errno == errno.EIO: data = b''
                else: raise
            assert data, ('terminal closed', expected, self.pending)
            text = data.decode(errors='replace')
            print(text, end='', flush=True)
            # Remove terminal control sequences for assertions, not from the printed transcript.
            self.pending += re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]|\x1b[=>]', '', text).replace('\r', '')
        before, self.pending = self.pending.split(expected, 1)
        return before

    def finish(self, status):
        end = time.monotonic() + 10
        while time.monotonic() < end:
            pid, result = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.pid = None
                assert os.waitstatus_to_exitcode(result) == status, result
                while True:
                    try:
                        data = os.read(self.fd, 65536)
                    except OSError as error:
                        if error.errno == errno.EIO: break
                        raise
                    if not data: break
                    print(data.decode(errors='replace'), end='', flush=True)
                os.close(self.fd)
                return
            time.sleep(.05)
        raise AssertionError('CLI did not exit')

    def close(self):
        if self.pid:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
            os.close(self.fd)

def piped():
    # Both directions use a real PTY; only one standard stream is redirected at a time.
    script = ('SELECT n FROM "' + topic + '";\nSELECT 2718 AS piped;\n').encode()
    expected = '[{"n":7}]\n[{"piped":2718}]\n'
    for input_pipe in [True, False]:
        master, slave = pty.openpty()
        with tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(command,
                                       stdin=subprocess.PIPE if input_pipe else slave,
                                       stdout=slave if input_pipe else subprocess.PIPE,
                                       stderr=errors, start_new_session=True)
            os.close(slave)
            try:
                if input_pipe:
                    process.communicate(script, timeout=20)
                    output = b''
                    while True:
                        try:
                            data = os.read(master, 65536)
                        except OSError as error:
                            if error.errno == errno.EIO: break
                            raise
                        if not data: break
                        output += data
                else:
                    os.write(master, script + b'\x04')
                    output, _ = process.communicate(timeout=20)
                errors.seek(0)
                error = errors.read().decode()
                text = output.decode().replace('\r', '')
                print(text, end='', flush=True)
                assert process.returncode == 0, (process.returncode, error, text)
                assert text == expected, text  # No prompt, terminal escape codes or input echo.
                assert error == '', error
            finally:
                process.kill()
                process.wait()
                errors.seek(0)
                print(errors.read().decode(), end='', flush=True)
                os.close(master)
    print('PTY checks passed')

if mode == ['pipe']:
    piped()
    sys.exit(0)

s = None
try:
    s = Session()
    # §14.1: mode is a session command only with an empty SQL buffer; here it is the SQL alias.
    s.send('SELECT 7 AS\n')
    assert s.read('       ...> ') == 'SELECT 7 AS\n'
    s.send('mode;\n')
    assert s.read('> ') == 'mode;\n[{"mode":7}]\nkafcacheql'
    # §14.1 Output modes: the option starts the session in JSON; commands never reach JDBC.
    for mode_command in ['mOdE;', 'mode']:
        s.send(mode_command + '\n')
        assert s.read('kafcacheql> ') == mode_command + '\nMode: json\n'
    s.send('MoDe LiNe;\n')
    assert s.read('kafcacheql> ') == 'MoDe LiNe;\nMode: line\n'
    query = 'SELECT n, NULL AS "absent" FROM "' + topic + '";'
    s.send(query + '\n')
    assert s.read('kafcacheql> ') == query + '\n     n: 7\nabsent: NULL\n'
    # §14.1: prompts separate results, so even a later line-mode result starts with its first row.
    s.send(query + '\n')
    assert s.read('kafcacheql> ') == query + '\n     n: 7\nabsent: NULL\n'
    # An empty result adds nothing and the next result still starts directly with its first row.
    empty_query = query[:-1] + ' LIMIT 0;'
    s.send(empty_query + '\n')
    assert s.read('kafcacheql> ') == empty_query + '\n'
    s.send(query + '\n')
    assert s.read('kafcacheql> ') == query + '\n     n: 7\nabsent: NULL\n'
    s.send('mode bogus\n')
    assert s.read('kafcacheql> ') == 'mode bogus\nERROR: Mode must be table, line or json.\n'
    s.send('mode;\n')
    assert s.read('kafcacheql> ') == 'mode;\nMode: line\n'
    s.send('mode json\n')
    assert s.read('kafcacheql> ') == 'mode json\nMode: json\n'
    # §14.1 Help: exact session help, with or without a semicolon, only at an empty SQL buffer.
    help_text = """Statements end with ';' at the end of a line. Quote names that contain '-' or '.'.

  SELECT ... ;               Query topics as tables; fetches what the query needs first
  SHOW TABLES;               Topics, with cached rows and bytes
  DESCRIBE "<table>";        A table's columns and types
  SYNC [<pattern>, ...];     Fetch everything new: cached topics, or names with * and ?
  DROP TABLE "<table>";      Remove a topic from the local cache; Kafka is unchanged

  help                       This list (no ';' needed)
  mode [table|line|json]     Show or change how results print
  quit, exit                 Leave (or Ctrl-D)
  Ctrl-C                     Cancel the running statement
  Up and down arrows         Earlier statements
  Ctrl-R                     Search earlier statements
  Ctrl-A, Ctrl-E             Start, end of the line

Docs: https://github.com/ifnesi/kafcacheql/blob/main/docs/cli.md
"""
    for help_command in ['help', 'help;']:
        s.send(help_command + '\n')
        output = s.read('kafcacheql> ')
        assert output == help_command + '\n' + help_text, output
    # Every rejected input gets the driver's exact message; only unknown statements add session help.
    with open(bad_inputs_file) as inputs:
        bad_inputs = json.load(inputs)
    for bad in bad_inputs:
        sql = bad['sql']
        s.send(sql + ('' if sql.endswith(';') else ';') + '\n')
        s.read('ERROR: ')
        message = bad['message']
        if message.startswith('Unknown statement '): message += ' Type help for the list.'
        output = s.read('kafcacheql> ')
        assert output == message + '\n', (sql, output)
    s.send('SELECT\n')
    s.read('       ...> ')
    s.send('help;\n')
    s.read('ERROR: '); s.read('help'); s.read('kafcacheql> ')
    # JLine owns cursor movement: insert a prefix at Ctrl-A, then append at Ctrl-E.
    s.send('SELECT 27')
    s.read('SELECT 27')
    s.send('\x01/* start */ \x0518 AS edited;\n')
    s.read('[{"edited":2718}]'); s.read('kafcacheql> ')
    s.send('SELECT n FROM "' + topic + '";\n')
    s.read('[{"n":7}]'); s.read('kafcacheql> ')
    s.send('SELECT missing FROM "' + topic + '";\n')
    s.read('ERROR: '); s.read('missing'); s.read('kafcacheql> ')
    # The echoed line is seen before sending Ctrl-C; a recursive billion-row aggregate cannot finish first.
    s.send('WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM x WHERE n<1000000000) SELECT sum(n) FROM x;\n')
    s.read('FROM x;')
    time.sleep(.5)
    s.send('\x03')
    s.read('cancel', timeout=10); s.read('kafcacheql> ')
    s.send('SELECT 314159 AS history;\n')
    s.read('[{"history":314159}]'); s.read('kafcacheql> ')
    # xterm is in application-cursor mode (JLine sent CSI ?1h): Up is SS3 A.
    s.send('\x1bOA\n')
    s.read('[{"history":314159}]'); s.read('kafcacheql> ')
    s.send('exit;\n'); s.finish(1)  # A prior statement failed, even though the session continued.
    for ending in ['quit', 'quit;', 'exit', 'exit;', '\x04']:
        s = Session()
        # A new session has no previous session's history.
        s.send('\x1bOA' + 'SELECT 2718 AS fresh;\n')
        s.read('[{"fresh":2718}]'); s.read('kafcacheql> ')
        s.send(ending + ('\n' if ending != '\x04' else ''))
        s.finish(0)
    s = Session(offline=True)
    s.send('SELECT n FROM "' + topic + '";\n')
    s.read('[{"n":7}]'); s.read('kafcacheql> ')
    s.send('quit\n'); s.finish(0)
    print('PTY checks passed')
finally:
    if s: s.close()
