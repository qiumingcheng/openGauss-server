"""Read-only windows on this test runtime's real text logger output.

No GUC/service changes, historical rescans or pg_current_logfile assumption.
The admitted append-only logger configuration supports new-file rotation; an
observed truncation, replacement race or unowned path fails the evidence.
"""
import os
from pathlib import Path
import re
import stat
import time


class NativeLogSource:
    def __init__(self, runtime, config):
        self.runtime = Path(runtime).resolve(strict=True)
        self.config = dict(config)
        data = Path(config['data_directory']).resolve(strict=True)
        if data != (self.runtime / 'data').resolve(strict=True):
            raise RuntimeError('Logger data directory differs from the selected runtime')
        self.uid = data.stat().st_uid
        if self.uid != os.getuid():
            raise RuntimeError('Logger runtime is not owned by this test OS user')
        self.collector = config['logging_collector'] == 'on'
        if self.collector:
            directory = Path(config['log_directory'])
            self.directory = (directory if directory.is_absolute() else data / directory).resolve(strict=True)
            if not self.directory.is_relative_to(self.runtime) or self.directory.stat().st_uid != self.uid:
                raise RuntimeError('Logger directory is outside the owned runtime')
            if config['log_truncate_on_rotation'] != 'off':
                raise RuntimeError('Log evidence requires append-only rotation; no GUC was changed')
            pattern = config['log_filename']
            if not pattern or Path(pattern).name != pattern or pattern in ('.', '..'):
                raise RuntimeError('Logger filename is not a controlled basename')
            escaped = re.escape(pattern.replace('%%', '\x01'))
            self.pattern = re.compile('^' + re.sub(r'%[A-Za-z]', '.*', escaped).replace('\x01', '%') + '$')
        else:
            self.directory = self.runtime
            self.pattern = re.compile(r'^server\.log$')
        self.identity = dict(runtime=str(self.runtime), directory=str(self.directory), collector=self.collector,
                             uid=self.uid, config=self.config)

    @classmethod
    def from_connection(cls, connection, runtime):
        config = {}
        # These are closed, read-only SHOW statements, never caller SQL.
        with connection.cursor() as cursor:
            for name in ('data_directory', 'logging_collector', 'log_directory', 'log_filename',
                         'log_truncate_on_rotation', 'log_destination'):
                cursor.execute('SHOW ' + name)
                config[name] = str(cursor.fetchone()[0])
        return cls(runtime, config)

    def files(self):
        files = []
        for path in self.directory.iterdir():
            if not self.pattern.fullmatch(path.name):
                continue
            try:
                info = path.lstat()
            except FileNotFoundError:  # Rotation between directory listing/stat.
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_uid != self.uid or path.resolve().parent != self.directory:
                raise RuntimeError('Logger candidate is not an owned regular file: ' + path.name)
            files.append((path, info))
        if not files or len(files) > 1024:
            raise RuntimeError('Owned logger file inventory is empty or exceeds1024')
        return sorted(files, key=lambda item: item[0].name)

    def mark(self, budget=16 * 1024 * 1024):
        return NativeLogWindow(self, budget)


class NativeLogWindow:
    def __init__(self, source, budget):
        if budget < 1 or budget > 16 * 1024 * 1024:
            raise ValueError('Native log window budget must be1..16MiB')
        self.source, self.budget = source, budget
        self.cursors, self.baseline, self.chunks, self.partial = {}, [], [], {}
        self.read_bytes, self.polls, self.elapsed, self.matched = 0, 0, 0, False
        for path, info in source.files():
            key = (info.st_dev, info.st_ino)
            if key in self.cursors:
                raise RuntimeError('Logger inode is aliased by multiple filenames')
            self.cursors[key] = info.st_size
            self.baseline.append(dict(name=path.name, device=key[0], inode=key[1], offset=info.st_size))
        self.final = []

    def read(self):
        self.polls += 1
        self.final = []
        for path, listed in self.source.files():
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
            with os.fdopen(descriptor, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if (info.st_dev, info.st_ino) != (listed.st_dev, listed.st_ino):
                    raise RuntimeError('Logger file identity changed while opening it')
                key = (info.st_dev, info.st_ino)
                offset = self.cursors.get(key, 0)  # Newly rotated inode starts0.
                if info.st_size < offset:
                    raise RuntimeError('Logger file truncated inside an evidence window')
                size = info.st_size - offset
                if size > self.budget - self.read_bytes:
                    raise RuntimeError('Native log evidence exceeds16MiB total read budget')
                if size:
                    stream.seek(offset)
                    data = stream.read(size)
                    if len(data) != size:
                        raise RuntimeError('Logger file shrank while reading evidence')
                    buffered = self.partial.get(key, b'') + data
                    newline = buffered.rfind(b'\n')
                    if newline >= 0:
                        self.chunks.append(buffered[:newline + 1])
                    self.partial[key] = buffered[newline + 1:] if newline >= 0 else buffered
                    self.read_bytes += len(data)
                    self.cursors[key] = offset + len(data)
                self.final.append(dict(name=path.name, device=key[0], inode=key[1], offset=self.cursors.get(key, 0)))
        # Parse complete records only, without joining fragments from distinct
        # logger files. A pending write remains owned by its original inode.
        return b''.join(self.chunks).decode('utf-8', 'replace')

    def wait(self, predicate, timeout=5.0):
        if timeout < 0 or timeout > 10:
            raise ValueError('Native logger drain deadline must be0..10seconds')
        start = time.monotonic()
        while True:
            text = self.read()
            self.elapsed = time.monotonic() - start
            self.matched = bool(predicate(text))
            if self.matched or self.elapsed >= timeout:
                return text
            time.sleep(min(.025, timeout - self.elapsed))

    def receipt(self):
        return dict(source=self.source.identity, starts=self.baseline, ends=self.final,
                    read_bytes=self.read_bytes, budget=self.budget, polls=self.polls,
                    elapsed_seconds=self.elapsed, expected_records_observed=self.matched,
                    incomplete_bytes=sum(len(value) for value in self.partial.values()))
