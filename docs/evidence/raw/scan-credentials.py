#!/usr/bin/env python3
"""Scan text and decompressed evidence without printing candidate credentials."""
import gzip
from pathlib import Path
import re
import sys

patterns = {
    "capstan_key": rb"cap_[A-Za-z0-9_-]{20,}",
    "anthropic_key": rb"sk-ant-",
    "aws_access_key": rb"(?:AKIA|ASIA|AIDA|AROA)[A-Z0-9]{16}",
    "aws_secret_assignment": rb"(?i)aws[_ -]?secret[_ -]?access[_ -]?key\s*[:=]\s*['\"]?[A-Za-z0-9/+=]{40}",
    "database_password": rb"postgres(?:ql)?://[^\s\"'<>]+",
    "private_key": rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
}
roots = [Path(arg) for arg in sys.argv[1:]]
if not roots:
    raise SystemExit("usage: scan-credentials.py PATH [PATH ...]")
count = 0
hits = {key: [] for key in patterns}
for root in roots:
    for path in sorted(root.rglob("*")) if root.is_dir() else [root]:
        if not path.is_file() or path.suffix in (".png", ".gif", ".py"):
            continue
        data = gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
        count += 1
        for label, pattern in patterns.items():
            for match in re.finditer(pattern, data):
                if label == "database_password":
                    token = match[0]
                    if b"@" not in token or re.match(rb"postgres(?:ql)?://capstan:capstan@", token):
                        continue
                    if b":" not in token.split(b"://", 1)[1].split(b"@", 1)[0]:
                        continue
                hits[label].append(f"{path}:{data[:match.start()].count(bytes([10])) + 1}")
print(f"Scanned {count} files (gzip decompressed; binary images and scanner source excluded).")
for label, locations in hits.items():
    print(f"{label}: {len(locations)} matches")
    for location in locations:
        print(f"  {location}")
print("Allowed database credentials: capstan:capstan only.")
raise SystemExit(any(hits.values()))
