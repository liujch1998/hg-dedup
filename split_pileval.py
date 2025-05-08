import json
import os

with open('data/pileval/val.jsonl', 'r') as f:
    lines = f.readlines()

os.makedirs('data/pileval-c1024', exist_ok=True)

# split into 1024 chunks and save to data/pileval-c1024/
for i in range(1024):
    with open(f'data/pileval-c1024/val-{i:04d}.jsonl', 'w') as f:
        s = i * len(lines) // 1024
        e = (i + 1) * len(lines) // 1024
        for line in lines[s:e]:
            f.write(line)
