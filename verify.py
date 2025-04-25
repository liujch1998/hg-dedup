import json

with open("data/pileval/val.jsonl", "r") as f:
    ds = []
    for line in f:
        ds.append(json.loads(line))

with open("data/pileval_minlen1000/val.jsonl", "r") as f:
    ds_minlen1000 = []
    for line in f:
        ds_minlen1000.append(json.loads(line))

for i, (d, d_minlen1000) in enumerate(zip(ds, ds_minlen1000)):
    if d["text"] != d_minlen1000["text"]:
        print(i)
        print("="*100)
        print(d["text"])
        print("="*100)
        print(d_minlen1000["text"])
        break
