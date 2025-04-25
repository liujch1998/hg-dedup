import zstandard as zstd
import json

with open("/data/exact_dupaware_0.01_minhashed/CC-MAIN-2023-06/shard_00000091.jsonl.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        decompressed_data = reader.read().decode('utf-8')
    lines = decompressed_data.split('\n')

with open("/data/exact_dupaware_0.01_minhashed_minlen1000/CC-MAIN-2023-06/shard_00000091.jsonl.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        decompressed_data = reader.read().decode('utf-8')
    lines_minlen1000 = decompressed_data.split('\n')

for i, (line, line_minlen1000) in enumerate(zip(lines, lines_minlen1000)):
    item = json.loads(line)
    item_minlen1000 = json.loads(line_minlen1000)
    if item["text"] != item_minlen1000["text"]:
        print(i)
        print("="*100)
        print(item["text"])
        print("="*100)
        print(item_minlen1000["text"])
        break
