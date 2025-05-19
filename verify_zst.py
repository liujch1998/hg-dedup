import zstandard as zstd
import json

with open("/data/deduplication_ablations_v1_minhash_suffarr_minhash_only/crawl=CC-MAIN-2013-20/shard_00000011.jsonl.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        decompressed_data = reader.read().decode('utf-8')
    lines = decompressed_data.split('\n')
    if lines[-1] == '':
        lines = lines[:-1]

with open("/data-1/deduplication_ablations_v1_minhash_suffarr_minhash_only_minlen500/crawl=CC-MAIN-2013-20/shard_00000011.jsonl.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        decompressed_data = reader.read().decode('utf-8')
    lines_deduped = decompressed_data.split('\n')
    if lines_deduped[-1] == '':
        lines_deduped = lines_deduped[:-1]

assert len(lines) == len(lines_deduped)
num_diff_lines = 0
total_bytes_orig = 0
total_bytes_deduped = 0
first_diff_displayed = False

for i, (line, line_deduped) in enumerate(zip(lines, lines_deduped)):
    item = json.loads(line)
    item_deduped = json.loads(line_deduped)
    if item["text"] != item_deduped["text"]:
        num_diff_lines += 1
        total_bytes_orig += len(item["text"].encode("utf-8"))
        total_bytes_deduped += len(item_deduped["text"].encode("utf-8"))

        if not first_diff_displayed:
            print(f"First diff is at line {i}")
            print("="*100)
            print(item["text"])
            print("="*100)
            print(item_deduped["text"])
            first_diff_displayed = True

print(f"Ratio of different lines: {num_diff_lines / len(lines):.4f}")
print(f"Ratio of removed bytes: {1 - total_bytes_deduped / total_bytes_orig:.4f}")

