import zstandard as zstd
import json
import os

with open("00-sorted_chunk_00000042.jsonl.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        decompressed_data = reader.read().decode('utf-8')
    lines = decompressed_data.split('\n')
    if lines[-1] == '':
        lines = lines[:-1]

num_diff_lines = 0
total_bytes_orig = 0
total_bytes_removed = 0
os.makedirs("00-sorted_chunk_00000042", exist_ok=True)

for i, line in enumerate(lines):
    item = json.loads(line)
    if item["sa_remove_ranges"] != []:
        num_diff_lines += 1
        total_bytes_orig += len(item["text"].encode("utf-8"))
        total_bytes_removed += sum(e - s for (s, e) in item["sa_remove_ranges"])

        with open(f"00-sorted_chunk_00000042/{i}.ansi", "w") as f:
            pos = 0
            for (s, e) in item["sa_remove_ranges"]:
                f.write("\033[0m")
                f.write(item["text"].encode("utf-8")[pos:s].decode("utf-8"))
                f.write("\033[31m")
                f.write(item["text"].encode("utf-8")[s:e].decode("utf-8"))
                pos = e
            f.write("\033[0m")
            f.write(item["text"].encode("utf-8")[pos:].decode("utf-8"))

        # if not first_diff_displayed:
        #     print(f"First diff is at line {i}")
        #     print(item["text"])
        #     print("="*100)
        #     for (s, e) in item["sa_remove_ranges"]:
        #         print(f"Removing bytes {s} to {e}")
        #         print(item["text"].encode("utf-8")[s:e].decode("utf-8"))
        #         print("-"*100)
        #     print("="*100)
        #     first_diff_displayed = True

print(f"Ratio of different lines: {num_diff_lines / len(lines):.4f}")
print(f"Ratio of removed bytes: {total_bytes_removed / total_bytes_orig:.4f}")

