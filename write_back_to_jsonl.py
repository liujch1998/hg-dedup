from cpp_engine_dedup import EngineDedup_U8
import json
import argparse
import os
import numpy as np
import gzip
import zstandard as zstd

parser = argparse.ArgumentParser()
parser.add_argument("--index_dir", type=str, required=True)
parser.add_argument("--remove_ranges_path", type=str, default=None)
parser.add_argument("--output_dir", type=str, required=True)
args = parser.parse_args()

engine = EngineDedup_U8([args.index_dir])
doc_cnt = engine.get_total_doc_cnt()

remove_ranges = np.zeros((0, 2), dtype=np.uint64)
if args.remove_ranges_path is not None:
    with open(args.remove_ranges_path, "rb") as f:
        remove_ranges = np.frombuffer(f.read(), dtype=np.uint64).reshape(-1, 2)

curr_path = None
curr_bufs = []
curr_range_ix = 0

def write_buf():
    global curr_path, curr_bufs

    abs_path = os.path.join(args.output_dir, curr_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    if curr_path.endswith(".zst"):
        cctx = zstd.ZstdCompressor(level=3)
        with open(abs_path, "wb") as fout:
            with cctx.stream_writer(fout) as compressor:
                for buf in curr_bufs:
                    compressor.write(buf.encode("utf-8"))
    elif curr_path.endswith(".gz"):
        with gzip.open(abs_path, "wt", encoding="utf-8") as fout:
            for buf in curr_bufs:
                fout.write(buf)
    else:
        with open(abs_path, "w") as fout:
            for buf in curr_bufs:
                fout.write(buf)

for doc_ix in range(doc_cnt):
    doc = engine.get_doc_by_ix(doc_ix)
    metadata = json.loads(doc.metadata)
    path, linenum = metadata["path"], metadata["linenum"]
    if curr_path != path:
        if curr_path is not None:
            write_buf()
            curr_bufs = []
        curr_path = path
    meta = metadata["metadata"]
    token_ids = doc.token_ids

    removed_bytes = 0
    while curr_range_ix < remove_ranges.shape[0] and remove_ranges[curr_range_ix, 0] < doc.doc_end_ptr:
        assert remove_ranges[curr_range_ix, 0] >= doc.doc_start_ptr
        assert remove_ranges[curr_range_ix, 1] <= doc.doc_end_ptr
        token_ids = token_ids[:remove_ranges[curr_range_ix, 0] - doc.doc_start_ptr - removed_bytes] + token_ids[remove_ranges[curr_range_ix, 1] - doc.doc_start_ptr - removed_bytes:]
        removed_bytes += remove_ranges[curr_range_ix, 1] - remove_ranges[curr_range_ix, 0]
        curr_range_ix += 1

    try:
        text = bytes(token_ids).decode("utf-8")
    except UnicodeDecodeError:
        text = "" # TODO: handle this
    item = {
        "text": text,
    }
    item = {**item, **meta}
    curr_bufs.append(json.dumps(item) + "\n")

assert curr_range_ix == remove_ranges.shape[0]
if curr_path is not None:
    write_buf()
