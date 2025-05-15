from cpp_engine_dedup import EngineDedup_U8
import json
import argparse
import os
import numpy as np
import glob
import gzip
import zstandard as zstd
import multiprocessing as mp

parser = argparse.ArgumentParser()
parser.add_argument("--index_dir", type=str, required=True)
parser.add_argument("--minlen", type=int, default=None)
parser.add_argument("--output_dir", type=str, required=True)
parser.add_argument("--num_workers", type=int, default=1)
parser.add_argument("--mode", type=str, default="remove", choices=["remove", "annotate"])
args = parser.parse_args()
if args.mode == "annotate":
    args.output_dir = args.output_dir.rstrip("/") + "_annotated"

def write_worker(index_dir):

    global args

    engine = EngineDedup_U8([index_dir], True)
    doc_cnt = engine.get_total_doc_cnt()

    remove_ranges = np.zeros((0, 2), dtype=np.uint64)
    if args.minlen is not None:
        remove_ranges_path = os.path.join(index_dir, f"dedup_minlen{args.minlen}", "remove_ranges")
        with open(remove_ranges_path, "rb") as f:
            remove_ranges = np.frombuffer(f.read(), dtype=np.uint64).reshape(-1, 2)

    curr_path = None
    curr_bufs = []
    curr_range_ix = 0
    kept_in_the_middle_lengths = [] # the lengths of kept segments between two removed ranges in the same document

    def write_buf(curr_path, curr_bufs):

        abs_path = os.path.join(args.output_dir, curr_path)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        if curr_path.endswith(".zst"):
            cctx = zstd.ZstdCompressor()
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
                write_buf(curr_path, curr_bufs)
                curr_bufs = []
            curr_path = path
        meta = metadata["metadata"]
        token_ids = doc.token_ids

        doc_remove_ranges = []
        while curr_range_ix < remove_ranges.shape[0] and remove_ranges[curr_range_ix, 0] < doc.doc_end_ptr:
            assert remove_ranges[curr_range_ix, 0] >= doc.doc_start_ptr
            assert remove_ranges[curr_range_ix, 1] <= doc.doc_end_ptr

            # clip to whole UTF-8 characters
            s = remove_ranges[curr_range_ix, 0] - doc.doc_start_ptr
            e = remove_ranges[curr_range_ix, 1] - doc.doc_start_ptr
            while s < len(token_ids) and 128 <= token_ids[s] < 192:
                s += 1
            if e != len(token_ids):
                while e >= 0 and 128 <= token_ids[e] < 192:
                    e -= 1
            assert s <= e

            doc_remove_ranges.append((int(s), int(e)))
            curr_range_ix += 1

        doc_keep_ranges = [ (r0[1], r1[0]) for r0, r1 in zip([(0, 0)] + doc_remove_ranges, doc_remove_ranges + [(len(token_ids), len(token_ids))]) ]
        if args.mode == "remove":
            token_ids = sum([ token_ids[s:e] for s, e in doc_keep_ranges ], [])
        for doc_keep_range in doc_keep_ranges[1:-1]:
            kept_in_the_middle_lengths.append(doc_keep_range[1] - doc_keep_range[0])

        text = bytes(token_ids).decode("utf-8")
        item = {
            "text": text,
        }
        if args.mode == "annotate":
            item["sa_remove_ranges"] = doc_remove_ranges
        item = {**item, **meta}
        curr_bufs.append(json.dumps(item) + "\n")

    assert curr_range_ix == remove_ranges.shape[0]
    if curr_path is not None:
        write_buf(curr_path, curr_bufs)

    kept_in_the_middle_lengths = sorted(kept_in_the_middle_lengths)
    if args.minlen is not None:
        with open(os.path.join(index_dir, f"dedup_minlen{args.minlen}", "kept_in_the_middle_lengths.txt"), "w") as f:
            for length in kept_in_the_middle_lengths:
                f.write(f"{length}\n")

with mp.get_context("fork").Pool(args.num_workers) as p:

    index_dirs = glob.glob(os.path.join(args.index_dir, "*"))
    index_dirs = sorted(index_dirs, key=lambda x: int(x.split("/")[-1]))

    p.map(write_worker, index_dirs)
