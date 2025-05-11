from cpp_engine_dedup import EngineDedup_U8
import json
import argparse
import os
import numpy as np
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

engine = EngineDedup_U8([args.index_dir], True)
doc_cnt = engine.get_total_doc_cnt()

remove_ranges = np.zeros((0, 2), dtype=np.uint64)
if args.minlen is not None:
    remove_ranges_path = os.path.join(args.index_dir, f"dedup_minlen{args.minlen}", "remove_ranges")
    with open(remove_ranges_path, "rb") as f:
        remove_ranges = np.frombuffer(f.read(), dtype=np.uint64).reshape(-1, 2)

def find_start_worker(w):

    global args, engine, doc_cnt, remove_ranges

    start_doc_ix = w * doc_cnt // args.num_workers
    while True:
        if start_doc_ix == 0 or start_doc_ix == doc_cnt:
            break
        meta_prev = json.loads(engine.get_doc_by_ix(start_doc_ix - 1).metadata)
        meta_curr = json.loads(engine.get_doc_by_ix(start_doc_ix).metadata)
        if meta_prev["path"] != meta_curr["path"]:
            break
        start_doc_ix += 1

    start_doc_start_ptr = engine.get_doc_by_ix(start_doc_ix).doc_start_ptr if start_doc_ix < doc_cnt else (2**64-1)
    # find the first range that starts after start_doc_start_ptr, using binary search
    start_range_ix = np.searchsorted(remove_ranges[:, 0], start_doc_start_ptr, side='left') # a[i-1] < v <= a[i]

    return start_doc_ix, start_range_ix

with mp.get_context("fork").Pool(args.num_workers) as p:
    results = p.map(find_start_worker, range(args.num_workers))
    start_doc_ix_by_worker = [r[0] for r in results]
    start_range_ix_by_worker = [r[1] for r in results]

def write_worker(w, start_doc_ix, end_doc_ix, start_range_ix, end_range_ix):

    print(f"Starting worker {w} with doc_ix [{start_doc_ix}, {end_doc_ix}) and range_ix [{start_range_ix}, {end_range_ix})")

    global args, engine, remove_ranges

    curr_path = None
    curr_bufs = []
    curr_range_ix = start_range_ix
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

    for doc_ix in range(start_doc_ix, end_doc_ix):
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
            item["sa_keep_ranges"] = doc_keep_ranges
        item = {**item, **meta}
        curr_bufs.append(json.dumps(item) + "\n")

    assert curr_range_ix == end_range_ix
    if curr_path is not None:
        write_buf(curr_path, curr_bufs)

    return kept_in_the_middle_lengths

with mp.get_context("fork").Pool(args.num_workers) as p:
    kept_in_the_middle_lengths_by_worker = p.starmap(write_worker, [(
        w,
        start_doc_ix_by_worker[w],
        start_doc_ix_by_worker[w + 1] if w < args.num_workers - 1 else doc_cnt,
        start_range_ix_by_worker[w],
        start_range_ix_by_worker[w + 1] if w < args.num_workers - 1 else remove_ranges.shape[0],
    ) for w in range(args.num_workers)])

# collect and write kept_in_the_middle_lengths
kept_in_the_middle_lengths = sorted(sum(kept_in_the_middle_lengths_by_worker, []))
if args.minlen is not None:
    kept_in_the_middle_lengths_path = os.path.join(args.index_dir, f"dedup_minlen{args.minlen}", "kept_in_the_middle_lengths.txt")
    with open(kept_in_the_middle_lengths_path, "w") as f:
        for length in kept_in_the_middle_lengths:
            f.write(f"{length}\n")
