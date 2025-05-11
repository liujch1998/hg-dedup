from cpp_engine_dedup import EngineDedup_U8
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--index_dir", type=str, required=True)
parser.add_argument("--min_len", type=int, required=True)
parser.add_argument("--num_threads", type=int, required=True)
parser.add_argument("--low_ram", default=False, action="store_true")
parser.add_argument("--num_batches", type=int, required=True)
args = parser.parse_args()

output_dir = os.path.join(args.index_dir, f"dedup_minlen{args.min_len}")

engine = EngineDedup_U8([args.index_dir], False)
engine.find_remove_ranges_parallel(
    min_len=args.min_len,
    num_threads=args.num_threads,
    output_dir=output_dir,
    low_ram=args.low_ram,
    num_batches=args.num_batches,
)
