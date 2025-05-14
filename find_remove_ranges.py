from cpp_engine_dedup import EngineDedup_U8
import argparse
import glob
import os
import resource

parser = argparse.ArgumentParser()
parser.add_argument("--index_dir", type=str, required=True)
parser.add_argument("--minlen", type=int, required=True)
parser.add_argument("--hack", type=int, default=100000)
parser.add_argument("--num_threads", type=int, required=True)
parser.add_argument("--low_ram", default=False, action="store_true")
parser.add_argument("--num_batches", type=int, required=True)
parser.add_argument("--ulimit", type=int, default=524288)
args = parser.parse_args()

resource.setrlimit(resource.RLIMIT_NOFILE, (args.ulimit, args.ulimit))

# engine = EngineDedup_U8([args.index_dir], False)
# engine.find_remove_ranges(
#     min_len=args.minlen,
#     num_threads=args.num_threads,
#     low_ram=args.low_ram,
#     num_batches=args.num_batches,
# )

index_dirs = glob.glob(os.path.join(args.index_dir, "*"))
engine = EngineDedup_U8(index_dirs, False)
engine.find_remove_ranges_sharded(
    min_len=args.minlen,
    hack=args.hack,
    num_threads=args.num_threads,
    low_ram=args.low_ram,
    num_batches=args.num_batches,
)
