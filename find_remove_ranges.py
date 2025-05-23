from cpp_engine_dedup import EngineDedup_U8
import argparse
import glob
import os
import resource

parser = argparse.ArgumentParser()
parser.add_argument("--index_dir", type=str, required=True)
parser.add_argument("--minlen", type=int, required=True)
parser.add_argument("--mode", default="naive", choices=["naive", "parallel", "parallel_sharded"])
parser.add_argument("--num_threads", type=int, default=1)
parser.add_argument("--low_ram", default=False, action="store_true")
parser.add_argument("--num_batches", type=int, default=1)
parser.add_argument("--ulimit", type=int, default=1048576)
args = parser.parse_args()

resource.setrlimit(resource.RLIMIT_NOFILE, (args.ulimit, args.ulimit))

if args.mode == "naive":
    engine = EngineDedup_U8([args.index_dir], False)
    engine.find_remove_ranges(args.minlen)

elif args.mode == "parallel":
    engine = EngineDedup_U8([args.index_dir], False)
    engine.find_remove_ranges_parallel(
        min_len=args.minlen,
        num_threads=args.num_threads,
        low_ram=args.low_ram,
        num_batches=args.num_batches,
    )

elif args.mode == "parallel_sharded":
    index_dirs = glob.glob(os.path.join(args.index_dir, "*"))
    index_dirs = sorted(index_dirs, key=lambda x: int(x.split("/")[-1]))
    engine = EngineDedup_U8(index_dirs, False)
    engine.find_remove_ranges_parallel_sharded(
        min_len=args.minlen,
        num_threads=args.num_threads,
        low_ram=args.low_ram,
        num_batches=args.num_batches,
    )
