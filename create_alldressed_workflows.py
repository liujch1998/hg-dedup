import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--num_nodes", type=int, default=22)
parser.add_argument("--num_shards", type=int, default=132)
parser.add_argument("--remote_dir", type=str, default="s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3")
parser.add_argument("--output_dir", type=str, default="aws_workflow_alldressed")
args = parser.parse_args()

os.makedirs(args.output_dir, exist_ok=True)

for rank in range(args.num_nodes):
    with open(f"aws_workflow_alldressed_template.sh", "r") as f:
        content = f.read()
    content = content.replace("[[INSTANCE_TYPE]]", "i7i")
    content = content.replace("[[NUM_SHARDS]]", str(args.num_shards))
    content = content.replace("[[NUM_NODES]]", str(args.num_nodes))
    content = content.replace("[[RANK]]", str(rank))
    content = content.replace("[[REMOTE_DIR]]", f"\"{args.remote_dir}\"")
    with open(f"{args.output_dir}/rank_{rank:04d}.sh", "w") as f:
        f.write(content)
    os.chmod(f"{args.output_dir}/rank_{rank:04d}.sh", 0o755)
