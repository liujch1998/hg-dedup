import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--num_nodes", type=int, default=22)
parser.add_argument("--num_shards", type=int, default=132)
args = parser.parse_args()

for rank in range(args.num_nodes):
    with open(f"aws_workflow_alldressed_template.sh", "r") as f:
        content = f.read()
    content = content.replace("[[INSTANCE_TYPE]]", "i7i")
    content = content.replace("[[NUM_SHARDS]]", str(args.num_shards))
    content = content.replace("[[NUM_NODES]]", str(args.num_nodes))
    content = content.replace("[[RANK]]", str(rank))
    with open(f"alldressed/aws_workflow_alldressed_{rank:04d}.sh", "w") as f:
        f.write(content)
    os.chmod(f"alldressed/aws_workflow_alldressed_{rank:04d}.sh", 0o755)
