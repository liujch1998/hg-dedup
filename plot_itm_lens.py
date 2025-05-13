import numpy as np
import matplotlib.pyplot as plt
import argparse
import math

# Parse command line arguments
parser = argparse.ArgumentParser(description='Plot distribution of integers with logarithmic bins.')
parser.add_argument('-i', '--input_path', type=str, help='Path to the input txt file (one positive integer per line)')
parser.add_argument('-o', '--output_path', type=str, help='Path to the output png file')
args = parser.parse_args()

# Read integers from file
with open(args.input_path, 'r') as f:
    data = [int(line.strip()) for line in f if line.strip()]

if not data:
    raise ValueError('No data found in the input file.')

# Determine bin edges: [2^0, 2^1), [2^1, 2^2), ... up to max(data)
max_val = max(data)
max_exp = math.ceil(math.log2(max_val + 1))
bin_edges = [2 ** i for i in range(0, max_exp + 1)]

# Plot histogram
plt.figure(figsize=(8, 6))
plt.hist(data, bins=bin_edges, edgecolor='black')
plt.xscale('log', base=2)
plt.xlabel('Value')
plt.ylabel('Count')
plt.title('Distribution of length of span between removed ranges')
plt.grid(True, which='both', ls='--', lw=0.5)
plt.tight_layout()
plt.savefig(args.output_path, dpi=300)

