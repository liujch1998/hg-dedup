# bsade (Best SA Dedup Ever)

This repo contains scripts to substring-dedup a pre-training dataset using suffix array (SA).
It is the best SA dedup ever because (1) it is very fast, and (2) we keep one copy of each repeated string, whereas previous implementations would remove all copies.

On machines with 2TB RAM, it can typically globally dedup a dataset up to 1TB (uncompressed text) in about 6 hours.
Larger datasets will need to be sharded.

## Example

Running `aws_workflow.sh` on an AWS `x2idn.32xlarge` instance will dedup a 291GB (compressed) / 594GB (uncompressed) dataset.
`aws_launch.sh` streamlines the instance creation and job launching.

## Steps

Deduping a dataset involves the following steps:
1. Download the dataset from S3 to local disk.
2. Index the dataset with infini-gram.
3. Find the ranges of text to be removed.
4. Writeback the deduped dataset into the original format.
5. Upload the deduped dataset to S3.

### Downloading the dataset

Use `s5cmd` for fast download.
For example:
```
s5cmd cp -sp s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/final_ablation/minhash_10x_b/* /data/${NAME}/
```

The dataset will be saved at `/data/${NAME}/` which contains many many `.jsonl.zst` files.

### Index the dataset

In this step, we prepare the text files by decompressing the data files, extracting the document text, concatenating them into a big binary file.
We then build SA of this byte array.

Example command:
```
python indexing_v6_sharded.py \
    --data_dir /data/${NAME} --save_dir /data/${INDEX_NAME} \
    --token_dtype u8 --cpus 128 --num_batches 8 --add_metadata
```

`--token_dtype u8` ensures that we're working on UTF-8 bytes.
`--add_metadata` preserves the document metadata in the index, so that we can fully reconstruct the original dataset from the index.

For efficiency reasons, we don't build a single big SA for a single big file.
Instead, we keep the document text in $S$ shards, and build an SA for each shard separately.
This is because building a single SA for a big file needs the `merge()` and `concat()` steps, which cost a lot of disk I/O and thus a lot of time.
For dedup purposes, we don't need this -- we can do a "pseudo merge" when we find duplicated ptrs in the next step.

The script assumes the number of shards $S = C \times B_S$, where $C$ is `--cpus` and $B_S$ is `--num_batches`.
Choosing $B_S$: The main constraint is RAM. Let's say the size of your RAM is $M$, and the total size of your document text is $N$. You will want to set $B_S$ such that $B_S \times M > 16 \times N$.
Also, make sure your dataset has at least $S$ separate files.

The script will write the index at `/data/{INDEX_NAME}`, which contains $S$ subdirectories named `0` through `S-1`.
Subdirectory `s` is the infini-gram index for shard $s$ and contains several files: `tokenized`, `metadata`, `offset`, `metaoff`, and `table`.

### Find the ranges of bytes to be removed

In this step, we will find the ranges of bytes in the SA that should be removed.

Example command:
```
python find_remove_ranges.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} --mode parallel_sharded --num_threads 128 --low_ram --num_batches 2
```

`--minlen` is the minimum number of bytes in the substring to be considered for duplication.

This script first finds all ptrs in the index to be removed.
We say ptr should be removed if the substring with length `minlen` and starting at offset `ptr` is duplicated and is not the first appearance in the entire dataset.
After having all the remove ptrs, the script then merges them into remove ranges.
The script will write a binary file `dedup_minlen{MINLEN}/remove_ranges` under each shard's subdirectory, where each chunk of 16 bytes indicate a pair of uint64 integers -- the left (inclusive) and right (exclusive) endpoints of a range to be removed.

This process will create a huge number of ptrs, which means RAM is again the constraint.
To save RAM, we work in batches, and use the `--low_ram` flag so that we dump the remove ptrs in each batch to disk and eventually mmap them back.
Let's say the size of your RAM is $M$, the total size of your document text is $N$, and you estimate the removal rate is $p$.
You should choose the number of batches $B_P$ such that $N + 8 \times p \times N / B_P < M$, and you might want to leave ample buffer here because otherwise the RAM won't have any space for caching which significantly slows down the script.

Another consideration is the number of open files.
For each shard, the script will dump $P = C \times B_P$ temporary `remove_ptrs.{p}` files in `dedup_minlen{MINLEN}/` under the shard's subdirectory.
To mmap them all back, it needs to open $S \times P = C^2 \times B_S \times B_P$ files simultaneously.
The Linux system has a limit on this, typically it is 1048576, and you can check this limit with `ulimit -u`.

### Writeback the deduped dataset

Now we're ready to actually remove some ranges and convert the deduped dataset back to the compressed format.

Example command:
```
python write_back_to_jsonl_sharded.py \
    --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} \
    --output_dir /data/${NAME}_minlen${MINLEN} \
    --num_workers 128 --mode annotate
```

By default (`--mode remove`), the script will remove the byte ranges from the `text` field of each document.
If you specify `--mode annotate`, the script will keep the `text` field intact, and add a `sa_remove_ranges` indicating the byte ranges to be removed for this document.
`sa_remove_ranges` is a list of 2-tuples, where each 2-tuple indicates the starting (inclusive) and ending (exclusive) byte offset of a range that should be removed.
**WARNING: the ranges speak in UTF-8 byte offsets, NOT character offsets!!!**
Sometimes the ranges cuts in the middle of UTF-8 characters; in this case, we take care of this by shortening the remove ranges a little bit to align with UTF-8 character boundaries.

### Upload to S3

Again, use `s5cmd`.
For example:
```
s5cmd cp -sp /data/${NAME}_minlen${MINLEN}_annotated/ s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/final_ablation/minhash_10x_b_suffarr_minlen${MINLEN}_annotated/
```
