python -m infini_gram.indexing \
    --data_dir /data/jiachengl/hg-dedup/data/toy \
    --save_dir /data/jiachengl/hg-dedup/index/v4_toy_u8 \
    --token_dtype u8 \
    --cpus 1 --mem 2048 \
    --add_metadata \
    --ulimit 524288

python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v4_toy_u8 \
    --minlen 32 \
    --num_threads 1 \
    --low_ram \
    --num_batches 1

python write_back_to_jsonl.py \
    --index_dir /data/jiachengl/hg-dedup/index/v4_toy_u8 \
    --minlen 32 \
    --output_dir /data/jiachengl/hg-dedup/data/toy_minlen32 \
    --num_workers 1
