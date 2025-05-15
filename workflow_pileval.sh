python -m infini_gram.indexing_v6 \
    --data_dir /data/jiachengl/hg-dedup/data/pileval-c1024 \
    --save_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --token_dtype u8 \
    --cpus 128 --mem 2048 \
    --add_metadata \
    --ulimit 524288
python indexing_v6_sharded.py \
    --data_dir /data/jiachengl/hg-dedup/data/pileval-c1024 \
    --save_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024-s1024_u8 \
    --token_dtype u8 \
    --cpus 128 \
    --num_batches 8 \
    --add_metadata \
    --ulimit 524288

python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --minlen 1000 \
    --mode naive
python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --minlen 1000 \
    --mode parallel \
    --num_threads 32 \
    --low_ram \
    --num_batches 2
python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024-s1024_u8 \
    --minlen 1000 \
    --mode parallel_sharded \
    --num_threads 32 \
    --low_ram \
    --num_batches 2

python write_back_to_jsonl.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --minlen 1000 \
    --output_dir /data/jiachengl/hg-dedup/data/pileval-c1024_minlen1000 \
    --num_workers 16
python write_back_to_jsonl_sharded.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024-s1024_u8 \
    --minlen 1000 \
    --output_dir /data/jiachengl/hg-dedup/data/pileval-c1024_minlen1000 \
    --num_workers 16
