python -m infini_gram.indexing \
    --data_dir /data/jiachengl/hg-dedup/data/pileval \
    --save_dir /data/jiachengl/hg-dedup/index/v6_pileval_u8 \
    --token_dtype u8 \
    --cpus 128 --mem 2048 \
    --add_metadata \
    --hack 1000
python -m indexing_v6_sharded.py \
    --data_dir /data/jiachengl/hg-dedup/data/pileval-c1024 \
    --save_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --token_dtype u8 \
    --cpus 128 \
    --add_metadata \
    --hack 1000

python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval_u8 \
    --minlen 1000 \
    --mode naive
python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval_u8 \
    --minlen 1000 \
    --mode parallel \
    --num_threads 32 \
    --low_ram \
    --num_batches 2
python find_remove_ranges.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval-c1024_u8 \
    --minlen 1000 \
    --mode parallel_sharded \
    --num_threads 32 \
    --low_ram \
    --num_batches 2

time python write_back_to_jsonl.py \
    --index_dir /data/jiachengl/hg-dedup/index/v6_pileval_u8 \
    --minlen 1000 \
    --output_dir /data/jiachengl/hg-dedup/data/pileval_minlen1000 \
    --num_workers 16
