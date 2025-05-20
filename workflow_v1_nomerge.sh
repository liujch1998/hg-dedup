export NAME="deduplication_ablations_v1_minhash_suffarr_minhash_only"
export INDEX_NAME="v6_${NAME}_u8"
export MINLEN="200"
export AWS_MAX_CONCURRENCY=128

aws s3 sync s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/minhash_suffarr/minhash_only/ /data/${NAME}/ --only-show-errors

python indexing_v6_sharded.py \
    --data_dir /data/${NAME} \
    --save_dir /data/${INDEX_NAME} \
    --token_dtype u8 \
    --cpus 128 \
    --num_batches 8 \
    --add_metadata

python find_remove_ranges.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --mode parallel_sharded \
    --num_threads 128 \
    --low_ram \
    --num_batches 2

python write_back_to_jsonl_sharded.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --output_dir /data/${NAME}_minlen${MINLEN} \
    --num_workers 128

aws s3 sync /data/${NAME}_minlen${MINLEN} s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/minhash_suffarr/minhash_suffarr_minlen${MINLEN} --only-show-errors
