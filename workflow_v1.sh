export NAME="deduplication_ablations_v1_minhash_suffarr_minhash_only"
export INDEX_NAME="v4_${NAME}_u8"
export MINLEN="200"

aws s3 cp --recursive s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/minhash_suffarr/minhash_only/ /data/${NAME}/

python -m infini_gram.indexing_v6 \
    --data_dir /data/${NAME} \
    --save_dir /data/${INDEX_NAME} \
    --token_dtype u8 \
    --cpus 128 --mem 2048 \
    --add_metadata \
    --hack 1000

# # move tokenized and metadata to local disk
# mkdir /data-local0/${INDEX_NAME}
# cp /data/${INDEX_NAME}/tokenized.0 /data-local0/${INDEX_NAME}/
# mv /data/${INDEX_NAME}/tokenized.0 /data/${INDEX_NAME}/backup.tokenized.0
# ln -s /data-local0/${INDEX_NAME}/tokenized.0 /data/${INDEX_NAME}/tokenized.0
# cp /data/${INDEX_NAME}/metadata.0 /data-local0/${INDEX_NAME}/
# mv /data/${INDEX_NAME}/metadata.0 /data/${INDEX_NAME}/backup.metadata.0
# ln -s /data-local0/${INDEX_NAME}/metadata.0 /data/${INDEX_NAME}/metadata.0

python find_remove_ranges.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --num_threads 128 \
    --low_ram \
    --num_batches 2

time python write_back_to_jsonl.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --output_dir /data/${NAME}_minlen${MINLEN} \
    --num_workers 128

aws s3 cp --recursive /data/${NAME}_minlen${MINLEN} s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/minhash_suffarr/minhash_suffarr_minlen${MINLEN}
