aws s3 cp --recursive s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/exact_dupaware/exact_dupaware_0.02_minhashed/ /data/exact_dupaware_0.02_minhashed

python -m infini_gram.indexing \
    --data_dir /data/exact_dupaware_0.02_minhashed \
    --save_dir /data/v4_exact_dupaware_0.02_minhashed_u8 \
    --token_dtype u8 \
    --cpus 128 --mem 2048 \
    --shards 1 --add_metadata \
    --ulimit 1048576