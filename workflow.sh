aws s3 cp --recursive s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/exact_dupaware/exact_dupaware_0.02_minhashed/ /data/exact_dupaware_0.02_minhashed

python -m infini_gram.indexing \
    --data_dir /data/exact_dupaware_0.02_minhashed \
    --save_dir /data/v4_exact_dupaware_0.02_minhashed_u8 \
    --token_dtype u8 \
    --cpus 128 --mem 2048 \
    --shards 1 --add_metadata \
    --ulimit 1048576

g++ -O3 -std=c++20 -pthread find_remove_ranges.cpp && ./a.out /data/v4_exact_dupaware_0.02_minhashed_u8 500 128 /data/v4_exact_dupaware_0.02_minhashed_u8/remove_ranges_minlen500 1 2

python write_back_to_jsonl.py --index_dir /data/v4_exact_dupaware_0.02_minhashed_u8 --remove_ranges_path /data/v4_exact_dupaware_0.02_minhashed_u8/remove_ranges_minlen500/remove_ranges --output_dir /data/exact_dupaware_0.02_minhashed_minlen500

aws s3 cp --recursive /data/exact_dupaware_0.02_minhashed_minlen500 s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/exact_dupaware/exact_dupaware_0.02_minhashed_substr-minlen500
