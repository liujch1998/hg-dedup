# Mount volumes
echo "Mount volumes: Starting ..."
DEVICE=/dev/$(lsblk -o NAME,SIZE | grep -E 'nvme[0-9]+n1' | awk '$2 == "16T" {print $1}')
sudo mkfs -t ext4 $DEVICE
sudo mkdir /data
sudo mount $DEVICE /data
sudo chown $USER:$USER /data
echo "Mount volumes: Done"
echo "================================================"

# Clone repo
echo "Clone repo: Starting ..."
git clone https://github.com/liujch1998/hg-dedup.git
cd hg-dedup
echo "Clone repo: Done"
echo "================================================"

# Install conda
echo "Install conda: Starting ..."
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh --quiet
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
# ~/miniconda3/bin/conda init bash
# source /home/ubuntu/.bashrc
eval "$(~/miniconda3/bin/conda 'shell.bash' 'hook' 2> /dev/null)"
conda env create -f environment.yml
conda activate hg-dedup
pip install transformers awscli
echo "Install conda: Done"
echo "================================================"

# Compile things
echo "Compile things: Starting ..."
cp ~/miniconda3/envs/hg-dedup/lib/python3.12/site-packages/infini_gram/rust_indexing .
c++ -std=c++20 -O3 -shared -fPIC $(python3 -m pybind11 --includes) cpp_engine_dedup.cpp -o cpp_engine_dedup$(python3-config --extension-suffix)
echo "Compile things: Done"
echo "================================================"

# Run workflow
echo "Run workflow: Starting ..."
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
    --add_metadata --ulimit 524288
python find_remove_ranges.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --mode parallel_sharded \
    --num_threads 128 \
    --low_ram \
    --num_batches 2 --ulimit 524288
python write_back_to_jsonl_sharded.py \
    --index_dir /data/${INDEX_NAME} \
    --minlen ${MINLEN} \
    --output_dir /data/${NAME}_minlen${MINLEN} \
    --num_workers 128
aws s3 sync /data/${NAME}_minlen${MINLEN} s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/minhash_suffarr/minhash_suffarr_minlen${MINLEN} --only-show-errors
rm -r /data/${NAME}
rm -r /data/${INDEX_NAME}
rm -r /data/${NAME}_minlen${MINLEN}
echo "Run workflow: Done"
echo "================================================"
