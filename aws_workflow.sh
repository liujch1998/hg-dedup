INSTANCE_TYPE="i7i"
if [ "$INSTANCE_TYPE" != "x2idn" ] && [ "$INSTANCE_TYPE" != "i4i" ] && [ "$INSTANCE_TYPE" != "i7i" ]; then
    echo "Invalid instance type: $INSTANCE_TYPE"
    exit 1
fi

# Mount volumes
echo "Mount volumes: Starting ..."
if [ "$INSTANCE_TYPE" == "x2idn" ]; then
    DEVICE=/dev/$(lsblk -o NAME,SIZE | grep -E 'nvme[0-9]+n1' | awk '$2 == "16T" {print $1}')
    sudo mkfs -t ext4 $DEVICE
    sudo mkdir /data
    sudo mount $DEVICE /data
    sudo chown $USER:$USER /data
elif [ "$INSTANCE_TYPE" == "i4i" ] || [ "$INSTANCE_TYPE" == "i7i" ]; then
    DEVICE=/dev/nvme8n1
    sudo mkfs -t ext4 $DEVICE
    sudo mkdir /data
    sudo mount $DEVICE /data
    sudo chown $USER:$USER /data
    for i in {1..7}; do
        DEVICE=/dev/nvme${i}n1
        sudo mkfs -t ext4 $DEVICE
        sudo mkdir /data-${i}
        sudo mount $DEVICE /data-${i}
        sudo chown $USER:$USER /data-${i}
    done
fi
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
wget https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
tar -xvzf s5cmd_2.2.2_Linux-64bit.tar.gz
sudo mv s5cmd /usr/local/bin
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
export NAME="deduplication_ablations_v1_final_ablation_minhash_10x"
export INDEX_NAME="v6_${NAME}_u8"
export MINLEN="500"

echo "Download data: Starting ..."
time s5cmd cp -sp s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/final_ablation/minhash_10x/* /data/${NAME}/
# keep 500GB
# rm /data/${NAME}/**/shuffled_shard_0001*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000092*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000093*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000094*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000095*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000096*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000097*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000098*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000099*.jsonl.zst
# keep 400GB
# rm /data/${NAME}/**/shuffled_shard_0001*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_00009*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_00008*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000079*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000078*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000077*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000076*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000075*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_000074*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_0000739*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_0000738*.jsonl.zst
# rm /data/${NAME}/**/shuffled_shard_0000737*.jsonl.zst
echo "Download data: Done"
echo "------------------------------------------------"

echo "Indexing: Starting ..."
if [ "$INSTANCE_TYPE" == "x2idn" ]; then
    time python indexing_v6_sharded.py --data_dir /data/${NAME} --save_dir /data/${INDEX_NAME} --token_dtype u8 --cpus 128 --num_batches 16 --add_metadata
elif [ "$INSTANCE_TYPE" == "i4i" ]; then
    time python indexing_v6_sharded.py --data_dir /data/${NAME} --save_dir /data/${INDEX_NAME} --token_dtype u8 --cpus 128 --num_batches 16 --add_metadata --num_volumes 8
elif [ "$INSTANCE_TYPE" == "i7i" ]; then
    time python indexing_v6_sharded.py --data_dir /data/${NAME} --save_dir /data/${INDEX_NAME} --token_dtype u8 --cpus 192 --num_batches 16 --add_metadata --num_volumes 8
fi
echo "Indexing: Done"
echo "------------------------------------------------"

echo "Find remove ranges: Starting ..."
if [ "$INSTANCE_TYPE" == "x2idn" ]; then
    time python find_remove_ranges.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --mode parallel_sharded --num_threads 128 --low_ram --num_batches 16
elif [ "$INSTANCE_TYPE" == "i4i" ]; then
    time python find_remove_ranges.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --mode parallel_sharded --num_threads 128 --low_ram --num_batches 16
elif [ "$INSTANCE_TYPE" == "i7i" ]; then
    time python find_remove_ranges.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --mode parallel_sharded --num_threads 192 --low_ram --num_batches 16
fi
echo "Find remove ranges: Done"
echo "------------------------------------------------"

echo "Write back to jsonl: Starting ..."
if [ "$INSTANCE_TYPE" == "x2idn" ]; then
    time python write_back_to_jsonl_sharded.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --output_dir /data/${NAME}_minlen${MINLEN} --num_workers 128 --mode annotate
elif [ "$INSTANCE_TYPE" == "i4i" ]; then
    time python write_back_to_jsonl_sharded.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --output_dir /data/${NAME}_minlen${MINLEN} --num_workers 128 --mode annotate
elif [ "$INSTANCE_TYPE" == "i7i" ]; then
    time python write_back_to_jsonl_sharded.py --index_dir /data/${INDEX_NAME} --minlen ${MINLEN} --output_dir /data/${NAME}_minlen${MINLEN} --num_workers 192 --mode annotate
fi
echo "Write back to jsonl: Done"
echo "------------------------------------------------"

echo "Upload data: Starting ..."
time s5cmd cp -sp /data/${NAME}_minlen${MINLEN}_annotated/ s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_subsamples/deduplication_ablations_v1/final_ablation/minhash_10x_suffarr_minlen${MINLEN}_annotated/
echo "Upload data: Done"
echo "------------------------------------------------"

rm -r /data/${NAME}
rm -r /data/${INDEX_NAME}
if [ "$INSTANCE_TYPE" == "i4i" ] || [ "$INSTANCE_TYPE" == "i7i" ]; then
    for i in {1..7}; do
        rm -r /data-${i}/${INDEX_NAME}
    done
fi
rm -r /data/${NAME}_minlen${MINLEN}_annotated
echo "Run workflow: Done"
echo "================================================"
