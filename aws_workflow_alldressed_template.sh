INSTANCE_TYPE=[[INSTANCE_TYPE]]
NUM_SHARDS=[[NUM_SHARDS]]
NUM_NODES=[[NUM_NODES]]
RANK=[[RANK]]

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
    # Dynamically find the first 8 devices with size 3.4T
    DEVICES=($(lsblk -o NAME,SIZE -dn | awk '$2 == "3.4T" {print $1}' | head -8))
    if [ ${#DEVICES[@]} -lt 8 ]; then
        echo "Error: Less than 8 NVMe devices with size 3.4T found."
        exit 1
    fi
    for i in ${!DEVICES[@]}; do
        DEVICE="/dev/${DEVICES[$i]}"
        if [ $i -eq 0 ]; then
            MOUNT_POINT="/data"
        else
            MOUNT_POINT="/data-$i"
        fi
        sudo mkfs -t ext4 $DEVICE
        sudo mkdir -p $MOUNT_POINT
        sudo mount $DEVICE $MOUNT_POINT
        sudo chown $USER:$USER $MOUNT_POINT
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

s5cmd cp -sp s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/minhash_10shard_v3/download_scripts_sa/* /data/download_scripts/

# Run workflow
for ((shard=$RANK; shard<$NUM_SHARDS; shard+=$NUM_NODES)); do

    echo "Run workflow for shard $shard: Starting ..."
    export NAME="shard_$(printf "%04d" $shard)"
    export INDEX_NAME="v6_${NAME}_u8"
    export MINLEN="500"

    echo "Download data: Starting ..."
    time s5cmd run /data/download_scripts/downloader_${NAME}.txt >/dev/null 2>&1
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
    time s5cmd cp -sp /data/${NAME}_minlen${MINLEN}_annotated/ s3://ai2-llm/pretraining-data/sources/cc_all_dressed/all_dressed_v2/sa_minlen${MINLEN}_annotated/${NAME}/
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
    echo "Run workflow for shard $shard: Done"
    echo "================================================"

done
