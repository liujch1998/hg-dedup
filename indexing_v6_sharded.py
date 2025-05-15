import argparse
from collections import defaultdict
import gc
import glob
import gzip
import json
import multiprocessing as mp
import numpy as np
import os
import resource
import shutil
import sys
import time
from tqdm import tqdm
import transformers
import zstandard as zstd
transformers.utils.logging.set_verbosity(40) # suppress warnings

tokenizer = None

def load_file(path):

    if path.endswith('.gz'):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            lines = f.readlines()
    elif path.endswith('.zst'):
        with open(path, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                decompressed_data = reader.read().decode('utf-8')
            lines = decompressed_data.split('\n')
            if lines[-1] == '':
                lines = lines[:-1]
    elif path.endswith('.jsonl'):
        with open(path, encoding='utf-8') as f:
            lines = f.readlines()
    else:
        raise ValueError(f'Unknown file type: {path}')
    return lines

def parse_line(args, line, rel_path, linenum):

    global tokenizer
    meta = json.loads(line.strip('\n'))
    if tokenizer is None:
        token_ids = meta['text'].encode('utf-8')
        if args.reversed:
            token_ids = token_ids[::-1].copy()
        data = token_ids
    else:
        token_ids = tokenizer.encode(meta['text'])
        if args.reversed:
            token_ids = token_ids[::-1].copy()
        data = np.array(token_ids, dtype=args.token_dtype).view(np.uint8).tobytes()
    del meta['text']
    data = args.doc_sep + data
    meta = (json.dumps({'path': rel_path, 'linenum': linenum, 'metadata': meta}) + '\n').encode('utf-8')
    return data, meta, token_ids

def prepare_fewfiles(args):

    ds_path = os.path.join(args.save_dir, f'tokenized')
    od_path = os.path.join(args.save_dir, f'offset')
    mt_path = os.path.join(args.save_dir, f'metadata')
    om_path = os.path.join(args.save_dir, f'metaoff')
    ug_path = os.path.join(args.save_dir, f'unigram')
    if all([os.path.exists(path) for path in [ds_path, od_path]]):
        print('Step 1 (prepare): Skipped. All files already exist.', flush=True)
        return

    print('Step 1 (prepare): Starting ...', flush=True)
    start_time = time.time()

    data_paths = list(sorted(glob.glob(f'{args.data_dir}/**/*.json*', recursive=True)))

    ds_fout = open(ds_path, 'wb')
    od_fout = open(od_path, 'wb')
    if args.add_metadata:
        mt_fout = open(mt_path, 'wb')
        om_fout = open(om_path, 'wb')
    if args.add_unigram:
        ug_fout = open(ug_path, 'w')
        unigram_counts = defaultdict(int)

    with mp.get_context('fork').Pool(args.cpus) as p:
        od = 0
        if args.add_metadata:
            om = 0
        for data_path in tqdm(data_paths):
            rel_path = data_path[len(args.data_dir)+1:]
            lines = load_file(data_path)
            for offset in range(0, len(lines), args.batch_size):
                batch_lines = lines[offset:min(offset+args.batch_size, len(lines))]
                results = p.starmap(parse_line, [(args, line, rel_path, offset+i) for i, line in enumerate(batch_lines)])
                for (data, meta, token_ids) in results:
                    ds_fout.write(data)
                    od_fout.write(np.array([od], dtype=np.uint64).view(np.uint8).tobytes())
                    od += len(data)
                    if args.add_metadata:
                        mt_fout.write(meta)
                        om_fout.write(np.array([om], dtype=np.uint64).view(np.uint8).tobytes())
                        om += len(meta)
                    if args.add_unigram:
                        for token_id in token_ids:
                            unigram_counts[token_id] += 1
                        unigram_counts[256**args.token_width-1] += 1
                del results
            del lines
            gc.collect()
    gc.collect()

    ds_fout.close()
    od_fout.close()
    if args.add_metadata:
        mt_fout.close()
        om_fout.close()
    if args.add_unigram:
        for token_id, count in sorted(unigram_counts.items()):
            ug_fout.write(f'{token_id} {count}\n')
        ug_fout.close()

    end_time = time.time()
    print(f'Step 1 (prepare): Done. Took {end_time-start_time:.2f} seconds', flush=True)

def prepare_manyfiles_map(args, s, paths):

    os.makedirs(f'{args.save_dir}/{s}', exist_ok=True)

    ds_fout = open(f'{args.save_dir}/{s}/tokenized', 'wb')
    od_fout = open(f'{args.save_dir}/{s}/offset', 'wb')
    if args.add_metadata:
        mt_fout = open(f'{args.save_dir}/{s}/metadata', 'wb')
        om_fout = open(f'{args.save_dir}/{s}/metaoff', 'wb')
    if args.add_unigram:
        ug_fout = open(f'{args.save_dir}/{s}/unigram', 'w')
        unigram_counts = defaultdict(int)
    od = 0
    if args.add_metadata:
        om = 0

    for path in paths:
        rel_path = path[len(args.data_dir)+1:]
        lines = load_file(path)

        for linenum, line in enumerate(lines):
            data, meta, token_ids = parse_line(args, line, rel_path, linenum)
            ds_fout.write(data)
            od_fout.write(np.array([od], dtype=np.uint64).view(np.uint8).tobytes())
            od += len(data)
            if args.add_metadata:
                mt_fout.write(meta)
                om_fout.write(np.array([om], dtype=np.uint64).view(np.uint8).tobytes())
                om += len(meta)
            if args.add_unigram:
                for token_id in token_ids:
                    unigram_counts[token_id] += 1
                unigram_counts[256**args.token_width-1] += 1

    ds_fout.close()
    od_fout.close()
    if args.add_metadata:
        mt_fout.close()
        om_fout.close()
    if args.add_unigram:
        for token_id, count in sorted(unigram_counts.items()):
            ug_fout.write(f'{token_id} {count}\n')
        ug_fout.close()

def prepare_manyfiles(args):

    print('Step 1 (prepare): Starting ...', flush=True)
    start_time = time.time()

    data_paths = list(sorted(glob.glob(f'{args.data_dir}/**/*.json*', recursive=True), key=lambda x: x.replace("crawl=", "")))
    num_shards = args.cpus * args.num_batches

    with mp.get_context('fork').Pool(args.cpus) as p:
        data_paths_by_shard = []
        for s in range(num_shards):
            b = len(data_paths) * s // num_shards
            e = len(data_paths) * (s+1) // num_shards
            data_paths_by_shard.append(data_paths[b:e])

        _ = p.starmap(prepare_manyfiles_map, [(args, s, data_paths) for (s, data_paths) in enumerate(data_paths_by_shard)])

    end_time = time.time()
    print(f'Step 1 (prepare): Done. Took {end_time-start_time:.2f} seconds', flush=True)

def prepare(args):

    ds_paths = [os.path.join(args.save_dir, f'{s}', 'tokenized') for s in range(args.num_batches * args.cpus)]
    od_paths = [os.path.join(args.save_dir, f'{s}', 'offset') for s in range(args.num_batches * args.cpus)]
    mt_paths = [os.path.join(args.save_dir, f'{s}', 'metadata') for s in range(args.num_batches * args.cpus)]
    om_paths = [os.path.join(args.save_dir, f'{s}', 'metaoff') for s in range(args.num_batches * args.cpus)]
    ug_paths = [os.path.join(args.save_dir, f'{s}', 'unigram') for s in range(args.num_batches * args.cpus)]
    if all([os.path.exists(path) for path in ds_paths + od_paths]):
        print('Step 1 (prepare): Skipped. All files already exist.', flush=True)
        return

    global tokenizer
    if args.tokenizer is None:
        tokenizer = None
    elif args.tokenizer == 'gpt2':
        tokenizer = transformers.AutoTokenizer.from_pretrained('gpt2', use_fast=False, add_bos_token=False, add_eos_token=False)
    elif args.tokenizer == 'llama':
        tokenizer = transformers.AutoTokenizer.from_pretrained('meta-llama/Llama-2-7b-hf', token=os.environ.get('HF_TOKEN'), use_fast=False, add_bos_token=False, add_eos_token=False) # The fast tokenizer seems unbearably slow ...
    elif args.tokenizer == 'olmo':
        tokenizer = transformers.AutoTokenizer.from_pretrained("allenai/OLMo-7B", add_bos_token=False, add_eos_token=False)
        # # The following is a faster version, but the result is a bit different
        # from dolma.tokenizer import Tokenizer
        # tokenizer = Tokenizer.from_pretrained('allenai/gpt-neox-olmo-dolma-v1_5', bos_token_id=None, eos_token_id=None, pad_token_id=1, segment_before_tokenization=True)
    else:
        raise ValueError(f'Unknown tokenizer: {args.tokenizer}')

    prepare_manyfiles(args)

def build_sa(args):

    sa_paths = [os.path.join(args.save_dir, f'{s}', 'table') for s in range(args.num_batches * args.cpus)]
    if all([os.path.exists(sa_path) for sa_path in sa_paths]):
        print('Step 2 (build_sa): Skipped. All files already exist.', flush=True)
        return

    print('Step 2 (build_sa): Starting ...', flush=True)
    start_time = time.time()

    for b in tqdm(list(range(args.num_batches))):
        shard_start = args.cpus * b
        shard_end = args.cpus * (b+1)
        pipes = []
        for s in range(shard_start, shard_end):
            ds_path = os.path.join(args.save_dir, f'{s}', 'tokenized')
            ds_size = os.path.getsize(ds_path)
            sa_dir = os.path.join(args.save_dir, f'{s}')
            ratio = int(np.ceil(np.log2(ds_size) / 8))
            pipes.append(os.popen(f'./rust_indexing make-part --data-file {ds_path} --parts-dir {sa_dir} --start-byte 0 --end-byte {ds_size} --ratio {ratio} --token-width {args.token_width}'))
        [pipe.read() for pipe in pipes]
        if any([pipe.close() is not None for pipe in pipes]):
            print('Step 2 (build_sa): Something went wrong', flush=True)
            exit(1)
        for s in range(shard_start, shard_end):
            ds_path = os.path.join(args.save_dir, f'{s}', 'tokenized')
            ds_size = os.path.getsize(ds_path)
            shutil.move(os.path.join(args.save_dir, f'{s}', f'0-{ds_size}'), os.path.join(args.save_dir, f'{s}', 'table'))

    end_time = time.time()
    print(f'Step 2 (build_sa): Done. Took {end_time-start_time:.2f} seconds', flush=True)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', type=str, required=True, help='Directory containing the raw text corpus. Must be absolute path.')
    parser.add_argument('--temp_dir', type=str, default=None, help='Directory where temporary indexing files are stored. Must be absolute path.')
    parser.add_argument('--save_dir', type=str, required=True, help='Directory where the final index files are stored. Must be absolute path.')
    parser.add_argument('--version', type=int, default=6, choices=[6], help='Version of the index.')
    parser.add_argument('--reversed', default=False, action='store_true', help='Whether to reverse the tokens in each document.')
    parser.add_argument('--tokenizer', type=str, default=None, choices=[None, 'gpt2', 'llama', 'olmo'])
    parser.add_argument('--token_dtype', type=str, default='u16', choices=['u8', 'u16', 'u32'], help='Data type for tokens.')
    parser.add_argument('--add_metadata', default=False, action='store_true', help='Whether to store document metadata in the index.')
    parser.add_argument('--add_unigram', default=False, action='store_true', help='Whether to precompute unigram counts.')
    parser.add_argument('--batch_size', type=int, default=65536, help='Batch size for tokenization.')
    parser.add_argument('--cpus', type=int, default=mp.cpu_count(), help='Number of CPU cores available to the program.')
    parser.add_argument('--num_batches', type=int, default=1, help='Number of batches to process the data.')
    parser.add_argument('--ulimit', type=int, default=1048576, help='Maximum number of open files allowed.')
    args = parser.parse_args()

    if args.temp_dir is None:
        args.temp_dir = args.save_dir
    args.data_dir = args.data_dir.rstrip('/')
    args.temp_dir = args.temp_dir.rstrip('/')
    args.save_dir = args.save_dir.rstrip('/')

    assert args.batch_size > 0
    assert args.cpus > 0

    if args.token_dtype == 'u8':
        args.token_width = 1
        args.doc_sep = b'\xff'
    elif args.token_dtype == 'u16':
        args.token_width = 2
        args.doc_sep = b'\xff\xff'
    elif args.token_dtype == 'u32':
        args.token_width = 4
        args.doc_sep = b'\xff\xff\xff\xff'
    else:
        raise ValueError(f'Unknown token_dtype: {args.token_dtype}')

    assert os.path.exists(args.data_dir)
    os.makedirs(args.temp_dir, exist_ok=True)
    os.makedirs(args.save_dir, exist_ok=True)

    assert sys.byteorder == 'little'
    resource.setrlimit(resource.RLIMIT_NOFILE, (args.ulimit, args.ulimit))

    prepare(args)
    build_sa(args)

if __name__ == '__main__':
    main()