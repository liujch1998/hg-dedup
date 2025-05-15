#include <cassert>
#include <cstdint> // for uint64_t
#include <cstdio>
#include <cstring> // for memcpy
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <filesystem>
#include <sys/mman.h> // for mmap, munmap
#include <sys/stat.h> // for struct stat
#include <fcntl.h> // for O_RDONLY
#include <unistd.h> // for close
#include <algorithm>
#include <random>
#include <thread>
#include <fstream>
#include <deque>
#include <queue>

#define U64 uint64_t
#define U32 uint32_t
#define U16 uint16_t
#define U8 uint8_t
#define PSS pair<size_t, size_t>

using namespace std;
namespace fs = std::filesystem;

void assert_little_endian() {
    unsigned int i = 1;
    char *c = (char*)&i;
    assert (*c);
}
const auto PAGESIZE = sysconf(_SC_PAGESIZE);

struct DatastoreShard {
    U8* ds;
    U8* sa;
    U64 tok_cnt;
    U64 ds_size;
    U8 ptr_size;
    U8* od;
    U64 doc_cnt;
    U8* mt;
    U64 mt_size;
    U8* om;
};
struct DupPtr {
    U64 ptr;
    bool dropped;
};
struct DupDoc {
    U64 doc_ix;
    U64 start_ptr;
    vector<DupPtr> dup_ptrs;
    vector<U8> text;
};
template<typename T=U8>
struct DocResult {
    U64 doc_ix;
    U64 doc_start_ptr;
    U64 doc_end_ptr;
    U64 doc_len;
    string metadata;
    vector<T> token_ids;
};

template<typename T=U8>
class EngineDedup {

public:

    EngineDedup(
        const vector<string> index_dirs, const bool load_metadata)
        : _doc_sep_id((T)(-1)), _doc_sep(vector<U8>(sizeof(T), 0xff)), _index_dirs(index_dirs)
    {

        assert_little_endian();

        for (const auto &index_dir : index_dirs) {
            assert (fs::exists(index_dir));

            auto [ds, ds_size] = load_file(index_dir + "/tokenized", true);
            auto [sa, sa_size] = load_file(index_dir + "/table", false);
            auto [od, od_size] = load_file(index_dir + "/offset", true);

            assert (ds_size % sizeof(T) == 0);
            U64 tok_cnt = ds_size / sizeof(T);
            assert (sa_size % tok_cnt == 0);
            U8 ptr_size = (U8)(sa_size / tok_cnt);
            assert (od_size % sizeof(U64) == 0);
            U64 doc_cnt = od_size / sizeof(U64);

            if (!load_metadata) {
                auto shard = DatastoreShard{ds, sa, tok_cnt, ds_size, ptr_size, od, doc_cnt};
                _shards.push_back(shard);
            } else {
                auto [mt, mt_size] = load_file(index_dir + "/metadata", true);
                auto [om, om_size] = load_file(index_dir + "/metaoff", true);

                assert (om_size == doc_cnt * sizeof(U64));

                auto shard = DatastoreShard{ds, sa, tok_cnt, ds_size, ptr_size, od, doc_cnt, mt, mt_size, om};
                _shards.push_back(shard);
            }
        }

        _num_shards = _shards.size();
    }

    ~EngineDedup() {
        for (auto &shard : _shards) {
            unload_file(shard.ds, shard.ds_size, true);
            unload_file(shard.sa, shard.tok_cnt * shard.ptr_size, false);
            unload_file(shard.od, shard.doc_cnt * sizeof(U64), true);
            if (shard.mt) {
                unload_file(shard.mt, shard.mt_size, true);
                unload_file(shard.om, shard.doc_cnt * sizeof(U64), true);
            }
        }
    }

    pair<U8*, U64> load_file(const string &path, const bool load_to_ram) const {
        if (load_to_ram) {
            /*
            ifstream f(path, ios::binary);
            assert (f.is_open());
            char* buffer = new char[1024 * 1024 * 1024];
            f.rdbuf()->pubsetbuf(buffer, 1024 * 1024 * 1024);
            f.seekg(0, ios::end);
            U64 size = f.tellg();
            f.seekg(0, ios::beg);
            U8 *buf = new U8[size];
            f.read(reinterpret_cast<char*>(buf), size);
            f.close();
            return {buf, size};
            */
            /*
            FILE *f = std::fopen(path.c_str(), "rb");
            setvbuf(f, nullptr, _IOFBF, 1024 * 1024 * 1024);
            std::fseek(f, 0, SEEK_END);
            size_t size = std::ftell(f);
            std::rewind(f);
            U8 *buf = new U8[size];
            std::fread((char*)buf, 1, size, f);
            std::fclose(f);
            return {buf, size};
            */
            /*
            int fd = open(path.c_str(), O_RDONLY); // | O_DIRECT);
            assert (fd != -1);
            struct stat st;
            fstat(fd, &st);
            size_t size = st.st_size;
            U8 *buf = new U8[size];
            size_t total_bytes_read = 0;
            while (total_bytes_read < size) {
                ssize_t r = read(fd, (char*)buf + total_bytes_read, size - total_bytes_read);
                assert (r > 0);
                total_bytes_read += r;
            }
            close(fd);
            return {buf, size};
            */
            int fd = open(path.c_str(), O_RDONLY);
            assert (fd != -1);
            struct stat st;
            fstat(fd, &st);
            size_t size = st.st_size;
            U8 *buf = new U8[size];
            size_t num_threads = 16;
            size_t chunk_size = (size + num_threads - 1) / num_threads;
            vector<thread> threads;
            for (size_t i = 0; i < num_threads; i++) {
                threads.emplace_back([&, i]() {
                    size_t offset = i * chunk_size;
                    size_t to_read = min(chunk_size, size - offset);
                    while (to_read > 0) {
                        ssize_t r = pread(fd, (char*)buf + offset, to_read, offset);
                        assert (r > 0);
                        offset += r;
                        to_read -= r;
                    }
                });
            }
            for (auto &t : threads) t.join();
            close(fd);
            return {buf, size};
        } else {
            int f = open(path.c_str(), O_RDONLY);
            assert (f != -1);
            struct stat s;
            auto fstat_ret = fstat(f, &s);
            assert (fstat_ret != -1);
            if (s.st_size == 0) {
                close(f);
                return {nullptr, 0};
            }
            U8 *ptr = static_cast<U8*>(mmap(NULL, s.st_size, PROT_READ, MAP_PRIVATE, f, 0));
            assert (ptr != MAP_FAILED);
            // madvise(ptr, s.st_size, MADV_SEQUENTIAL);
            close(f);
            return {ptr, s.st_size};
        }
    }

    void unload_file(U8* ptr, U64 size, const bool load_to_ram) const {
        if (load_to_ram) {
            delete[] ptr;
        } else {
            munmap(ptr, size);
        }
    }

    void find_remove_ranges(const size_t min_len) const {

        const auto &shard = _shards[0];
        string output_dir = _index_dirs[0] + "/dedup_minlen" + to_string(min_len);
        if (!fs::exists(output_dir)) {
            fs::create_directory(output_dir);
        }
        vector<thread> threads;

        cout << "Finding remove_ptrs ..." << endl;
        auto start_time = chrono::high_resolution_clock::now();
        vector<U64> remove_ptrs;
        vector<U64> ptrs{_convert_rank_to_ptr(shard, 0)};
        for (U64 rank = 1; rank < shard.tok_cnt; rank++) {
            // if rank-1 and rank share prefix of length min_len, keep moving
            U64 ptr1 = ptrs.back();
            U64 ptr2 = _convert_rank_to_ptr(shard, rank);
            if (ptr1 + min_len <= shard.ds_size &&
                ptr2 + min_len <= shard.ds_size &&
                memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len) == 0 &&
                find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len) {
                ptrs.push_back(ptr2);
                continue;
            }
            if (ptrs.size() > 1) { // the buffer has more than one element, keep the smallest one and remove the rest
                U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
                for (auto ptr : ptrs) {
                    if (ptr != smallest_ptr) {
                        remove_ptrs.push_back(ptr);
                    }
                }
            }
            ptrs.clear();
            ptrs.push_back(ptr2);
        }
        if (ptrs.size() > 1) { // process the remaining buffer
            U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
            for (auto ptr : ptrs) {
                if (ptr != smallest_ptr) {
                    remove_ptrs.push_back(ptr);
                }
            }
        }
        sort(remove_ptrs.begin(), remove_ptrs.end());
        auto remove_ptrs_size = remove_ptrs.size();
        cout << "total_remove_ptrs: " << remove_ptrs_size << endl;
        auto end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;

        // // check the distance between remove_ptrs
        // map<size_t, bool> exists;
        // for (size_t i = 0; i + 1 < remove_ptrs.size(); i++) {
        //     size_t dist = remove_ptrs[i+1] - remove_ptrs[i];
        //     exists[dist] = true;
        // }
        // for (const auto &[dist, exists] : exists) {
        //     cout << dist << " ";
        // }
        // cout << endl;

        // // check that no remove_ptr's minlen-prefix contains a FF byte
        // for (size_t i = 0; i < remove_ptrs_size; i++) {
        //     U64 ptr = remove_ptrs[i];
        //     assert (ptr + min_len <= shard.ds_size);
        //     assert (find(shard.ds + ptr, shard.ds + ptr + min_len, (U8)-1) == shard.ds + ptr + min_len);
        // }

        cout << "Merging remove_ptrs into remove_ranges ..." << endl;
        start_time = chrono::high_resolution_clock::now();
        vector<PSS> remove_ranges;
        for (size_t i = 0; i < remove_ptrs_size; i++) {
            U64 ptr = remove_ptrs[i];
            if (remove_ranges.size() > 0 && remove_ranges.back().second >= ptr) {
                remove_ranges.back().second = ptr + min_len;
            } else {
                remove_ranges.push_back({ptr, ptr + min_len});
            }
        }
        U64 total_remove_bytes = accumulate(remove_ranges.begin(), remove_ranges.end(), U64(0), [](U64 a, const PSS &b) { return a + b.second - b.first; });
        string filename = _index_dirs[0] + "/dedup_minlen" + to_string(min_len) + "/remove_ranges";
        ofstream fout(filename, ios::binary);
        fout.write(reinterpret_cast<const char*>(remove_ranges.data()), remove_ranges.size() * sizeof(PSS));
        fout.close();
        cout << "total_remove_ranges: " << remove_ranges.size() << endl;
        cout << "total_remove_bytes: " << total_remove_bytes << endl;
        end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;
    }

    void find_remove_ranges_parallel(const size_t min_len, const size_t num_threads, const bool low_ram, const size_t num_batches) const {

        const auto &shard = _shards[0];
        string output_dir = _index_dirs[0] + "/dedup_minlen" + to_string(min_len);
        if (!fs::exists(output_dir)) {
            fs::create_directory(output_dir);
        }
        vector<thread> threads;

        // This function: do not write parts to disk. Requires more RAM, less disk swap space, and faster
        // To save some RAM: write parts to disk and mmap back. Requires less RAM, more disk swap space, and slower
        // To save more RAM: mmap the text file. Requires less RAM, and slower

        cout << "Finding remove_ptrs in different parts ..." << endl;
        auto start_time = chrono::high_resolution_clock::now();
        size_t num_parts = num_batches * num_threads;
        vector<vector<U64>> remove_ptrs_by_part(num_parts);
        U64 start_rank = 0;
        for (size_t p = 0; p < num_parts; p++) {
            U64 end_rank = shard.tok_cnt * (p + 1) / num_parts;
            // move forward end_rank until end_rank-1 and end_rank do not share prefix of length min_len
            while (true) {
                if (end_rank >= shard.tok_cnt) {
                    break;
                }
                U64 ptr1 = _convert_rank_to_ptr(shard, end_rank - 1);
                U64 ptr2 = _convert_rank_to_ptr(shard, end_rank);
                if (!(ptr1 + min_len <= shard.ds_size &&
                      ptr2 + min_len <= shard.ds_size &&
                      memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len) == 0 &&
                      find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len)) {
                    break;
                }
                end_rank++;
            }
            threads.emplace_back(&EngineDedup::find_remove_ptrs_part, this, min_len, p, start_rank, end_rank, low_ram ? nullptr : &remove_ptrs_by_part[p]);
            start_rank = end_rank;
            if ((p + 1) % num_threads == 0) {
                cout << "Batch " << p / num_threads << " starting ..." << endl;
                for (auto &thread : threads) {
                    thread.join();
                }
                threads.clear();
                cout << "Batch " << p / num_threads << " done" << endl;
            }
        }
        vector<pair<U64*, U64>> raw_remove_ptrs_by_part(num_parts); // {ptr, size in u64}
        for (size_t p = 0; p < num_parts; p++) {
            if (low_ram) {
                auto [ptr, size] = load_file(_index_dirs[0] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p), false);
                assert (size % sizeof(U64) == 0);
                raw_remove_ptrs_by_part[p] = {reinterpret_cast<U64*>(ptr), size / sizeof(U64)};
            } else {
                raw_remove_ptrs_by_part[p] = {remove_ptrs_by_part[p].data(), remove_ptrs_by_part[p].size()};
            }
        }
        size_t total_remove_ptrs = accumulate(raw_remove_ptrs_by_part.begin(), raw_remove_ptrs_by_part.end(), size_t(0), [](size_t a, const pair<U64*, U64> &b) { return a + b.second; });
        cout << "total_remove_ptrs: " << total_remove_ptrs << endl;
        auto end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;

        cout << "Merging remove_ptrs into remove_ranges ..." << endl;
        start_time = chrono::high_resolution_clock::now();
        size_t num_workers = num_threads;
        vector<vector<size_t>> start_by_part_by_worker(num_workers, vector<size_t>(num_parts));
        for (size_t w = 0; w < num_workers; w++) {
            start_by_part_by_worker[w][0] = raw_remove_ptrs_by_part[0].second * w / num_workers;
            U64 anchor = raw_remove_ptrs_by_part[0].first[start_by_part_by_worker[w][0]];
            for (size_t p = 1; p < num_parts; p++) {
                start_by_part_by_worker[w][p] = (w == 0) ? 0 : std::lower_bound(raw_remove_ptrs_by_part[p].first, raw_remove_ptrs_by_part[p].first + raw_remove_ptrs_by_part[p].second, anchor) - raw_remove_ptrs_by_part[p].first;
            }
        }
        vector<vector<PSS>> start_end_by_part_by_worker(num_workers, vector<PSS>(num_parts));
        for (size_t w = 0; w < num_workers; w++) {
            for (size_t p = 0; p < num_parts; p++) {
                start_end_by_part_by_worker[w][p].first = start_by_part_by_worker[w][p];
                start_end_by_part_by_worker[w][p].second = (w == num_workers - 1) ? raw_remove_ptrs_by_part[p].second : start_by_part_by_worker[w+1][p];
            }
        }
        vector<vector<PSS>> remove_ranges_by_worker(num_workers);
        for (size_t w = 0; w < num_workers; w++) {
            threads.emplace_back(&EngineDedup::merge_ptrs_into_ranges_worker, this, min_len, &raw_remove_ptrs_by_part, &start_end_by_part_by_worker[w], &remove_ranges_by_worker[w]);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        if (low_ram) {
            for (size_t p = 0; p < num_parts; p++) {
                munmap(reinterpret_cast<U8*>(raw_remove_ptrs_by_part[p].first), raw_remove_ptrs_by_part[p].second * sizeof(U64));
                fs::remove(_index_dirs[0] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p));
            }
        }
        vector<PSS> remove_ranges;
        for (size_t w = 0; w < num_workers; w++) {
            for (size_t i = 0; i < remove_ranges_by_worker[w].size(); i++) {
                if (i == 0 && remove_ranges.size() > 0 && remove_ranges.back().second >= remove_ranges_by_worker[w][i].first) { // should merge these ranges
                    remove_ranges.back().second = remove_ranges_by_worker[w][i].second;
                } else {
                    remove_ranges.push_back(remove_ranges_by_worker[w][i]);
                }
            }
        }
        U64 total_remove_bytes = accumulate(remove_ranges.begin(), remove_ranges.end(), U64(0), [](U64 a, const PSS &b) { return a + b.second - b.first; });
        string filename = _index_dirs[0] + "/dedup_minlen" + to_string(min_len) + "/remove_ranges";
        ofstream fout(filename, ios::binary);
        fout.write(reinterpret_cast<const char*>(remove_ranges.data()), remove_ranges.size() * sizeof(PSS));
        fout.close();
        cout << "total_remove_ranges: " << remove_ranges.size() << endl;
        cout << "total_remove_bytes: " << total_remove_bytes << endl;
        end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;
    }

    void find_remove_ptrs_part(const size_t min_len, const size_t p, const U64 start_rank, const U64 end_rank, vector<U64>* const out_remove_ptrs) const {

        const auto &shard = _shards[0];

        vector<U64> remove_ptrs;
        vector<U64> ptrs{_convert_rank_to_ptr(shard, start_rank)};
        for (U64 rank = start_rank + 1; rank < end_rank; rank++) {
            if (p == 0 && rank % 100000000 == 0) {
                cout << "Part 0 processing rank " << rank << " / " << end_rank << ", remove_ptrs.size(): " << remove_ptrs.size() << endl;
            }
            // if rank-1 and rank share prefix of length min_len, keep moving
            U64 ptr1 = ptrs.back();
            U64 ptr2 = _convert_rank_to_ptr(shard, rank);
            if (ptr1 + min_len <= shard.ds_size &&
                ptr2 + min_len <= shard.ds_size &&
                memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len) == 0 &&
                find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len) {
                ptrs.push_back(ptr2);
                continue;
            }
            if (ptrs.size() > 1) { // the buffer has more than one element, keep the smallest one and remove the rest
                U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
                for (auto ptr : ptrs) {
                    if (ptr != smallest_ptr) {
                        remove_ptrs.push_back(ptr);
                    }
                }
            }
            ptrs.clear();
            ptrs.push_back(ptr2);
        }
        if (ptrs.size() > 1) { // process the remaining buffer
            U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
            for (auto ptr : ptrs) {
                if (ptr != smallest_ptr) {
                    remove_ptrs.push_back(ptr);
                }
            }
        }

        sort(remove_ptrs.begin(), remove_ptrs.end());
        auto remove_ptrs_size = remove_ptrs.size();

        if (out_remove_ptrs) { // return remove_ptrs to the caller
            *out_remove_ptrs = move(remove_ptrs);
        } else { // write remove_ptrs to a binary file
            string filename = _index_dirs[0] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p);
            ofstream fout(filename, ios::binary);
            fout.write(reinterpret_cast<const char*>(remove_ptrs.data()), remove_ptrs.size() * sizeof(U64));
            fout.close();
        }

        if (p == 0) {
            cout << "Part 0 done, remove_ptrs.size(): " << remove_ptrs_size << endl;
        }
    }

    void merge_ptrs_into_ranges_worker(const size_t min_len, const vector<pair<U64*, U64>>* const raw_remove_ptrs_by_part, const vector<PSS>* const start_end_by_part, vector<PSS>* const remove_ranges) const {

        using HeapElement = pair<U64, size_t>;
        priority_queue<HeapElement, vector<HeapElement>, greater<HeapElement>> min_heap;
        size_t num_parts = raw_remove_ptrs_by_part->size();
        vector<size_t> ptr_by_part(num_parts, 0);
        for (size_t p = 0; p < num_parts; p++) {
            ptr_by_part[p] = (*start_end_by_part)[p].first;
            if (ptr_by_part[p] < (*start_end_by_part)[p].second) {
                min_heap.push({(*raw_remove_ptrs_by_part)[p].first[ptr_by_part[p]], p});
                ptr_by_part[p]++;
            }
        }
        while (!min_heap.empty()) {
            auto [ptr, p] = min_heap.top();
            min_heap.pop();
            if (remove_ranges->size() > 0 && remove_ranges->back().second >= ptr) {
                remove_ranges->back().second = ptr + min_len;
            } else {
                remove_ranges->push_back({ptr, ptr + min_len});
            }
            if (ptr_by_part[p] < (*start_end_by_part)[p].second) {
                min_heap.push({(*raw_remove_ptrs_by_part)[p].first[ptr_by_part[p]], p});
                ptr_by_part[p]++;
            }
        }
    }

    void find_remove_ranges_parallel_sharded(const size_t min_len, const size_t num_threads, const bool low_ram, const size_t num_batches) const {

        for (size_t s = 0; s < _num_shards; s++) {
            string output_dir = _index_dirs[s] + "/dedup_minlen" + to_string(min_len);
            if (!fs::exists(output_dir)) {
                fs::create_directory(output_dir);
            }
        }
        vector<thread> threads;

        // This function: do not write parts to disk. Requires more RAM, less disk swap space, and faster
        // To save some RAM: write parts to disk and mmap back. Requires less RAM, more disk swap space, and slower
        // To save more RAM: mmap the text file. Requires less RAM, and slower

        cout << "Partitioning SAs into different parts ..." << endl;
        auto start_time = chrono::high_resolution_clock::now();
        size_t num_parts = num_batches * num_threads;
        vector<vector<vector<U64>>> remove_ptrs_by_shard_by_part(num_parts, vector<vector<U64>>(_num_shards));
        vector<vector<U64>> start_rank_by_shard_by_part(num_parts, vector<U64>(_num_shards));
        for (size_t p = 0; p < num_parts; p++) {
            threads.emplace_back(&EngineDedup::partition_sharded, this, min_len, num_parts, p, &start_rank_by_shard_by_part[p]);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        threads.clear();
        // for (size_t s = 0; s < _num_shards; s++) {
        //     cout << "shard " << s << " (" << _shards[s].tok_cnt << " tokens): ";
        //     for (size_t p = 0; p < num_parts; p++) {
        //         cout << start_rank_by_shard_by_part[p][s] << " ";
        //     }
        //     cout << endl;
        // }
        vector<vector<pair<U64, U64>>> start_end_rank_by_shard_by_part(num_parts, vector<pair<U64, U64>>(_num_shards));
        for (size_t p = 0; p < num_parts; p++) {
            for (size_t s = 0; s < _num_shards; s++) {
                start_end_rank_by_shard_by_part[p][s].first = start_rank_by_shard_by_part[p][s];
                start_end_rank_by_shard_by_part[p][s].second = (p == num_parts - 1) ? _shards[s].tok_cnt : start_rank_by_shard_by_part[p+1][s];
            }
        }
        auto end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;

        cout << "Finding remove_ptrs in different parts ..." << endl;
        start_time = chrono::high_resolution_clock::now();
        // the actual heavy lifting
        for (size_t p = 0; p < num_parts; p++) {
            threads.emplace_back(&EngineDedup::find_remove_ptrs_sharded_part, this, min_len, p, &start_end_rank_by_shard_by_part[p], low_ram ? nullptr : &remove_ptrs_by_shard_by_part[p]);
            if ((p + 1) % num_threads == 0) {
                cout << "Batch " << p / num_threads << " starting ..." << endl;
                for (auto &thread : threads) {
                    thread.join();
                }
                threads.clear();
                cout << "Batch " << p / num_threads << " done" << endl;
            }
        }
        // read back the remove_ptrs.* files
        vector<vector<pair<U64*, U64>>> raw_remove_ptrs_by_part_by_shard(_num_shards, vector<pair<U64*, U64>>(num_parts)); // {ptr, size in u64}
        size_t total_remove_ptrs = 0;
        for (size_t p = 0; p < num_parts; p++) {
            for (size_t s = 0; s < _num_shards; s++) {
                if (low_ram) {
                    auto [ptr, size] = load_file(_index_dirs[s] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p), false);
                    assert (size % sizeof(U64) == 0);
                    raw_remove_ptrs_by_part_by_shard[s][p] = {reinterpret_cast<U64*>(ptr), size / sizeof(U64)};
                } else {
                    raw_remove_ptrs_by_part_by_shard[s][p] = {remove_ptrs_by_shard_by_part[p][s].data(), remove_ptrs_by_shard_by_part[p][s].size()};
                }
                total_remove_ptrs += raw_remove_ptrs_by_part_by_shard[s][p].second;
            }
        }
        cout << "total_remove_ptrs: " << total_remove_ptrs << endl;
        end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;

        cout << "Merging remove_ptrs into remove_ranges ..." << endl;
        start_time = chrono::high_resolution_clock::now();
        vector<U64> remove_ranges_cnt_by_shard(_num_shards, 0);
        vector<U64> remove_bytes_cnt_by_shard(_num_shards, 0);
        // the actual heavy lifting
        for (size_t s = 0; s < _num_shards; s++) {
            threads.emplace_back(&EngineDedup::merge_ptrs_into_ranges_sharded_worker, this, min_len, s, &raw_remove_ptrs_by_part_by_shard[s], &remove_ranges_cnt_by_shard[s], &remove_bytes_cnt_by_shard[s]);
        }
        for (auto &thread : threads) {
            thread.join();
        }
        threads.clear();
        // delete the remove_ptrs.* files
        if (low_ram) {
            for (size_t s = 0; s < _num_shards; s++) {
                for (size_t p = 0; p < num_parts; p++) {
                    munmap(raw_remove_ptrs_by_part_by_shard[s][p].first, raw_remove_ptrs_by_part_by_shard[s][p].second * sizeof(U64));
                    fs::remove(_index_dirs[s] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p));
                }
            }
        }
        U64 total_remove_ranges = accumulate(remove_ranges_cnt_by_shard.begin(), remove_ranges_cnt_by_shard.end(), (U64)0);
        U64 total_remove_bytes = accumulate(remove_bytes_cnt_by_shard.begin(), remove_bytes_cnt_by_shard.end(), (U64)0);
        cout << "total_remove_ranges: " << total_remove_ranges << endl;
        cout << "total_remove_bytes: " << total_remove_bytes << endl;
        end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;
    }

    void partition_sharded(const size_t min_len, const size_t num_parts, const size_t p, vector<U64>* const out_start_rank_by_shard) const {

        U64 rank_in_s0 = _shards[0].tok_cnt * p / num_parts;
        U64 ptr_in_s0 = _convert_rank_to_ptr(_shards[0], rank_in_s0);
        // rewind rank_in_s0 until it does not share a minlen-prefix with the previous rank
        while (rank_in_s0 > 0) {
            U64 rank = rank_in_s0 - 1;
            U64 ptr = _convert_rank_to_ptr(_shards[0], rank);
            if (!(ptr_in_s0 + min_len <= _shards[0].ds_size &&
                  ptr + min_len <= _shards[0].ds_size &&
                  memcmp(_shards[0].ds + ptr_in_s0, _shards[0].ds + ptr, min_len) == 0)) {
                break;
            }
            rank_in_s0 = rank;
            ptr_in_s0 = ptr;
        }
        (*out_start_rank_by_shard)[0] = rank_in_s0;

        for (size_t s = 1; s < _num_shards; s++) {
            (*out_start_rank_by_shard)[s] = (p == 0) ? 0 : _lower_bound(s, rank_in_s0, min_len);
        }

        // // if the smallest one from this part shares a minlen-prefix with the largest one from the previous part, we need to advance these ranks
        // if (p > 0) {
        //     // find the largest (s, ptr) in the previous part
        //     size_t s_best = 0;
        //     U64 rank_best = (*out_start_rank_by_shard)[0] - 1;
        //     U64 ptr_best = _convert_rank_to_ptr(_shards[s_best], rank_best);
        //     for (size_t s = 1; s < _num_shards; s++) {
        //         U64 rank = (*out_start_rank_by_shard)[s] - 1;
        //         if (rank == (U64)-1) continue;
        //         U64 ptr = _convert_rank_to_ptr(_shards[s], rank);
        //         if (lexicographical_compare(
        //             _shards[s_best].ds + ptr_best, _shards[s_best].ds + min(ptr_best + min_len, _shards[s_best].ds_size),
        //             _shards[s].ds + ptr, _shards[s].ds + min(ptr + min_len, _shards[s].ds_size)
        //         )) { // ptr_best < ptr
        //             s_best = s;
        //             ptr_best = ptr;
        //         }
        //     }
        //     // advance this part's start until its minlen-prefix is different from (s_best, ptr_best)
        //     for (size_t s = 0; s < _num_shards; s++) {
        //         while ((*out_start_rank_by_shard)[s] < _shards[s].tok_cnt) {
        //             U64 rank = (*out_start_rank_by_shard)[s];
        //             U64 ptr = _convert_rank_to_ptr(_shards[s], rank);
        //             if (!(ptr_best + min_len <= _shards[s_best].ds_size &&
        //                   ptr + min_len <= _shards[s].ds_size &&
        //                   memcmp(_shards[s_best].ds + ptr_best, _shards[s].ds + ptr, min_len) == 0) &&
        //                   find(_shards[s].ds + ptr, _shards[s].ds + ptr + min_len, (U8)-1) == _shards[s].ds + ptr + min_len) { // (s_best, ptr_best) != (s, ptr)
        //                 break;
        //             }
        //             (*out_start_rank_by_shard)[s]++;
        //         }
        //     }
        // }
    }

    void find_remove_ptrs_sharded_part(const size_t min_len, const size_t p, const vector<pair<U64, U64>>* const start_end_rank_by_shard, vector<vector<U64>>* const out_remove_ptrs_by_shard) const {

        // TODO: add sa prefetching

        using HeapElement = pair<size_t, U64>; // {s, ptr}
        auto cmp = [&](const HeapElement &a, const HeapElement &b) { // a > b
            return lexicographical_compare(
                _shards[b.first].ds + b.second, _shards[b.first].ds + min(b.second + min_len, _shards[b.first].ds_size),
                _shards[a.first].ds + a.second, _shards[a.first].ds + min(a.second + min_len, _shards[a.first].ds_size)
            );
        };
        priority_queue<HeapElement, vector<HeapElement>, decltype(cmp)> min_heap(cmp);
        vector<U64> next_rank_by_shard(_num_shards);
        for (size_t s = 0; s < _num_shards; s++) {
            next_rank_by_shard[s] = (*start_end_rank_by_shard)[s].first;
            // push the first rank from shard s to the heap, if there is one
            if (next_rank_by_shard[s] < (*start_end_rank_by_shard)[s].second) {
                U64 rank = next_rank_by_shard[s];
                U64 ptr = _convert_rank_to_ptr(_shards[s], rank);
                min_heap.push({s, ptr});
                next_rank_by_shard[s]++;
            }
        }
        vector<vector<U64>> remove_ptrs_by_shard(_num_shards);
        vector<HeapElement> buff;
        while (!min_heap.empty()) {
            auto [s, ptr] = min_heap.top();
            min_heap.pop();
            // push the next rank from shard s to the heap, if there is one
            if (next_rank_by_shard[s] < (*start_end_rank_by_shard)[s].second) {
                U64 rank = next_rank_by_shard[s];
                U64 ptr = _convert_rank_to_ptr(_shards[s], rank);
                min_heap.push({s, ptr});
                next_rank_by_shard[s]++;
            }

            if (buff.size() == 0) {
                buff.push_back({s, ptr});
                continue;
            }
            auto [s_prev, ptr_prev] = buff.back();
            // if (s_prev, ptr_prev) and (s, ptr) share minlen-prefix, keep moving
            if (ptr + min_len <= _shards[s].ds_size &&
                ptr_prev + min_len <= _shards[s_prev].ds_size &&
                memcmp(_shards[s].ds + ptr, _shards[s_prev].ds + ptr_prev, min_len) == 0 &&
                find(_shards[s].ds + ptr, _shards[s].ds + ptr + min_len, (U8)-1) == _shards[s].ds + ptr + min_len) {
                buff.push_back({s, ptr});
                continue;
            }
            if (buff.size() > 1) { // the buffer has more than one element, keep the smallest one and remove the rest
                HeapElement smallest_elem = *min_element(buff.begin(), buff.end());
                for (const auto &elem : buff) {
                    if (elem != smallest_elem) {
                        remove_ptrs_by_shard[elem.first].push_back(elem.second);
                    }
                }
            }
            buff.clear();
            buff.push_back({s, ptr});
        }
        if (buff.size() > 1) { // process the remaining buffer
            HeapElement smallest_elem = *min_element(buff.begin(), buff.end());
            for (const auto &elem : buff) {
                if (elem != smallest_elem) {
                    remove_ptrs_by_shard[elem.first].push_back(elem.second);
                }
            }
        }

        size_t remove_ptrs_size = 0;
        for (size_t s = 0; s < _num_shards; s++) {
            sort(remove_ptrs_by_shard[s].begin(), remove_ptrs_by_shard[s].end());
            remove_ptrs_size += remove_ptrs_by_shard[s].size();
        }

        if (out_remove_ptrs_by_shard) { // return remove_ptrs to the caller
            *out_remove_ptrs_by_shard = move(remove_ptrs_by_shard);
        } else { // write remove_ptrs to a binary file
            for (size_t s = 0; s < _num_shards; s++) {
                string filename = _index_dirs[s] + "/dedup_minlen" + to_string(min_len) + "/remove_ptrs." + to_string(p);
                ofstream fout(filename, ios::binary);
                fout.write(reinterpret_cast<const char*>(remove_ptrs_by_shard[s].data()), remove_ptrs_by_shard[s].size() * sizeof(U64));
                fout.close();
            }
        }

        if (p == 0) {
            cout << "Part 0 done, remove_ptrs_size: " << remove_ptrs_size << endl;
        }
    }

    void merge_ptrs_into_ranges_sharded_worker(const size_t min_len, const size_t s, const vector<pair<U64*, U64>>* const raw_remove_ptrs_by_part, U64* const out_remove_ranges_cnt, U64* const out_remove_bytes_cnt) const {

        vector<PSS> remove_ranges;
        size_t num_parts = raw_remove_ptrs_by_part->size();

        using HeapElement = pair<U64, size_t>; // {ptr, p}
        priority_queue<HeapElement, vector<HeapElement>, greater<HeapElement>> min_heap;
        vector<size_t> next_by_part(num_parts, 0);
        for (size_t p = 0; p < num_parts; p++) {
            if (next_by_part[p] < (*raw_remove_ptrs_by_part)[p].second) {
                min_heap.push({(*raw_remove_ptrs_by_part)[p].first[next_by_part[p]], p});
                next_by_part[p]++;
            }
        }
        while (!min_heap.empty()) {
            auto [ptr, p] = min_heap.top();
            min_heap.pop();
            if (next_by_part[p] < (*raw_remove_ptrs_by_part)[p].second) {
                min_heap.push({(*raw_remove_ptrs_by_part)[p].first[next_by_part[p]], p});
                next_by_part[p]++;
            }

            if (remove_ranges.size() > 0 && remove_ranges.back().second >= ptr) {
                remove_ranges.back().second = ptr + min_len;
            } else {
                remove_ranges.push_back({ptr, ptr + min_len});
            }
        }

        *out_remove_ranges_cnt = 0;
        *out_remove_bytes_cnt = 0;
        for (const auto &[s, e] : remove_ranges) {
            *out_remove_ranges_cnt += 1;
            *out_remove_bytes_cnt += e - s;
        }

        string filename = _index_dirs[s] + "/dedup_minlen" + to_string(min_len) + "/remove_ranges";
        ofstream fout(filename, ios::binary);
        fout.write(reinterpret_cast<const char*>(remove_ranges.data()), remove_ranges.size() * sizeof(PSS));
        fout.close();
    }

    vector<DupPtr> find_dup_ptrs(const size_t min_len) const {
        const auto &shard = _shards[0];
        U64 last_rank = 0; // the rank at which [last_rank, rank) share prefix of length min_len
        vector<DupPtr> dup_ptrs;
        for (U64 rank = 1; rank < shard.tok_cnt; rank++) {
            if (rank % 100000000 == 0) {
                cout << "Processing rank " << rank << " / " << shard.tok_cnt << ", dup_ptrs.size(): " << dup_ptrs.size() << endl;
            }

            // if rank-1 and rank share prefix of length min_len, keep moving
            U64 ptr1 = _convert_rank_to_ptr(shard, rank - 1);
            U64 ptr2 = _convert_rank_to_ptr(shard, rank);
            if (ptr1 + min_len * sizeof(T) <= shard.ds_size &&
                ptr2 + min_len * sizeof(T) <= shard.ds_size &&
                memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len * sizeof(T)) == 0 &&
                find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len) {
                continue;
            }

            // process [last_rank, rank)
            if (last_rank < rank - 1) { // the segment has more than one element
                vector<U64> ptrs = _convert_ranks_to_ptrs(shard, last_rank, rank);
                U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
                for (U64 r = last_rank; r < rank; r++) {
                    dup_ptrs.push_back(DupPtr{ .ptr = ptrs[r - last_rank], .dropped = (ptrs[r - last_rank] != smallest_ptr)});
                }
            }

            last_rank = rank;
        }

        sort(dup_ptrs.begin(), dup_ptrs.end(), [](const DupPtr &a, const DupPtr &b) {
            return a.ptr < b.ptr;
        });

        return dup_ptrs;
    }

    vector<DupDoc> find_dup_docs(const size_t min_len) const {
        const auto &shard = _shards[0];
        vector<DupPtr> dup_ptrs = find_dup_ptrs(min_len);

        vector<DupDoc> dup_docs;
        U64 doc_ix = (U64)(-1);
        U64 doc_ptr = (U64)(-1);
        U64 next_doc_ptr = (U64)(-1);
        vector<DupPtr> span_dup_ptrs;
        for (const auto &dup_ptr : dup_ptrs) {
            if (doc_ix == (U64)(-1) || dup_ptr.ptr >= next_doc_ptr) { // march to a new doc
                if (doc_ix != (U64)(-1)) {
                    vector<U8> text(shard.ds + doc_ptr + 1, shard.ds + next_doc_ptr);
                    dup_docs.push_back(DupDoc{ .doc_ix = doc_ix, .start_ptr = doc_ptr + 1, .dup_ptrs = span_dup_ptrs, .text = text });
                }
                doc_ix = _convert_ptr_to_doc_ix(shard, dup_ptr.ptr);
                doc_ptr = _convert_doc_ix_to_ptr(shard, doc_ix);
                next_doc_ptr = _convert_doc_ix_to_ptr(shard, doc_ix + 1);
                span_dup_ptrs.clear();
            }
            span_dup_ptrs.push_back(dup_ptr);
        }
        if (doc_ix != (U64)(-1)) {
            vector<U8> text(shard.ds + doc_ptr + 1, shard.ds + next_doc_ptr);
            dup_docs.push_back(DupDoc{ .doc_ix = doc_ix, .start_ptr = doc_ptr + 1, .dup_ptrs = span_dup_ptrs, .text = text });
        }

        return dup_docs;
    }

    DocResult<T> get_doc_by_ix(const U64 doc_ix) const {

        assert (doc_ix < get_total_doc_cnt());

        size_t s = 0;
        U64 local_doc_ix = doc_ix;
        while (local_doc_ix >= _shards[s].doc_cnt) {
            local_doc_ix -= _shards[s].doc_cnt;
            s++;
        }
        const auto &shard = _shards[s];

        U64 doc_start_ptr = _convert_doc_ix_to_ptr(shard, local_doc_ix) + sizeof(T); // because we want to skip the document separator
        U64 doc_end_ptr = _convert_doc_ix_to_ptr(shard, local_doc_ix + 1);
        U64 doc_len = (doc_end_ptr - doc_start_ptr) / sizeof(T);

        string metadata = "";
        if (shard.mt) {
            U64 meta_start_ptr = _convert_doc_ix_to_meta_ptr(shard, local_doc_ix);
            U64 meta_end_ptr = _convert_doc_ix_to_meta_ptr(shard, local_doc_ix + 1);
            vector<U8> meta_chars(shard.mt + meta_start_ptr, shard.mt + meta_end_ptr);
            metadata = string(meta_chars.begin(), meta_chars.end());
        }

        vector<T> token_ids(reinterpret_cast<T*>(shard.ds + doc_start_ptr), reinterpret_cast<T*>(shard.ds + doc_end_ptr));

        return DocResult<T>{ .doc_ix = doc_ix, .doc_start_ptr = doc_start_ptr, .doc_end_ptr = doc_end_ptr, .doc_len = doc_len, .metadata = metadata, .token_ids = token_ids, };
    }

    U64 get_total_doc_cnt() const {
        U64 total_doc_cnt = 0;
        for (const auto &shard : _shards) {
            total_doc_cnt += shard.doc_cnt;
        }
        return total_doc_cnt;
    }

    void verify_sa_correctness(const size_t hack) const {
        for (size_t s = 0; s < _num_shards; s++) {
            const auto &shard = _shards[s];
            for (U64 rank = 0; rank + 1 < shard.tok_cnt; rank++) {
                U64 ptr0 = _convert_rank_to_ptr(shard, rank);
                U64 ptr1 = _convert_rank_to_ptr(shard, rank + 1);
                if (lexicographical_compare(
                    shard.ds + ptr1, shard.ds + min(ptr1 + hack, shard.ds_size),
                    shard.ds + ptr0, shard.ds + min(ptr0 + hack, shard.ds_size)
                )) {
                    cout << "SA is incorrect between rank " << rank << " and " << rank + 1 << endl;
                    return;
                }
            }
        }
        cout << "SA is correct" << endl;
    }

private:

    inline U64 _convert_rank_to_ptr(const DatastoreShard &shard, const U64 rank) const {
        assert (rank < shard.tok_cnt);
        U64 ptr = 0; // need to zero-initialize such that all 8 bytes are filled
        memcpy(&ptr, shard.sa + rank * shard.ptr_size, shard.ptr_size);
        return ptr;
    }

    inline vector<U64> _convert_ranks_to_ptrs(const DatastoreShard &shard, const U64 rank_start, const U64 rank_end) const {
        assert (rank_start <= rank_end);
        assert (rank_end <= shard.tok_cnt);
        vector<U64> ptrs(rank_end - rank_start);
        U64 ptr = 0; // need to zero-initialize such that all 8 bytes are filled
        for (U64 rank = rank_start; rank < rank_end; rank++) {
            memcpy(&ptr, shard.sa + rank * shard.ptr_size, shard.ptr_size);
            ptrs[rank - rank_start] = ptr;
        }
        return ptrs;
    }

    inline U64 _convert_doc_ix_to_ptr(const DatastoreShard &shard, const U64 doc_ix) const {
        assert (doc_ix <= shard.doc_cnt);
        if (doc_ix == shard.doc_cnt) {
            return shard.ds_size;
        }
        U64 ptr = 0;
        memcpy(&ptr, shard.od + doc_ix * sizeof(U64), sizeof(U64));
        return ptr;
    }

    inline U64 _convert_ptr_to_doc_ix(const DatastoreShard &shard, const U64 ptr) const {
        assert (ptr < shard.ds_size);
        assert (ptr % sizeof(T) == 0);
        U64 lo = 0, hi = shard.doc_cnt;
        while (hi - lo > 1) {
            U64 mi = (lo + hi) >> 1;
            U64 p = _convert_doc_ix_to_ptr(shard, mi);
            if (p <= ptr) {
                lo = mi;
            } else {
                hi = mi;
            }
        }
        return lo;
    }

    inline U64 _convert_doc_ix_to_meta_ptr(const DatastoreShard &shard, const U64 doc_ix) const {
        assert (doc_ix <= shard.doc_cnt);
        if (doc_ix == shard.doc_cnt) {
            return shard.mt_size;
        }
        U64 ptr = 0;
        memcpy(&ptr, shard.om + doc_ix * sizeof(U64), sizeof(U64));
        return ptr;
    }

    U64 _lower_bound(const size_t s, const U64 rank_in_s0, const size_t len) const {
        U64 ptr_in_s0 = _convert_rank_to_ptr(_shards[0], rank_in_s0);
        U64 lo = -1, hi = _shards[s].tok_cnt; // lo is always < ptr_in_s0, hi is always >= ptr_in_s0
        while (hi - lo > 1) {
            U64 mi = (lo + hi) >> 1;
            U64 ptr = _convert_rank_to_ptr(_shards[s], mi);
            if (lexicographical_compare(
                _shards[s].ds + ptr, _shards[s].ds + min(ptr + len, _shards[s].ds_size),
                _shards[0].ds + ptr_in_s0, _shards[0].ds + min(ptr_in_s0 + len, _shards[0].ds_size)
            )) { // ptr < ptr_in_s0
                lo = mi;
            } else {
                hi = mi;
            }
        }
        return hi;
    }

private:

    T _doc_sep_id;
    vector<U8> _doc_sep;
    vector<string> _index_dirs;
    size_t _num_shards;
    vector<DatastoreShard> _shards;
};
