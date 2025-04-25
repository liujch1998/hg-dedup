#include <cassert>
#include <cstdint> // for uint64_t
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
        : _doc_sep_id((T)(-1)), _doc_sep(vector<U8>(sizeof(T), 0xff))
    {

        assert_little_endian();

        for (const auto &index_dir : index_dirs) {
            assert (fs::exists(index_dir));

            vector<string> ds_paths, sa_paths, od_paths, mt_paths, om_paths, ug_paths;
            for (const auto &entry : fs::directory_iterator(index_dir)) {
                if (entry.path().string().find("tokenized") != string::npos) {
                    ds_paths.push_back(entry.path());
                } else if (entry.path().string().find("table") != string::npos) {
                    sa_paths.push_back(entry.path());
                } else if (entry.path().string().find("offset") != string::npos) {
                    od_paths.push_back(entry.path());
                } else if (entry.path().string().find("metadata") != string::npos) {
                    mt_paths.push_back(entry.path());
                } else if (entry.path().string().find("metaoff") != string::npos) {
                    om_paths.push_back(entry.path());
                } else if (entry.path().string().find("unigram") != string::npos) {
                    ug_paths.push_back(entry.path());
                }
            }
            sort(ds_paths.begin(), ds_paths.end());
            sort(sa_paths.begin(), sa_paths.end());
            sort(od_paths.begin(), od_paths.end());
            sort(mt_paths.begin(), mt_paths.end());
            sort(om_paths.begin(), om_paths.end());
            assert (sa_paths.size() == ds_paths.size());
            assert (od_paths.size() == ds_paths.size());
            assert (mt_paths.size() == 0 || mt_paths.size() == ds_paths.size());
            assert (om_paths.size() == mt_paths.size());
            assert (ug_paths.size() == 0 || ug_paths.size() == ds_paths.size());

            for (size_t s = 0; s < ds_paths.size(); s++) {
                auto [ds, ds_size] = load_file(ds_paths[s], true);
                auto [sa, sa_size] = load_file(sa_paths[s], false);
                auto [od, od_size] = load_file(od_paths[s], true);

                assert (ds_size % sizeof(T) == 0);
                U64 tok_cnt = ds_size / sizeof(T);
                assert (sa_size % tok_cnt == 0);
                U8 ptr_size = (U8)(sa_size / tok_cnt);
                assert (od_size % sizeof(U64) == 0);
                U64 doc_cnt = od_size / sizeof(U64);

                if (!load_metadata || mt_paths.size() == 0) {
                    auto shard = DatastoreShard{ds, sa, tok_cnt, ds_size, ptr_size, od, doc_cnt};
                    _shards.push_back(shard);
                } else {
                    auto [mt, mt_size] = load_file(mt_paths[s], true);
                    auto [om, om_size] = load_file(om_paths[s], true);

                    assert (om_size == doc_cnt * sizeof(U64));

                    auto shard = DatastoreShard{ds, sa, tok_cnt, ds_size, ptr_size, od, doc_cnt, mt, mt_size, om};
                    _shards.push_back(shard);
                }
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

    pair<U8*, U64> load_file(const string &path, const bool load_to_ram) {
        if (load_to_ram) {
            ifstream f(path, ios::binary);
            assert (f.is_open());
            f.seekg(0, ios::end);
            U64 size = f.tellg();
            f.seekg(0, ios::beg);
            U8 *buf = new U8[size];
            f.read(reinterpret_cast<char*>(buf), size);
            f.close();
            return {buf, size};
        } else {
            int f = open(path.c_str(), O_RDONLY);
            assert (f != -1);
            struct stat s;
            auto fstat_ret = fstat(f, &s);
            assert (fstat_ret != -1);
            U8 *ptr = static_cast<U8*>(mmap(NULL, s.st_size, PROT_READ, MAP_PRIVATE, f, 0));
            assert (ptr != MAP_FAILED);
            // madvise(ptr, s.st_size, MADV_SEQUENTIAL);
            close(f);
            return {ptr, s.st_size};
        }
    }

    void unload_file(U8* ptr, U64 size, const bool load_to_ram) {
        if (load_to_ram) {
            delete[] ptr;
        } else {
            munmap(ptr, size);
        }
    }

    void find_remove_ranges_parallel(const size_t min_len, const size_t num_threads, const string output_dir) const {
        const auto &shard = _shards[0];
        if (!fs::exists(output_dir)) {
            fs::create_directory(output_dir);
        }

        // Basically ...
        // Option 1: do not write parts to disk. Requires more RAM, less disk swap space, and faster
        // Option 2: write parts to disk and mmap back. Requires less RAM, more disk swap space, and slower

        cout << "Launching threads to find remove_ptrs in different ranges of ranks ..." << endl;
        auto start_time = chrono::high_resolution_clock::now();
        vector<vector<U64>> remove_ptrs_by_thread(num_threads);
        U64 start_rank = 0;
        vector<thread> threads;
        for (size_t t = 0; t < num_threads; t++) {
            U64 end_rank = shard.tok_cnt * (t + 1) / num_threads;
            // move forward end_rank until end_rank-1 and end_rank do not share prefix of length min_len
            while (true) {
                if (end_rank >= shard.tok_cnt) {
                    break;
                }
                U64 ptr1 = _convert_rank_to_ptr(shard, end_rank - 1);
                U64 ptr2 = _convert_rank_to_ptr(shard, end_rank);
                if (!(ptr1 + min_len * sizeof(T) <= shard.ds_size &&
                    ptr2 + min_len * sizeof(T) <= shard.ds_size &&
                    memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len * sizeof(T)) == 0 &&
                    find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len)) {
                    break;
                }
                end_rank++;
            }
            threads.emplace_back(&EngineDedup::find_remove_ptrs_thread, this, min_len, t, start_rank, end_rank, output_dir, &remove_ptrs_by_thread[t]);
            start_rank = end_rank;
        }
        for (auto &thread : threads) {
            thread.join();
        }
        size_t total_remove_ptrs = 0;
        for (size_t t = 0; t < num_threads; t++) {
            total_remove_ptrs += remove_ptrs_by_thread[t].size();
        }
        cout << "total_remove_ptrs: " << total_remove_ptrs << endl;
        auto end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;

        cout << "Merging remove_ptrs into remove_ranges ..." << endl;
        start_time = chrono::high_resolution_clock::now();
        vector<vector<size_t>> start_by_thread_by_worker(num_threads, vector<size_t>(num_threads));
        for (size_t w = 0; w < num_threads; w++) {
            start_by_thread_by_worker[w][0] = remove_ptrs_by_thread[0].size() * w / num_threads;
            U64 anchor = remove_ptrs_by_thread[0][start_by_thread_by_worker[w][0]];
            for (size_t t = 1; t < num_threads; t++) {
                start_by_thread_by_worker[w][t] = (w == 0) ? 0 : std::lower_bound(remove_ptrs_by_thread[t].begin(), remove_ptrs_by_thread[t].end(), anchor) - remove_ptrs_by_thread[t].begin();
            }
        }
        vector<vector<PSS>> start_end_by_thread_by_worker(num_threads, vector<PSS>(num_threads));
        for (size_t w = 0; w < num_threads; w++) {
            for (size_t t = 0; t < num_threads; t++) {
                start_end_by_thread_by_worker[w][t].first = start_by_thread_by_worker[w][t];
                start_end_by_thread_by_worker[w][t].second = (w == num_threads - 1) ? remove_ptrs_by_thread[t].size() : start_by_thread_by_worker[w+1][t];
            }
        }
        vector<vector<PSS>> remove_ranges_by_worker(num_threads);
        vector<thread> workers;
        for (size_t w = 0; w < num_threads; w++) {
            workers.emplace_back(&EngineDedup::merge_ptrs_into_ranges_worker, this, min_len, &remove_ptrs_by_thread, &start_end_by_thread_by_worker[w], &remove_ranges_by_worker[w]);
        }
        for (auto &worker : workers) {
            worker.join();
        }
        vector<PSS> remove_ranges;
        for (size_t w = 0; w < num_threads; w++) {
            for (size_t i = 0; i < remove_ranges_by_worker[w].size(); i++) {
                if (i == 0 && remove_ranges.size() > 0 && remove_ranges.back().second >= remove_ranges_by_worker[w][i].first) { // should merge these ranges
                    remove_ranges.back().second = remove_ranges_by_worker[w][i].second;
                } else {
                    remove_ranges.push_back(remove_ranges_by_worker[w][i]);
                }
            }
        }
        cout << "remove_ranges.size(): " << remove_ranges.size() << endl;
        end_time = chrono::high_resolution_clock::now();
        cout << "Done, time taken: " << chrono::duration_cast<chrono::seconds>(end_time - start_time).count() << " seconds" << endl;
    }

    void find_remove_ptrs_thread(const size_t min_len, const size_t t, const U64 start_rank, const U64 end_rank, const string output_dir, vector<U64>* const out_remove_ptrs) const {

        const auto &shard = _shards[0];

        vector<U64> remove_ptrs;
        vector<U64> ptrs{_convert_rank_to_ptr(shard, start_rank)};
        for (U64 rank = start_rank + 1; rank < end_rank; rank++) {
            if (t == 0 && rank % 100000000 == 0) {
                cout << "Thread 0 processing rank " << rank << " / " << end_rank << ", remove_ptrs.size(): " << remove_ptrs.size() << endl;
            }

            // if rank-1 and rank share prefix of length min_len, keep moving
            U64 ptr1 = ptrs.back();
            U64 ptr2 = _convert_rank_to_ptr(shard, rank);
            if (ptr1 + min_len * sizeof(T) <= shard.ds_size &&
                ptr2 + min_len * sizeof(T) <= shard.ds_size &&
                memcmp(shard.ds + ptr1, shard.ds + ptr2, min_len * sizeof(T)) == 0 &&
                find(shard.ds + ptr1, shard.ds + ptr1 + min_len, (U8)-1) == shard.ds + ptr1 + min_len) {
                ptrs.push_back(ptr2);
                continue;
            }

            if (ptrs.size() > 1) { // the segment has more than one element
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

        sort(remove_ptrs.begin(), remove_ptrs.end());
        auto remove_ptrs_size = remove_ptrs.size();

        if (out_remove_ptrs) { // return remove_ptrs to the caller
            *out_remove_ptrs = move(remove_ptrs);
        } else { // write remove_ptrs to a binary file
            string filename = output_dir + "/remove_ptrs." + to_string(t);
            ofstream fout(filename, ios::binary);
            fout.write(reinterpret_cast<const char*>(remove_ptrs.data()), remove_ptrs.size() * sizeof(U64));
            fout.close();
        }

        if (t == 0) {
            cout << "Thread 0 done, remove_ptrs.size(): " << remove_ptrs_size << endl;
        }
    }

    void merge_ptrs_into_ranges_worker(const size_t min_len, const vector<vector<U64>>* const remove_ptrs_by_thread, const vector<PSS>* const start_end_by_thread, vector<PSS>* const remove_ranges) const {
        using HeapElement = pair<U64, size_t>;
        priority_queue<HeapElement, vector<HeapElement>, greater<HeapElement>> min_heap;
        size_t num_threads = remove_ptrs_by_thread->size();
        vector<size_t> ptr_by_thread(num_threads, 0);
        for (size_t t = 0; t < num_threads; t++) {
            ptr_by_thread[t] = (*start_end_by_thread)[t].first;
            if (ptr_by_thread[t] < (*start_end_by_thread)[t].second) {
                min_heap.push({(*remove_ptrs_by_thread)[t][ptr_by_thread[t]], t});
                ptr_by_thread[t]++;
            }
        }
        while (!min_heap.empty()) {
            auto [ptr, t] = min_heap.top();
            min_heap.pop();
            if (remove_ranges->size() > 0 && remove_ranges->back().second >= ptr) {
                remove_ranges->back().second = ptr + min_len * sizeof(T);
            } else {
                remove_ranges->push_back({ptr, ptr + min_len * sizeof(T)});
            }
            if (ptr_by_thread[t] < (*start_end_by_thread)[t].second) {
                min_heap.push({(*remove_ptrs_by_thread)[t][ptr_by_thread[t]], t});
                ptr_by_thread[t]++;
            }
        }
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

private:

    T _doc_sep_id;
    vector<U8> _doc_sep;
    size_t _num_shards;
    vector<DatastoreShard> _shards;
};
