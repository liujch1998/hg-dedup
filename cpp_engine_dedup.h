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
struct DupSpan {
    U64 start_ptr;
    vector<DupPtr> dup_ptrs;
    vector<U8> text;
};
struct DupDoc {
    U64 doc_ix;
    U64 start_ptr;
    vector<DupPtr> dup_ptrs;
    vector<U8> text;
};

template<typename T=U8>
class EngineDedup {

public:

    EngineDedup(
        const vector<string> index_dirs, const T eos_token_id, const T vocab_size, const size_t version)
        : _eos_token_id(eos_token_id), _vocab_size(vocab_size), _version(version),
          _doc_sep_id((T)(-1)), _doc_sep(vector<U8>(sizeof(T), 0xff))
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

                if (mt_paths.size() == 0) {
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
            madvise(ptr, s.st_size, MADV_RANDOM);
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

    void get_lcp_len_at_rank(const size_t s, const U64 rank, U64* const out_lcp_len) {
        const auto &shard = _shards[s];
        U64 ptr1 = _convert_rank_to_ptr(shard, rank);
        U64 ptr2 = _convert_rank_to_ptr(shard, rank + 1);
        U64 lcp_len = 0;
        // this only works on U8!
        while (ptr1 < shard.tok_cnt && ptr2 < shard.tok_cnt && shard.ds[ptr1] == shard.ds[ptr2] && shard.ds[ptr1] != (U8)-1) {
            lcp_len++;
            ptr1++;
            ptr2++;
        }
        *out_lcp_len = lcp_len;
    }

    vector<DupDoc> find_dup_docs(size_t min_len) {
        const auto &shard = _shards[0];
        U64 start_rank = 0; // the rank at which [start_rank, rank) share prefix of length min_len
        vector<DupPtr> dup_ptrs;
        for (U64 rank = 1; rank < shard.tok_cnt; rank++) {
            if (rank % 100000000 == 0) {
                cout << "Processing rank = " << rank << endl;
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

            // process [start_rank, rank)
            if (start_rank < rank - 1) { // the segment has more than one element
                vector<U64> ptrs = _convert_ranks_to_ptrs(shard, start_rank, rank);
                U64 smallest_ptr = *min_element(ptrs.begin(), ptrs.end());
                for (U64 r = start_rank; r < rank; r++) {
                    dup_ptrs.push_back(DupPtr{ .ptr = ptrs[r - start_rank], .dropped = (ptrs[r - start_rank] != smallest_ptr)});
                }
            }

            start_rank = rank;
        }

        sort(dup_ptrs.begin(), dup_ptrs.end(), [](const DupPtr &a, const DupPtr &b) {
            return a.ptr < b.ptr;
        });

        // vector<DupSpan> dup_spans;
        // U64 start_ptr = dup_ptrs[0].ptr;
        // vector<DupPtr> span_dup_ptrs;
        // span_dup_ptrs.push_back(dup_ptrs[0]);
        // for (size_t i = 1; i < dup_ptrs.size(); i++) {
        //     if (dup_ptrs[i].ptr <= dup_ptrs[i-1].ptr + min_len) {
        //         span_dup_ptrs.push_back(dup_ptrs[i]);
        //         continue;
        //     }
        //     vector<U8> text(shard.ds + start_ptr, shard.ds + dup_ptrs[i-1].ptr + min_len);
        //     dup_spans.push_back(DupSpan{ .start_ptr = start_ptr, .dup_ptrs = span_dup_ptrs, .text = text});
        //     start_ptr = dup_ptrs[i].ptr;
        //     span_dup_ptrs.clear();
        //     span_dup_ptrs.push_back(dup_ptrs[i]);
        // }

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

private:

    T _eos_token_id;
    T _vocab_size;
    size_t _version;
    T _doc_sep_id;
    vector<U8> _doc_sep;
    size_t _num_shards;
    vector<DatastoreShard> _shards;
};
