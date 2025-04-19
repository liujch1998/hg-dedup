// g++ -O3 -std=c++20 -pthread find_dup_ptrs.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    if (argc != 4) {
        cout << "Usage: " << argv[0] << " <index_dir> <min_len> <num_threads>" << endl;
        return 1;
    }

    auto engine = EngineDedup<U8>({argv[1]});
    size_t min_len = stoi(argv[2]);
    size_t num_threads = stoi(argv[3]);

    auto dup_ptrs = engine.find_dup_ptrs_parallel(min_len, num_threads);
    cout << "dup_ptrs.size(): " << dup_ptrs.size() << endl;

    string output_dir = string(argv[1]) + "/dup_ptrs";
    if (!fs::exists(output_dir)) {
        fs::create_directory(output_dir);
    }

    string filename = output_dir + "/minlen" + to_string(min_len) + ".txt";
    ofstream fout(filename);
    for (const auto &dup_ptr : dup_ptrs) {
        fout << dup_ptr.ptr << " " << dup_ptr.dropped << endl;
    }
    fout.close();

    return 0;
}