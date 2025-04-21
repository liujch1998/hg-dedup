// g++ -O3 -std=c++20 -pthread find_remove_ranges.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    if (argc != 5) {
        cout << "Usage: " << argv[0] << " <ds_name> <index_dir> <min_len> <num_threads>" << endl;
        return 1;
    }

    string ds_name = argv[1];
    auto engine = EngineDedup<U8>({argv[2]});
    size_t min_len = stoi(argv[3]);
    size_t num_threads = stoi(argv[4]);

    engine.find_remove_ranges_parallel(min_len, num_threads, ds_name);

    return 0;
}