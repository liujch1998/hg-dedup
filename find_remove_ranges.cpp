// g++ -O3 -std=c++20 -pthread find_remove_ranges.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    if (argc != 5) {
        cout << "Usage: " << argv[0] << " <index_dir> <min_len> <num_threads> <output_dir>" << endl;
        return 1;
    }

    auto engine = EngineDedup<U8>({argv[1]});
    size_t min_len = stoi(argv[2]);
    size_t num_threads = stoi(argv[3]);
    string output_dir = argv[4];

    engine.find_remove_ranges_parallel(min_len, num_threads, output_dir);

    return 0;
}