// g++ -O3 -std=c++20 -pthread find_remove_ranges.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    if (argc != 6) {
        cout << "Usage: " << argv[0] << " <index_dir> <min_len> <num_threads> <output_dir> <low_ram>" << endl;
        return 1;
    }
    string index_dir = argv[1];
    size_t min_len = stoi(argv[2]);
    size_t num_threads = stoi(argv[3]);
    string output_dir = argv[4];
    bool low_ram = stoi(argv[5]);

    auto engine = EngineDedup<U8>({index_dir}, false);
    engine.find_remove_ranges_parallel(min_len, num_threads, output_dir, low_ram);

    return 0;
}