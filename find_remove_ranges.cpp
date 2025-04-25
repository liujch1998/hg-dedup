// g++ -O3 -std=c++20 -pthread find_remove_ranges.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    if (argc != 7) {
        cout << "Usage: " << argv[0] << " <index_dir> <min_len> <num_threads> <output_dir> <low_ram> <num_batches>" << endl;
        return 1;
    }
    string index_dir = argv[1];
    size_t min_len = stoi(argv[2]);
    size_t num_threads = stoi(argv[3]);
    string output_dir = argv[4];
    bool low_ram = stoi(argv[5]);
    size_t num_batches = stoi(argv[6]);

    auto engine = EngineDedup<U8>({index_dir}, false);
    engine.find_remove_ranges_parallel(min_len, num_threads, output_dir, low_ram, num_batches);

    return 0;
}