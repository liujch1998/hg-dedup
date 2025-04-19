// g++ -O3 -std=c++20 -pthread find_dup_spans.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    auto engine = EngineDedup<U8>({"../ha-infini-gram/index/v4_pileval_u8"}, 255, 255, 4);
    // U64 min_len = 70000;
    U64 min_len = 1000;
    auto dup_spans = engine.find_dup_spans(min_len);
    cout << "dup_spans.size(): " << dup_spans.size() << endl;

    for (const auto &dup_span : dup_spans) {
        cout << dup_span.start_ptr << endl;
        vector<bool> droppeds(dup_span.text.size(), false);
        vector<bool> kepts(dup_span.text.size(), false);
        size_t pos_dropped = 0;
        size_t pos_kept = 0;
        for (size_t i = 0; i < dup_span.dup_ptrs.size(); i++) {
            if (dup_span.dup_ptrs[i].dropped) {
                if (pos_dropped == 0) {
                    pos_dropped = dup_span.dup_ptrs[i].ptr - dup_span.start_ptr;
                }
                for (size_t j = pos_dropped; j < dup_span.dup_ptrs[i].ptr - dup_span.start_ptr + min_len; j++) {
                    droppeds[j] = true;
                }
                pos_dropped = dup_span.dup_ptrs[i].ptr - dup_span.start_ptr + min_len;
            } else {
                if (pos_kept == 0) {
                    pos_kept = dup_span.dup_ptrs[i].ptr - dup_span.start_ptr;
                }
                for (size_t j = pos_kept; j < dup_span.dup_ptrs[i].ptr - dup_span.start_ptr + min_len; j++) {
                    kepts[j] = true;
                }
                pos_kept = dup_span.dup_ptrs[i].ptr - dup_span.start_ptr + min_len;
            }
        }
        for (size_t i = 0; i < dup_span.text.size(); i++) {
            if (droppeds[i] && !kepts[i]) { // red
                cout << "\033[31m" << (char)dup_span.text[i] << "\033[0m";
            } else if (droppeds[i] && kepts[i]) { // yellow
                cout << "\033[33m" << (char)dup_span.text[i] << "\033[0m";
            } else if (!droppeds[i] && kepts[i]) { // green
                cout << "\033[32m" << (char)dup_span.text[i] << "\033[0m";
            } else { // white
                cout << (char)dup_span.text[i];
            }
            // cout << (char)dup_span.text[i];
        }
        cout << endl;
        cout << "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------" << endl;
    }

    return 0;
}
