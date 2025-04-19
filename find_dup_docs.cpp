// g++ -O3 -std=c++20 -pthread find_dup_docs.cpp

#include "cpp_engine_dedup.h"

int main(int argc, char **argv) {

    auto engine = EngineDedup<U8>({"../ha-infini-gram/index/v4_pileval_u8"}, 255, 255, 4);
    U64 min_len = 200;
    auto dup_docs = engine.find_dup_docs(min_len);
    cout << "dup_docs.size(): " << dup_docs.size() << endl;

    string dir = "dup_docs/pileval_minlen" + to_string(min_len);
    if (!fs::exists(dir)) {
        fs::create_directory(dir);
    }

    for (const auto &dup_doc : dup_docs) {
        ofstream fout(dir + "/doc" + to_string(dup_doc.doc_ix) + ".ansi");
        vector<bool> droppeds(dup_doc.text.size(), false);
        vector<bool> kepts(dup_doc.text.size(), false);
        size_t pos_dropped = 0;
        size_t pos_kept = 0;
        for (size_t i = 0; i < dup_doc.dup_ptrs.size(); i++) {
            if (dup_doc.dup_ptrs[i].dropped) {
                if (pos_dropped == 0) {
                    pos_dropped = dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr;
                }
                for (size_t j = max(pos_dropped, dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr); j < dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr + min_len; j++) {
                    droppeds[j] = true;
                }
                pos_dropped = dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr + min_len;
            } else {
                if (pos_kept == 0) {
                    pos_kept = dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr;
                }
                for (size_t j = max(pos_kept, dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr); j < dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr + min_len; j++) {
                    kepts[j] = true;
                }
                pos_kept = dup_doc.dup_ptrs[i].ptr - dup_doc.start_ptr + min_len;
            }
        }
        string curr_color = "white";
        for (size_t i = 0; i < dup_doc.text.size(); i++) {
            if (droppeds[i] && !kepts[i]) { // red
                if (curr_color != "red") {
                    fout << "\033[31m";
                    curr_color = "red";
                }
            } else if (droppeds[i] && kepts[i]) { // yellow
                if (curr_color != "yellow") {
                    fout << "\033[33m";
                    curr_color = "yellow";
                }
            } else if (!droppeds[i] && kepts[i]) { // green
                if (curr_color != "green") {
                    fout << "\033[32m";
                    curr_color = "green";
                }
            } else { // white
                if (curr_color != "white") {
                    fout << "\033[0m";
                    curr_color = "white";
                }
            }
            fout << (char)dup_doc.text[i];
        }
        fout.close();
    }

    return 0;
}
