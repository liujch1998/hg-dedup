// c++ -std=c++20 -O3 -shared -fPIC $(python3 -m pybind11 --includes) cpp_engine_dedup.cpp -o cpp_engine_dedup$(python3-config --extension-suffix)

#include "cpp_engine_dedup.h"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
using namespace pybind11::literals;

PYBIND11_MODULE(cpp_engine_dedup, m) {

    py::class_<DocResult<U8>>(m, "DocResult_U8")
        .def_readwrite("doc_ix", &DocResult<U8>::doc_ix)
        .def_readwrite("doc_start_ptr", &DocResult<U8>::doc_start_ptr)
        .def_readwrite("doc_end_ptr", &DocResult<U8>::doc_end_ptr)
        .def_readwrite("doc_len", &DocResult<U8>::doc_len)
        .def_readwrite("metadata", &DocResult<U8>::metadata)
        .def_readwrite("token_ids", &DocResult<U8>::token_ids);

    py::class_<EngineDedup<U8>>(m, "EngineDedup_U8")
        .def(py::init<const vector<string>, const bool>())
        .def("get_total_doc_cnt", &EngineDedup<U8>::get_total_doc_cnt)
        .def("get_doc_by_ix", &EngineDedup<U8>::get_doc_by_ix, "doc_ix"_a)
        .def("find_remove_ranges", &EngineDedup<U8>::find_remove_ranges, "min_len"_a, "num_threads"_a, "low_ram"_a, "num_batches"_a)
        .def("find_remove_ranges_sharded", &EngineDedup<U8>::find_remove_ranges_sharded, "min_len"_a, "num_threads"_a, "low_ram"_a, "num_batches"_a);
}
