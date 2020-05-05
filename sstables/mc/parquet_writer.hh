
#pragma once

#include <mutation_fragment.hh>
#include <tombstone.hh>
#include <dht/i_partitioner.hh>
#include <range_tombstone.hh>
#include <parquet4seastar/file_writer.hh>
#include <seastar/core/sstring.hh>
#include <schema.hh>
#include <iostream>


namespace parquet4seastar {


// Rookie debug printers and helpers that will be removed
std::ostream &operator<<(std::ostream &os, const column_kind &p) {
    switch (p) {
        case column_kind::partition_key:
            os << "partition_key";
            break;
        case column_kind::clustering_key:
            os << "clustering_key";
            break;
        case column_kind::regular_column:
            os << "regular_column";
            break;
        case column_kind::static_column:
            os << "static_column";
            break;
    }
    return os;
}


struct col_def_printer {
    const column_definition &def;
};

// Brief printer
std::ostream &operator<<(std::ostream &os, const col_def_printer &def) {
    os << "ColDef{column=" << def.def.name_as_text() << ",kind=" << def.def.kind << ",id=" << def.def.id << "}";
    return os;
}

// tail -f /tmp/parquet-trace.log
std::ofstream _trace = std::ofstream("/tmp/parquet-trace.log", std::ofstream::binary);

using pq_schema = writer_schema::schema;
using cell_buffer = std::vector<std::optional<::bytes>>;
using sstable_column = std::pair<column_kind, column_id>;
using buffer_per_column = std::unordered_map<sstable_column, cell_buffer, utils::tuple_hash>;
using sstable_schema = ::schema;

class parquet_writer {

private:


    static constexpr auto test_keyspace = "mk";

    const std::string _file_tag;
    const sstable_schema &_sstable_schema;
    std::vector<sstable_column> _pq_column_order;
    const pq_schema _pq_schema;
    std::vector<format::Type::type> _pq_types;
    std::unique_ptr<file_writer> _pq_file_writer;
    buffer_per_column _clustering_buffer;
    buffer_per_column _partition_buffer;


    std::ostream &trace(const std::string &tag) {
        return _trace << "[" << tag << ":" << _file_tag << "]";
    }

    logical_type::logical_type map_column_type(abstract_type::kind sstable_type) {
        using namespace logical_type;
        using _type = abstract_type::kind;
        switch (sstable_type) {
            case _type::int32:
                return INT32{};
            case _type::utf8:
                return BYTE_ARRAY{};
            default:
                return UNKNOWN{}; // unsupported type
        }
    }

    static std::vector<format::Type::type> collect_leaf_types(const pq_schema& sch) {
        using namespace writer_schema;
        std::vector<format::Type::type> leaf_types;
        for (const node& field : sch.fields) {
            const auto& pn = std::get<primitive_node>(field);
            auto physical_type = std::visit([] (const auto& x) { return x.physical_type; }, pn.logical_type);
            leaf_types.push_back(physical_type);
        }
        return leaf_types;
    }

    pq_schema convert_to_parquet(const sstable_schema &src_schema) {
        using namespace writer_schema;
        schema sch;
        // TODO write normal code
        if (is_test_ks()) {
            const auto &columns = src_schema.all_columns();
            for (const auto &col_def : columns) {
                if (!col_def.is_atomic()) continue;
                auto name = col_def.name_as_text();

                auto sstable_type = col_def.type->get_kind();
                auto pq_type = map_column_type(sstable_type);

                if (!std::holds_alternative<logical_type::UNKNOWN>(pq_type)) {
                    _pq_column_order.push_back(std::make_pair(col_def.kind, col_def.id));
                    sch.fields.push_back(primitive_node{
                            name,
                            true,
                            pq_type,
                            {},
                            format::Encoding::PLAIN,
                            format::CompressionCodec::GZIP});
                }
            }
        }
        return sch;
    }

    std::unique_ptr<file_writer> create_pq_file_writer() {
        std::string file = std::string(_file_tag);
        std::replace(file.begin(), file.end(), '/', '-');
        seastar::sstring parquet_file = "/tmp/scylla-parquet/" + file + ".parquet";

        return file_writer::open(parquet_file, _pq_schema).get0();
    }


    // TODO will be removed
    std::string init_trace_tag(const std::string &str, const sstable_schema &schema) {
        auto tag = std::string(schema.cf_name().c_str());
        _trace << "[" << tag << "]"
               << "================== NEW INSTANCE ================================================================"
               << std::endl;
        return tag;
    }


    // TODO this is added since this code is crashing for system keyspaces with some weird memory issues
    bool is_test_ks() {
        return true;// strcmp(_sstable_schema.ks_name().c_str(), test_keyspace) == 0;
    }

public:

    parquet_writer(seastar::sstring &&filename, const sstable_schema &schema) :
            _file_tag{init_trace_tag(std::string(filename.c_str()), schema)},
            _sstable_schema{schema},
            _pq_column_order{},
            _pq_schema{convert_to_parquet(schema)},
            _pq_types{collect_leaf_types(_pq_schema)},
            _pq_file_writer{create_pq_file_writer()},
            _clustering_buffer{},
            _partition_buffer{} {
        if (!is_test_ks()) return;
    }

    ~parquet_writer() {
        _pq_file_writer->close().get0();
    }


    void consume_new_partition(const dht::decorated_key &dk) {
        if (!is_test_ks()) return;
        trace("consume_new_partition")
                << "----------------------------------------------------------------------------------"
                << std::endl;
        buffer_key_columns(dk.key(), column_kind::partition_key, _partition_buffer);
    }

    void consume(tombstone t) {
        if (!is_test_ks()) return;
        trace("consume_tombstone") << t << std::endl;
    }

    void consume(static_row &sr) {
        if (!is_test_ks()) return;
        trace("consume_static_row ") << std::endl;
        buffer_non_key_colums(sr.cells(), _partition_buffer, column_kind::static_column);
    }


    void consume(clustering_row &cr) {
        if (!is_test_ks()) return;
        trace("consume_clustering_row_regular_column") << std::endl;

        // Add clustering columns to the buffer
        buffer_key_columns(cr.key(), column_kind::clustering_key, _clustering_buffer);

        // Add regular columns to the buffer
        buffer_non_key_colums(cr.cells(), _clustering_buffer, column_kind::regular_column);

    }

    void consume(range_tombstone &rt) {
        if (!is_test_ks()) return;

        trace("consume_range_tombstone") << rt << std::endl;
    }

    void consume_end_of_partition() {
        if (!is_test_ks()) return;
        flush_buffer();

        trace("consume_end_of_partition") << std::endl
                                          << "******************************************************************"
                                          << std::endl;

    }

private:

    template<typename T>
    void buffer_key_columns(T &k, column_kind kind, buffer_per_column &buf) {
        auto schema_wrapper = k.with_schema(_sstable_schema);
        const auto&[schema, key] = schema_wrapper;
        auto type_iterator = key.get_compound_type(schema)->types().begin();
        column_id id = 0;
        for (auto &&e : key.components(schema)) {
            auto bytes = to_bytes(e);
            buf[std::make_pair(kind, id)].push_back({bytes});
            ++type_iterator;
            ++id;
        }
    }


    // Used for buffering clustering static and regular columns and
    void buffer_non_key_colums(row &row, buffer_per_column &buf, column_kind kind) {
				std::unordered_map<column_id, ::bytes> non_null_columns;
        row.for_each_cell([&](column_id id, const cell_and_hash &ch) {
            const column_definition &col_def = _sstable_schema.column_at(kind, id);
            if (col_def.is_atomic()) {
                atomic_cell_view acv = ch.cell.as_atomic_cell(
                        _sstable_schema.column_at(col_def.kind, id));
                if (acv.is_live()) {
                    auto bytes = acv.value().linearize();
                    trace("buffer_non_key_columns") << col_def_printer{col_def} << " bytes: " << bytes.length()
                                                    << std::endl;
                    non_null_columns[col_def.id] = bytes;
                }
            }
        });
				trace("coulumn_count") << _sstable_schema.columns_count(kind) << std::endl;
        for(column_id id = 0; id < _sstable_schema.columns_count(kind); ++id){
            auto it = non_null_columns.find(id);
            if (it == non_null_columns.end()){
	              buf[std::make_pair(kind, id)].push_back({});
            }else {
	              buf[std::make_pair(kind, id)].push_back({it->second});
            }
        }

    }

    template<typename T>
    std::string vec_to_string(std::vector<T> &vec) {
        std::ostringstream stream;
        stream << "[";
        for (const auto &it: vec) {
            stream << it << ",";
        }
        stream << "]";
        return stream.str();
    }

    template<typename ValueType>
    std::vector<ValueType> deserialize(const cell_buffer &cell_vec, const column_definition &def) {
        std::vector<ValueType> vec;
        for (const auto &cell: cell_vec) {
            if (cell) {
                const data_value value = def.type->deserialize_value(*cell);
                vec.push_back(value_cast<ValueType>(value));
                trace("deserialize_converted") << value << std::endl;
            }
        }
        return vec;
    }

    template<format::Type::type ParquetType, typename ValueType>
    void write_column(int col_index, const cell_buffer &cells, const column_definition &def) {
        std::vector<ValueType> deserialized_values = deserialize<ValueType>(cells, def);
        auto& col_writer = _pq_file_writer->column<ParquetType>(col_index);
        int non_null_count = 0;
        for (size_t i = 0; i < cells.size(); ++i) {
        	if (cells[i]){
                col_writer.put(1, 0, deserialized_values[non_null_count]);
                non_null_count++;
        	}else {
        	    ValueType t;
        	    col_writer.put(0, 0, t);
        	}

        }
    }

    void write_byte_array(int col_index, const cell_buffer &cells, const column_definition &def) {
        auto& col_writer = _pq_file_writer->column<format::Type::BYTE_ARRAY>(col_index);

        for (const auto &cell: cells) {
            if (cell) {
                // TODO this is probably bad, need to use deserialize_value
                sstring str = def.type->to_string(*cell);
                std::basic_string_view<uint8_t> value{reinterpret_cast<const uint8_t*>(str.c_str()), str.size()};
                col_writer.put(1, 0, value);
            } else {
                std::basic_string_view<uint8_t> value;
                col_writer.put(0, 0, value);
            }
        }
    }


    cell_buffer buffer_for(sstable_column &col) {
        auto &col_def = _sstable_schema.column_at(col.first, col.second);
        int partition_size = _clustering_buffer.size() == 0 ? 0 : _clustering_buffer.begin()->second.size();
        if (partition_size == 0) {
            return {};
        }
        if (col_def.is_partition_key() || col_def.is_static()) {
            trace("buffer_for") << col_def.name_as_text() << " size: " << partition_size << " buffer_size: "
                                << _partition_buffer[col].size() << std::endl;
            assert(partition_size == 0 || _partition_buffer[col].size() > 0);
            cell_buffer b;
            for (int i = 0; i < partition_size; ++i) {
                b.push_back(*_partition_buffer[col].begin());
            }
            return b;
        } else {
            return _clustering_buffer[col];
        }

    }


    void flush_buffer() {
        // TODO split into row groups more accurately
        //  now we write into one big row group

        size_t target_rows_count = _clustering_buffer.size() == 0 ? 0 : _clustering_buffer.begin()->second.size();
        int col_index = 0;
        // We can assume that all vectors in _clustering_buffer have equal size
        for (auto &col_id : _pq_column_order) {

            auto cells = buffer_for(col_id);
            auto &col_def = _sstable_schema.column_at(col_id.first, col_id.second);
            trace("flush_buffer") << col_def_printer{col_def} << " size:" << cells.size() << std::endl;
            assert(cells.size() == target_rows_count);

            auto type = _pq_types[col_index];
            switch (type) {
                case format::Type::INT32:
                    write_column<format::Type::INT32, int32_t>(col_index, cells, col_def);
                    break;
                case format::Type::BYTE_ARRAY:
                    write_byte_array(col_index, cells, col_def);
                    break;
                default:
                    break;

            }

            ++col_index;
        }

        _clustering_buffer.clear();
        _partition_buffer.clear();
    }

public:

    void consume_end_of_stream() {
        if (!is_test_ks()) return;
        trace("consume_end_of_stream") << std::endl;
    }

};

}
