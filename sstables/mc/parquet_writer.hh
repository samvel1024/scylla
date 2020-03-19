
#pragma once

#include <seastar/parquet/parquet/column_writer.h>
#include <mutation_fragment.hh>
#include <tombstone.hh>
#include <dht/i_partitioner.hh>
#include <range_tombstone.hh>
#include <seastar/parquet/parquet/file_writer.h>
#include <seastar/parquet/parquet/schema.h>
#include <seastar/parquet/parquet/properties.h>
#include <seastar/core/sstring.hh>
#include <schema.hh>
#include <iostream>

namespace parquet {

// TODO will be removed
std::ofstream _trace = std::ofstream("/tmp/parquet-trace.log", std::ofstream::binary);

using pq_schema = std::shared_ptr<schema::GroupNode>;
using row_buffer = std::unordered_map<column_id, std::vector<bytes>>;
using sstable_schema = ::schema;


class parquet_writer {

private:


    static constexpr auto test_keyspace = "mk";

    const std::string _file_tag;
    const sstable_schema &_sstable_schema;
    const pq_schema _pq_schema;
    std::shared_ptr<seastarized::FileFutureOutputStream> _pq_ostream;
    std::shared_ptr<seastarized::ParquetFileWriter> _pq_file_writer;
    row_buffer _buffer;


    std::ostream &trace(const std::string &tag) {
        return _trace << "[" << tag << ":" << _file_tag << "]";
    }

    Type::type map_column_type(abstract_type::kind sstable_type) {
        using _type = abstract_type::kind;
        switch (sstable_type) {
            case _type::int32:
                return Type::INT32;
            case _type::utf8:
                return Type::BYTE_ARRAY;
            default:
                return Type::UNDEFINED; // unsupported type
        }
    }


    pq_schema convert_to_parquet(const sstable_schema &src_schema) {
        schema::NodeVector fields;

        const auto &columns = src_schema.all_columns();
        for (const auto &col_def : columns) {
            auto name = col_def.name_as_text();
            auto sstable_type = col_def.type->get_kind();
            auto parquet_type = map_column_type(sstable_type);
            if (parquet_type != Type::UNDEFINED)
                fields.push_back(schema::PrimitiveNode::Make(
                        name, Repetition::REQUIRED,
                        parquet_type, ConvertedType::NONE));
        }

        return std::static_pointer_cast<schema::GroupNode>(
                schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
    }

    std::shared_ptr<seastarized::FileFutureOutputStream>
    create_pq_ostream() {
        std::string file = std::string(_file_tag);
        std::replace(file.begin(), file.end(), '/', '-');
        seastar::sstring parquet_file = "/tmp/scylla-parquet/" + file + ".parquet";

        seastar::open_flags oflags =
                seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        seastar::file out_file = seastar::open_file_dma(parquet_file, oflags).get0();
        return std::make_shared<seastarized::FileFutureOutputStream>(seastar::make_file_output_stream(out_file));
    }

    std::shared_ptr<seastarized::ParquetFileWriter>
    create_pq_file_writer() {

        WriterProperties::Builder prop_builder;
        prop_builder.compression(parquet::Compression::SNAPPY);
        std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

        std::string file = std::string(_file_tag);
        std::replace(file.begin(), file.end(), '/', '-');
        seastar::sstring parquet_file = "/tmp/scylla-parquet/" + file + ".parquet";

        std::shared_ptr<parquet::seastarized::ParquetFileWriter> file_writer =
                parquet::seastarized::ParquetFileWriter::Open(_pq_ostream, _pq_schema, writer_properties).get0();
        return file_writer;

    }


    // TODO will be removed
    std::string init_trace_tag(const std::string &str) {
        std::string dir;
        {
            std::string l = str.substr(0, str.rfind('/'));
            std::string ll = l.substr(0, l.rfind('-'));
            dir = ll.substr(ll.rfind('/') + 1);
        }
        std::string file = str.substr(str.rfind('/') + 1);
        return dir + "/" + file;
    }

    row_buffer init_row_buffer(const sstable_schema &schema) {
        row_buffer buf = {};
        for (const auto &col_def: schema.all_columns()) {
            buf[col_def.id] = {};
        }
        return buf;
    }

    // TODO this is added since this code is crashing for system keyspaces with some weird memory issues
    bool is_test_ks() {
        return strcmp(_sstable_schema.ks_name().c_str(), test_keyspace) == 0;
    }

public:

    parquet_writer(seastar::sstring &&filename, const sstable_schema &schema) :
            _file_tag{init_trace_tag(std::string(filename.c_str()))},
            _sstable_schema{schema},
            _pq_schema{convert_to_parquet(schema)},
            _pq_ostream{create_pq_ostream()},
            _pq_file_writer{create_pq_file_writer()},
            _buffer{init_row_buffer(schema)} {
        if (!is_test_ks()) return;
        trace("construct") << filename << std::endl;
    }

    ~parquet_writer() {
        _pq_file_writer->Close().get0();
        _pq_ostream->Close().get0();
    }

    void consume_new_partition(const dht::decorated_key &dk) {
        if (!is_test_ks()) return;
        auto &pk = dk._key;
        auto vec = pk.explode(this->_sstable_schema);

        for (bytes &v: vec) {
            const char *str = reinterpret_cast<const char *>(v.c_str());
            std::string string(str, v.size());
            trace("consume_new_partition PK : ") << string << std::endl;
        }

    }

    void consume(tombstone t) {
        if (!is_test_ks()) return;
        trace("consume_tombstone") << t << std::endl;
    }

    void consume(static_row &sr) {
        if (!is_test_ks()) return;
        try {
            row::printer pr(this->_sstable_schema, column_kind::regular_column, sr.cells());
            trace("consume_static_row ") << pr << std::endl;
        } catch (...) {
        }
    }


    void consume(clustering_row &cr) {
        if (!is_test_ks()) return;
        cr.cells().for_each_cell([&](column_id id, const cell_and_hash &ch) {
            const column_definition &col_def = _sstable_schema.column_at(column_kind::clustering_key, id);
            if (col_def.is_atomic()) {
                atomic_cell_view acv = ch.cell.as_atomic_cell(
                        _sstable_schema.column_at(col_def.kind, id));
                auto bytes = acv.value().linearize();
                trace("raw_to_string") << col_def.type->to_string(bytes) << std::endl;
                _buffer[id].push_back(bytes);
            }

        });
        row::printer pr(this->_sstable_schema, column_kind::regular_column, cr.cells());
        trace("consume_clustering_row") << pr << std::endl;

    }

    void consume(range_tombstone &rt) {
        if (!is_test_ks()) return;

        trace("consume_range_tombstone") << rt << std::endl;
    }

    void consume_end_of_partition() {
        if (!is_test_ks()) return;

        trace("consume_end_of_partition") << std::endl;
    }

private:


    template<typename ValueType>
    std::vector<ValueType> deserialize(const std::vector<bytes> &cell_vec, const column_definition &def) {
        std::vector<ValueType> vec;
        vec.reserve(cell_vec.size());
        for (const auto &cell: cell_vec) {
            const data_value value = def.type->deserialize_value(cell);
            vec.push_back(value_cast<ValueType>(value));
        }
        return vec;
    }

    template<typename Writer, typename ValueType>
    void write_column(seastarized::RowGroupWriter *rgw, const std::vector<bytes> &cells, const column_definition &def) {
        std::vector<ValueType> deserialized_values = deserialize<ValueType>(cells, def);
        Writer *col_writer = static_cast<Writer *>(rgw->NextColumn().get0());
        col_writer->WriteBatch(deserialized_values.size(), nullptr, nullptr, deserialized_values.data()).get0();
    }

    void write_byte_array(seastarized::RowGroupWriter *rgw,
                          const std::vector<bytes> &cells,
                          const column_definition &def) {
        seastarized::ByteArrayWriter *col_writer = static_cast<seastarized::ByteArrayWriter *>(rgw->NextColumn().get0());

        for (const auto &cell: cells) {
            // TODO this is probably bad, need to use deserialize_value
            sstring str = def.type->to_string(cell);
            parquet::ByteArray value;
            value.ptr = (const uint8_t *) (str.c_str());
            value.len = str.size();
            int16_t definition_level = 1;
            col_writer->WriteBatch(1, &definition_level, nullptr, &value).get();
        }
    }


    void flush_buffer() {
        // TODO split into row groups more accurately
        //  now we write into one big row group
        auto rgw = _pq_file_writer->AppendRowGroup().get0();

        // In this iteration we assume that the ordering of columns
        // in the schema has not changed after convert_to_parquet method
        const auto &columns = _sstable_schema.all_columns();
        for (const auto &col_def : columns) {
            auto cells_it = _buffer.find(col_def.id);
            auto type = map_column_type(col_def.type->get_kind());
            if (cells_it == _buffer.end() || type == Type::UNDEFINED) {
                continue;
            }

            switch (type) {

                case Type::INT32:
                    write_column<seastarized::Int32Writer, int32_t>(rgw, cells_it->second, col_def);
                    break;
                case Type::BYTE_ARRAY:
                    write_byte_array(rgw, cells_it->second, col_def);
                    break;
                default:
                    break;

            }

        }
    }

public:

    void consume_end_of_stream() {
        if (!is_test_ks()) return;
        trace("consume_end_of_stream") << std::endl;
        flush_buffer();
    }

};

}