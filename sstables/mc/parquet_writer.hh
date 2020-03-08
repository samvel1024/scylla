
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

std::ofstream _trace = std::ofstream("/tmp/parquet-trace.log", std::ofstream::binary);

using pq_schema = std::shared_ptr<schema::GroupNode>;
using pq_file_writer = std::shared_ptr<seastarized::ParquetFileWriter>;

using sstable_schema = ::schema;

class parquet_writer {

private:
    const std::string _file_tag;
    const sstable_schema &_sstable_schema;
    const pq_schema _pq_schema;
    seastar::file _pq_file;
    const pq_file_writer _pq_writer;

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
                return Type::UNDEFINED;
        }
    }

    pq_schema convert_to_parquet(const sstable_schema &src_schema) {
        schema::NodeVector fields;

        const auto &columns = src_schema.all_columns();
        for (const auto &col_def : columns) {
            auto name = col_def.name_as_text();
            auto sstable_type = col_def.type->get_kind();
            auto parquet_type = map_column_type(sstable_type);
            fields.push_back(schema::PrimitiveNode::Make(
                    name, Repetition::OPTIONAL,
                    parquet_type, ConvertedType::NONE));
            trace("convert_to_parquet") << name << " Type: " << int(sstable_type) << std::endl;
        }

        return std::static_pointer_cast<schema::GroupNode>(
                schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));
    }

    pq_file_writer create_parquet_writer(const seastar::file &file, const pq_schema &schema) {
        auto out_file = std::make_shared<seastarized::FileFutureOutputStream>(
                seastar::make_file_output_stream(file));
        return seastarized::ParquetFileWriter::Open(out_file, schema).get0();
    }

    seastar::file open_parquet_file(const seastar::sstring &sstable_file) {
        seastar::sstring parquet_file = sstable_file + ".parquet";
        seastar::open_flags flags =
                seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate;
        return seastar::open_file_dma(parquet_file, flags).get0();
    }

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

public:

    parquet_writer(seastar::sstring &&filename, const sstable_schema &schema) :
            _file_tag(init_trace_tag(std::string(filename.c_str()))),
            _sstable_schema(schema),
            _pq_schema(convert_to_parquet(schema)),
            _pq_file(std::move(open_parquet_file(filename))),
            _pq_writer(create_parquet_writer(_pq_file, _pq_schema)) {
        trace("construct") << filename << std::endl;
    }

    ~parquet_writer() {
        _pq_file.close().get();
        _pq_writer->Close().get();
    }

    void consume_new_partition(const dht::decorated_key &dk) {
        auto &pk = dk._key;
        auto vec = pk.explode(this->_sstable_schema);

        for (bytes &v: vec) {
            const char *str = reinterpret_cast<const char *>(v.c_str());
            std::string string(str, v.size());
            trace("consume_new_partition PK : ") << string << std::endl;
        }

    }

    void consume(tombstone t) {
        trace("consume_tombstone") << t << std::endl;
    }

    void consume(static_row &sr) {
        try {
            row::printer pr(this->_sstable_schema, column_kind::regular_column, sr.cells());
            trace("consume_static_row ") << pr << std::endl;
        } catch (...) {
        }
    }


    void consume(clustering_row &cr) {
        try {
            row::printer pr(this->_sstable_schema, column_kind::regular_column, cr.cells());
            trace("consume_clustering_row") << pr << std::endl;
        } catch (...) {
        }
    }

    void consume(range_tombstone &rt) {
        trace("consume_range_tombstone") << rt << std::endl;
    }

    void consume_end_of_partition() {
        trace("consume_end_of_partition") << std::endl;
    }

    void consume_end_of_stream() {
        trace("consume_end_of_stream") << std::endl;
    }


};

}