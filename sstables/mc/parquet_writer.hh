#include <seastar/parquet/parquet/column_writer.h>
#include <mutation_fragment.hh>
#include <tombstone.hh>
#include <dht/i_partitioner.hh>
#include <range_tombstone.hh>
#include <seastar/parquet/parquet/file_writer.h>
#include <seastar/core/sstring.hh>
#include <schema.hh>


std::ofstream _trace = std::ofstream("/tmp/parquet-trace.log", std::ofstream::binary);

class parquet_writer {

private:
    const schema &_schema;
    std::string _file;

    std::ostream &trace(std::string tag) {
        return _trace << "[" << tag << "]";
    }

public:

    parquet_writer(seastar::sstring &&filename, const schema &schema) : _schema(schema) {
        trace("construct") << filename << std::endl;
        std::string fn = std::string(filename.c_str());
        _file = fn.substr(fn.rfind('/') + 1);
    }

    ~parquet_writer() {
        trace("destruct") << std::endl << std::endl;
    }

    void consume_new_partition(const dht::decorated_key &dk) {
        auto &pk = dk._key;
        auto vec = pk.explode(this->_schema);

        for (bytes &v: vec) {
            const char *str = reinterpret_cast<const char *>(v.c_str());
            std::string string(str, v.size());
            trace("consume_new_partition PK : ") << string << std::endl;
        }

    }

    void consume(tombstone t) {
        trace("consume_tombstone") << std::endl;
    }

    void consume(static_row &sr) {
        try {
            row::printer pr(this->_schema, column_kind::regular_column, sr.cells());
            trace("consume_static_row ") << pr << std::endl;
        } catch (...) {
        }
    }


    void consume(clustering_row &cr) {
        try {
            row::printer pr(this->_schema, column_kind::regular_column, cr.cells());
            trace("consume_clustering_row") << pr << std::endl;
        } catch (...) {
        }
    }

    void consume(range_tombstone &rt) {
        trace("consume_range_tombstone") << std::endl;
    }

    void consume_end_of_partition() {
        trace("consume_end_of_partition") << std::endl;
    }

    void consume_end_of_stream() {
        trace("consume_end_of_stream") << std::endl;
    }


};



