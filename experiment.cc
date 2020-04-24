#include <seastar/parquet/parquet/column_writer.h>
#include <mutation_fragment.hh>
#include <seastar/parquet/parquet/file_writer.h>
#include <seastar/include/seastar/core/app-template.hh>
#include <seastar/include/seastar/core/thread.hh>
#include <seastar/include/seastar/parquet/parquet/schema.h>

using namespace std;
using namespace parquet;
using namespace schema;


constexpr int rows_per_rowgroup_ = 50;


class Dummy {

public:

    std::shared_ptr<seastarized::FileFutureOutputStream> file_ostream;
    std::shared_ptr<parquet::seastarized::ParquetFileWriter> file_writer;

    Dummy() : file_ostream{init2()}, file_writer{init()} {}

    ~Dummy() {
        file_writer->Close().get();
        file_ostream->Close().get();
    }

    void check() {
        if (file_ostream) {
            std::cout << "initialized\n";
        } else {
            std::cout << "uninit\n";
        }
    }


    std::shared_ptr<seastarized::FileFutureOutputStream> init2() {
        std::string file = "/tmp/pq-test.parquet";

        seastar::open_flags oflags =
                seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate;
        seastar::file out_f = seastar::open_file_dma(file, oflags).get0();
        return std::make_shared<seastarized::FileFutureOutputStream>(seastar::make_file_output_stream(out_f));
    }

    std::shared_ptr<seastarized::ParquetFileWriter> init() {

        WriterProperties::Builder prop_builder;
        prop_builder.compression(parquet::Compression::SNAPPY);
        std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

        // Create a ParquetFileWriter instance
        auto schema_ = setup_schema();
        return parquet::seastarized::ParquetFileWriter::Open(file_ostream, schema_, writer_properties).get0();
    }

    std::shared_ptr<GroupNode> setup_schema() {
        parquet::schema::NodeVector fields;
        fields.push_back(PrimitiveNode::Make("field1", Repetition::OPTIONAL, Type::INT32,
                                             ConvertedType::NONE));
        fields.push_back(PrimitiveNode::Make("field2", Repetition::OPTIONAL, Type::BYTE_ARRAY,
                                             ConvertedType::NONE));
        return std::static_pointer_cast<GroupNode>(
                GroupNode::Make("schema", Repetition::REQUIRED, fields));
    }


    void write_example_parquet() {

        auto row_group_writer = file_writer->AppendRowGroup().get0();

        // Write the Int32 column
        parquet::seastarized::Int32Writer *int32_writer =
                static_cast<parquet::seastarized::Int32Writer *>(row_group_writer->NextColumn().get0());

        std::vector<int32_t> values = {1, 2};
//
        int16_t jp[] = {0, 1};
        int32_writer->WriteBatch(values.size(), jp, nullptr, values.data()).get0();


        // Write the Int32 column
        parquet::seastarized::ByteArrayWriter *ba_writer = static_cast<parquet::seastarized::ByteArrayWriter *>(row_group_writer->NextColumn().get0());


        std::string s1 = "HELLO";
        std::string s2 = "WORLD";
        std::vector<parquet::ByteArray> v =
                {
                        parquet::ByteArray(static_cast<uint32_t>(s1.size()),
                                           reinterpret_cast<const uint8_t *>(s1.c_str())),
                        parquet::ByteArray(static_cast<uint32_t>(s2.size()),
                                           reinterpret_cast<const uint8_t *>(s2.c_str())),
                };

        int16_t dl[] = {0, 0};
        v.data();
        ba_writer->WriteBatch(1, dl, nullptr, nullptr).get0();
        ba_writer->WriteBatch(1, dl, nullptr, nullptr).get0();
        check();


    }
};


int main(int argc, char **argv) {
//    A a;
//    a.stuff();
    seastar::app_template app;
    app.run(argc, argv, []() {
        return seastar::async([]() {
            Dummy a;
            a.check();
            a.write_example_parquet();
        });
    });
}