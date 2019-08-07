// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdint>
#include <iostream>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
// #include <parquet/arrow/reader.h>
// #include <parquet/arrow/writer.h>
// #include <parquet/exception.h>


// Build dummy data to pass around
// To have some input data, we first create an Arrow Table that holds
// some data.
std::shared_ptr<arrow::Table> generate_table() {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder i64builder(pool);
  i64builder.AppendValues({1, 2, 3, 4, 5});
  std::shared_ptr<arrow::Array> i64array;
  i64builder.Finish(&i64array);

  arrow::DebugPrint(*i64array, 2);
  std::cout << "\n";

  arrow::StringBuilder strbuilder(pool);
  strbuilder.Append("some");
  strbuilder.Append("string");
  strbuilder.Append("content");
  strbuilder.Append("in");
  strbuilder.Append("rows");
  std::shared_ptr<arrow::Array> strarray;
  strbuilder.Finish(&strarray);

  arrow::DebugPrint(*strarray, 2);
  std::cout << "\n";

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

  return arrow::Table::Make(schema, {i64array, strarray});
}


// Define an extension type

class UUIDArray : public arrow::ExtensionArray {
 public:
  using ExtensionArray::ExtensionArray;
};

class UUIDType : public arrow::ExtensionType {
 public:
  UUIDType() : ExtensionType(arrow::int64()) {}

  std::string extension_name() const override { return "uuid"; }

  bool ExtensionEquals(const ExtensionType& other) const override {
    const auto& other_ext = static_cast<const ExtensionType&>(other);
    if (other_ext.extension_name() != this->extension_name()) {
      return false;
    }
    return true;
  }

  std::shared_ptr<arrow::Array> MakeArray(std::shared_ptr<arrow::ArrayData> data) const override {
    //DCHECK_EQ(data->type->id(), arrow::Type::EXTENSION);
    //DCHECK_EQ("uuid", static_cast<const ExtensionType&>(*data->type).extension_name());
    return std::make_shared<UUIDArray>(data);
  }

  arrow::Status Deserialize(std::shared_ptr<DataType> storage_type,
                     const std::string& serialized,
                     std::shared_ptr<DataType>* out) const override {
    if (serialized != "uuid-type-unique-code") {
      return arrow::Status::Invalid("Type identifier did not match");
    }
    //DCHECK(storage_type->Equals(*arrow::fixed_size_binary(16)));
    *out = std::make_shared<UUIDType>();
    return arrow::Status::OK();
  }

  std::string Serialize() const override { return "uuid-type-unique-code"; }
};

std::shared_ptr<arrow::DataType> uuid() { return std::make_shared<UUIDType>(); };

// std::shared_ptr<Array> ExampleUUID() {
//   auto storage_type = fixed_size_binary(16);
//   auto ext_type = uuid();

//   auto arr = ArrayFromJSON(
//       storage_type,
//       "[null, \"abcdefghijklmno0\", \"abcdefghijklmno1\", \"abcdefghijklmno2\"]");

//   auto ext_data = arr->data()->Copy();
//   ext_data->type = ext_type;
//   return MakeArray(ext_data);
// }

// TEST_F(TestExtensionType, IpcRoundtrip) {
//   auto ext_arr = ExampleUUID();



std::shared_ptr<arrow::Table> generate_table_extension() {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  arrow::Int64Builder i64builder(pool);
  i64builder.AppendValues({1, 2, 3, 4, 5});
  std::shared_ptr<arrow::Array> i64array;
  i64builder.Finish(&i64array);

  arrow::DebugPrint(*i64array, 2);
  std::cout << "\n";


  auto storage_type = arrow::int64();
  auto ext_type = uuid();
  arrow::Int64Builder extbuilder(pool);
  extbuilder.AppendValues({11111, 22222, 33333, 44444, 55555});
  std::shared_ptr<arrow::Array> storage_array;
  extbuilder.Finish(&storage_array);

  auto ext_data = storage_array->data()->Copy();
  ext_data->type = ext_type;
  auto ext_arr = MakeArray(ext_data);

  arrow::DebugPrint(*ext_arr, 2);
  std::cout << "\n";

  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("int", arrow::int64()), arrow::field("ext", uuid())});

  std::cout << "Schema:" << "\n";
  arrow::PrettyPrint(*schema, 2, &std::cout);
  std::cout << "\n";

  // std::cout << "Creating table:" << "\n";
  std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {i64array, ext_arr});

  // std::cout << "Done!" << "\n";
  return table;
}

// // #1 Write out the data as a Parquet file
// void write_parquet_file(const arrow::Table& table) {
//   std::shared_ptr<arrow::io::FileOutputStream> outfile;
//   PARQUET_THROW_NOT_OK(
//       arrow::io::FileOutputStream::Open("parquet-arrow-example.parquet", &outfile));
//   // The last argument to the function call is the size of the RowGroup in
//   // the parquet file. Normally you would choose this to be rather large but
//   // for the example, we use a small value to have multiple RowGroups.
//   PARQUET_THROW_NOT_OK(
//       parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
// }

// // #2: Fully read in the file
// void read_whole_file() {
//   std::cout << "Reading parquet-arrow-example.parquet at once" << std::endl;
//   std::shared_ptr<arrow::io::ReadableFile> infile;
//   PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(
//       "parquet-arrow-example.parquet", arrow::default_memory_pool(), &infile));

//   std::unique_ptr<parquet::arrow::FileReader> reader;
//   PARQUET_THROW_NOT_OK(
//       parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
//   std::shared_ptr<arrow::Table> table;
//   PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
//   std::cout << "Loaded " << table->num_rows() << " rows in " << table->num_columns()
//             << " columns." << std::endl;
// }

// int main(int argc, char** argv) {
//   std::shared_ptr<arrow::Table> table = generate_table();
//   write_parquet_file(*table);
//   read_whole_file();
//   read_single_rowgroup();
//   read_single_column();
//   read_single_column_chunk();
// }


#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);


// int main(int argc, char** argv) {
//   std::shared_ptr<arrow::Table> table = generate_table();
//   write_parquet_file(*table);
//   read_whole_file();
//   read_single_rowgroup();
//   read_single_column();
//   read_single_column_chunk();
// }



// auto RoundtripBatch = [](const std::shared_ptr<RecordBatch>& batch,
//                          std::shared_ptr<RecordBatch>* out) {
//   std::shared_ptr<io::BufferOutputStream> out_stream;
//   ASSERT_OK(io::BufferOutputStream::Create(1024, default_memory_pool(), &out_stream));
//   ASSERT_OK(ipc::WriteRecordBatchStream({batch}, out_stream.get()));

//   std::shared_ptr<Buffer> complete_ipc_stream;
//   ASSERT_OK(out_stream->Finish(&complete_ipc_stream));

//   io::BufferReader reader(complete_ipc_stream);
//   std::shared_ptr<RecordBatchReader> batch_reader;
//   ASSERT_OK(ipc::RecordBatchStreamReader::Open(&reader, &batch_reader));
//   ASSERT_OK(batch_reader->ReadNext(out));
// };


// TEST_F(TestExtensionType, IpcRoundtrip) {
//   auto ext_arr = ExampleUUID();
//   auto batch = RecordBatch::Make(schema({field("f0", uuid())}), 4, {ext_arr});

//   std::shared_ptr<RecordBatch> read_batch;
//   RoundtripBatch(batch, &read_batch);
//   CompareBatch(*batch, *read_batch, false /* compare_metadata */);

//   // Wrap type in a ListArray and ensure it also makes it
//   auto offsets_arr = ArrayFromJSON(int32(), "[0, 0, 2, 4]");
//   std::shared_ptr<Array> list_arr;
//   ASSERT_OK(
//       ListArray::FromArrays(*offsets_arr, *ext_arr, default_memory_pool(), &list_arr));
//   batch = RecordBatch::Make(schema({field("f0", list(uuid()))}), 3, {list_arr});
//   RoundtripBatch(batch, &read_batch);
//   CompareBatch(*batch, *read_batch, false /* compare_metadata */);
// }


// Status DebugPrint(const Array& arr, int indent) {
//   return PrettyPrint(arr, indent, &std::cout);
// }


int main(int argc, char** argv) {

  std::cout << "Generating a dummy table" << "\n";
  std::shared_ptr<arrow::Table> table = generate_table();
  std::cout << "Done" << "\n";

  std::cout << "Converting table to a record batch" << "\n";
  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::TableBatchReader converter(*table);
  converter.ReadNext(&batch);
  arrow::PrettyPrint(*batch, 2, &std::cout);
  std::cout << "\n";
  std::cout << "Done" << "\n";

  std::cout << "Writing record batch to IPC file" << "\n";
  std::shared_ptr<arrow::io::BufferOutputStream> out_stream;
  arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool(), &out_stream);
  arrow::ipc::WriteRecordBatchStream({batch}, out_stream.get());
  std::shared_ptr<arrow::Buffer> complete_ipc_stream;
  out_stream->Finish(&complete_ipc_stream);

  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  arrow::io::FileOutputStream::Open("arrow-example-ipc.arrow", &outfile);
  arrow::ipc::WriteRecordBatchStream({batch}, outfile.get());
  outfile->Close();

  arrow::RegisterExtensionType(std::make_shared<UUIDType>());

  std::cout << "Generating a dummy table with extension type" << "\n";
  std::shared_ptr<arrow::Table> table2 = generate_table_extension();
  std::cout << "Done" << "\n";

  std::cout << "Converting table to a record batch" << "\n";
  std::shared_ptr<arrow::RecordBatch> batch2;
  arrow::TableBatchReader converter2(*table2);
  converter2.ReadNext(&batch2);
  arrow::PrettyPrint(*batch2, 2, &std::cout);
  std::cout << "\n";
  std::cout << "Done" << "\n";

  std::cout << "Writing record batch to IPC file" << "\n";
  std::shared_ptr<arrow::io::FileOutputStream> outfile2;
  arrow::io::FileOutputStream::Open("arrow-example-ipc-extension.arrow", &outfile2);
  arrow::ipc::WriteRecordBatchStream({batch2}, outfile2.get());
  outfile2->Close();
  std::cout << "Done" << "\n";

  // std::vector<data_row> rows = {
  //     {1, 1.0, {1.0}}, {2, 2.0, {1.0, 2.0}}, {3, 3.0, {1.0, 2.0, 3.0}}};

  // std::shared_ptr<arrow::Table> table;
  // EXIT_ON_FAILURE(VectorToColumnarTable(rows, &table));

  // std::vector<data_row> expected_rows;
  // EXIT_ON_FAILURE(ColumnarTableToVector(table, &expected_rows));

  // assert(rows.size() == expected_rows.size());

  return EXIT_SUCCESS;
}
