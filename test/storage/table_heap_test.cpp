#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }
  //   char *characters = new char[90];
  // Fields *fields =
  //       new Fields{Field(TypeId::kTypeInt, 10), Field(TypeId::kTypeChar, const_cast<char *>(characters), 90, true),
  //                  Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
  // Row row(*fields);
  // RowId temp(2,4);
  // RowId temp2(2,5);
  // table_heap->UpdateTuple(row,temp,nullptr);
  // table_heap->ApplyDelete(temp2,nullptr);
  // table_heap->UpdateTuple(row,temp2,nullptr);
  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    std::cout << "size:" <<size<<std::endl;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  TableIterator it = table_heap->Begin(nullptr);
  LOG(INFO) <<"first page:" <<table_heap->GetFirstPageId() << " it:" << it.rowId.GetPageId()<<std::endl;
  for(;it!=table_heap->End();it++)
  {
      LOG(INFO)<<it.rowId.GetPageId()<<std::endl;
      for(auto its:(*it).GetFields())
      {
        if(its->type_id_== kTypeChar)
        cout << its->value_.chars_ << std::endl;
      }
  }
  ASSERT_EQ(size, 0);
}
