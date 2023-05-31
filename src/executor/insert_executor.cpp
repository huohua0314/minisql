//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table);
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),all_Index);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row temp;
  RowId temp_id;
  while(child_executor_->Next(&temp,&temp_id))
  {
    bool flag = true;
    for(auto iter: all_Index)
    {
      vector<RowId> result;
      Row key_row;
      temp.GetKeyFromRow(table->GetSchema(),iter->GetIndexKeySchema(),key_row);
      if(iter->GetIndex()->ScanKey(key_row,result,nullptr) == DB_SUCCESS)
      {
        flag = false;
        break;
      }
    }
    int cur = 0;
    for(auto iter:table->GetSchema()->GetColumns())
    {
      if(temp.GetField(cur)->IsNull())
      {
        if(table->GetSchema()->GetColumn(cur)->IsNullable()==false)
        {
          flag = false;
          break;
        }
      }
      cur ++ ;
    }
    if(flag == true)
    {
      if(table->GetTableHeap()->InsertTuple(temp,nullptr))
      {
        for(auto it: all_Index)
        {
          dberr_t test;
          Row key_row;
          temp.GetKeyFromRow(table->GetSchema(),it->GetIndexKeySchema(),key_row);
          test = it->GetIndex()->InsertEntry(key_row,temp.GetRowId(),nullptr);
          ASSERT(test == DB_SUCCESS,"insert not success");
        }
      }
      return true ;
    }
  }
    return false;
  
}