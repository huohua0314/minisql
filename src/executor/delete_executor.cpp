//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  dberr_t test = exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes_ );
  ASSERT(test == DB_SUCCESS,"get_index_fail") ;
  test = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info_);
  ASSERT(test == DB_SUCCESS,"get table_info fail");
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row temp;
  RowId row_id;
  while(child_executor_->Next(&temp,&row_id))
  {
    table_info_ -> GetTableHeap()->ApplyDelete(row_id,nullptr);
    for(auto iter: indexes_)
    {
      Row key_row;
      temp.GetKeyFromRow(table_info_->GetSchema(),iter->GetIndexKeySchema(),key_row);
      iter->GetIndex()->RemoveEntry(key_row,row_id,nullptr);
    }
    return true;
  }
  return false;
}