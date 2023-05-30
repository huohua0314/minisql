//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),index_info_);
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info_);
  schema_ = table_info_->GetSchema();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row temp;
  RowId row_id;
  while(child_executor_->Next(&temp,&row_id))
  {
    bool flag = true;
    temp.SetRowId(row_id);

    Row update_row = GenerateUpdatedTuple(temp);

    table_info_->GetTableHeap()->UpdateTuple(update_row,update_row.GetRowId(),nullptr);
    for(auto iter:index_info_)
    {
      iter->GetIndex()->RemoveEntry(temp,temp.GetRowId(),nullptr);
      iter->GetIndex()->InsertEntry(update_row,update_row.GetRowId(),nullptr);
    }
    return true;
  }
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  ASSERT(src_row.GetFieldCount()==schema_->GetColumnCount(),"row don't equal schema");
  auto& update_attrs = plan_->GetUpdateAttr();
  vector<Field> fixed;
  for(int i=0;i<src_row.GetFieldCount();i++)
  {
    if(update_attrs.count(i)==1)  
    {
      fixed.push_back(update_attrs.at(i)->Evaluate(nullptr));
    }
    else
    {
      fixed.push_back(*src_row.GetField(i));
    }
  }
  Row a(fixed);
  a.SetRowId(src_row.GetRowId());
  return  a;
  
}