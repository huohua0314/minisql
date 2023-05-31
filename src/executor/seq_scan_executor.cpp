//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  #ifdef EXECUTE_DEBUG
    LOG(WARNING) << "Seq Init" << std::endl;
  #endif
  TableInfo * table_info;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
  table_schema = table_info->GetSchema();
  table_iterator_ = table_info->GetTableHeap()->Begin(nullptr);
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  #ifdef EXECUTE_DEBUG
    LOG(WARNING) << "Seq Next" << std::endl;
  #endif
  const Schema * out_schema = GetOutputSchema() ;
  while(1)
  {
    if(table_iterator_ == TableIterator())
      return false;
    
    Row temp = *table_iterator_;
    auto exprs = plan_->filter_predicate_;
    if(exprs!=nullptr)
    {
      auto values = exprs->Evaluate(&temp);
      if(values.CompareEquals(Field(kTypeInt,1))) 
      {
        temp.GetKeyFromRow(table_schema,out_schema,*row);
        *rid = temp.GetRowId();
        row->SetRowId(temp.GetRowId());
        table_iterator_++;
        #ifdef EXECUTE_DEBUG
          LOG(INFO) << "RowId: page:" << rid->GetPageId()<<" solt_num:" <<rid->GetSlotNum()<<std::endl;
        #endif
        return true;
      }
      else
      {
        table_iterator_++;
      }
    }
    else
    {
       temp.GetKeyFromRow(table_schema,out_schema,*row);
        *rid = temp.GetRowId();
        row->SetRowId(temp.GetRowId());
        table_iterator_++;
        #ifdef EXECUTE_DEBUG
          LOG(INFO) << "RowId: page:" << rid->GetPageId()<<" solt_num:" <<rid->GetSlotNum()<<std::endl;
        #endif
        return true;
    }
  }
}
