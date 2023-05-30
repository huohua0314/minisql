#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
bool Cmp(RowId a,RowId b)
{
  if(a.GetPageId()>b.GetPageId())
  {
    return true;
  }
  else if(a.GetPageId()==b.GetPageId())
  {
    return a.GetSlotNum() >= b.GetSlotNum();
  }
  else return false;
}

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  result.clear();
  vector<vector<RowId>> all_row_id;
  GetRowId(all_row_id,plan_->GetPredicate());
  result = all_row_id[0];
  for(int i=1;i<all_row_id.size();i++) 
  {
    vector <RowId> new_result;
    set_intersection(all_row_id[i].begin(),all_row_id[i].end(),result.begin(),result.end(),back_inserter(new_result),Cmp);
    result = new_result;
  }

}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  TableInfo * table;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table);
  auto heap = table->GetTableHeap();
  Row temp;
  RowId temp_id;
  while(1){
    if(cursor_ >= result.size()) 
    {
      return false;
    }
    else
    {
      temp_id = result[cursor_++];
      temp.SetRowId(temp_id);
      if(heap->GetTuple(&temp,nullptr))
      {
        Row key_row;
        temp.GetKeyFromRow(table->GetSchema(),GetOutputSchema(),key_row);
        if(plan_->need_filter_)
        {
          if(plan_->GetPredicate()->Evaluate(&temp).CompareEquals(Field(kTypeInt,1)))
          {
            *row = key_row;
            return true;
          }
        }
        else
        {
            *row = key_row;
            return true;
        }
      }
    }
  } 
}

void IndexScanExecutor::GetRowId(vector<vector<RowId>> &all , AbstractExpressionRef compare)
{
  if(compare->GetType() != ExpressionType::ComparisonExpression)
  {
    GetRowId(all,compare->GetChildAt(0));
    GetRowId(all,compare->GetChildAt(1));
    return ;
  }

  ASSERT(compare->GetType() ==  ExpressionType::ComparisonExpression,"not comparsion");
  auto column = std::reinterpret_pointer_cast<ColumnValueExpression > (compare->GetChildAt(0));
  auto constant = std::reinterpret_pointer_cast<ConstantValueExpression > (compare->GetChildAt(1));
  vector<RowId> temp ;
  uint32_t table_index = column->GetColIdx();
  vector<Field> key_field{constant->Evaluate(nullptr)};
  IndexInfo * index;
  for(auto iter:plan_->indexes_)
  {
    if(iter->GetIndexKeySchema()->GetColumn(0)->GetTableInd()==table_index)
    {
      index = iter;
      break;
    }
  }
  Row key(key_field);
  std::string com_type = compare->GetComparisonType();
  ASSERT(com_type!="INVAILD_COMPARE_TYPE","invaild compare type");
  index->GetIndex()->ScanKey(key,temp,nullptr,com_type);
  all.push_back(temp);
}