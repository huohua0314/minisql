#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include<iomanip>
#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

#define DB_NAME_LEN  20
#define DB_TABLE_LEN 20
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}
ExecuteEngine::ExecuteEngine(DBStorageEngine * dbs)
{
  current_db_="./databases/executor_test.db";
  dbs_[current_db_] = dbs;
}
std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  #ifdef EXECUTE_DEBUG
    LOG(WARNING) << "Begin ExecutePlan-------" << std::endl;
  #endif
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  #ifdef EXECUTE_DEBUG
    LOG(WARNING) << "End ExecutePlan-------" << std::endl;
  #endif
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  if(context ==nullptr)
  {
    cout << "choose databaase";
    return DB_FAILED;
  }
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
 if(ast->type_!=kNodeCreateDB)
 {
  cout << "create dababase failed";
  return DB_FAILED;
 }

  string db_name(ast->child_->val_);
  
  if(db_name.size()>36)
  {
    cout << "databse name too long "<<std::endl;
    return DB_FAILED;
  }

  if(dbs_.count(db_name)==1)
  {
    return DB_ALREADY_EXIST;
  }

  DBStorageEngine * db = new DBStorageEngine(db_name,true);
  dbs_.insert(make_pair(db_name,db));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
  if(ast->type_!=kNodeDropDB)
  {
    return DB_FAILED;
  }

  string table_name(ast->child_->val_);
  if(dbs_.count(table_name)==0)
  {
    return DB_NOT_EXIST;
  }

  vector<TableInfo *> table_infos;
  dbs_[table_name]->catalog_mgr_->GetTables(table_infos);
  for(auto iter:table_infos)
  {
    dbs_[table_name]->catalog_mgr_->DropTable(iter->GetTableName());
  }
  delete dbs_[table_name];
  dbs_.erase(table_name);
  string db_file_name_ = "./databases/" + table_name;
  remove(db_file_name_.c_str());
  current_db_.clear();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context){
  int len = DB_TABLE_LEN;
  for(auto iter: dbs_)
  {
    if(iter.first.size()>len)
    {
      len = iter.first.size();
    }
  }

  DrawUp(len);
  for(auto iter:dbs_)
  {
    DrawName(iter.first,len);
    DrawUp(len);
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
  if(ast->type_!=kNodeUseDB)
  {
    cout << "undefine error" << std::endl;
  }

  string db_name(ast->child_->val_);
  if(dbs_.count(db_name)==0)
  {
    cout << "database " << db_name << "not exist" << endl;
    return DB_NOT_EXIST;
  }
  
  current_db_ = db_name;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
  if(ast->type_!=kNodeShowTables)
  {
    return DB_FAILED;
  }
  if(dbs_.count(current_db_)==0)
  {
    cout << "no databases choosed!" <<std::endl ;
    return DB_FAILED;
  }

  vector<TableInfo *> table_infoes;
  int max_len=DB_TABLE_LEN;
  context->GetCatalog()->GetTables(table_infoes);
  for(auto iter:table_infoes)
  {
    int len = iter->GetTableName().size();
    if(len > max_len)
      max_len = len;
  }

  DrawUp(max_len);
  for(auto iter:table_infoes)
  {
    DrawName(iter->GetTableName(),max_len);
    cout << "|";
    DrawUp(max_len);
  }

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
 if(ast->type_!=kNodeCreateTable)
  {
    return DB_FAILED;
  }
  if(dbs_.count(current_db_)==0)
  {
    cout << "no databases choosed!" <<std::endl ;
    return DB_FAILED;
  }
  ast = ast->child_;
  string table_name(ast->val_);
  vector<Column *> columns;
  int index =0;

  if(table_name.size()>36)
  {
    cout << "table name too long "<<std::endl;
    return DB_FAILED;
  }
  ast = ast->next_;
  if(ast == nullptr || ast->type_ != kNodeColumnDefinitionList)
  {
    return DB_FAILED;
  }

  ast = ast->child_;
  while(ast!=nullptr && ast->type_ != kNodeColumnList)
  {
    string line(ast->child_->val_);
    string return_type(ast->child_->next_->val_);
    TypeId type;
    int len;
    string column_type;
    bool ischar = false;
    
    if(ast->val_!=nullptr)
    {
      column_type = string(ast->val_);
    }
    if(return_type.compare("int")==0) 
    {
      type = TypeId::kTypeInt;
    }
    else
    if(return_type.compare("char")==0) 
    {
      ischar = true;
      if(judge(len,ast->child_->next_->child_->val_))
      {
        type = TypeId::kTypeChar;
      }
      else
      {
        cout << "char define error" <<std::endl;
        return DB_FAILED;
      }
    }
    else
    if(return_type.compare("float")==0)
    {
      type = TypeId::kTypeFloat;
    }
    if(line.size()>36)
    {
      cout <<"column name too long" <<std::endl;
      return DB_FAILED;
    }
    if(column_type.compare("unique")==0 || !column_type.compare("primary keys"))
    { 
      if(type ==TypeId::kTypeChar)
        columns.push_back(new Column(line,type,len,index,false,true));
      else
        columns.push_back(new Column(line,type,index,false,true));
    }
    else 
    {
      if(type == TypeId::kTypeChar)
        columns.push_back(new Column(line,type,len,index,true,false));
      else
        columns.push_back(new Column(line,type,index,true,false));
    }
    index ++;
    ast = ast->next_;
  }
  while(ast!=nullptr)
  {
    string cons(ast->val_);
    string col(ast->child_->val_);
    if(cons.compare("primary keys")==0 || cons.compare("unique")==0)
    {
      for(auto iter:columns)
      {
        if(iter->GetName().compare(col)==0)
        {
          iter->SetNullable(false);
          iter->SetUnique(true);
          break;
        }
      }
    }
    else if(0)
    {

    }
    ast = ast->next_;
  }
  Schema* schema = new Schema(columns,true) ;
  TableInfo * dump;
  context->GetCatalog()->CreateTable(table_name,schema,nullptr,dump);
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
  if(context==nullptr)
  {
    cout << "choose databases" << endl;
    return DB_FAILED;
  }

  if(ast->type_!=kNodeDropTable)
  {
    return DB_FAILED;
  }
  string table_name(ast->child_->val_);
  context->GetCatalog()->DropTable(table_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
  if(context==nullptr)
  {
    cout << "choose databases" << endl;
    return DB_FAILED;
  }

  if(ast->type_!=kNodeShowIndexes)
  {
    return DB_FAILED;
  }
  
  int m_index_len = DB_NAME_LEN ;
  int m_table_len = DB_TABLE_LEN;

  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>> temp = 
  context->GetCatalog()->GetAllIndexs();
  for(auto iter:temp)
  {
    for(auto it:iter.second)
    {
      if(it.first.size() >m_index_len)
      {
        m_index_len =it.first.size();
      }
    }
    if(iter.second.size()>m_table_len)
    {
      m_table_len = iter.second.size();
    }
  }
  DrawUp(m_index_len + m_table_len+5);
  DrawName_("Index_name",m_index_len);
  DrawName_("Table_name",m_table_len);
  cout<<"|";
  cout << endl;
  DrawUp(m_index_len + m_table_len+5);
  for(auto iter:temp)
  {
    for(auto it:iter.second)
    {
      DrawName_(it.first,m_index_len);
      DrawName_(iter.first,m_table_len);
      cout <<"|";
      cout <<endl;
    }
    DrawUp(m_index_len + m_table_len+5);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
   if(context==nullptr)
  {
    cout << "choose databases" << endl;
    return DB_FAILED;
  }

  if(ast->type_!=kNodeCreateIndex)
  {
    return DB_FAILED;
  }

  ast = ast->child_;
  string index_name(ast->val_);
  ast = ast->next_;
  string table_name(ast->val_);
  ast = ast->next_->child_;
  vector<string> index_keys;

  TableInfo * table_info;
  IndexInfo *dump;
  string dump_string;
  dberr_t test = context->GetCatalog()->GetTable(table_name,table_info);
  if(test!=DB_SUCCESS)
    return test;
  
  while(ast!=nullptr)
  {
    index_keys.push_back(string(ast->val_));
    ast = ast->next_;
  }

  return context->GetCatalog()->CreateIndex(table_name,index_name,index_keys,nullptr,dump,dump_string);

}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
    if(context==nullptr)
  {
    cout << "choose databases" << endl;
    return DB_FAILED;
  }

  if(ast->type_!=kNodeDropIndex)
  {
    return DB_FAILED;
  }

  string index_name(ast->child_->val_);
  return context->GetCatalog()->DropIndex(index_name);
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string name (ast->child_->val_);
  name = "../testfile/" + name;
  ifstream in_file;
  in_file.open(name);
  if(in_file.is_open())
  {
    string cmd;
    while(getline(in_file,cmd))
    {
      YY_BUFFER_STATE yy_buffer = yy_scan_string(cmd.c_str());
      yy_switch_to_buffer(yy_buffer);
      MinisqlParserInit();
      yyparse();
      Execute(MinisqlGetParserRootNode());
    }
    return DB_SUCCESS;
  }
  else
  {
    cout <<"file open fail" << endl;
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
  if(ast->type_!=kNodeQuit)
  {
    return DB_FAILED;
  }

  return DB_QUIT;
}
bool ExecuteEngine::judge(int &len,const string number)
{
  int k = 0;
  for(int i=0;i<number.size();i++)
  {
    if(number[i]<'0' || number[i]>'9')
    {
      return false;
    }
    k = k * 10 + number[i] - '0';
  }
  len = k;
  return true;
}

void ExecuteEngine::DrawUp(int len)
{
  cout << "+" ;
  for(int i=0;i<len+4;i++)
  {
    cout <<"-";
  }
  cout <<"+" << endl;
}
void ExecuteEngine:: DrawName(const string name,int len)
{
  DrawName_(name,len);
  cout << endl;
}
void ExecuteEngine:: DrawName_(const string name,int len)
{
  cout <<"|";
  cout <<"  ";
  cout <<std::left << setw(len) <<name << "  " ;
}

