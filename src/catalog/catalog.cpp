#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t length = 0;
  length += 4; //magic
  length += 4*2; //two size;
  length += 4*(table_meta_pages_.size()+index_meta_pages_.size());
  return length;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    std::cout <<"CatalogManager initialize:init is:"<<init<<"----------------" << std::endl;
    if(init == true)
    {
      catalog_meta_ = CatalogMeta::NewInstance();
      auto page = reinterpret_cast<char *>(buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID));
      catalog_meta_->SerializeTo(page);
      buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
      next_index_id_ = INVALID_INDEX_ID;
      next_table_id_ = INVALID_TABLE_ID;
    }
    else
    {
      auto page = reinterpret_cast<char *>(buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID));
      catalog_meta_ = CatalogMeta::DeserializeFrom(page);
      for(auto iter :catalog_meta_->table_meta_pages_)
      {
        char *temp = reinterpret_cast<char *>(buffer_pool_manager->FetchPage(iter.second));
        TableMetadata *table_meta;
        TableHeap *table_heap;
        TableInfo * table_info = TableInfo::Create();
        TableMetadata::DeserializeFrom(temp,table_meta);
        table_heap = TableHeap::Create(buffer_pool_manager,table_meta->GetFirstPageId(),table_meta->GetSchema(),nullptr,nullptr) ;
        table_info->Init(table_meta,table_heap);
        buffer_pool_manager->UnpinPage(iter.second,false);
        table_names_.insert(make_pair(table_info->GetTableName(),table_info->GetTableId()));
        tables_.insert(make_pair(table_info->GetTableId(),table_info));
        index_names_.insert(make_pair(table_info->GetTableName(),std::unordered_map<std::string, index_id_t>()));
      }
      for(auto iter : catalog_meta_->index_meta_pages_)
      {
        char *temp = reinterpret_cast<char *>(buffer_pool_manager->FetchPage(iter.second));
        IndexMetadata * index_meta;
        IndexMetadata::DeserializeFrom(temp,index_meta);
        IndexInfo *index_info = IndexInfo::Create();
        index_info->Init(index_meta,tables_[index_meta->GetTableId()],buffer_pool_manager);

        TableInfo *table_info = tables_[index_meta->GetTableId()];
        
        indexes_[index_meta->GetIndexId()] = index_info;
        index_names_[table_info->GetTableName()][index_meta->GetIndexName()] = index_meta->GetIndexId();
        buffer_pool_manager->UnpinPage(iter.second,false);
      }
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin CreateTable:" << table_name  <<std::endl;
  #endif
  if(table_names_.count(table_name)==1)
  {
    #ifdef CATALOG_DEBUG
    LOG(WARNING) << "end  CreateTable:" << table_name <<"table already exist" <<std::endl;
    #endif
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t new_table_id = next_table_id_++;
  page_id_t new_table_page;
  page_id_t new_page;
  char * buf = reinterpret_cast<char *>(buffer_pool_manager_->NewPage(new_table_page));

  TableHeap *new_table_heap = TableHeap::Create(buffer_pool_manager_,schema,nullptr,nullptr,nullptr);

  new_page = new_table_heap->GetFirstPageId();

  TableMetadata * new_table_meta = TableMetadata::Create(new_table_id,table_name,new_page,schema);
  
  TableInfo *new_table_info = TableInfo::Create();

  new_table_info->Init(new_table_meta,new_table_heap);
  table_names_[table_name] = new_table_id;
  tables_[new_table_id] = new_table_info;
  index_names_[table_name] = std::unordered_map<std::string, index_id_t>();
  catalog_meta_->table_meta_pages_[new_table_id] = new_table_page;

  new_table_meta->SerializeTo(buf);
  table_info = new_table_info;
  FlushCatalogMetaPage();
  buffer_pool_manager_->UnpinPage(new_table_page,true);
  #ifdef CATALOG_DEBUG
    LOG(WARNING) << "end  CreateTable:" << table_name <<"success " <<std::endl;
  #endif
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {

  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin GetTable:" << table_name  <<std::endl;
  #endif
  if(table_names_.count(table_name) == 0)
  {
    LOG(WARNING) << "Not found table:" << table_name<<std::endl;
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    table_info = tables_[table_names_[table_name]];
    return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin GetTables:"  <<std::endl;
  #endif
  vector<TableInfo *> temp;
  for(auto iter:tables_)
  {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
 
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin CreateIndex index:"<<table_name<<" index:"<<index_name  <<std::endl;
  #endif
  if(table_names_.count(table_name)==0)
  {
    LOG(WARNING) << "END CREATEINEDX" <<" table not exist"<<std::endl;
    return DB_TABLE_NOT_EXIST;
  }

  if (index_names_[table_name].count(index_name) == 1) {
    #ifdef CATALOG_DEBUG
    LOG(WARNING) << "end CreateIndex index:"<<table_name<<" index:"<<index_name <<"index exist" <<std::endl;
    #endif

    return DB_INDEX_ALREADY_EXIST;
  }

  page_id_t new_index_page;
  char * buf = reinterpret_cast<char *>(buffer_pool_manager_->NewPage(new_index_page));

  table_id_t table_id = table_names_[table_name];
  TableInfo * table_info = tables_[table_id];
  vector<uint32_t> key_map;
  for(auto iter:index_keys)
  {
    uint32_t index;
    dberr_t test;
    test = table_info->GetSchema()->GetColumnIndex(iter,index);
    if(test == DB_SUCCESS)
    {
      key_map.push_back(index);
    }
    else
      return test;
  }
  index_id_t new_index_id = next_index_id_ ++;
  IndexMetadata *new_index_meta = IndexMetadata::Create(new_index_id,index_name,table_id,key_map);
  IndexInfo *new_index_info = IndexInfo::Create();
  new_index_info->Init(new_index_meta,table_info,buffer_pool_manager_);
  new_index_meta->SerializeTo(buf);
  index_info = new_index_info;
  indexes_[new_index_id] = new_index_info;
  index_names_[table_name][index_name] = new_index_id;
  catalog_meta_->index_meta_pages_[new_index_id] = new_index_page;
  FlushCatalogMetaPage() ;
  buffer_pool_manager_->UnpinPage(new_index_page,true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin GetIndex" << "table:" <<table_name <<std::endl ;
  #endif
  if(table_names_.count(table_name)==0)
  {
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_.at(table_name).count(index_name)==0)
  {
    return DB_INDEX_NOT_FOUND;
  }

  index_info = indexes_.at(index_names_.at(table_name).at(index_name));

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin GetIndexe" << "table:" <<table_name <<std::endl ;
  #endif
  if(table_names_.count(table_name)==0)
  {
    return DB_TABLE_NOT_EXIST;
  }

  for(auto it:index_names_.at(table_name))
  {
    indexes.push_back(indexes_.at(it.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.count(table_name)==0) 
  {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t drop_table_id;
  page_id_t drop_page;
  drop_table_id = table_names_[table_name];
  table_names_.erase(table_name);
  tables_[drop_table_id]->GetTableHeap()->FreeTableHeap();

  delete tables_[drop_table_id];
  tables_.erase(drop_table_id);
  for(auto iter:index_names_[table_name])
  {
    delete indexes_[iter.second];
    indexes_.erase(iter.second);
    DropIndex(table_name,iter.first);
  }
  
  drop_page = catalog_meta_->table_meta_pages_[drop_table_id];
  buffer_pool_manager_->DeletePage(drop_page);

  catalog_meta_->table_meta_pages_.erase(drop_table_id);
  FlushCatalogMetaPage();
  return  DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.count(table_name)==0)
  {
    LOG(WARNING) << "END CREATEINEDX" <<" table not exist"<<std::endl;
    return DB_TABLE_NOT_EXIST;
  }

  if (index_names_[table_name].count(index_name) == 0) {
    #ifdef CATALOG_DEBUG
    LOG(WARNING) << "end CreateIndex index:"<<table_name<<" index:"<<index_name <<"index exist" <<std::endl;
    #endif

    return DB_INDEX_ALREADY_EXIST;
  }
  
  index_id_t drop_index_id;
  IndexInfo * drop_index_info;
  page_id_t drop_page;
  drop_index_id = index_names_.at(table_name).at(index_name);
  index_names_.at(table_name).erase(index_name);
  drop_index_info = indexes_.at(drop_index_id);
  indexes_.erase(drop_index_id);
  drop_page = catalog_meta_->index_meta_pages_[drop_index_id] ;
  catalog_meta_->index_meta_pages_.erase(drop_index_id);
  
  drop_index_info->GetIndex()->Destroy();
  delete drop_index_info;
  FlushCatalogMetaPage();
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto buf= reinterpret_cast<char *>( buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID));
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  #ifdef CATALOG_DEBUG
  LOG(WARNING) << "Begin GetTable" << "table_id:" <<table_id <<std::endl ;
  #endif
  if(tables_.count(table_id)==0)
  {
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    table_info = tables_[table_id];
    return DB_SUCCESS;
  }
}