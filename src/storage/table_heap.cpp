#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
   #ifdef RECORD_DEBUG
      LOG(INFO) <<std::endl<<"Begin InsertTuple" <<std::endl;
    #endif 
  if(first_page_id_ == INVALID_PAGE_ID)
  {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
    ASSERT(page!=nullptr && first_page_id_!=INVALID_PAGE_ID,"new page fail");
    page->Init(first_page_id_,INVALID_TABLE_ID,nullptr,nullptr);
    buffer_pool_manager_->UnpinPage(first_page_id_,true);
  }

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while(page->GetPageId()!=INVALID_PAGE_ID)
  {
    if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_))
    {
      #ifdef RECORD_DEBUG
        std::cout<<"row.page_ID:" << row.GetRowId().GetPageId() << " row.soltnum" << row.GetRowId().GetSlotNum() <<std::endl;
      #endif 
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
      return true;
    }
    else
    {
      if(page->GetNextPageId()!=INVALID_PAGE_ID)
      {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      }
      else
        break;
    }
  }
  if(page->GetNextPageId()==INVALID_PAGE_ID)
  {
    page_id_t pre;
    page_id_t next;
    auto page2 = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next));
    if(next == INVALID_PAGE_ID || page2 ==nullptr)
    {
      LOG(WARNING) << "buffer pool full, couldn't new page" << std::endl;
      return false;
    }
    page->SetNextPageId(next);
    page2->Init(next,page->GetPageId(),log_manager_,txn);
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);

    if(page2->InsertTuple(row,schema_,txn,lock_manager_,log_manager_))
    {
      return true;
    }
    else
    {
      ASSERT(false,"Insert into new page fail");
      return false;
    }
  }
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple( Row &row, const RowId &rid, Transaction *txn) {
    #ifdef RECORD_DEBUG
      LOG(INFO) <<std::endl<<"Begin update Tuple" <<std::endl;
      std::cout<<"rid page id:" << rid.GetPageId()<<" solu_num" << rid.GetSlotNum() <<std::endl;
    #endif 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  Row old_row(rid);
  if(!page->GetTuple(&old_row,schema_,txn,lock_manager_))
  {
    LOG(WARNING) << "update tuple not exist" << std::endl;
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return false;
  }
  
  int flag = 0;
  int type;
  flag = page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
  switch(flag)
  {
    case 1: return true;buffer_pool_manager_->UnpinPage(page->GetPageId(),true);break;
    case 0: type = 1; break;
    case -1 :type = 2; break;
  }
  if(type == 2)
  {
    LOG(WARNING) << "UpdateTuple is deleted" << std::endl;
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return false;
  }
  else if(type == 1)
  {
    LOG(INFO) << "update recorf too large, now d and i" << std::endl;
    page->ApplyDelete(rid,txn,log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    InsertTuple(row,txn);
    LOG(INFO) << "new RowID is "<< " page_id:"<<row.GetRowId().GetPageId()<<" solt_num:"<< row.GetRowId().GetSlotNum()<<std::endl;
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page ==nullptr)
  {
    LOG(WARNING) << "get page falsed when applydelete" << std::endl;
    return;
  }
  page ->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);

}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  #ifdef RECORD_DEBUG
  LOG(INFO) << "Begin getuple page_id:" << row->GetRowId().GetPageId() << " solt num:" << row->GetRowId().GetSlotNum() <<std::endl;
  #endif;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page == nullptr)
  {
    LOG(WARNING) << "get nullptr when GetTuple" << std::endl;
    return false;
  }
  cout << "aaa"<< endl;
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  if(page->GetTuple(row,schema_,txn,lock_manager_))
  return true;
  else
    return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator(first_page_id_,buffer_pool_manager_,schema_);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator();
}
