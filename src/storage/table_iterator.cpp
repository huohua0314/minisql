#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(){
  rowId.Set(INVALID_PAGE_ID,INVALID_PAGE_ID);
}

TableIterator::TableIterator(page_id_t const first_page,BufferPoolManager * buffer,Schema * s)
{
  if(first_page == INVALID_PAGE_ID)
  {
    ASSERT(false,"call interator on INVAILD_PAGE ID");
  }
  buffer_pool = buffer;
  schema_ = s;
  auto page = reinterpret_cast<TablePage *>(buffer->FetchPage(first_page));
  if(page==nullptr)
  {
    LOG(WARNING) << "Iterator fetch page fail" << std::endl;
    count = 0;
    buffer->UnpinPage(first_page,false);
    rowId.Set(INVALID_PAGE_ID,INVALID_PAGE_ID);
    return ;
  }
  if(!page->GetFirstTupleRid(&rowId))
  {
     LOG(WARNING) << "Iterator fetch first row failed" << std::endl;
     count = 0;
     buffer->UnpinPage(first_page,false);
    rowId.Set(INVALID_PAGE_ID,INVALID_PAGE_ID);
     return ;
  }
  #ifdef RECORD_DEBUG
  LOG(INFO) << "iterator begin() get pageId:" << rowId.GetPageId() <<" solt_num:" << rowId.GetSlotNum()<<std::endl;
  #endif
  row.SetRowId(rowId);
  page->GetTuple(&row,schema_,nullptr,nullptr);
  count = 1;
  buffer->UnpinPage(first_page,false);
}

TableIterator::TableIterator(const TableIterator &other) {
 
  rowId = other.rowId;
  buffer_pool = other.buffer_pool;
  schema_ = other.schema_;
  count = other.count;
  row = other.row;
}

TableIterator::~TableIterator() {
  row.destroy()  ;
}



 Row &TableIterator::operator*() {
  return row;
}

Row *TableIterator::operator->() {
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  rowId = itr.rowId;
  buffer_pool = itr.buffer_pool;
  count = itr.count;
  schema_ = itr.schema_;
  row = itr.row; 
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(buffer_pool->FetchPage(row.GetRowId().GetPageId()));
  RowId next;
  int flag;
  row.destroy();
  if(page->GetNextTupleRid(rowId,&next))
  {
    flag = 1;
    count++;
    rowId.Set(next.GetPageId(),next.GetSlotNum());
    row.SetRowId(rowId);
    page->GetTuple(&row,schema_,nullptr,nullptr);
    buffer_pool->UnpinPage(rowId.GetPageId(),false);
    return *this;
  }
  else
  {
    buffer_pool->UnpinPage(rowId.GetPageId(),false);
    page_id_t next_page = page->GetNextPageId();
    if(next_page == INVALID_PAGE_ID)
    {
      rowId.Set(INVALID_PAGE_ID,INVALID_PAGE_ID);
      return *this;
    }
    else
    {
      page = reinterpret_cast<TablePage *>(buffer_pool->FetchPage(next_page));
      if(page->GetFirstTupleRid(&rowId))
      {
          row.SetRowId(rowId);
          page->GetTuple(&row,schema_,nullptr,nullptr);
          buffer_pool->UnpinPage(rowId.GetPageId(),false);
          return *this;
      }
      else
      {
        ASSERT(false,"iterator false");
      }
    }
  }

}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp(*this);
  ++*this;
  return TableIterator(temp);
}
