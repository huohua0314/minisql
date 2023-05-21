#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include"table_heap.h"

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();
  explicit TableIterator(page_id_t const first_page,BufferPoolManager *,Schema * schema);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const
  {
     if(rowId.GetPageId()==INVALID_PAGE_ID &&rowId.GetSlotNum()== INVALID_PAGE_ID&&itr.rowId.GetPageId()==INVALID_PAGE_ID && itr.rowId.GetSlotNum()==INVALID_PAGE_ID)
      return true;
      return (rowId.GetPageId()==itr.rowId.GetPageId()) &&  (rowId.GetSlotNum()==itr.rowId.GetSlotNum()) && count ==itr.count;
  }

  inline bool operator!=(const TableIterator &itr) const
  {
    return !(*this == itr);
  }

  Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);

// private:
  public:
  Row row;
  RowId rowId;
  BufferPoolManager * buffer_pool;
  Schema * schema_;
  uint64_t count;
};

#endif  // MINISQL_TABLE_ITERATOR_H
