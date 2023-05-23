#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_ + LEAF_PAGE_HEADER_SIZE)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetKeySize(key_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  for(int i=0;i<GetSize();i++)
  {
    if(KM.CompareKeys(KeyAt(i),key)>0)
      return i;
  }
  LOG(WARNING) << "could fund index" <<std::endl;
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    // replace with your own code
    return make_pair(nullptr, RowId());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int i;
  #ifdef BTREE_DEBUG
    LOG(INFO) << "begin insert into LeafPage------------" <<endl; 
    cout <<" RowId PageId:" << value.GetPageId() <<" SoltNum:"<<value.GetSlotNum() <<endl;
  #endif
  if(GetSize()==GetMaxSize())
  {
    LOG(WARNING) << "leaf page full, need spilt" << std::endl;
  }
  for(i=0;i<GetSize();i++)
  {
    if(KM.CompareKeys(key,KeyAt(i))>0)
    {
      continue;
    }
    else if(KM.CompareKeys(key,KeyAt(i)) < 0)
    {
      break;
    }
    else
    {
      LOG(WARNING) << "insert same value end of insert into leafpage--------" << std::endl;
      return GetSize();
    }
  }
  int index = i;
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "index in insert leaf:" << index << std::endl;
  #endif
  for(i = GetSize()-1;i>=index;i--)
  {
    SetValueAt(i+1,ValueAt(i));
    SetKeyAt(i+1,KeyAt(i));
  }
  SetValueAt(index,value);
  SetKeyAt(index,key);
  IncreaseSize(1);
  #ifdef BTREE_DEBUG
    LOG(INFO) << "end insert into LeafPage----------" <<endl; 
  #endif
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize() /2;
  int remain_size = GetSize() - size;
  recipient -> CopyNFrom(PairPtrAt(remain_size),size);
  SetSize(remain_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  memcpy(pairs_off + GetSize() * pair_size,src,size * val_off);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
  TODO: binary search
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "begin loop up in leaf, size is:" << GetSize() << std::endl;
  #endif
  for(int i=0;i<GetSize();i++)
  {
    if(KM.CompareKeys(key,KeyAt(i))==0)
    {
      value = ValueAt(i);
      #ifdef BTREE_DEBUG
      LOG(WARNING) << "end loop up in leaf" << std::endl;
      #endif
      return true;
    }
  }
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "end loop up in leaf" << std::endl;
  #endif
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */

int LeafPage::findIndex(const GenericKey *key, const KeyManager &KM)
{
   #ifdef BTREE_DEBUG
      LOG(WARNING) << "begin findIndex in leaf" << std::endl;
    #endif
    for(int i=0;i<GetSize();i++) 
    {
      if(KM.CompareKeys(key,KeyAt(i))==0)
    {
      #ifdef BTREE_DEBUG
      LOG(WARNING) << "end loop up in leaf" << std::endl;
      #endif
      return i;
    }
    }
    return -1;
}
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int pos = findIndex(key,KM);
  if(pos == -1) 
    return GetSize();
  for(int i=pos+1;i<GetSize();i++)
  {
    SetValueAt(i-1,ValueAt(i));
    SetKeyAt(i-1,KeyAt(i));
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  for(int i=0;i<GetSize();i++)
  {
    recipient->CopyLastFrom(KeyAt(i),ValueAt(i));
  }
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(0),ValueAt(0));
  for(int i=0;i<GetSize();i++)
  {
    SetKeyAt(i,KeyAt(i+1));
    SetValueAt(i,ValueAt(i+1));
  }
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetValueAt(GetSize(),value);
  SetKeyAt(GetSize(),key);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(KeyAt(GetSize()-1),ValueAt(GetSize()-1));
  IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  for(int i=GetSize()-1;i>=0;i++)
  {
    SetValueAt(i+1,ValueAt(i));
    SetKeyAt(i+1,KeyAt(i));
  }
  SetValueAt(0,value);
  SetKeyAt(0,key);
  IncreaseSize(1);
}