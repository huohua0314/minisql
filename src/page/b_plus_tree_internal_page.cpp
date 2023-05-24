#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1;
  int right = GetSize() - 1;
  GenericKey * temp;
  while(right>=left)
  {
    int middle = (left + right) /2;
    #ifdef BTREE_DEBUG
      LOG(INFO) <<"begin look up in InternalPage"<<std::endl<< "middle value:" << middle <<" left:"<<left << " right:" << right<< std::endl;
    #endif
    temp = KeyAt(middle) ;
    if(KM.CompareKeys(temp,key)>0)
    {
      right = middle-1;
    }
    else if(KM.CompareKeys(temp,key)<0)
    {
      left = middle+1;
    }
    else
    {
      return ValueAt(middle);
    }
  
  }
  #ifdef BTREE_DEBUG
  std::cout << "lookup page_id:" <<ValueAt(left-1) <<std::endl;
  LOG(INFO) <<"Endof Lookup"<<std::endl;
  #endif
  return ValueAt(left-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetValueAt(0,old_value);
  SetKeyAt(1,new_key);
  SetValueAt(1,new_value);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index = ValueIndex(old_value);
   if(GetSize()==GetMaxSize())
  {
    LOG(WARNING) << "internal page full, need spilt" << std::endl;
  }
  if(index==-1)
  {
    ASSERT(false,"InteranlPage out of size");
    LOG(WARNING) << "Insert old value not found" << std::endl;
  }
  else
  {
    for(int i=GetSize()-1;i>index;i--)
    {
      page_id_t temp = ValueAt(i) ;
      GenericKey * key = KeyAt(i);
      SetKeyAt(i+1,key);
      SetValueAt(i+1,temp);
    }
    SetKeyAt(index+1,new_key);
    SetValueAt(index+1,new_value);
    IncreaseSize(1);
    LOG(WARNING) << GetSize() <<std::endl;
    return GetSize();
  }
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize() /2;
  int remain_size = GetSize() - GetSize() / 2;
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "remove size:" << size << "remain size" << remain_size <<std::endl;
  #endif

  recipient -> CopyNFrom(PairPtrAt(remain_size),size,buffer_pool_manager);
  SetSize(remain_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int in_size = GetSize();
    if(in_size + size > GetMaxSize())
    {
      ASSERT(false,"CopyNFrom fail");
    }
    memcpy(pairs_off + pair_size*GetSize(),src,size*pair_size);
    for(int i=0;i<size;i++)
    {
      auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(ValueAt(GetSize()+i)));
      page->SetParentPageId(GetPageId());
      buffer_pool_manager->UnpinPage(ValueAt(GetSize()+i),true);
    }
    SetSize(GetSize() + size);
  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  for(int i=index+1;i<GetSize();i++)
  {
    SetKeyAt(i-1,KeyAt(i));
    SetValueAt(i-1,ValueAt(i));
  }
  SetSize(GetSize()-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "Begin MoveallTo" << std::endl;
  #endif
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);
  recipient->CopyNFrom((void *) KeyAt(1),GetSize()-1,buffer_pool_manager);
  SetSize(0);
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "END MoveallTo" << std::endl;
  #endif
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(middle_key,ValueAt(0),buffer_pool_manager);
  for(int i=1;i<GetSize();i++)
  {
    GenericKey * temp;
    page_id_t page;
    temp = KeyAt(i);
    page = ValueAt(i);
    SetKeyAt(i-1,temp);
    SetValueAt(i-1,page);
  }
  IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int index = GetSize();
  auto page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager->FetchPage(value));
  SetValueAt(index,value);
  SetKeyAt(index,key);
  IncreaseSize(1);
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    recipient->CopyFirstFrom(ValueAt(size-1),buffer_pool_manager);
    recipient->SetKeyAt(1,middle_key);
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  for(int i=GetSize()-1;i>=0;i--)
  {
    SetValueAt(i+1,ValueAt(i)) ;
    SetKeyAt(i+1,KeyAt(i));
  }
  IncreaseSize(1);
  SetValueAt(0,value);
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value));
  page ->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
}