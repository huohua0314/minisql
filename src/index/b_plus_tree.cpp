#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
      if(leaf_max_size_==UNDEFINED_SIZE)
      {
        int length = KM.GetKeySize() + sizeof(RowId);
        leaf_max_size_ = (((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / length) - 1);
        cout << "lenth:"<<length<<endl;
        cout <<"aaa" <<PAGE_SIZE - LEAF_PAGE_HEADER_SIZE<<endl;
      }
      if(internal_max_size_==UNDEFINED_SIZE)
      {
        int length = KM.GetKeySize() + sizeof(page_id_t);
        internal_max_size_ = (((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / length) - 1);

      }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  ASSERT(root_page_id_!=INVALID_PAGE_ID,"root empty!");
  LeafPage * temp = reinterpret_cast<LeafPage*>( FindLeafPage(key,root_page_id_,0));
  RowId temp_row;
    if(temp->Lookup(key,temp_row,processor_))
    {
      result.push_back(temp_row);
      buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
      return true;
    }
    else
    {
      buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
      LOG(WARNING) << "lookup fail" <<std::endl;
      return false;
    }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  ASSERT(key != nullptr,"nullptr!");
  if(root_page_id_ == INVALID_PAGE_ID)
  {
    StartNewTree(key,value);
    return true;
  }

  return InsertIntoLeaf(key,value,nullptr);

}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  #ifdef BTREE_DEBUG
    LOG(INFO) <<"Begin StartNewTree"<<std::endl;
    cout << "key:" ;
    cout.write(key->data,processor_.GetKeySize());
    cout <<" RowId PageId:" << value.GetPageId() <<" SoltNum:"<<value.GetSlotNum() <<endl;
  #endif
  page_id_t page_id;
  auto page = reinterpret_cast<BPlusTreeLeafPage * >( buffer_pool_manager_->NewPage(page_id));
  page->Init(page_id,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
  page->Insert(key,value,processor_) ;
  root_page_id_ = page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(page_id,true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  ASSERT(root_page_id_!=INVALID_PAGE_ID,"empty tree!");
  #ifdef BTREE_DEBUG
  LOG(INFO) <<"Begin InsertintoLEAf" <<std::endl;
  #endif
  page_id_t page_id = root_page_id_;
  LeafPage * leaf = reinterpret_cast<LeafPage*>( FindLeafPage(key,root_page_id_,0));
  if(!leaf->Insert(key,value,processor_))
  {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),false);
    return false;
  }
  else
  {
    if(leaf->GetSize() <= leaf_max_size_)
    {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
      return true;
    }
    auto new_node = Split(leaf,nullptr) ;
    InsertIntoParent(leaf,new_node->KeyAt(0),new_node,nullptr);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    return true;
  }
   #ifdef BTREE_DEBUG
    cout <<endl;
  #endif
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t page_id;
  auto page = reinterpret_cast<InternalPage *> (buffer_pool_manager_->NewPage(page_id));
  page->Init(page_id,node->GetParentPageId(),processor_.GetKeySize(),internal_max_size_);
  node->MoveHalfTo(page,buffer_pool_manager_);
  return page;
}

//TODO:buffer Unpin;
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t page_id;
   auto page = reinterpret_cast<BPlusTreeLeafPage * >( buffer_pool_manager_->NewPage(page_id));
   ASSERT(page != nullptr,"nullptr!");
   page->Init(page_id,node->GetParentPageId(),processor_.GetKeySize(),leaf_max_size_);
   node->MoveHalfTo(page);
   page->SetNextPageId(node->GetNextPageId());
   node->SetNextPageId(page_id);
   return page ;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  page_id_t parent_page = old_node->GetParentPageId();
  page_id_t new_root;
  if(parent_page == INVALID_PAGE_ID)
  {
    LOG(ERROR) << "get new root" << std::endl;
    auto new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_root)) ;
    new_page->Init(new_root,INVALID_PAGE_ID,processor_.GetKeySize(),internal_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    //TODO:adjuest root
    root_page_id_ = new_root;
    old_node->SetParentPageId(new_root);
    new_node->SetParentPageId(new_root);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(new_root,true);
    return;
  }
  auto page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page));
  page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
  new_node->SetParentPageId(parent_page);
  if(page->GetSize()> page->GetMaxSize())
  {
    auto new_page = Split(page,nullptr);
    InsertIntoParent(page,new_page->KeyAt(0),new_page,nullptr);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(),true);
  }
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "BEGIN REMOVE----------------------------" << std::endl;
  #endif
  ASSERT(root_page_id_!=INVALID_PAGE_ID,"empty tree!");
  page_id_t page_id = root_page_id_;
  LeafPage * leaf = reinterpret_cast<LeafPage*>( FindLeafPage(key,root_page_id_));
  // #ifdef BTREE_DEBUG
  //   LOG(INFO) << "leaf:"<<leaf->GetPageId()<<" parent:" << leaf->GetParentPageId() <<std::endl;
  //   #endif
  ASSERT(leaf!=nullptr,"leaf is nullptr!");
  RowId temp;
  if(leaf->Lookup(key,temp,processor_))
  {
    if(leaf->RemoveAndDeleteRecord(key,processor_)< leaf->GetMinSize())
    {
      if(CoalesceOrRedistribute(leaf,nullptr))
      {
        buffer_pool_manager_->DeletePage(leaf->GetPageId());
      }
      else
      {
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
      }
    }
  }
  else{
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(),false);
    LOG(WARNING) << "delete index not found" << std::endl;
  }
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "END REMOVE----------------------------" << std::endl;
  #endif
  return ;
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) <<"Begin CoalesceOrRedistributr-------------------------------"<<std::endl;
  #endif
  if(node->IsRootPage())
  {
  #ifdef BTREE_DEBUG
  LOG(WARNING) <<"END COALESCEORREDISTRIBUTR-------------------------------"<<std::endl;
  #endif
    return AdjustRoot(node);
  }
  else
  {
    BPlusTreePage * silibing;
    auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index = parent->ValueIndex(node->GetPageId());
    #ifdef BTREE_DEBUG
    LOG(INFO) << "node:"<<node->GetPageId()<<" parent:" << node->GetParentPageId() <<" "<<parent->GetPageId()<<" Index:" << index <<std::endl;
    #endif
    if(index == 0)
    {
      silibing = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1)));
    }
    else
    {
      silibing = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index-1)));
    }
    if(node->IsLeafPage())
    {
      auto leaf = reinterpret_cast<LeafPage *>(node);
      auto sili_leaf = reinterpret_cast<LeafPage *>(silibing);
      if(leaf->GetSize()+sili_leaf->GetSize()>leaf->GetMaxSize())
      {
        Redistribute(sili_leaf,leaf,index);
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(sili_leaf->GetPageId(),true);
  #ifdef BTREE_DEBUG
  LOG(WARNING) <<"END COALESCEORREDISTRIBUTR-------------------------------"<<std::endl;
  #endif
        return false;
      }
      else
      {
        int flag;
        flag = Coalesce(sili_leaf,leaf,parent,index,nullptr);
        if(flag)
        {
          buffer_pool_manager_->DeletePage(parent->GetPageId());
        }
        else
        {
          buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
        }
        return true;
      }
    }
    else
    {
      auto internal = reinterpret_cast<InternalPage *>(node);
      auto sili_internal = reinterpret_cast<InternalPage*>(silibing);
      if(internal->GetSize()+ sili_internal->GetSize()> internal->GetMaxSize())
      {
        Redistribute(sili_internal,internal,index);
        buffer_pool_manager_->UnpinPage(internal->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(sili_internal->GetPageId(),true);
  #ifdef BTREE_DEBUG
  LOG(WARNING) <<"END COALESCEORREDISTRIBUTR-------------------------------"<<std::endl;
  #endif
        return false;
      }
      else
      {
        int flag;
        flag = Coalesce(sili_internal,internal,parent,index,nullptr);
      
        if(flag)
        {
          buffer_pool_manager_->DeletePage(parent->GetPageId());
        }
        else
        {
          buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
        }
      }
  #ifdef BTREE_DEBUG
  LOG(WARNING) <<"END COALESCEORREDISTRIBUTR-------------------------------"<<std::endl;
  #endif
      return true;
    }
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "BEGIN COALESCE on leaf ********************" <<std::endl;
  #endif
  ASSERT(neighbor_node->GetParentPageId()==node->GetParentPageId()&&parent->GetPageId()==node->GetParentPageId(),"parent not equal!");
  if(index == 0)
  {
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    parent->Remove(1);
    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  }
  else
  {
    node->MoveAllTo(neighbor_node);
    parent->Remove(index);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
  }
  if(parent->GetSize()<parent->GetMinSize())
  {
    #ifdef BTREE_DEBUG
    LOG(WARNING) << "end COALESCE********************" <<std::endl;
    #endif
    return CoalesceOrRedistribute(parent,nullptr);
  }
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "end COALESCE********************" <<std::endl;
  #endif
    return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "BEGIN COALESCE on internal ********************" <<std::endl;
  #endif
  ASSERT(neighbor_node->GetParentPageId()==node->GetParentPageId()&&parent->GetPageId()==node->GetParentPageId(),"parent not equal!");
  if(index == 0)
  {
    neighbor_node->MoveAllTo(node,parent->KeyAt(1),buffer_pool_manager_);
    parent->Remove(1) ;
    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  }
  else
  {
    node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);
    parent->Remove(index);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
    buffer_pool_manager_->DeletePage(node->GetPageId());
  }
  if(parent->GetSize()<parent->GetMinSize())
  {
    #ifdef BTREE_DEBUG
  LOG(WARNING) << "end COALESCE********************" <<std::endl;
  #endif
    return CoalesceOrRedistribute(parent,nullptr);
  }
  #ifdef BTREE_DEBUG
  LOG(WARNING) << "end COALESCE********************" <<std::endl;
  #endif
    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
   #ifdef BTREE_DEBUG
  LOG(WARNING) << "BEGIN redis on leaf ********************" <<std::endl;
  #endif
  ASSERT(neighbor_node->GetParentPageId()==node->GetParentPageId(),"parent not equal!");
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  if(index==0)
  {
    neighbor_node->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(1,neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
    
  }
  else
  {
    neighbor_node->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(index,node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  }
   #ifdef BTREE_DEBUG
  LOG(WARNING) << "end redis on leaf ********************" <<std::endl;
  #endif
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
   #ifdef BTREE_DEBUG
  LOG(WARNING) << "BEGIN redis on internal ********************" <<std::endl;
  #endif
  ASSERT(neighbor_node->GetParentPageId()==node->GetParentPageId(),"parent not equal!");
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  if(index!=0)
  {
    neighbor_node->MoveLastToFrontOf(node,parent_page->KeyAt(index),buffer_pool_manager_);
    parent_page->SetKeyAt(index,node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  }
  else
  {
    neighbor_node->MoveFirstToEndOf(node,parent_page->KeyAt(1),buffer_pool_manager_);
    parent_page->SetKeyAt(1,neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  }
   #ifdef BTREE_DEBUG
  LOG(WARNING) << "end redis on leaf ********************" <<std::endl;
  #endif
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetSize()==1&& !old_root_node->IsLeafPage())
  {
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(reinterpret_cast<InternalPage*>( old_root_node)->ValueAt(0)));
    child_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_page->GetPageId();
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(),true);
    UpdateRootPageId(0);
    return true;
  }
  else if(old_root_node->GetSize()==0&&old_root_node->IsLeafPage())
  {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  else
  {
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  return IndexIterator();
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
   return IndexIterator();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t new_page_id, bool leftMost) {
  #ifdef BTREE_DEBUG
  LOG(INFO) <<"Begin FindLeafPage" <<std::endl;
  #endif
  page_id_t page_id = new_page_id;
  while(1)
    {
      auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id));
      if(page->IsLeafPage())
      {
        #ifdef BTREE_DEBUG
  LOG(INFO) <<"End FindLeafPage" <<std::endl;
  cout <<std::endl;
  #endif
        return (Page *)page ;
      }
      else
      {
        cout << "test:"<<endl;
        auto internal = reinterpret_cast<BPlusTreeInternalPage *> (page);
        buffer_pool_manager_->UnpinPage(page_id,false);
        if(!leftMost)
          page_id = internal->Lookup(key,processor_);
        else
          page_id = internal->ValueAt(0);
      }
    }
  #ifdef BTREE_DEBUG
  LOG(INFO) <<"End FindLeafPage" <<std::endl;
  cout <<std::endl;
  #endif
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
    auto head = reinterpret_cast<IndexRootsPage  *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    if(insert_record == 0)
    {
      head->Update(index_id_,root_page_id_);
    }
    else if(insert_record == 1)
    {
      head->Insert(index_id_,root_page_id_);
    }
    else 
    {
      head->Delete(index_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      // out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}