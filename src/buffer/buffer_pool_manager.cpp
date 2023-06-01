#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"


static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    LOG(WARNING) << page.first <<endl;
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // #ifdef  BTREE_DEBUG
  //     LOG(INFO) <<"start fetch page:" <<page_id << endl;
  //   #endif 
  if(page_table_.count(page_id) == 1)
  {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return pages_ + page_table_[page_id];
  }
  else
  {
    if(free_list_.size()>0)
    {
      int new_frame = *free_list_.begin();
      free_list_.pop_front();
      pair<page_id_t,frame_id_t> p1(page_id,new_frame);
      replacer_->Unpin(new_frame); // managered by lru
      replacer_->Pin(new_frame);
      page_table_.insert(p1);

     
      disk_manager_->ReadPage(page_id,pages_[new_frame].data_);
      pages_[new_frame].page_id_ = page_id;
      pages_[new_frame].pin_count_ = 1;

      return pages_ + new_frame;
    }
    else
    {
      int new_frame;
      if(replacer_->Size() > 0)
      {
        replacer_->Victim(&new_frame);
        if(pages_[new_frame].IsDirty()) 
        {
          if(FlushPage(pages_[new_frame].GetPageId()) == 0)
          {
            LOG(WARNING) << "FLUSHPAGE when Fetch page fail" << endl;
          }
        }
        disk_manager_->ReadPage(page_id,pages_[new_frame].data_);
        page_table_.erase(pages_[new_frame].page_id_);

        pages_[new_frame].is_dirty_ = 0;
        pages_[new_frame].page_id_ = page_id;
        pages_[new_frame].pin_count_ = 1;       //change the data before

        page_table_[page_id] = new_frame;

        replacer_->Unpin(new_frame);
        replacer_->Pin(new_frame);
        return pages_ + new_frame;
      }
      else
      {
        LOG(WARNING) << "No free pages are available to use in buffer_pool";
      }
    }
  }
  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  #ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"start get new Page" <<page_id << endl;
    #endif 
  if(free_list_.size() > 0)
  {
    
    int new_frame = *free_list_.begin();
    free_list_.pop_front();
    
    replacer_->Unpin(new_frame); // managered by lru
    replacer_->Pin(new_frame);

    page_id = disk_manager_->AllocatePage();
    pair<page_id_t,frame_id_t> p1(page_id,new_frame);
    page_table_.insert(p1);
    // LOG(WARNING) <<"NewPage:"<<page_id << endl;
    #ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"get from freelist page_id:" <<page_id << endl;
    #endif 
    disk_manager_->ReadPage(page_id,pages_[new_frame].data_);
    pages_[new_frame].page_id_ = page_id;
    pages_[new_frame].pin_count_ = 1;
    return pages_ + new_frame; 
  }
  else
  {
    int new_frame;
    if(replacer_->Size() > 0)
      {
        replacer_->Victim(&new_frame);
        if(pages_[new_frame].IsDirty()) 
        {
          if(FlushPage(pages_[new_frame].GetPageId()) == 0)
          {
            LOG(WARNING) << "FLUSHPAGE when Fetch page fail" << endl;
          }
        }
        page_id = disk_manager_->AllocatePage();
        #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) <<"get from victim page id:" <<page_id << endl;
        #endif 
        page_table_.erase(pages_[new_frame].page_id_);
        pages_[new_frame].ResetMemory();

        disk_manager_->ReadPage(page_id,pages_[new_frame].data_);
        pages_[new_frame].is_dirty_ = 0;
        pages_[new_frame].page_id_ = page_id;
        pages_[new_frame].pin_count_ = 1;       //change the data before

        page_table_[page_id] = new_frame;

        replacer_->Unpin(new_frame);
        replacer_->Pin(new_frame);
        return pages_ + new_frame;
      }
    else
    {
      LOG(WARNING) <<"ALL pages in buffer are pinned" << endl;
      page_id = INVALID_PAGE_ID;
    }
  }
  return nullptr;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  // #ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"start delete page id:" <<page_id << endl;
    // #endif 
  if(page_table_.count(page_id) == 0) //no such page
  {
    #ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "deleted page not in table" << endl;
    #endif
    return true;
  }
  else
  {
    if(pages_[page_table_[page_id]].pin_count_>0) //page pined
    {
      #ifdef ENABLE_BPM_DEBUG
      LOG(INFO) <<"deleted page fail for pin_count > 0" << endl;
      #endif
    }
    else //page unpined
    {
      int frame_id = page_table_[page_id];
      if(pages_[frame_id].IsDirty())
      {
        FlushPage(page_id);
      }
      dynamic_cast <LRUReplacer *> (replacer_) ->Remove(frame_id);
      page_table_.erase(page_id);
      free_list_.push_back(frame_id);
      DeallocatePage(page_id);
    }
    return true;
  } 
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_id == INVALID_PAGE_ID)
  {
    ASSERT(false,"Unpin INVAILD_PAGE ID");
  }
  if(page_table_.count(page_id) == 1) 
  {
    if(pages_[page_table_[page_id]].pin_count_>0)
    {
      pages_[page_table_[page_id]].pin_count_ --;
      if(is_dirty)
      {
        pages_[page_table_[page_id]].is_dirty_ = 1;
      }
      if(pages_[page_table_[page_id]].pin_count_ == 0)
      {
        replacer_->Unpin(page_table_[page_id]);
      }
      return true;
    }
  }
  else
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.count(page_id)==0)
  {
    LOG(WARNING) << "page couldn't be flush for it not in buffer" << endl;
    return false;
  }
  else
  {
    disk_manager_->WritePage(page_id,pages_[page_table_[page_id]].data_);
    return true;
  }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}