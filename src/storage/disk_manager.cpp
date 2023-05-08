#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage * MetaPage = reinterpret_cast<DiskFileMetaPage *>  (meta_data_);
  uint32_t extent_num = MetaPage->num_extents_;
  uint32_t extent_index;
  uint32_t page_offset;
  char bitmap[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * BitMap = reinterpret_cast<BitmapPage<PAGE_SIZE> *> (bitmap);
  //get vaild extent index

  for(extent_index=0;extent_index<extent_num;extent_index++)
  {
      if(MetaPage->extent_used_page_[extent_index] < BITMAP_SIZE)
      {
        break;
      }
  }
  if(extent_index == extent_num)
  {
    if(extent_num == MAX_EXTENT_SIZE)
    {
      #ifdef ENABLE_BPM_DEBUG
        LOG(WARNING) << "out of MAX_EXTENT_SIZE" << std::endl;
        LOG(WARNING) << "return page_id -1" << std::endl;
      #endif 
      return INVALID_PAGE_ID;
    }
    MetaPage->num_extents_++;
  }
  //get page;
  ReadPhysicalPage(1+extent_index * EXTENT_SIZE,bitmap);
      #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "extent_index:" << extent_index<<std::endl;
      #endif 
  if(BitMap->AllocatePage(page_offset))
  {
    MetaPage->extent_used_page_[extent_index] ++ ;
    MetaPage->num_allocated_pages_++;
      #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "MetaPage->num_allocated_pages_:" << MetaPage->num_allocated_pages_<<std::endl;
      #endif 
    WritePhysicalPage(1+extent_index * EXTENT_SIZE,bitmap);
    return extent_index * BITMAP_SIZE + page_offset;
  }
  else
  {
    #ifdef ENABLE_BPM_DEBUG
        LOG(WARNING) << "AllocatePage fail in disk_manage" << std::endl;
    #endif 
    return INVALID_PAGE_ID;
  }


}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
   DiskFileMetaPage * MetaPage = reinterpret_cast<DiskFileMetaPage *>  (meta_data_);
  uint32_t extent_index = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;

  if(extent_index >= MetaPage->GetExtentNums())
  {
    #ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "DeAllocatePage in unused extents" << std::endl;
    #endif 
    return ;
  }

  char bitmap[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * BitMap = reinterpret_cast<BitmapPage<PAGE_SIZE> *> (bitmap);
  ReadPhysicalPage(1+extent_index * EXTENT_SIZE,bitmap);


  if(BitMap->DeAllocatePage(page_offset))
  {
    MetaPage->extent_used_page_[extent_index]--;
    MetaPage->num_allocated_pages_--;
  }
  else
  {
    #ifdef ENABLE_BPM_DEBUG
        LOG(WARNING) << "DeAllocatePage was free at first" << std::endl;
      #endif   
  }
  WritePhysicalPage(1+extent_index * EXTENT_SIZE,bitmap);

}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent_index = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;
  char bitmap[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * BitMap = reinterpret_cast<BitmapPage<PAGE_SIZE> *> (bitmap);
  ReadPhysicalPage(1+extent_index * EXTENT_SIZE,bitmap);
  // std::cout << bitmap << std::endl;
  return BitMap->IsPageFree(page_offset);

}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  if(logical_page_id>MAX_VALID_PAGE_ID)
  {
    #ifdef ENABLE_BPM_DEBUG
      LOG(WARNING) << "logical_page_id > MAX_VALID_PAGE_ID" << std::endl;
      LOG(WARNING) <<"return page_id -1" << std::endl;
    #endif 
    return INVALID_PAGE_ID;
  }
  else
    #ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "MapPageId:" << logical_page_id<<"to:"<<(logical_page_id / BITMAP_SIZE) * (EXTENT_SIZE)  + (logical_page_id % BITMAP_SIZE) + 2<< std::endl;
    #endif 
  return (logical_page_id / BITMAP_SIZE) * (EXTENT_SIZE)  + (logical_page_id % BITMAP_SIZE) + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  #ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "read physical_page_id:" << physical_page_id<<std::endl<<std::endl;
  #endif
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
  #ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page type1" << std::endl;
  #endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page type2" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
   #ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "write physical_page_id:" << physical_page_id<<std::endl<<std::endl;
  #endif
  // set write cursor to offset
  db_io_.seekp(offset);
  if(db_io_.eof())
  {
    LOG(ERROR) << "large than offset" << std::endl;
  }
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}