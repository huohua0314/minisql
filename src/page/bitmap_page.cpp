#include "page/bitmap_page.h"
#include<iostream>
#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  int byte_index,bit_index;
  int i,j;
  #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "page_allocated:" << page_allocated_<< std::endl;
    #endif 
  if (page_allocated_ < 8 * MAX_CHARS) {
    page_allocated_ ++ ;
    if(next_free_page_!=0xFFFFFFFF)
    {
      byte_index = next_free_page_ /8;
      bit_index = next_free_page_ % 8;
      page_offset = next_free_page_;
      next_free_page_ = 0xFFFFFFFF;
    }

    else
    {
      for (i = 0; i < MAX_CHARS; i++) {
        if (bytes[i] != 255) break;
      }
      for (j = 0; j < 8; j++) {
        if ((bytes[i] & (1 << j)) == 0) break;
      }
      byte_index = i;
      bit_index = j;
      page_offset = 8 * i + j;
    } 
    #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "page_allocated:" << page_allocated_<< std::endl;
        LOG(INFO) << "page_offset:" << page_offset<< std::endl;
        LOG(INFO) << "allocate pageoffset is:" << page_offset << std::endl;
    #endif 
      bytes[byte_index] = bytes[byte_index] | (1 << bit_index);
    // std::cout << "MAX_CHARS:" << MAX_CHARS <<std::endl;
    // std::cout << "i:" << i << " j:" << j <<" allocated:"<<page_allocated_<<std::endl;
    // std::cout << "byte1:" << (int)bytes[i]  << std::endl;
    // std::cout <<"next:" << next_free_page_ << " " << "page:"<<page_offset << " ";
    return true;
  } else
    return false;
}
/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    int byte_index = page_offset /8;
    int bit_index = page_offset % 8;
    
    uint32_t mask = 0xFFFFFFFF;
    if(bytes[byte_index] & (1 << bit_index))
    {
      mask = mask ^ (1 << bit_index);
      bytes[byte_index] &= mask; 
      page_allocated_--;
      next_free_page_ = page_offset;
      #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "Deallocate pageoffset is:" << page_offset << std::endl;
      #endif 
      return true;
    }
    else
      return false;

}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    int byte_index = page_offset /8;
    int bit_index = page_offset % 8;
    int test = bytes[byte_index] & (1<<bit_index);
    if(test ) //if page_bit == 1;
      return false;
    else
      return true;

}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;