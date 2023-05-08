#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):num_pages_(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.size()==0)
  {
    return false;
  }
  else
  {
    *frame_id = *lru_list.begin();
    lru_list.pop_front();
    lru_list_pages.erase(*frame_id);
    #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "victim lru_list page:" << frame_id << std::endl;
    #endif 
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_list_pages.find(frame_id) != lru_list_pages.end())
    pin_pages.insert(frame_id);
  #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "pin page:" << frame_id << std::endl;
  #endif 
  lru_list.remove(frame_id);
  #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "pin page:" << frame_id <<" removed from lru_list"<< std::endl;
  #endif 
  lru_list_pages.erase(frame_id);
}
/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(pin_pages.find(frame_id) != pin_pages.end())
  {
    pin_pages.erase(frame_id);
    #ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "unpin page in pin set:" << frame_id << std::endl;
    #endif 
  }
  else
  {
    #ifdef ENABLE_BPM_DEBUG
        LOG(warning) << "unpin page:" << frame_id <<" not be pined"<< std::endl;
    #endif 
  }
  if(lru_list_pages.find(frame_id) != lru_list_pages.end())
  {
    #ifdef ENABLE_BPM_DEBUG
        LOG(warning) << "Unpin page:" << frame_id <<"already in list"<< std::endl;
    #endif 
  }
  else
  {
    lru_list.push_back(frame_id);
    lru_list_pages.insert(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}