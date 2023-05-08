#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  void Remove(frame_id_t frame_id)
  {
    lru_list.remove(frame_id);
  }

  size_t Size() override;

private:
  unordered_set<frame_id_t> pin_pages;
  unordered_set<frame_id_t> lru_list_pages;
  list<frame_id_t> lru_list;
  int num_pages_;
  // add your own private member variables here
};

#endif  // MINISQL_LRU_REPLACER_H
