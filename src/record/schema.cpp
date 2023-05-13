#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t  number = columns_.size();
  uint32_t offset = 0;
  MACH_WRITE_TO(uint32_t,buf+offset,number);
  offset += sizeof(uint32_t);
  for(int i=0;i<number;i++)
  {
    int length;
    length = columns_[i]->SerializeTo(buf + offset);
    offset += length;
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t offset = 0;
  offset += sizeof(uint32_t);
  for(int i=0;i<columns_.size();i++) 
  {
    offset += columns_[i]->GetSerializedSize();
  }
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  uint32_t number;
  std::vector<Column *> col;
  number = MACH_READ_FROM(uint32_t,buf);
  offset += sizeof(uint32_t);
  for(int i=0;i<number;i++)
  {
    Column * temp=nullptr;
    uint32_t len;
    len = temp->DeserializeFrom(buf+offset,temp);
    col.push_back(temp);
    offset += len;
  }
  schema = new Schema(col,true);
  return offset;
}