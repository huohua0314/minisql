#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t offset = 0;
  uint32_t num = fields_.size();
  MACH_WRITE_TO(uint32_t,buf+offset,num);
  offset += sizeof(uint32_t);
  #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Row serialize Begin"<< std::endl;
      std::cout <<"field_Size:" << num <<std::endl;
  #endif
  char * bitmap;
  int byte = num / 8 + ((num % 8 >0) ? 1: 0);
  bitmap = new char(byte);
  memset(bitmap,0,byte);
  for(int i=0;i<num;i++)
  {
    int byteoff  = i /8;
    int bitoff = i % 8;
    if(!fields_[i]->IsNull())
    {
      bitmap[byteoff] = bitmap[byteoff] | 1 << bitoff;
    }
  }
  memcpy(buf+offset,bitmap,byte);
  offset += byte;
  for(int i=0;i<num;i++)
  {
    uint32_t len;
    len = fields_[i]->SerializeTo(buf + offset);
    
    #ifdef RECORD_DEBUG 
      std::cout <<"fields_.value.int:"<<fields_[i]->value_.integer_ << std::endl;;
      std::cout <<"serial temp offset:"<<offset << std::endl;
    #endif
    
    offset += len;
  }
  delete [] bitmap;
  #ifdef RECORD_DEBUG 
      std::cout <<"serial final offset:"<<offset << std::endl;
  #endif
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf,Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t offset = 0;
  uint32_t number = 0;
  
  number = MACH_READ_FROM(u_int32_t,buf+offset);
  offset += sizeof(uint32_t);
  // #ifdef BTREE_DEBUG 
  //     LOG(INFO) <<" Begin deserial "<< std::endl<<"filed_number:"<<number<<" now offset:"<<offset << std::endl;
  // #endif
  char *bitmap;
  int byte = number / 8 + ((number % 8 >0) ? 1: 0);
  bitmap = new char(byte);
  memset(bitmap,0,byte);

  memcpy(bitmap,buf+offset,byte);
  offset += byte;
  for(uint32_t i=0;i<number;i++)
  {
    bool flag;
    int byteoff  = i /8;
    int bitoff = i % 8;
    int len;
    Field * temp;
    const Column * col = schema->GetColumn(i);
    flag = ((bitmap[byteoff] &(1<<bitoff))==0);
    
    len = temp->DeserializeFrom(buf+offset,col->GetType(),&temp,flag);
    #ifdef RECORD_DEBUG 
      std::cout <<"deserial temp offset:"<<offset << std::endl;
    #endif
    offset += len;
    fields_.push_back(temp) ;
      // std::cout <<"fields_.value.int:"<<fields_[i]->value_.integer_ << std::endl;;
  }
  delete [] bitmap;
  // #ifdef BTREE_DEBUG 
  //   //  std::cout <<"deserial final offset:"<<offset << std::endl;
  //    LOG(INFO) << "end of deserial"<<std::endl;
  // #endif
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t offset = 0;
  uint32_t number = fields_.size();
  uint32_t byte = number / 8 + ((number % 8 >0) ? 1: 0);
  offset += sizeof(uint32_t) + byte;
  for(int i=0;i<number;i++)
  {
    offset += fields_[i]->GetSerializedSize();
  }
  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
