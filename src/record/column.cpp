#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t len = 0;
  if(type_ == kTypeChar)
  {
    char c = 1;
    memcpy(buf+len,&c,sizeof(char));
    len+=sizeof(char);
    memcpy(buf+len,&len_,sizeof(uint32_t));
    len += sizeof(uint32_t);
  }
  else
  {
    char c = 0;
    memcpy(buf+len,&c,sizeof(char));
    len += sizeof(char);
  }
  memcpy(buf+len,&type_,sizeof(TypeId));
  len += sizeof(TypeId);
  MACH_WRITE_TO(size_t,buf+len,name_.size());
  len += sizeof(size_t);
  memcpy(buf+len,name_.c_str(),name_.size());
  #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Column serial name:"<<name_ << " string[0]:"<<*((char *) (buf+len))<< std::endl;
  #endif
  len += name_.size();
  memcpy(buf+len,&table_ind_,sizeof(uint32_t));
  len += sizeof(uint32_t) ;
  memcpy(buf+len,&nullable_,sizeof(bool));
  len += sizeof(bool);
  memcpy(buf+len,&unique_,sizeof(bool));
  len += sizeof(bool);
   #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Column serial final offset:"<<len << std::endl;
    #endif
  return len;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  int i = 0;
  if(type_ == kTypeChar)
  {
     i = sizeof(uint32_t);
  }
  return i + sizeof(char)+sizeof(TypeId)+sizeof(size_t) + name_.size()+sizeof(uint32_t) + sizeof(bool)+sizeof(bool);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize."<< std::endl;
  }

  char c;
  char *column_name;
  std::string column_name_;
  TypeId column_type;
  uint32_t len;
  uint32_t column_index;
  bool isnull;
  bool isunique;
  uint32_t offset = 0;
  size_t strlen;

  c = MACH_READ_FROM(char, buf + offset);
  offset += sizeof(char);
  if(c==1)
  {
    len = MACH_READ_FROM(u_int32_t,buf + offset);
    offset += sizeof(u_int32_t);
  }
  column_type = MACH_READ_FROM(TypeId,buf + offset);
  offset += sizeof(TypeId);
  strlen = MACH_READ_FROM(size_t,buf+offset);
  offset += sizeof(size_t);
  column_name = new char[strlen+1];
  memset(column_name,0,strlen+1);
  memcpy(column_name,buf+offset,strlen);
  #ifdef RECORD_DEBUG 
      LOG(INFO) <<"strlen:" <<strlen<<" Column serial name:"<<column_name <<  std::endl;
  #endif
  offset+=strlen;
  column_name_ = column_name;
  delete [] column_name;
  column_index = MACH_READ_FROM(uint32_t,buf + offset);
  
  offset += sizeof(uint32_t);
  isnull = MACH_READ_FROM(bool,buf  + offset);
  offset += sizeof(bool);
  isunique= MACH_READ_FROM(bool,buf  + offset);
  offset += sizeof(bool);
  if(c == 1)
  {
    column = new Column(column_name_,column_type,len,column_index,isnull,isunique);
  }
  else
  {
    column = new Column(column_name_,column_type,column_index,isnull,isunique);
  }
  #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Column deserial name:"<<column_name_<< std::endl;
  #endif
  #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Column deserial column_index:"<<column_index << std::endl;
  #endif
  
   #ifdef RECORD_DEBUG 
      LOG(INFO) <<"Column deserial final offset:"<<offset << std::endl;
  #endif
  return offset;
}
