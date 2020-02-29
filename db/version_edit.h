// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include <cmath>
#include <map>

namespace leveldb {
inline uint32_t decode_fixed32_kc(const char* ptr) {
  // Load the raw bytes
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
  return result;
}
class VersionSet;
class SSTBloom_Opt_2
{
public:

  SSTBloom_Opt_2(const int32_t n, const double false_positive_p) 
        : bits_(), k_(0), m_(0), n_(n), check_(0),check_hit_(0), p_(false_positive_p)
  {
    k_ = static_cast<int32_t>(-std::log(p_) / std::log(2));
    m_ = static_cast<int32_t>(k_ * n * 1.0 / std::log(2));
    bits_.resize((m_ + 7) / 8, 0);
    ele=0;
  }
  //void insert(const T &key);
  int getEle(){
    return ele;
  }
  //bool key_may_match(const T &key);
  uint32_t hash_func(const char* data, size_t n, uint32_t seed)const{
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);

    // Pick up four bytes at a time
    while (data + 4 <= limit) {
        uint32_t w = decode_fixed32_kc(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }
    // Pick up remaining bytes
    switch (limit - data) {
        case 3:
            h += static_cast<unsigned char>(data[2]) << 16;
        case 2:
            h += static_cast<unsigned char>(data[1]) << 8;
        case 1:
            h += static_cast<unsigned char>(data[0]);
            h *= m;
            h ^= (h >> r);
            break;
    }
    return h;
  }
  uint32_t hash_(uint32_t seed,const std::string &key_) const
  {
	return hash_func(key_.data(),key_.size(),seed);
  }
  void insert(const std::string &key){
      uint32_t hash_val = hash_(0xbc9f1d34,key);
      const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
      for (int i = 0; i < k_; ++i) {
		const uint32_t bit_pos = hash_val % m_;
        bits_[bit_pos/8] |= 1 << (bit_pos % 8);
        hash_val += delta;
      }
      ele++;
  }
  bool contains_(const std::string &key,uint32_t& hash_val_){
    uint32_t hash_val = hash_(0xbc9f1d34,key);
    hash_val_=hash_val;
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        const uint32_t bit_pos = hash_val % m_;
        if ((bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0) {
            return false;
        }
        hash_val += delta;
    }
    
    return true;
  }
  bool contains_Part(const std::string &key,uint32_t hash_val_){
    //uint32_t hash_val = hash_(0xbc9f1d34,key);
    uint32_t hash_val = hash_val_;
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        const uint32_t bit_pos = hash_val % m_;
        if ((bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0) {
            return false;
        }
        hash_val += delta;
    }
    
    return true;
  }
   bool contains(const std::string &key){
     check_++;
    uint32_t hash_val = hash_(0xbc9f1d34,key);
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        const uint32_t bit_pos = hash_val % m_;
        if ((bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0) {
            return false;
        }
        hash_val += delta;
    }
    check_hit_++;
    return true;
  }
  int32_t get_k(){return k_;}
  int32_t get_check(){return check_;}
  int32_t get_checkHit(){return check_hit_;}
  int32_t get_m(){return m_;}
  int32_t get_n(){return n_;}
  double get_p(){return p_;}
  double get_BSIZE(){return bits_.size();}
  std::vector<char> bits_;
 private:
  
  int ele;
  int32_t k_;
  int32_t m_;
  int32_t n_;
  int32_t check_;
  int32_t check_hit_;
  double p_;

};
class SSTBloom_Opt{
  private:
		//std::vector<bool> array;
    std::vector<bool>* array;
    //std::bitset<20000000> array;
		unsigned int size;
		unsigned int nElements;
		unsigned int RSHash(const char * key, int len, unsigned int seed){
            // 'm' and 'r' are mixing constants generated offline.
            // They're not really 'magic', they just happen to work well.
            const uint m = 0x5bd1e995;
            const int r = 24;
            // Initialize the hash to a 'random' value
            uint h = seed ^ len;
            // Mix 4 bytes at a time into the hash
            const unsigned char * data = (const unsigned char *)key;
            while(len >= 4)
            {
                uint k = *(unsigned int *)data;
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
                data += 4;
                len -= 4;
            }
            // Handle the last few bytes of the input array
            switch(len)
            {
                case 3: h ^= data[2] << 16;
                case 2: h ^= data[1] << 8;
                case 1: h ^= data[0];
                h *= m;
            };
            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.
            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;
            return h%size;
		}
		int arrays[10]={5, 7, 11, 13, 31, 37, 61, 21, 1, 92};
	public:
    
		SSTBloom_Opt(unsigned int size){
            
            
            array=new std::vector<bool>;
            nElements = 0;
            //array.reserve(size);
            array->reserve(size);
            //array.assign(size,false);
            array->assign(size,false);
            
            this->size = size;
		}
		~SSTBloom_Opt(){
      //delete array;
    }
		void DeleteArray(){
      delete array;
      return;
    }
    unsigned int getSize(){return size;}
    unsigned int getEle(){return nElements;}
		float getFalseRate(){return std::pow(1-std::exp(-2 * ((float)nElements)/((float)size)),2);}
		bool contains(std::string value){

      
            for(int i=0;i<5;i++){
                if((*array)[RSHash(value.c_str(),value.size(),arrays[i])]!=true)
                    return false;
            }
            return true;
      
     return false;
		}

		void insert(std::string value){

      
            for(int i=0;i<5;i++){
                (*array)[RSHash(value.c_str(),value.size(),arrays[i])]=true;
            }
            nElements++;
      
		}
		void resize(unsigned int size){
            array->reserve(size);
            array->assign(size,false);
            this->size = size;
            nElements=0;
            
		}
		void clear(){
            array->clear();
            nElements = 0;
		}

		//It would be nice to have a function to resize the filter to obtain the falseRate that we want
		//unsigned int getFalseRate(float rate);
};
class SSTBloom_Opt_1{
  private:
		std::vector<bool>* Current_array;
    std::vector< std::pair< std::pair<int,int>, std::vector<bool>* > > arrayVec; //<start point,<end point,filter string>>
    //std::bitset<20000000> array;
    int start_point;
    int tmp_end_point;
    unsigned int Part_Ele;
		unsigned int nElements;
    int k;
    int bits_per_key;
    unsigned int Part_Size;
		unsigned int RSHash(const char * key, int len, unsigned int seed){
            // 'm' and 'r' are mixing constants generated offline.
            // They're not really 'magic', they just happen to work well.
            const uint m = 0x5bd1e995;
            const int r = 24;
            // Initialize the hash to a 'random' value
            uint h = seed ^ len;
            // Mix 4 bytes at a time into the hash
            const unsigned char * data = (const unsigned char *)key;
            while(len >= 4)
            {
                uint k = *(unsigned int *)data;
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
                data += 4;
                len -= 4;
            }
            // Handle the last few bytes of the input array
            switch(len)
            {
                case 3: h ^= data[2] << 16;
                case 2: h ^= data[1] << 8;
                case 1: h ^= data[0];
                h *= m;
            };
            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.
            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;
            return h%Part_Size;
		}
		unsigned int arrays[20]={93,86,27,15,79,35,56,49,66,42,23,127,90,159,77,96,40,326,972,57};
	public:
    
		SSTBloom_Opt_1(unsigned int Part_Elements){
            bits_per_key=10;
            //k=bits_per_key*0.69;
            k=7;
            Current_array=NULL;
            this->Part_Ele=Part_Elements;
            Part_Size=Part_Ele*bits_per_key;
            nElements = 0;
            start_point=tmp_end_point=0;
		}
		~SSTBloom_Opt_1(){
      //delete array;
    }
		void DeleteArray(){
      if(Current_array!=NULL){
        delete Current_array;
      }
      for(std::vector< std::pair< std::pair<int,int>, std::vector<bool>* > >::iterator it=arrayVec.begin();
          it!=arrayVec.end();it++){
          delete it->second;
      }
      arrayVec.clear();
      return;
    }
    //unsigned int getSize(){return size;}
    unsigned int getEle(){
      return nElements;}

		//float getFalseRate(){return std::pow(1-std::exp(-2 * ((float)nElements)/((float)size)),2);}
		int FindFilter(std::string& value){
      int tmp_value=atoi(value.c_str());
      if(Current_array!=NULL && tmp_value>=start_point&&tmp_value<=tmp_end_point){
        return -1;
      }else if(!arrayVec.empty()){
        int low=0;
        int high=arrayVec.size()-1;
        int mid;
        while(low<=high){
          mid=(low+high)/2;
          if(tmp_value>=arrayVec[mid].first.first && tmp_value<=arrayVec[mid].first.second){
            return mid;
          }
          if(tmp_value>arrayVec[mid].first.second){
            low=mid+1;
          }
          if(tmp_value<arrayVec[mid].first.first){
            high=mid-1;
          }
        }
        return -2;
      }else{
        return -2;
      }
    }
    bool contains(std::string value){
        int Index=FindFilter(value);
        if(Index==-2){
          return false;
        }else if(Index==-1){
            for(int i=0;i<k;i++){
              if((*Current_array)[RSHash(value.c_str(),value.size(),arrays[i])]!=true){
                return false;
              }
            }
            return true;
        }else{
          for(int i=0;i<k;i++){
              if((*arrayVec[Index].second)[RSHash(value.c_str(),value.size(),arrays[i])]!=true){
                return false;
              }
          }
          return true;
        }
		}
    
    void InitFilter(std::string& value){
      start_point=atoi(value.c_str());
      Current_array=new std::vector<bool>;
      //array.reserve(size);
      Current_array->reserve(Part_Size);
      //array.assign(size,false);
      Current_array->assign(Part_Size,false);
    }
    void Build_Finish(){
        arrayVec.push_back(std::make_pair(std::make_pair(start_point,tmp_end_point),Current_array));
        Current_array=NULL;
    }
		void insert(std::string value){
      if(nElements==0){
        InitFilter(value);
      }
      else if(nElements%Part_Ele==0){//elements==2000
        //not init procedure
        arrayVec.push_back(std::make_pair(std::make_pair(start_point,tmp_end_point),Current_array));
        //arrayMap[start_point]=std::make_pair(tmp_end_point,Current_array);
        Current_array=NULL;
        InitFilter(value);
              
      }
      for(int i=0;i<k;i++){
          (*Current_array)[RSHash(value.c_str(),value.size(),arrays[i])]=true;
      }
      nElements++;
      //prepare end point
      tmp_end_point=atoi(value.c_str());
		}
		void resize(unsigned int size){ // actually it have no use
      for(std::vector< std::pair< std::pair<int,int>, std::vector<bool>* > >::iterator it=arrayVec.begin();
            it!=arrayVec.end();it++){
          it->second->reserve(Part_Size);
          it->second->assign(Part_Size,false);
      }
      nElements=0;
		}
		void clear(){
      for(std::vector< std::pair< std::pair<int,int>, std::vector<bool>* > >::iterator it=arrayVec.begin();
              it!=arrayVec.end();it++){
          it->second->clear();
      }
      nElements=0;
		}

		//It would be nice to have a function to resize the filter to obtain the falseRate that we want
		//unsigned int getFalseRate(float rate);
};
struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table
  uint64_t NumEntries;
  int Hotness;
  int EndFlag;
  SSTBloom_Opt_2* stb;
  int BySeek;
  bool InLog;
  FileMetaData() : InLog(false),BySeek(0),EndFlag(0),stb(NULL),Hotness(0),NumEntries(0), refs(0), allowed_seeks(1 << 30), file_size(0) { }
};




class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

 void AddFile_Stb(int level, uint64_t file,
               uint64_t file_size,
               SSTBloom_Opt_2* stb,
               const InternalKey& smallest,
               const InternalKey& largest,
               uint64_t NumEntries,
               int EndFlag,
               int Hotness,
               int BySeek
               ) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    f.stb=stb;
    f.NumEntries=NumEntries;
    f.EndFlag=EndFlag;
    f.Hotness=Hotness;
    f.BySeek=BySeek;
    new_files_.push_back(std::make_pair(level, f));
  }
  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
