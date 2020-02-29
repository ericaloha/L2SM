// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include <unordered_map>

//include by hkc
#include <deque>
#include <list>
#include <mutex>
#include <condition_variable>


FILE* Filter;



int get_start=0;
extern std::mutex mtx_Log;
extern std::condition_variable produce_Log, consume_Log;

int l4=0;
int l44=0;
int score=0;
int seek=0;
int nothing=0;
int seekInLog=0;


int Loadflag=0;

int avg_hotness[7]={0,0,100000,50000,30000,20000,10000};
float avg_dens[7]={0,0,0.9,0.8,0.8,0.8,0.8};


int HitLoad=0;
int MissLoad=0;
std::set<int> FileNameForFilter;

int SeekMissLevel0=0;


int SeekMissFIlter=0;
int seekHitInLevel0=0;


int SeekMissLSM=0;
int FilterHit=0;
int level0Hit=0;
int level0Miss=0;
int Level0MultiMiss=0;
int FilterMiss=0;
int FilterMultiMiss=0;



int readtime=0;
int seektime=0;
int seekHitInLSM=0;
int seekHit=0;
int seekHitInLog=0;



int weakHit=0;
int NullFilter=0;
int weakMiss=0;

/*SST Log Field*/
//1. global SST input Buffer
extern std::deque<std::pair<int,leveldb::FileMetaData*> > InBuffer;
extern std::deque<std::pair<int,leveldb::FileMetaData*> > OutBuffer;
std::list<leveldb::FileMetaData* > TmpOutPut[7];
int TargetTmpSize[7]={100,100,15,10,10,10,10};
//2. global SSTLog maintaining structure
extern std::list<leveldb::FileMetaData*> SSTLog[7];
struct cmp_key
{
    bool operator()(const uint64_t &k1, const uint64_t &k2)const
    {
      return k1 >k2;
    }
};
extern std::map<uint64_t,leveldb::FileMetaData*,cmp_key> LogIndex[7];
std::map<uint64_t, leveldb::FileMetaData*,cmp_key> SSTKeyLog[7];
std::set<uint64_t> LogLiveFiles;
//3. global SST output buffer
std::deque<leveldb::FileMetaData*> OutBuffer_thres[7];
//std::deque<leveldb::FileMetaData*> OutBuffer[7];
std::deque<leveldb::FileMetaData*> OutBuffer_accumu[7];

extern std::set<uint64_t> LogFiles;

extern std::set<uint64_t> KeyLogNames;
extern std::unordered_map<uint64_t,leveldb::SSTBloom_Opt_2*> LogFilterBuffer;

extern int Thres_density[7];
extern int Thres_thres[7];
float Level_density[7]={0,0,0.05,0.1,0.2,0.3,0.4};

extern int WarmUpFlag;

/* Info Collection Filed*/
extern std::unordered_map<uint64_t,std::pair<std::pair<uint64_t,int>,leveldb::SSTBloom_Opt_2*> > SSTAttriMap; //filenumber,<Keys,Hotness>

/* Info collection*/
//1.avg density per level
extern int total_density_count[7];
extern float total_density[7];
//2.avg hotness per level
extern int total_hotness_count[7];
extern long total_hotness[7];



namespace leveldb {
/*
float GetDensity(FileMetaData* f){
    int left=atoi(f->smallest.user_key().ToString().c_str());
    int right=atoi(f->largest.user_key().ToString().c_str());
    return (float)f->NumEntries/(right-left);
        

}
int CloseEnough(FileMetaData* target,FileMetaData* closed){

}
int max(int a, int b){
  if(a>=b) return a;
  else return b;
}
int min(int a, int b){
  if(a<=b) return a;
  else return b;
}
int CoverOrNot(FileMetaData* f1,FileMetaData* f2){
    //max(A.start,B.start)<=min(A.end,B,end)
    int a=atoi(f1->smallest.user_key().ToString().c_str());
    int b=atoi(f2->smallest.user_key().ToString().c_str());
    int c=atoi(f1->largest.user_key().ToString().c_str());
    int d=atoi(f2->largest.user_key().ToString().c_str());
    int check=max(a,b)<= min(c,d)? 1:0;
    if(check==1 ){
      if(a<=b&&d<=c){
        return 1;
      }else{
        return 0;
      }
    }else{
      return 0;
    }
}
int OverlapOrNot(FileMetaData* f1,FileMetaData* f2){
    //max(A.start,B.start)<=min(A.end,B,end)
    int a=atoi(f1->smallest.user_key().ToString().c_str());
    int b=atoi(f2->smallest.user_key().ToString().c_str());
    int c=atoi(f1->largest.user_key().ToString().c_str());
    int d=atoi(f2->largest.user_key().ToString().c_str());
    return max(a,b)<= min(c,d)? 1:0;
}
void InsertIntoSSTLog(int Log_level,FileMetaData* target){
  if(SSTLog[Log_level].empty()){
      SSTLog[Log_level].push_back(target);
      return;
    }
  int left=atoi(target->smallest.user_key().ToString().c_str());
  int tmp_left;
  for(std::list<FileMetaData*>::iterator it=SSTLog[Log_level].begin();
      it!=SSTLog[Log_level].end();
      it++)
  {
    tmp_left=atoi((*it)->smallest.user_key().ToString().c_str());
    if(left<=tmp_left){
      SSTLog[Log_level].insert(it,target);
      break;
    }else if(*it==SSTLog[Log_level].back()){
      SSTLog[Log_level].push_back(target);
      break;
    }
  }
  return;
}
int Log2Buffer(int Log_level,std::list<std::pair<int,FileMetaData*>>& files){
  
    FileMetaData* targetIn=SSTLog[Log_level].back();
    SSTLog[Log_level].pop_back();
    InsertIntoSSTLog(Log_level,targetIn);
    int size_=SSTLog[Log_level].size();
    if(size_>Thres_density[Log_level]){
      int maxHotness=0;
      int minHotness=999999999;
      float smallestDensity=1.0;
      std::list<FileMetaData*>::iterator maxHotnessIter;
      std::list<FileMetaData*>::iterator minHotnessIter;
      std::list<FileMetaData*>::iterator smallestDensityIter;
      //1.info collection
      long total_hotness_this_time=0;
      int count_=0;
      float total_density_this_time=0.0;
      for(std::list<FileMetaData*>::iterator it1=SSTLog[Log_level].begin();
          it1!=SSTLog[Log_level].end();
          it1++)
      {
        //1.check Density,which have the same effect as range 
        //  cuz with same count keys(we suppose that key count 
        //  is raletivly close) bigger range means smaller density 
        count_++;
        float density=GetDensity(*it1);
        total_density_this_time+=density;
        if(density<smallestDensity){
          smallestDensity=density;
          smallestDensityIter=it1;
        }
        //2.check overlap situation, we need to define how hot a sst is
        //  using filter to measure how many hot keys in a SST, to show how hot a sst is 
        total_hotness_this_time+=(*it1)->Hotness;
        if((*it1)->Hotness>maxHotness){
          maxHotness=(*it1)->Hotness;
          maxHotnessIter=it1;
        }
        if((*it1)->Hotness<minHotness){
          minHotness=(*it1)->Hotness;
          minHotnessIter=it1;
        }
      }
      total_density[Log_level]+=(total_density_this_time/count_);
      total_density_count[Log_level]++;
      total_hotness[Log_level]+=(total_hotness_this_time/count_);
      total_hotness_count[Log_level]++;
      //2. check this time which attri we should take care of
      int CoverFlag=0;
      int OverlapFlag_hot=0;
      int OverlapFlag_cold=0;
      for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();
          it2!=SSTLog[Log_level].end();
          it2++)
        {
          //if(CoverOrNot(*smallestDensityIter,*it2)==1 
          if(OverlapOrNot(*smallestDensityIter,*it2)==1 
              && (*smallestDensityIter)->number!=(*it2)->number){
            //CoverBeginIter=it2;
            CoverFlag++;
          }
          if(OverlapOrNot(*minHotnessIter,*it2)==1
              && (*minHotnessIter)->number!=(*it2)->number){
                OverlapFlag_cold++;
              }
          if(OverlapOrNot(*maxHotnessIter,*it2)==1
              && (*maxHotnessIter)->number!=(*it2)->number){
                OverlapFlag_hot++;
              }
        }
      if(CoverFlag==0 && OverlapFlag_cold==0 && OverlapFlag_hot==0){
        if( size_>Thres_thres[Log_level]){
          //if(smallestDensity<Level_density[Log_level]){
          
          // try smallset density
          FileMetaData* target=SSTLog[Log_level].front();
          target->EndFlag=1;
          files.push_back(std::make_pair(Log_level,target));
          SSTLog[Log_level].pop_front();
          // check cover state, if no cover happens, forget it
          //std::list<FileMetaData*>::iterator CoverBeginIter;
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=1;
              it2=SSTLog[Log_level].erase(it2);
            }else if(atoi((*it2)->smallest.user_key().ToString().c_str())>
              atoi(target->largest.user_key().ToString().c_str())
              ){
                break;
                //it2++;
          }else{
              it2++;
            }
          }
          //files.back().second->EndFlag=1;//
          return 1;
        }else{
          return 0;
        }
      }else if(CoverFlag*1.5>max(OverlapFlag_cold,OverlapFlag_hot)){ 
      //if(smallestDensity<Level_density[Log_level]){
        int getFlag=0;
        FileMetaData* target=*smallestDensityIter;
        target->EndFlag=2;
        // check cover state, if no cover happens, forget it
        //std::list<FileMetaData*>::iterator CoverBeginIter;
        for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();
            it2!=SSTLog[Log_level].end();
            )
        {
          if(target->number == (*it2)->number){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=2;
              it2=SSTLog[Log_level].erase(it2);
          //}else if(CoverOrNot(target,*it2)==1){
            }else if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=2;
              it2=SSTLog[Log_level].erase(it2);
              getFlag=1;
          }else if(atoi((*it2)->smallest.user_key().ToString().c_str())>
              atoi(target->largest.user_key().ToString().c_str())
              ){
                break;
                //it2++;
          }
          else{
              it2++;
          }
        }
        if(getFlag==1){
          target->EndFlag=1314;
        }
        //files.back().second->EndFlag=2;//
        return 2;        
      }else{
        if(OverlapFlag_cold>OverlapFlag_hot){
          int getflag=0;
          FileMetaData* target=*minHotnessIter;
          target->EndFlag=3;
          files.push_back(std::make_pair(Log_level,target));
          SSTLog[Log_level].erase(minHotnessIter);
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=3;
              getflag=1;
              it2=SSTLog[Log_level].erase(it2);
            }else if(atoi((*it2)->smallest.user_key().ToString().c_str())>
              atoi(target->largest.user_key().ToString().c_str())
              ){
                break;
                //it2++;
          }else{
              it2++;
            }
          }
          //files.back().second->EndFlag=3;
          if(getflag==1){
            target->EndFlag=1314;
          }
          return 3;
        }else{
          int getflag=0;
          FileMetaData* target=*maxHotnessIter;
          files.push_back(std::make_pair(Log_level,target));
          target->EndFlag=4;
          SSTLog[Log_level].erase(maxHotnessIter);
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=4;
              getflag=1;
              it2=SSTLog[Log_level].erase(it2);
            }
            else if(atoi((*it2)->smallest.user_key().ToString().c_str())>
              atoi(target->largest.user_key().ToString().c_str())
              ){
                break;
                //it2++;
          }
          
          else{
              it2++;
            }
          }
          //files.back().second->EndFlag=4;
          if(getflag==1){
            target->EndFlag=1314;
          }
          return 4;
        } 
      }
    }
  
  return 0; 
}
// a new thread to analyze log 

void BackGroundLogManager()
{
  int log_level=-1;
  FileMetaData* inputF=NULL;
  std::list<std::pair<int,FileMetaData*>> outputFiles;
  int InPutFlag=0;
  int OutPutFlag=0;
  while(true){

    // data exchange
    InPutFlag=0;
    std::unique_lock<std::mutex> lck(mtx_Log);
    consume_Log.wait(lck, [] {return InBuffer.size() > 1 ; });     // wait(block) consumer until q.size() != 0 is true
     if(InBuffer.size()!=0){
       log_level=InBuffer.front().first;
       inputF=InBuffer.front().second;
       InBuffer.pop_front();
       SSTLog[log_level].push_back(inputF);
       InPutFlag=1;
     }
     if(OutBuffer.size()<10 && OutPutFlag!=0){
       for(std::list<std::pair<int,FileMetaData*>>::iterator iter=outputFiles.begin();
            iter!=outputFiles.end(); ){
              OutBuffer.push_back(*iter);
              iter=outputFiles.erase(iter);
        }
        OutPutFlag=0;
     }
     produce_Log.notify_all();
     
     if(InPutFlag==1){
       // we can make a judge by the return value 
       //to verify which threshold or policy have been touched
       OutPutFlag=Log2Buffer(log_level,outputFiles);
     }
     if(ThreadEnd==1){
       break;
     }
  }
}
*/
static int TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);  //have been changed from 10 to 4
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);   //have been changed from 25 to 10
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0; 

  while (level > 1) {
    result *= 10;   //have been changed from 10 to 4
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files,
             const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindFile(icmp, files, small.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->size()) {        // Marks as invalid
  }
  virtual bool Valid() const {
    return index_ < flist_->size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  virtual void Next() {
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]),
      &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(
        vset_->table_cache_->NewIterator(
            options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}
bool CheckPotential(std::vector<bool>& HasChance){
    for(int i=0;i<HasChance.size();i++){
        if(HasChance[i]==true)
            return true;
    }
    return false;
}
int CheckPotential_Index(std::vector<bool>& HasChance){
    for(int i=0;i<HasChance.size();i++){
        if(HasChance[i]==true)
            return i;
    }
    return -1;
}

//return index not fnumber
int Multi_May_Exist(const std::vector<FileMetaData*>& STBList,const std::string &key ){ 
    int k_=STBList.front()->stb->get_k();
    int m_=STBList.front()->stb->get_m();
    std::vector<bool> HasChance;
    std::vector<bool> Checked;
    for(int i=0;i<STBList.size();i++){
        HasChance.push_back(true);
    }
    for(int i=0;i<k_;i++){
        Checked.push_back(false);
    }
    uint32_t hash_val = STBList.front()->stb->hash_(0xbc9f1d34,key);
    const uint32_t hash_val_origin=hash_val;
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    
    
    for(int x=k_/2;x>0;x/=2){
        hash_val=hash_val_origin;
        for (int i = 0; i < k_; i+=x) {
            if(Checked[i]==true) {
                hash_val += (delta*x);
                continue;
            }else{
               if(CheckPotential_Index(HasChance)==-1) return -1; //no filter can satisfy
                const uint32_t bit_pos = hash_val % m_;
                for(int j=0;j<STBList.size();j++){
                    if(HasChance[j]){
                        if((STBList[j]->stb->bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0){
                        //if( ((STBList[j].second->bits_)[bit_pos/8] & (1 << (bit_pos%8))) == 0){
                            HasChance[j]=false;
                        }
                        /*else if( (i == (k_-1)) && (x==1) ){
                          return STBList[j]->number;
                        }
                        */
                    }
                }
                hash_val += (delta*x);
                Checked[i]=true; 
            }
        }
    }
    
    return CheckPotential_Index(HasChance);
}
bool Multi_May_0Exist(std::vector<FileMetaData*>& STBList,
                      const std::string &key,
                      int &index){
    for (size_t i=0;i<STBList.size();){
      if(STBList[i]->stb==NULL){
        STBList.erase(STBList.begin()+i);
      }else{
        i++;
      }
    }
    if(STBList.size()==0) {
        return false;
    }else if(STBList.size()==1){
      index=0;
      return STBList.front()->stb->contains(key);  
    } 
    
    int size_=STBList.size();
    int k_=STBList.front()->stb->get_k();
    int m_=STBList.front()->stb->get_m();
    std::vector<bool> Has_Chance;
    Has_Chance.resize(size_,true);
    uint32_t tmp_hash_val = STBList.front()->stb->hash_(0xbc9f1d34,key);
    std::vector<uint32_t> BitPos;
    BitPos.resize(k_,0);
    const uint32_t delta = (tmp_hash_val >> 17) | (tmp_hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        const uint32_t bit_pos = tmp_hash_val % m_;
        BitPos[i]=bit_pos;
        const uint32_t bit_pos_devided=bit_pos/8;
        if(bit_pos_devided > (m_ + 7) / 8){
          index=0;
          return false;
        }
        if ((STBList[0]->stb->bits_[bit_pos_devided] & (1 << (bit_pos % 8))) == 0) {
            Has_Chance[0]=false;
        }
        tmp_hash_val += delta;
    }
    if(Has_Chance[0]== true){
      index=0;
      return true;
    }
    for(int j=1;j<size_;j++){
      for (int i = 0; i < k_; ++i) {
        const uint32_t bit_pos = BitPos[i];
        const uint32_t bit_pos_devided=bit_pos/8;
        if(bit_pos_devided > (m_ + 7) / 8 ){
          index=j;
          return false;
        }
        if ((STBList[j]->stb->bits_[bit_pos_devided] & (1 << (bit_pos % 8))) == 0) {
            Has_Chance[j]=false;
            break;
        }
      }
      if(Has_Chance[j]==true){
        index=j;
        return true;
      }
    }
    if(Has_Chance[size_-1]==true){
      index=size_-1;
      return true;
    }else{
      return false;
    }
    


}

FileMetaData* May_Exist(const std::vector<FileMetaData*>& STBList,const std::string &key ){
    int k_=STBList[0]->stb->get_k();
    int m_=STBList[0]->stb->get_m();
    std::vector<bool> HasChance;
    for(int i=0;i<STBList.size();i++){
        HasChance.push_back(true);
    }
    uint32_t hash_val = STBList[0]->stb->hash_(0xbc9f1d34,key);
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        if(CheckPotential(HasChance)==false) break;
        const uint32_t bit_pos = hash_val % m_;
        for(int j=0;j<STBList.size();j++){
            if(HasChance[j]){
                if((STBList[j]->stb->bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0){
                //if( ((STBList[j].second->bits_)[bit_pos/8] & (1 << (bit_pos%8))) == 0){
                    HasChance[j]=false;
                }else if(i == (k_-1)){
                    return STBList[j];
                }
            }
        }
        
        hash_val += delta;
    }
    return NULL;
}

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {

  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;
  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  std::vector<FileMetaData*> tmp;
  FileMetaData* tmp2;
  std::vector<FileMetaData*> SSTtarget;
  int GetFlag;
  readtime++;
  for (int level = 0; level < config::kNumLevels; level++) {
    GetFlag=0;
    size_t num_files = files_[level].size();
    if (num_files == 0 && LogIndex[level].empty() && SSTKeyLog[level].empty()) continue;
    // Get the list of files to search in this level
    FileMetaData* const* files = &files_[level][0];
    if (level == 0) {
      // Level-0 files may overlap each other.  Find all files that
      // overlap user_key and process them in order from newest to oldest.
      tmp.reserve(num_files);
      std::vector<FileMetaData*> Candidate_;
      Candidate_.reserve(LogIndex[level].size());
      for (uint32_t i = 0; i < num_files; i++) {
        FileMetaData* f = files[i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
            ucmp->Compare(user_key, f->largest.user_key()) <= 0 ){
              if(f->stb!=NULL &&f->stb->contains(user_key.ToString())){
                tmp.push_back(f);
               if(GetFlag==1){
                 break;
               }
                GetFlag=1;
                level0Hit++;
        
              }else if(f->stb==NULL)
              {
                tmp.push_back(f);
                if(GetFlag==1){
                 break;
               }
              }
        }
      }
      
      if (tmp.empty()) continue;

      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      files = &tmp[0];
      num_files = tmp.size();
    } else {
      //find in Log first
      SSTtarget.clear();
      if(WarmUpFlag==-10 &&!SSTKeyLog[level].empty()){
        for(std::map<uint64_t, leveldb::FileMetaData*,cmp_key>::iterator it=SSTKeyLog[level].begin();
                 it!=SSTKeyLog[level].end();
                 it++)
        {
           if( ucmp->Compare(user_key, (*it).second->smallest.user_key()) >= 0 &&
              ucmp->Compare(user_key, (*it).second->largest.user_key()) <= 0 
          ){
            if((*it).second->stb != NULL ){
              if( (*it).second->stb->contains(user_key.ToString())){
                //tmp2=it->second;
                SSTtarget.push_back(it->second);
                //files = &tmp2;
                //num_files=1;
                //SSTtarget.push_back( (*it).second );
                GetFlag=1;

                break;
            }
            else{
                //SSTtarget.push_back((*it).second);
                //GetFlag=2;

            }
            }else{
                NullFilter++;
              }

          }

        }
      }
      if( WarmUpFlag==-1 && GetFlag==0  && !LogIndex[level].empty()){
        //1. find in log
        std::vector<FileMetaData*> Candidate;
        Candidate.reserve(2);
        for(std::map<uint64_t,leveldb::FileMetaData*>::iterator iter=LogIndex[level].begin();
                iter!=LogIndex[level].end();iter++){
          if( ucmp->Compare(user_key, (*iter).second->smallest.user_key()) >= 0 &&
              ucmp->Compare(user_key, (*iter).second->largest.user_key()) <= 0 
          ){
            
            Candidate.push_back((*iter).second);
          }
        }
        //2. find in LSM
        // Binary search to find earliest index whose largest key >= ikey.
        uint32_t index = FindFile(vset_->icmp_, files_[level], ikey); //根据index定位到具体的sst
        if (index >= num_files) {
        } else {
          tmp2 = files[index];
          if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
            // All of "tmp2" is past any data for user_key
          } else {
            Candidate.push_back(tmp2);
          }
        }
        // 3.match
        if(!Candidate.empty()){
          if(Candidate.size()==1){
            if(Candidate.front()->stb==NULL){
              SSTtarget.push_back(Candidate[0]);
            }else if(Candidate.front()->stb->contains(user_key.ToString())){
              SSTtarget.push_back(Candidate[0]);
              GetFlag=1;
              seekInLog++;
              FilterHit++;
            }else{
              FilterMiss++;
            }
          }else{
            int Index=-1;
            bool ok=Multi_May_0Exist(Candidate,user_key.ToString(),Index);
            //for(int i=0;i<Candidate.size();i++){
            //    Candidate[i]->allowed_seeks-=1;
            //  }
            if(!ok){
              //for(int i=0;i<Candidate.size();i++){
                //Candidate[i]->allowed_seeks-=1;
              //}
              if(Index!=-1){
                int wrong=Index;
              }
              FilterMultiMiss++;
            }else{
              SSTtarget.push_back(Candidate[Index]);
              //SSTtarget.back()->allowed_seeks+=1;
              GetFlag=1;
              seekInLog++;
              FilterHit++;
            }
            
          }
        }
        //4. collect table
        files = &SSTtarget[0];
        num_files = SSTtarget.size();
      }else {
        //only find in LSM    
        // Binary search to find earliest index whose largest key >= ikey.
        uint32_t index = FindFile(vset_->icmp_, files_[level], ikey); //根据index定位到具体的sst
        if (index >= num_files) {
          files = &SSTtarget[0];
          num_files = SSTtarget.size();
        } else {
            tmp2 = files[index];
            if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
              // All of "tmp2" is past any data for user_key
            } else {
             /*
              //if(SSTtarget.empty() && tmp2->allowed_seeks<30){
              if(SSTtarget.empty() ){
                if(tmp2->stb!=NULL){
                  if(tmp2->stb->contains(user_key.ToString())){
                    SSTtarget.push_back(tmp2);
                    files=&SSTtarget[0];
                    num_files = SSTtarget.size(); 
                    weakHit++; 
                  }else{
                    //tmp2->EndFlag=12138;
                    files = &SSTtarget[0];
                    num_files = SSTtarget.size();
                    weakMiss++;
                  }
                }else{
                  NullFilter++;
                }
              }else{  
                */            
               /*
                if(tmp2->allowed_seeks < 5){
                  tmp2->EndFlag=12138;
                }
                */
                if(tmp2->stb!=NULL&& tmp2->stb->contains(user_key.ToString())){
                  SSTtarget.push_back(tmp2);
                }else{
                  //SSTtarget.push_back(tmp2);
                  //tmp2->allowed_seeks-=2;
                }
              
            }
            files = &SSTtarget[0];
            num_files = SSTtarget.size();
        }
      }
    }
    

    for (uint32_t i = 0; i < num_files; ++i) {
      if (last_file_read != NULL && stats->seek_file == NULL) {
        // We have had more than one seek for this read.  Charge the 1st file.
        if(last_file_read->InLog==false && last_file_read_level<6){
          stats->seek_file = last_file_read;
          stats->seek_file_level = last_file_read_level;
        }
      }

      FileMetaData* f = files[i];
      last_file_read = f;
      last_file_read_level = level;

      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;
      //在table_cache中查找key对应的value
      s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                   ikey, &saver, SaveValue);
      seektime++;
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          //f->allowed_seeks-=1;
          /*
          if(f->allowed_seeks<0 && f->InLog==false && level < 6){
              stats->seek_file = f;
              stats->seek_file_level = level;
            }
            */
          if(level==0 ){
            SeekMissLevel0++;
            /*
            //Read Policy 1
            
            if(f->allowed_seeks<0 && f->InLog==false){
              stats->seek_file = f;
              stats->seek_file_level = level;
            }*/
            
          }
          if(f->InLog==true){
            
            SeekMissFIlter++;
            /*
            //Read Policy 2
            
            f->allowed_seeks-=5;
            if(f->allowed_seeks<5){
              f->EndFlag=12138;
            }*/
            

          }else{
            SeekMissLSM++;
          }
          if(FileNameForFilter.find(f->number)!=FileNameForFilter.end()){
            MissLoad++;
          }
          break;      // Keep searching in other files
        case kFound:
            seekHit++;
          if(level==0){
            seekHitInLevel0++;
          }
          if(f->InLog==true){
            seekHitInLog++;
          }else{
            seekHitInLSM++;
          }
          if(FileNameForFilter.find(f->number)!=FileNameForFilter.end()){
            HitLoad++;
          }
          
          
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
     
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
         
          return s;
      }
    }
  }

  return Status::NotFound(Slice());  // Use an empty error message for speed
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != NULL) {
    user_begin = begin->user_key();
  }
  if (end != NULL) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size(); ) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    // Update compaction pointers
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    if(!edit->deleted_files_.empty()){
      const VersionEdit::DeletedFileSet& del = edit->deleted_files_;
      for (VersionEdit::DeletedFileSet::const_iterator iter = del.begin();
           iter != del.end();
           ++iter) {
        const int level = iter->first;
        const uint64_t number = iter->second;
        levels_[level].deleted_files.insert(number);
        if(LogLiveFiles.count(number)!=0){
          LogLiveFiles.erase(number);
        }
      }
    }
    


    // Add new files
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;
      std::unordered_map<uint64_t,std::pair<std::pair<uint64_t,int>,leveldb::SSTBloom_Opt_2*> >::iterator it=SSTAttriMap.find(f->number);
      if(it!=SSTAttriMap.end()){ //it didn't work for move 
        f->NumEntries=it->second.first.first;
        f->Hotness=it->second.first.second;
        f->stb=it->second.second;
        SSTAttriMap.erase(it);
      }else{
        //int x=0;
        f->stb=edit->new_files_[i].second.stb;
      }

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number); 
      //levels_[level].added_files->insert(f);
       if(KeyLogNames.find(f->number)!=KeyLogNames.end()){
         //for compaction,not move
        std::unordered_map<uint64_t,leveldb::SSTBloom_Opt_2*>::iterator it=LogFilterBuffer.find(f->number);
        f->stb=it->second;
        LogFilterBuffer.erase(it);
        SSTKeyLog[level][f->number]=f;
      }else{
        levels_[level].added_files->insert(f);
      }
    }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added = levels_[level].added_files;
      v->files_[level].reserve(base_files.size() + added->size());
      for (FileSet::const_iterator added_iter = added->begin();
           added_iter != added->end();
           ++added_iter) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, *added_iter);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i-1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    prev_end.DebugString().c_str(),
                    this_begin.DebugString().c_str());
            abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
      
      /*
      if(f->stb!=NULL){
        f->stb->DeleteArray();
        delete f->stb;
        f->stb=NULL;
      }
      */
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

Status VersionSet::Recover(bool *save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  }

  return s;
}

bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == NULL);
  assert(descriptor_log_ == NULL);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == NULL);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels-1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      
      score = v->files_[level].size() /
          static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
  if(best_level==0){
    l4++;
    if(best_score>1){
      l44++;
    }
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "files[ %d %d %d %d %d %d %d ]",
           int(current_->files_[0].size()),
           int(current_->files_[1].size()),
           int(current_->files_[2].size()),
           int(current_->files_[3].size()),
           int(current_->files_[4].size()),
           int(current_->files_[5].size()),
           int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }

    if(!LogLiveFiles.empty()){
       for(std::set<uint64_t>::iterator it=LogLiveFiles.begin();it!=LogLiveFiles.end();it++){
           live->insert(*it); 
       }
     }
    if(!KeyLogNames.empty()){
       for(std::set<uint64_t>::iterator it=KeyLogNames.begin();it!=KeyLogNames.end();it++){
           live->insert(*it); 
       }
    }
   
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}
/*
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(
              options, files[i]->number, files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}
*/
Iterator *VersionSet::MakeInputIterator(Compaction *c)
{
  for(int i=0;i<25;i++){
    //Log(options_->info_log, "TS0");
    if(tmplist[i].size()!=0){
      Log(options_->info_log, "Clear-1");
      tmplist[i].clear();
    }
  }
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
   //Log(options_->info_log, "TS1");
   if(c->fromLog == 1 ){
    Log(options_->info_log, "Modified Compaction.");
    const int space =  c->inputs_[0].size()+1;
    Log(options_->info_log, "Iter space is %d.",space);
    Iterator **list = new Iterator *[space];
    int num = 0;
    for (int which = 0; which < 2; which++)
    {
      //Log(options_->info_log, "TS4");
      if (!c->inputs_[which].empty())
      {
        //Log(options_->info_log, "TS5");
        if (which == 0)
        {
          //Log(options_->info_log, "TS6");
          const std::vector<FileMetaData *> &files = c->inputs_[which];
          for (size_t i = 0; i < files.size(); i++)
          {
            tmplist[i].push_back(c->inputs_[which][i]);
            const std::vector<FileMetaData *> &tmp=tmplist[i];
            //Log(options_->info_log, "TS7");
            //list[num++] = table_cache_->NewIterator(
              //  options, files[i]->number, files[i]->file_size);
            list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, &tmp),
              &GetFileIterator, table_cache_, options);   
            Log(options_->info_log, "Current Iter-%d OK",(int)i);
          }
        }
        else
        {
          //Log(options_->info_log, "TS12");
          // Create concatenating iterator for the files from this level
          list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
              &GetFileIterator, table_cache_, options);
          //Log(options_->info_log, "TS13");
        }
      }
    }
    //Log(options_->info_log, "TS14");
    assert(num <= space);
    Iterator *result = NewMergingIterator(&icmp_, list, num);
    //Log(options_->info_log, "TS15");
    delete[] list;
    //Log(options_->info_log, "TS6");
    return result;
  }else{
    //Log(options_->info_log, "Tm1");
    const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
    Iterator **list = new Iterator *[space];
    int num = 0;
    for (int which = 0; which < 2; which++)
    {
      //Log(options_->info_log, "Tm2");
      if (!c->inputs_[which].empty())
      {
        if (c->level() + which == 0)
        {
          const std::vector<FileMetaData *> &files = c->inputs_[which];
          for (size_t i = 0; i < files.size(); i++)
          {
            list[num++] = table_cache_->NewIterator(
                options, files[i]->number, files[i]->file_size);
          }
        }
        else
        {
          // Create concatenating iterator for the files from this level
          list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
              &GetFileIterator, table_cache_, options);
        }
      }
    }
    assert(num <= space);
    //Log(options_->info_log, "Tm3");
    Iterator *result = NewMergingIterator(&icmp_, list, num);
    //Log(options_->info_log, "Tm4");
    delete[] list;
    //Log(options_->info_log, "Tm5");
    return result;
  }
  
}
/*
int Log2Buffer(int Log_level,std::vector<std::pair<int,FileMetaData*>>& files){
  
    int size_=SSTLog[Log_level].size();
    if(size_>Thres_density[Log_level]){
      int maxHotness=0;
      int minHotness=999999999;
      std::list<FileMetaData*>::iterator maxHotnessIter;
      std::list<FileMetaData*>::iterator minHotnessIter;
      float smallestDensity=1.0;
      std::list<FileMetaData*>::iterator smallestDensityIter;
      //1.info collection
      long total_hotness_this_time=0;
      int count_=0;
      float total_density_this_time=0.0;
      for(std::list<FileMetaData*>::iterator it1=SSTLog[Log_level].begin();
          it1!=SSTLog[Log_level].end();
          it1++)
      {
        //1.check Density,which have the same effect as range 
        //  cuz with same count keys(we suppose that key count 
        //  is raletivly close) bigger range means smaller density 
        count_++;
        float density=GetDensity(*it1);
        total_density_this_time+=density;
        if(density<smallestDensity){
          smallestDensity=density;
          smallestDensityIter=it1;
        }
        //2.check overlap situation, we need to define how hot a sst is
        //  using filter to measure how many hot keys in a SST, to show how hot a sst is 
        total_hotness_this_time+=(*it1)->Hotness;
        if((*it1)->Hotness>maxHotness){
          maxHotness=(*it1)->Hotness;
          maxHotnessIter=it1;
        }
        if((*it1)->Hotness<minHotness){
          minHotness=(*it1)->Hotness;
          minHotnessIter=it1;
        }
      }
      total_density[Log_level]+=(total_density_this_time/count_);
      total_density_count[Log_level]++;
      total_hotness[Log_level]+=(total_hotness_this_time/count_);
      total_hotness_count[Log_level]++;
      //2. check this time which attri we should take care of
      int CoverFlag=0;
      int OverlapFlag_hot=0;
      int OverlapFlag_cold=0;
      for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();it2++){
          if(CoverOrNot(*smallestDensityIter,*it2)==1 
              && (*smallestDensityIter)->number!=(*it2)->number){
            //CoverBeginIter=it2;
            CoverFlag++;
          }
          if(OverlapOrNot(*minHotnessIter,*it2)==1
              && (*minHotnessIter)->number!=(*it2)->number){
                OverlapFlag_cold++;
              }
          if(OverlapOrNot(*maxHotnessIter,*it2)==1
              && (*maxHotnessIter)->number!=(*it2)->number){
                OverlapFlag_hot++;
              }
        }
      if(CoverFlag==0 && OverlapFlag_cold==0 && OverlapFlag_hot==0){
        if( size_>Thres_thres[Log_level]){
          //if(smallestDensity<Level_density[Log_level]){
          FileMetaData* target=SSTLog[Log_level].front();
          files.push_back(std::make_pair(Log_level,target));
          SSTLog[Log_level].pop_front();
          // check cover state, if no cover happens, forget it
          //std::list<FileMetaData*>::iterator CoverBeginIter;
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(CoverOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              it2=SSTLog[Log_level].erase(it2);
            }else{
              it2++;
            }
          }
          files.back().second->EndFlag=1;
          return 1;
        }else{
          return 0;
        }
      }else if(CoverFlag*2>max(OverlapFlag_cold,OverlapFlag_hot)){ 
      //if(smallestDensity<Level_density[Log_level]){
        FileMetaData* target=*smallestDensityIter;
        // check cover state, if no cover happens, forget it
        //std::list<FileMetaData*>::iterator CoverBeginIter;
        for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
          if(target->number == (*it2)->number){
              files.push_back(std::make_pair(Log_level,*it2));
              it2=SSTLog[Log_level].erase(it2);
          }else if(CoverOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              it2=SSTLog[Log_level].erase(it2);
          }else{
              it2++;
          }
        }
        files.back().second->EndFlag=1;
        return 2;        
      }else{
        if(OverlapFlag_cold>OverlapFlag_hot){
          FileMetaData* target=*minHotnessIter;
          files.push_back(std::make_pair(Log_level,target));
          SSTLog[Log_level].erase(minHotnessIter);
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              it2=SSTLog[Log_level].erase(it2);
            }else{
              it2++;
            }
          }
          files.back().second->EndFlag=1;
          return 3;
        }else{
          FileMetaData* target=*maxHotnessIter;
          files.push_back(std::make_pair(Log_level,target));
          SSTLog[Log_level].erase(maxHotnessIter);
          for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              it2=SSTLog[Log_level].erase(it2);
            }else{
              it2++;
            }
          }
          files.back().second->EndFlag=1;
          return 4;
        } 
      }
    }
  
  return 0; 
  }

void VersionSet::Buffer2Log(int Log_level){
  std::list<std::pair<int,FileMetaData*>> files_;
  int flag=0;
  std::unique_lock<std::mutex> lck(mtx_Log);
  produce_Log.wait(lck, [] {return InBuffer.size() < 10 ; });   // wait(block) producer until q.size() != maxSize is true

  consume_Log.notify_all();
  if(!InBuffer.empty()){
      while(!InBuffer.empty()){
        Log(options_->info_log," %d-%ld Buffer to Log",InBuffer.front().first,InBuffer.front().second->number);
        SSTLog[InBuffer.front().first].push_back(InBuffer.front().second);
        InBuffer.pop_front();
      }
    }
  
  //int flag=Log2Buffer(Log_level,files_);

  if(flag==2){
    // add to buffer_density
    for(std::list<std::pair<int,FileMetaData*>>::iterator it=files_.begin();it!=files_.end();it++){
        Log(options_->info_log,"%d-%ld Log to Buffer Density",it->first,it->second->number);
        OutBuffer_thres[it->first].push_back(it->second);
    }
  }else if(flag == 3 ){
     // add to buffer_hot
     for(std::list<std::pair<int,FileMetaData*>>::iterator it=files_.begin();it!=files_.end();it++){
       Log(options_->info_log,"%d-%ld Log to Buffer Coldness",it->first,it->second->number);
       OutBuffer_accumu[it->first].push_back(it->second);
     }
  }else if(flag == 4 ){
     // add to buffer_hot
     for(std::list<std::pair<int,FileMetaData*>>::iterator it=files_.begin();it!=files_.end();it++){
       Log(options_->info_log,"%d-%ld Log to Buffer Hotness",it->first,it->second->number);
       OutBuffer_accumu[it->first].push_back(it->second);
     }
  }else if(flag == 1 ){
     // add to buffer_hot
     for(std::list<std::pair<int,FileMetaData*>>::iterator it=files_.begin();it!=files_.end();it++){
       Log(options_->info_log,"%d-%ld Log to Buffer Threshold",it->first,it->second->number);
       OutBuffer_accumu[it->first].push_back(it->second);
     }
  }
}
*/
void InsertFileIntoMeta(  std::vector<FileMetaData*>& files,FileMetaData* f){
  if(files.empty()){
    files.push_back(f);
    return;
  } 
  int left=atoi(f->smallest.user_key().ToString().c_str());
  int tmp_left;
  for(std::vector<FileMetaData*>::iterator it=files.begin();it!=files.end();){
    tmp_left=atoi((*it)->smallest.user_key().ToString().c_str());
    if(left<=tmp_left){
      files.insert(it,f);
      break;
    }else if(*it==files.back()){
      files.push_back(f);
      break;
    }else{
      it++;
    }
  }
  return;

}int max_(int a, int b){
  if(a>=b) return a;
  else return b;
}
int min_(int a, int b){
  if(a<=b) return a;
  else return b;
}
int Overlap_NotChange(int& left,int& right,FileMetaData* f2){
    //max(A.start,B.start)<=min(A.end,B,end)
    int b=atoi(f2->smallest.user_key().ToString().c_str());
    int d=atoi(f2->largest.user_key().ToString().c_str());
    return max_(left,b)<= min_(right,d)? 1:0;
    
}
int Overlap_Change(int& left,int& right,FileMetaData* f2){
    //max(A.start,B.start)<=min(A.end,B,end)
    int b=atoi(f2->smallest.user_key().ToString().c_str());
    int d=atoi(f2->largest.user_key().ToString().c_str());
    int flag=max_(left,b)<= min_(right,d)? 1:0;
    if(flag==1){
      left=min_(left,b);
      right=max_(right,d);
      return 1;
    }else{
      return 0;
    }
}
int CheckSSTInNextLevel(int left,int right,std::vector<FileMetaData*>& Next_files){
  int count_=0;
  for(std::vector<FileMetaData*>::iterator it=Next_files.begin();it!=Next_files.end();it++){
    if(Overlap_NotChange(left,right,*it)==1){
      count_++;
    }
  }
  return count_;
}
int RangeForecast(int& left,int& right,FileMetaData* target,std::vector<FileMetaData*>& Next_files){
  int tmp_l=left;
  int tmp_r=right;
  if(Overlap_Change(tmp_l,tmp_r,target)==1){
    int next=CheckSSTInNextLevel(tmp_l,tmp_r,Next_files);
    //remark!!!!!!!!!!!
    if(next!=0){
      left=tmp_l;
      right=tmp_r;
    }
    return next;
  }else{
    //not overlap with current files, so it must enlarge wa
    return -1;
  }
}
int TmpOutPutMax(){
  int count_=1;
  int level=-1;
  for(int i=0;i<7;i++){
    if(TmpOutPut[i].size()>count_){
      count_=TmpOutPut[i].size();
      level=i;
    }
  }
  if(count_>TargetTmpSize[level])
    return level;
  return 0;
}
Compaction* VersionSet::CheckOutBuffer(int Log_level,std::list<std::pair<int,FileMetaData*>>& outfiles){
  int Tmplevel=-1;
  FileMetaData* TmpF;
  if(!outfiles.empty()){
    Log(options_->info_log,"Check Out Buffer");
    while(!outfiles.empty()){
      Tmplevel=outfiles.front().first;
      TmpF=outfiles.front().second;
      outfiles.pop_front();
      if(TmpF->BySeek==0){
        TmpOutPut[Tmplevel].push_back(TmpF);
      }else{
        Log(options_->info_log,"-1 InValid SST:%ld",TmpF->number);
      }
    }
    if(TmpOutPut[Log_level].size()>5 && Tmplevel!=Log_level){
      Log(options_->info_log,"Compact level changed %d ==> %d:",Tmplevel,Log_level);
      Tmplevel=Log_level;
    }
  }else{
    Tmplevel=TmpOutPutMax();
    Log(options_->info_log,"Check Tmp Existing SST,Compact level:%d",Log_level);
  }
  int level=Tmplevel-1;
  if(!TmpOutPut[Tmplevel].empty()){
    //find the loosest
    int x=0;
    int y=0;
    FileMetaData* f;
    int getCandidate=0;
    for(std::list<leveldb::FileMetaData* >::iterator iter=TmpOutPut[Tmplevel].begin();
        iter!=TmpOutPut[Tmplevel].end();)
    {
          if( (*iter)->BySeek==1){
            Log(options_->info_log,"0 InValid SST:%ld",(*iter)->number);
            iter=TmpOutPut[Tmplevel].erase(iter);
            if(TmpOutPut[Tmplevel].empty()){
               Log(options_->info_log,"Tmp is empty now");
              return NULL;
            }
          }else{
            if((*iter)->EndFlag==1314){
              f=*iter;
              Log(options_->info_log,"The Choosen One:%ld",(*iter)->number);
              iter=TmpOutPut[Tmplevel].erase(iter);
              getCandidate=1;
              break;
            }else if((*iter)==TmpOutPut[Tmplevel].back()){
              // can be changed to wait only sst
              f=*iter;
              Log(options_->info_log,"The Last One:%ld,EndFalg:%d",(*iter)->number,(*iter)->EndFlag);
              TmpOutPut[Tmplevel].pop_back();
              getCandidate=1;
              break;
            }
            iter++;
          }

    }
    if(getCandidate==0){
      f=TmpOutPut[Tmplevel].front();
      TmpOutPut[Tmplevel].pop_front();
      Log(options_->info_log,"No suitable SST,then %ld",f->number);
    }
    Compaction* c = new Compaction(options_, level);
    InsertFileIntoMeta(c->inputs_[0],f);
    //c->inputs_[0].push_back(f);
    c->input_version_ = current_;
    c->input_version_->Ref();
    SetupOtherInputsForLog(c);
    if(c->inputs_[1].empty()){
      Log(options_->info_log,"%d-%ld Maybe Move",level,c->inputs_[0][0]->number);
      return c;
    } 
    float formalRatio=c->inputs_[1].size()/(float)c->inputs_[0].size();
    float currentRatio=formalRatio;
    Log(options_->info_log,"0 %ld.ldb EndFlag:%d,first Ratio:%f,Hotness:%d",f->number, f->EndFlag,formalRatio,f->Hotness);
    if(formalRatio>10){
        Log(options_->info_log,"0 BigRatio:%f",formalRatio);
    }
    int TmpLeft=atoi(f->smallest.user_key().ToString().c_str());
    int TmpRight=atoi(f->largest.user_key().ToString().c_str());
    for(std::list<leveldb::FileMetaData* >::iterator iter=TmpOutPut[Tmplevel].begin();
        iter!=TmpOutPut[Tmplevel].end();)
    {
      if((*iter)->BySeek==1){
        Log(options_->info_log,"1 InValid SST:%ld",(*iter)->number);
        iter=TmpOutPut[Tmplevel].erase(iter);
      }
      //while(currentRatio<5.0 || currentRatio<=formalRatio){
      if(TmpOutPut[Tmplevel].empty() || c->inputs_[0].size()>24)  break;
      if(!(currentRatio<5.0 || currentRatio<=formalRatio)) {
        Log(options_->info_log,"WA:%f",currentRatio);
        break;
      }
        
      //f=TmpOutPut[Log_level].front();
      f=*iter;
      // may add sst in level i to this compaction is valuable
      
      //remark!!!!!!
      
      int count_next=RangeForecast(TmpLeft,TmpRight,f,current_->files_[level+1]);
      if(count_next==-1 || count_next/(1+(float)c->inputs_[0].size())>8.0 || count_next/(1+(float)c->inputs_[0].size())>formalRatio ){
        Log(options_->info_log,"%ld may cause extra WA",f->number);
        iter++;
        continue;
        //break;
      }
      formalRatio=currentRatio;
      c->fromLog=1;
      InsertFileIntoMeta(c->inputs_[0],f);
      //TmpOutPut[Log_level].pop_front();
      iter=TmpOutPut[Tmplevel].erase(iter);
      SetupOtherInputsForLog(c);
      currentRatio=c->inputs_[1].size()/(float)c->inputs_[0].size();
      Log(options_->info_log,"1 %ld.ldb EndFlag:%d,Current Ratio:%f",f->number,f->EndFlag,currentRatio);
      if(currentRatio>10){
        Log(options_->info_log,"1 BigRatio:%f",currentRatio);
      }
      //if(f->EndFlag==1) break;
    }
    Log(options_->info_log,"Log compact %ld#%d + %ld#%d by Log",c->inputs_[0].size(),level,
                          c->inputs_[1].size(),level+1);
    return c;
  }
  else{
    return NULL;
  }
}
void VersionSet::CheckLogStats(){
  Log(options_->info_log,"{  Total Live SST in Log:%ld, Lives:%ld,LogIndex:%ld",LogLiveFiles.size(),LogFiles.size(),
                     LogIndex[2].size()+LogIndex[3].size()+LogIndex[4].size()+LogIndex[5].size()+LogIndex[6].size() );
  Log(options_->info_log,"   SSTLog: [%ld %ld %ld %ld %ld %ld %ld]",
          SSTLog[0].size(),SSTLog[1].size(),SSTLog[2].size(),SSTLog[3].size(),
          SSTLog[4].size(),SSTLog[5].size(),SSTLog[6].size());
  Log(options_->info_log,"   InBuffer:%ld",InBuffer.size());
  Log(options_->info_log,"   OutBuffer:%ld",OutBuffer.size());
  Log(options_->info_log,"   TmpOutPut: [%ld %ld %ld %ld %ld %ld %ld]   }",
        TmpOutPut[0].size(),TmpOutPut[1].size(),TmpOutPut[2].size(),
        TmpOutPut[3].size(),TmpOutPut[4].size(),TmpOutPut[5].size(),TmpOutPut[6].size());
}
int VersionSet::SSTExchangeWithLog(int log_level,FileMetaData* target,
                std::list<std::pair<int,FileMetaData*>>& outputFiles){
  int outFlag=0;
  std::unique_lock<std::mutex> lck(mtx_Log);
  produce_Log.wait(lck, [] {return InBuffer.size() != 50 ; });   // wait(block) producer until q.size() != maxSize is true
  InBuffer.push_back(std::make_pair(log_level,target));
  LogLiveFiles.insert(target->number);
  LogFiles.insert(target->number);
  LogIndex[log_level][target->number]=target;
  if(!OutBuffer.empty()){
    while(!OutBuffer.empty()){
      outputFiles.push_back(OutBuffer.front());
      OutBuffer.pop_front();
    }
    outFlag=1;
  }
  consume_Log.notify_all();
  return outFlag;
}
int TmpOutPutNotNull(){
  for(int i=0;i<7;i++){
    if(TmpOutPut[i].size()>TargetTmpSize[i]){
      return 1;
    }
  }
}
float GetDensity_LSM(FileMetaData* f){
    int left=atoi(f->smallest.user_key().ToString().c_str());
    int right=atoi(f->largest.user_key().ToString().c_str());
    return (float)f->NumEntries/(right-left);
        

}
Compaction* VersionSet::AddSSTIntoLog(int level ){
    CheckLogStats();
    int GetFlag=0;
    InternalKey larger;
    FileMetaData* target;
    std::list<std::pair<int,FileMetaData*>> outputFiles;
    // Pick the first file that comes after compact_pointer_[level]
    /*
    //Read Policy 3
    
    if(WarmUpFlag==-1 && get_start==1){
          for (size_t i = 0; i < current_->files_[level].size(); i++) {
            FileMetaData* f = current_->files_[level][i];
            
            if (f->EndFlag==12138) {
              
              target=f;
              target->InLog=true;
              //target->allowed_seeks+=101;
              
              current_->files_[level].erase(current_->files_[level].begin()+i);
              GetFlag=1;
              Log(options_->info_log,"Get_seek %ld@%d,%ld.ldb\n",i,level,target->number);
              break;
          }

          else  if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
              //InBuffer.push_back(std::make_pair(level+1,f));
              target=f;
              target->InLog=true;
              /*
              //Read Polci 4.1
              *//*
              if(WarmUpFlag==-1){
                //target->allowed_seeks+=101;
              }
              
            current_->files_[level].erase(current_->files_[level].begin()+i);
            GetFlag=1;
            Log(options_->info_log,"Get_0 %ld@%d,%ld.ldb\n",i,level,target->number);
            break;
            }
      }
    }
    */

    //check hotness and density
    if(GetFlag==0){
      int hotestIndex=0;
      int hotest=0;
      float density=1.0;
      int densityIndex=-1;
      int NormalIndex=-1;
      larger=current_->files_[level][0]->largest;
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (f->Hotness>hotest) {
            hotest=f->Hotness;
            hotestIndex=i;
        }
        float tmp_density=GetDensity_LSM(f);
        if(tmp_density<density){
          density=tmp_density;
          densityIndex=i;
        }
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
            //InBuffer.push_back(std::make_pair(level+1,f));
            NormalIndex=i;
            larger=f->largest;
            break;
        }
      }
      if(hotestIndex>-1&& hotest>avg_hotness[level+1]){
        GetFlag=1;
        target=current_->files_[level][hotestIndex];
        target->InLog=true;
        current_->files_[level].erase(current_->files_[level].begin()+hotestIndex);
        Log(options_->info_log,"Get_hotest %d@%d,%ld.ldb\n",hotestIndex,level,target->number);
      }else if(densityIndex>-1 && density>avg_dens[level]){
        GetFlag=1;
        target=current_->files_[level][densityIndex];
        target->InLog=true;
        current_->files_[level].erase(current_->files_[level].begin()+densityIndex);
        Log(options_->info_log,"Get_density %d@%d,%ld.ldb\n",densityIndex,level,target->number);
      }else if(NormalIndex>-1){
        GetFlag=1;
        target=current_->files_[level][NormalIndex];
        target->InLog=true;
        current_->files_[level].erase(current_->files_[level].begin()+NormalIndex);
        Log(options_->info_log,"Get_Normal %d@%d,%ld.ldb\n",NormalIndex,level,target->number);
      }else{
        GetFlag=1;
        target=current_->files_[level][0];
        target->InLog=true;
        Log(options_->info_log,"Get_0 0@%d,%ld.ldb\n",level,target->number);
        current_->files_[level].erase(current_->files_[level].begin());
      }

    }

    /*
    if(GetFlag==0){
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
            //InBuffer.push_back(std::make_pair(level+1,f));
              target=f;
              target->InLog=true;
              
              if(WarmUpFlag==-1){
                //target->allowed_seeks+=101;
              }
              
            current_->files_[level].erase(current_->files_[level].begin()+i);
            GetFlag=1;
            Log(options_->info_log,"Get_0 %ld@%d,%ld.ldb\n",i,level,target->number);
            break;
        }
      }
    }
    if(GetFlag==0){
      //InBuffer.push_back(std::make_pair(level+1,current_->files_[level][0]));
      target=current_->files_[level][0];
      target->InLog=true;
      
      if(WarmUpFlag==-1){
         //target->allowed_seeks+=10;
      }
      
      //LogLiveFiles.insert(current_->files_[level][0]->number);
      Log(options_->info_log,"Get_1 0@%d,%ld.ldb\n",level,target->number);
      current_->files_[level].erase(current_->files_[level].begin());
      GetFlag=1;
    }
    */

    if(GetFlag==1){
      int OutFlag=SSTExchangeWithLog(level+1,target,outputFiles);
      //set next times compact pointer
      //InternalKey larger=target->largest;
      compact_pointer_[level] = larger.Encode().ToString();
      if(OutFlag==1 || TmpOutPutNotNull()==1){
        // if log[level+1] has no suitable ssts, choose another level 
        Compaction* c=CheckOutBuffer(level+1,outputFiles); 
        if(c==NULL){
          Log(options_->info_log,"Have Space2");
          Finalize(current_);
          return NULL;
        }else{
          c->edit_.SetCompactPointer(level, larger);
          return c;
        }
      }else{
        Log(options_->info_log,"Have Space0");
        return NULL;
      }
    }else{
      Log(options_->info_log,"Have Space1");
      return NULL;
    }
}
Compaction* VersionSet::AddSSTIntoLogForSeek(int& level,FileMetaData* target){
    
    
    std::list<std::pair<int,FileMetaData*>> outputFiles;
    int OutFlag=SSTExchangeWithLog(level+1,target,outputFiles);
      //set next times compact pointer
      InternalKey larger=target->largest;
      compact_pointer_[level] = larger.Encode().ToString();
      if(OutFlag==1 || TmpOutPutNotNull()==1){
        // if log[level+1] has no suitable ssts, choose another level 
        Compaction* c=CheckOutBuffer(level+1,outputFiles); 
        if(c==NULL){
          Log(options_->info_log,"Seek Have Space2");
          Finalize(current_);
          return NULL;
        }else{
          c->edit_.SetCompactPointer(level, larger);
          return c; 
        }
      }else{
        Log(options_->info_log,"Seek Have Space0");
        return NULL;
      }
    
}


FileMetaData* VersionSet::GetTarget(int fno){
  for(int i=0;i<7;i++){
    if(!current_->files_[i].empty()){
      for(size_t j=0;j<current_->files_[i].size();j++){
        if(current_->files_[i][j]->number == fno){
          return current_->files_[i][j];
        }
      }
    }
  }
  return NULL;
}
int Char2Ascii(FILE* f){
  //804* 89 -117 38 -98 -94 -67 -124 -37 57 -102 95 -56
  std::vector<char> eles;
  char ele=fgetc(f);
  while (ele !=' '){
    eles.push_back(ele);
    ele=fgetc(f);
  }
  if(eles[eles.size()-1]=='*'){
    int num=0;
    int exp=0;
    //804*
    for(int j=eles.size()-2;j>-1;j--){
      num+=(eles[j]-'0')*pow(10,exp);
      exp++;
    }
    return num;
  }else{
    //804* 89 -117 38 -98 -94 -67 -124 -37 57 -102 95 -56
    if(eles[0]=='-'){
      int exp=0;
      int num=0;
      for(int j=eles.size()-1;j>0;j--){
        num+=(eles[j]-'0')*pow(10,exp);
        exp++;
      }
      return num*-1; 
    }else{
      int exp=0;
      int num=0;
      for(int j=eles.size()-1;j>-1;j--){
        num+=(eles[j]-'0')*pow(10,exp);
        exp++;
      }
      return num; 

    }
  }
}
Compaction* VersionSet::PickCompaction() {
  if(Loadflag==1000){ //forbid
    Log(options_->info_log,"Record begin");
    Filter=fopen("/home/hkc/Bloom00.txt","w");
    FILE* FilterIndex=fopen("/home/hkc/BloomIndex.txt","w");
    //record all bloom filter
    int offset=0;
    int index=1;
    for(int level=0;level<7;level++)
      if(!current_->files_[level].empty()){
        for(size_t i = 0; i < current_->files_[level].size(); i++){
          fprintf(FilterIndex,"%ld\n",current_->files_[level][i]->number);
          int count=0;
          int x=current_->files_[level][i]->stb->bits_.size();
          if(x!=12985){
              Log(options_->info_log,"Offset Invalid:%d",x);
          }
          for(size_t j=0;j<x;j++){
            fprintf(Filter,"%c",current_->files_[level][i]->stb->bits_[j]);
          }
          offset+=1;
      }
    }
    fclose(Filter);
    fclose(FilterIndex);
    Log(options_->info_log,"Record offset:%d",offset);
    Loadflag=2;
  }
  if(get_start==1000){ //forbid 
    Log(options_->info_log,"Load Begin");
    //load filter
    FILE * bf=fopen("/home/hkc/Bloom00.txt","r");
    FILE * bfIndex=fopen("/home/hkc/BloomIndex.txt","r");
    char buff[10];
    std::vector<int> fname;
    int offset=0;
    int fileoffset=0;
    int fileindex=1;
    while(fgets(buff,10,bfIndex)){
      fname.push_back(std::atoi(buff));
      FileNameForFilter.insert(std::atoi(buff));
    }

    for(std::vector<int>::iterator it=fname.begin();it!=fname.end();it++){
      FileMetaData* target=GetTarget(*it);
      char buffer[2]={'1','2'};
      int s=0;
      if(target==NULL){
        for(int i=0;i<12985;i++){
          s=fscanf(bf,"%c",buffer);
        }
      }else{
        target->stb=new SSTBloom_Opt_2(8000,0.001);
        for(int i=0;i<12985;i++){
          s=fscanf(bf,"%c",buffer);
          target->stb->bits_[i]=buffer[0];
        }
        fileoffset++;
      }
      offset++;
    }
    fclose(bf);
    fclose(bfIndex);
    get_start=2;
    Log(options_->info_log,"Load End, Total offset:%d,Valid offset:%d",offset,fileoffset);
  }
  Finalize(current_);
  Compaction* c;
  int level;
  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != NULL);
  if(WarmUpFlag==0){
    Log(options_->info_log,"BreakPoint");
    WarmUpFlag=-1;
  }
  if (size_compaction) {
    score++;
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level+1 < config::kNumLevels);
    // simu leveldb with our filter do not forget HotDegree
    if(level >= 1 && WarmUpFlag==-1){
    //if(level >= 1){
      //CheckLogStats();
      Compaction* c=AddSSTIntoLog(level);
      return c;
    }else{
      c = new Compaction(options_, level);

      // Pick the first file that comes after compact_pointer_[level]
      for (size_t i = 0; i < current_->files_[level].size(); i++) {
        FileMetaData* f = current_->files_[level][i];
        if (compact_pointer_[level].empty() ||
            icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
          c->inputs_[0].push_back(f);
          break;
        }
      }
      if (c->inputs_[0].empty()) {
        // Wrap-around to the beginning of the key space
        c->inputs_[0].push_back(current_->files_[level][0]);
      }
    }
  } else if (seek_compaction) {
    
    seek++;
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    if(current_->file_to_compact_->InLog==true){
      Log(options_->info_log,"Seek From Log");
    }else{
      Log(options_->info_log,"Seek From LSM");
    }
    c->inputs_[0].push_back(current_->file_to_compact_);
     /*else if(LogFiles.count(current_->file_to_compact_->number)!=0){
      //Seek candidate is from Log (it couldn't happen if in a normal status)  
      Log(options_->info_log,"Seek From Buffer:%ld",current_->file_to_compact_->number);
      LogFiles.erase(current_->file_to_compact_->number);
      current_->file_to_compact_->BySeek=1;
      std::unique_lock<std::mutex> lck(mtx_Log);
      produce_Log.wait(lck, [] {return InBuffer.size() != 50 ; });   // wait(block) producer until q.size() != maxSize is true
      InBuffer.push_back(std::make_pair(-1,new FileMetaData()));
      consume_Log.notify_all();
    }else{
        Log(options_->info_log,"Seek From LSM:%ld",current_->file_to_compact_->number);
    }
    */
    /*
    else{
      Log(options_->info_log,"Seek From LSM:%ld",current_->file_to_compact_->number);
      FileMetaData* target=current_->file_to_compact_;
      target->allowed_seeks=100;
      int target_level=current_->file_to_compact_level_;
      current_->file_to_compact_=NULL;
      current_->file_to_compact_level_=-1;
      return AddSSTIntoLogForSeek(target_level,target);
    }
    */
  } else {
    nothing++;
    Log(options_->info_log,"Nothing");
    return NULL;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);
  return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}
void VersionSet::SetupOtherInputsForLog(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level,
            int(c->inputs_[0].size()),
            int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size),
            int(expanded0.size()),
            int(expanded1.size()),
            long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  if (false) {
    Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        level,
        smallest.DebugString().c_str(),
        largest.DebugString().c_str());
  }
  //we do not want to change pointer by log SST because this environment is totally separate 

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  //compact_pointer_[level] = larger.Encode().ToString();
  //c->edit_.SetCompactPointer(level, larger);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return NULL;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    for (; level_ptrs_[lvl] < files.size(); ) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

}  // namespace leveldb
