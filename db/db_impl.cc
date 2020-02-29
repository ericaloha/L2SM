// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include <fstream>
#include <unordered_map>

#include <mutex>
#include <condition_variable>
#include <thread>

int Stopflag =0;

int get0=0;
int write0=0;
int l0=0;
int l8=0;
int l12=0;
int pureRead = 0;
int picktir=0;

int KeyInLevel=0;

struct cmp_key
{
    bool operator()(const uint64_t &k1, const uint64_t &k2)const
    {
      return k1 >k2;
    }
};
 
/* hkc coding */
std::set<uint64_t> LogFiles;
std::map<uint64_t,leveldb::FileMetaData*,cmp_key> LogIndex[7];
std::unordered_map<uint64_t,std::pair<std::pair<uint64_t,int>,leveldb::SSTBloom_Opt_2*> > SSTAttriMap; //filenumber,<Keys,Hotness>
int WarmUpFlag=1;
int FilterSize=500000;
int FilterSize_=500000;
int FiltersLevel=5;  //used to be 5
std::vector< leveldb::SSTBloom_Opt_2*> STBs;
leveldb::SSTBloom_Opt_2* Recorder;
std::vector<std::string>* KeyBuffer=NULL;
std::map<int,std::string> KeyMap;
int oldData=0;
std::unordered_map<uint64_t,leveldb::SSTBloom_Opt_2*> LogFilterBuffer;
std::set<uint64_t> KeyLogNames;
std::set<std::pair<uint64_t,int>> obsoleteByMove;

int hkc_level=0;
/* make for background manager*/
std::mutex mtx_Log;
std::condition_variable produce_Log, consume_Log;
std::deque<std::pair<int,leveldb::FileMetaData*> > InBuffer;
std::deque<std::pair<int,leveldb::FileMetaData*> > OutBuffer;
std::list<leveldb::FileMetaData*> SSTLog[7];
std::deque<std::vector<std::string>*> KeyIn[7];
std::deque<std::map<int,std::string>*> KeyOut[7];

/* Info collection*/
//1.avg density per level
//1 1 12 103 357 819 0
//0 0 20 13 23 10 0
//0,0,40,30,35,40,50
//int Thres_density[7]={0,0,20,40,160,250,350};
int Thres_density[7]={0,0,60,120,200,140,180};
//int Thres_density[7]={0,0,15,20,25,30,35};
int total_density_count[7]={0,0,0,0,0,0,0};
float total_density[7]={0.0,0.0,0.0,0.0,0.0,0.0,0.0};
//2.avg hotness per level
int total_hotness_count[7]={0,0,0,0,0,0,0};
long total_hotness[7]={0,0,0,0,0,0,0};

//int Thres_thres[7]={0,0,40,30,35,40,50};
//int Thres_thres[7]={0,0,30,40,20,30,50};
// read 0 int Thres_thres[7]={0,0,40,40,50,50,50};
// scr r0int Thres_thres[7]={0,0,40,60,80,100,60};
// ran r3 int Thres_thres[7]={0,0,60,120,200,200,300};
int Thres_thres[7]={0,0,80,150,200,200,200};
//9 8 86 862 6459 6273 0
// 80M  9 7 92 826 5818 398 0 
//int Thres_thres[7]={0,0,30,40,20,30,50};
/*end of background manager container*/

namespace leveldb {

/*
bool ParseLogInternalKey(std::string& ikey,int& seq,int& usKey) {//const Slice& internal_key,
  const size_t n = ikey.size();
  //internal_key.size();
  if (n < 8) return false;
  //uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint64_t num = DecodeFixed64(ikey.data() + n - 8);
  //unsigned char c = num & 0xff;
  seq = num >> 8;
  //result->type = static_cast<ValueType>(c);
  usKey = atoi(ikey.substr(0,16).c_str());
  //Slice(internal_key.data(), n - 8).ToString();
  return true; 
  //(c <= static_cast<unsigned char>(kTypeValue));
}
std::map<int,std::string>* SortingKeysInBuffer(std::vector<std::string>* Buffer){
  std::map<int,std::string>* KeyMap_=new std::map<int,std::string>;
  int size=0;
  int tmpKey;
  int seq;
  int preseq;
  for(std::vector<std::string>::iterator it=Buffer->begin();it!=Buffer->end();it++){
    std::string sK=(*it).substr(0,it->find("d"));
    ParseLogInternalKey(sK,seq,tmpKey);
    std::map<int,std::string>::iterator iter=KeyMap_->find(tmpKey);
    if(iter==KeyMap_->end()){
      (*KeyMap_)[tmpKey]=*it;
      size++;
    }else{
      std::string Pre_sK=(*iter).second.substr(0,iter->second.find("d"));
      ParseLogInternalKey(Pre_sK,preseq,tmpKey);
      if (preseq>seq){
        oldData++;
      }else{
        iter->second=*it;
      }
    }
  }
  delete Buffer;
  return KeyMap_;
}
*/

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
      int smallestReadThres=0;
      std::list<FileMetaData*>::iterator maxHotnessIter;
      std::list<FileMetaData*>::iterator minHotnessIter;
      std::list<FileMetaData*>::iterator smallestDensityIter;
      std::list<FileMetaData*>::iterator ReadUnfriendlyIter;
      ReadUnfriendlyIter=maxHotnessIter=minHotnessIter=smallestDensityIter=SSTLog[Log_level].begin();
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
        //if(WarmUpFlag==-1 && (*it1)->allowed_seeks<smallestReadThres){
        //  smallestReadThres=(*it1)->allowed_seeks;
        //  ReadUnfriendlyIter=it1;
        //}
      }
      total_density[Log_level]+=(total_density_this_time/count_);
      total_density_count[Log_level]++;
      total_hotness[Log_level]+=(total_hotness_this_time/count_);
      total_hotness_count[Log_level]++;
      //2. check this time which attri we should take care of
      int CoverFlag=0;
      int OverlapFlag_hot=0;
      int OverlapFlag_cold=0;
      int Overlap_read=0;
      for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();
          it2!=SSTLog[Log_level].end();
          )
        {
          //if(LogFiles.count((*it2)->number)==0){
          if((*it2)->BySeek == 1){
            it2=SSTLog[Log_level].erase(it2);
          }
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
         
          
          //if(WarmUpFlag==-1&& OverlapOrNot(*ReadUnfriendlyIter,*it2)==1
           //   && (*ReadUnfriendlyIter)->number!=(*it2)->number){
          //      Overlap_read++;
          //    }
          
          it2++;
        }
      /*
      if(WarmUpFlag==-1 && Overlap_read>=1){
          FileMetaData* target=*ReadUnfriendlyIter;
          target->EndFlag=1;
          files.push_back(std::make_pair(Log_level,target));
          ReadUnfriendlyIter=SSTLog[Log_level].erase(ReadUnfriendlyIter);
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
        
      }
      else 
      */
      if(WarmUpFlag==-100 && Overlap_read > 0 && (Overlap_read >CoverFlag || Overlap_read > OverlapFlag_hot || Overlap_read>OverlapFlag_hot)){
        FileMetaData* target=*ReadUnfriendlyIter;
        target->EndFlag=2333;
        files.push_back(std::make_pair(Log_level,target));
        SSTLog[Log_level].erase(ReadUnfriendlyIter);
        for(std::list<FileMetaData*>::iterator it2=SSTLog[Log_level].begin();it2!=SSTLog[Log_level].end();){
            if(OverlapOrNot(target,*it2)==1){
              files.push_back(std::make_pair(Log_level,*it2));
              (*it2)->EndFlag=2333;
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
        return 1;
      }
      else if(CoverFlag==0 && OverlapFlag_cold==0 && OverlapFlag_hot==0){
        FileMetaData* target=SSTLog[Log_level].front();
        if( size_>Thres_thres[Log_level]){
          //if(smallestDensity<Level_density[Log_level]){
          if(WarmUpFlag==-10 &&Overlap_read>=0&&target->allowed_seeks<0){
            target=*ReadUnfriendlyIter;
            target->EndFlag=1;
            files.push_back(std::make_pair(Log_level,target));
            SSTLog[Log_level].erase(ReadUnfriendlyIter);
          }else{
            target=SSTLog[Log_level].front();
            target->EndFlag=1;
            files.push_back(std::make_pair(Log_level,target));
            SSTLog[Log_level].pop_front();
          }
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
      }else if( CoverFlag > max(OverlapFlag_cold,OverlapFlag_hot)){ 
      //}else if(CoverFlag*1.5>max(OverlapFlag_cold,OverlapFlag_hot)){ 
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



void DoCompactionWorkForLog(){

}
void DealWithObsolete(){
  for(int i=0;i<7;i++){
    if(!SSTLog[i].empty()){
      for(std::list<FileMetaData*>::iterator it1=SSTLog[i].begin();
          it1!=SSTLog[i].end();)
      {
        //if(LogFiles.count( (*it1)->number)==0){
        if( (*it1)->BySeek ==1){
          it1=SSTLog[i].erase(it1);
        }else{
          it1++;
        }
      }
    }
  }
  return;
}

int BackGroundLogManager()
{
  int log_level=-1;
  FileMetaData* inputF=NULL;
  std::list<std::pair<int,FileMetaData*>> outputFiles;
  int InPutFlag=0;
  int OutPutFlag=0;
  int keyQueueInFlag=0;
  int keyQueueOutFlag=0;
  int Outlevel=0;
  //std::vector<std::string>* candidateIn;
  //std::map<int,std::string>* candidateOut;
  while(true){
    if(Stopflag!=0){
      break;
    }
    /*
    while(WarmUpFlag==1){
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      if(WarmUpFlag==-1) break;
    }
    */
    // data exchange
    InPutFlag=0;
    keyQueueInFlag=0;
    std::unique_lock<std::mutex> lck(mtx_Log);
    consume_Log.wait(lck, [] {return InBuffer.size() != 0; });     // wait(block) consumer until q.size() != 0 is true
     if(InBuffer.size()!=0){
       log_level=InBuffer.front().first;
       inputF=InBuffer.front().second;
       InBuffer.pop_front();
       if(log_level==-1){
         delete inputF;
         inputF=NULL;
         DealWithObsolete();
       }else{
         SSTLog[log_level].push_back(inputF);
         InPutFlag=1;
       }
     }
     if(OutBuffer.size()<10 && OutPutFlag!=0){
       for(std::list<std::pair<int,FileMetaData*>>::iterator iter=outputFiles.begin();
            iter!=outputFiles.end(); ){
              OutBuffer.push_back(*iter);
              iter=outputFiles.erase(iter);
        }
        OutPutFlag=0;
     }

     /*
     int i=0;
     while(KeyIn[i].empty()) i++;
     if(i<7){
      KeyInLevel=i;
      std::unique_lock<std::mutex> lck(mtx_Log);
      consume_Log.wait(lck, [] {return KeyIn[KeyInLevel].size() != 0; });     // wait(block) consumer until q.size() != 0 is true
      candidateIn=KeyIn[KeyInLevel].front();
      Outlevel=i;
      KeyIn[Outlevel].pop_front();
      produce_Log.notify_all();
      keyQueueInFlag=1;
      if(keyQueueOutFlag==1){
       KeyOut[Outlevel].push_back(candidateOut);
       keyQueueOutFlag=0;
      }
     }
     */


     
     produce_Log.notify_all();
     
     if(InPutFlag==1){
       // we can make a judge by the return value 
       //to verify which threshold or policy have been touched
       OutPutFlag=Log2Buffer(log_level,outputFiles);
     }
     /*
     if(keyQueueInFlag==1){
       candidateOut=SortingKeysInBuffer(candidateIn);
       keyQueueOutFlag=1;
     }
     */
     
  }
  return 1;
}




//define the timedif file path
//std::fstream otf("/home/bonnie/eric/latency",std::ios::app);
//std::fstream open_timedif_file=otf;
const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL) {
  has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        for(std::set<std::pair<uint64_t,int>>::iterator it1=obsoleteByMove.begin();
            it1!=obsoleteByMove.end();)
          {
            if(!LogIndex[it1->second].empty()){
              std::map<uint64_t,leveldb::FileMetaData*,cmp_key>::iterator target;
              target=LogIndex[it1->second].find(it1->first);
              if(target!=LogIndex[it1->second].end()){
                LogIndex[it1->second].erase(it1->first);
                LogFiles.erase(it1->first);
                Log(options_.info_log, "Delete Index and Id From Log By Move:%d-%ld\n",it1->second,it1->first);
              }
            }
            it1=obsoleteByMove.erase(it1);
          }
        std::map<uint64_t,leveldb::FileMetaData*>::iterator it;
        //if(LogFiles.count(number)!=0){
          for(int i=0;i<7;i++){
            if(!LogIndex[i].empty()){
              it=LogIndex[i].find(number);
              if(it!=LogIndex[i].end()){
                 if(it->second->stb!=NULL){
                  //it->second->stb->DeleteArray();
                  delete it->second->stb;
                  it->second->stb=NULL;
                  }


                LogIndex[i].erase(it);
                LogFiles.erase(number);
                Log(options_.info_log, "Delete Index and Id From Log:%d-%ld\n",i,number);
                break;
              }
            }
          }
          
        //}

       
        

        
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile_Stb(level, meta.number, meta.file_size,meta.stb,
                  meta.smallest, meta.largest,0,0,0,0);
                   // we can collect this info but there is no need,cuz we do not compact level 0
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compactiowriten since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if ( imm_ == NULL &&
             manual_compaction_ == NULL &&
             //!versions_->NeedsCompaction()  ) {
             !versions_->NeedsCompaction()  ) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    picktir++;
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
    /*
    if(c->fromLog==1){
      FileMetaData* f ;
      for(int i=0;i<c->num_input_files(0);i++){
        FileMetaData* f = c->input(0, i);
        Log(options_.info_log," Move %ld from Log ToLSM",f->number);
        //do not need to delete cuz they haven't belonged to LSM 
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
      } 
      status = versions_->LogAndApply(c->edit(), &mutex_);
      if (!status.ok()) {
        RecordBackgroundError(status);
      } 
    }else{
    */
    if(WarmUpFlag!=-100){
        FileMetaData* f = c->input(0, 0);
        c->edit()->DeleteFile(c->level(), f->number);
        c->edit()->AddFile_Stb(c->level() + 1, f->number, f->file_size,
                                f->stb,f->smallest, f->largest,
                                f->NumEntries,f->EndFlag,f->Hotness,f->BySeek);
      
    status = versions_->LogAndApply(c->edit(), &mutex_);
    obsoleteByMove.insert(std::make_pair(f->number,c->level()+1));
    Log(options_.info_log, "Mark %d-%ld obsolete by move",c->level()+1,f->number);
    //f->stb=NULL;
    if (!status.ok()) {
      RecordBackgroundError(status);
    } 
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number),
          c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(),
          versions_->LevelSummary(&tmp));
    }else{
      // Move file to next level
  
      FileMetaData* f = c->input(0, 0);
      c->edit()->DeleteFile(c->level(), f->number);
      c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest);
      status = versions_->LogAndApply(c->edit(), &mutex_);
      if (!status.ok()) {
        RecordBackgroundError(status);
      }
      VersionSet::LevelSummaryStorage tmp;
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
    }
    //}
  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input,
                                          int hotness,int flag,void * stb) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  SSTBloom_Opt_2* stbptr=(SSTBloom_Opt_2*)stb;
  //stbptr->Build_Finish();
  stb=NULL;
  SSTAttriMap[output_number]=std::make_pair(std::make_pair(current_entries,hotness),stbptr);

  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      if(flag==1){
        KeyLogNames.insert(compact->current_output()->number);
        Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes ==>into Log",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
      }else{
        Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes ==> into LSM",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
      }

    }
  }
  return s;
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}
bool CheckPotential_(std::vector<bool>& HasChance){
    for(int i=0;i<HasChance.size();i++){
        if(HasChance[i]==true)
            return true;
    }
    return false;
}
int May_Exist_(const std::vector<SSTBloom_Opt_2*>& STBList,const std::string &key ){
    int k_=STBList[0]->get_k();
    int m_=STBList[0]->get_m();
    std::vector<bool> Score;
    Score.resize(STBList.size(),0);
    std::vector<bool> HasChance;
    for(int i=0;i<STBList.size();i++){
        HasChance.push_back(true);
    }
    uint32_t hash_val = STBList[0]->hash_(0xbc9f1d34,key);
    const uint32_t delta = (hash_val >> 17) | (hash_val << 15);
    for (int i = 0; i < k_; ++i) {
        if(CheckPotential_(HasChance)==false) break;
        const uint32_t bit_pos = hash_val % m_;
        for(int j=0;j<STBList.size();j++){
            if(HasChance[j]){
                if((STBList[j]->bits_[bit_pos/8] & (1 << (bit_pos % 8))) == 0){
                //if( ((STBList[j].second->bits_)[bit_pos/8] & (1 << (bit_pos%8))) == 0){
                    HasChance[j]=false;
                }
                else if(i == (k_-1)){
                  Score[j]=true;
                 //   return STBList[j];
                }
            }
        }
        
        hash_val += delta;
    }
    int Score_=0;
    for(auto i: Score){
      if(Score[i]==true){
        Score_+=(i*i);
      }
    }
    return Score_;
}
int CalcHotNess(std::string key){
  if(STBs.empty()){
    return 0;
  }
  int weight=0;
  uint32_t hash_;
  if(STBs[0]->contains_(key,hash_)){
    weight+=1;
    for(int i=1;i<STBs.size();i--){
      if(STBs[i]->contains_Part(key,hash_)){
        weight+=pow(i+1,2);
      }else{
        break;
      }

    }
  }
  return weight;
  /*
  uint32_t hash_;
  
  if(STBs[0]->contains_(key,hash_)==true){

  }else{
    return 0;
  }
  
  int weight=0;
  for(int i=STBs.size()-1;i>=0;i--){
    if(STBs[i]->contains(key)){
      weight+=pow(i+1,2);
    }
  }
  return weight;
  */
  //return May_Exist_(STBs,key);
}
bool HotOrNot(std::string Key){
  if(STBs.empty()){
    return false;
  }else{
    if(STBs.back()->contains(Key)==1){
      return true;
    }else{
      return false;
    }
  }
  
}

bool ParseLogInternalKey(std::string& ikey,int& seq,int& usKey) {//const Slice& internal_key,
  const size_t n = ikey.size();
  //internal_key.size();
  if (n < 8) return false;
  //uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint64_t num = DecodeFixed64(ikey.data() + n - 8);
  //unsigned char c = num & 0xff;
  seq = num >> 8;
  //result->type = static_cast<ValueType>(c);
  usKey = atoi(ikey.substr(0,16).c_str());
  //Slice(internal_key.data(), n - 8).ToString();
  return true; 
  //(c <= static_cast<unsigned char>(kTypeValue));
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  int HotDegree;
  int HotSeed;
  int HotDegree_log;
  SSTBloom_Opt_2* stb;
  SSTBloom_Opt_2* log_stb;
  hkc_level=compact->compaction->level();
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  //  remover by hkc 
  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input,HotDegree,0,stb);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      
      if(hkc_level==100 && WarmUpFlag==-1 && HotOrNot(ikey.user_key.ToString())){
      //if(HotOrNot(ikey.user_key.ToString())){
      //if(hkc_level==1 && HotOrNot(ikey.user_key.ToString())){
        //if(KeyBuffer==NULL){
          //KeyBuffer=new std::vector<std::string>;
        //}
        //KeyBuffer->push_back(key.ToString()+"d"+input->value().ToString());
        std::map<int,std::string>::iterator candidate=KeyMap.find(atoi(ikey.user_key.ToString().c_str()));        
        if(candidate==KeyMap.end()){
          KeyMap[atoi(ikey.user_key.ToString().c_str())]=key.ToString()+"d"+input->value().ToString();
        }else{
          std::string pre_key=candidate->second.substr(0,candidate->second.find("d"));
          int pre_seq= DecodeFixed64(pre_key.data() + pre_key.size() - 8) >> 8;
          int seq= DecodeFixed64(key.ToString().data() + key.ToString().size() - 8) >> 8;
          if(pre_seq>seq){
            oldData++;
          }else{
            candidate->second=key.ToString()+"d"+input->value().ToString();
          }
        }
        
        //HotDegree_log+=CalcHotNess(ikey.user_key.ToString());
        if(KeyMap.size()>20000){
          /*
          KeyInLevel=hkc_level;
          //std::unique_lock<std::mutex> lck(mtx_Log);
          //produce_Log.wait(lck, [] {return KeyIn[KeyInLevel].size() != 50 ; });
             // wait(block) producer until q.size() != maxSize is true
          if(KeyIn[hkc_level].size()<10){
            KeyIn[hkc_level].push_back(KeyBuffer);
            KeyBuffer=NULL;
          }
          //consume_Log.notify_all();
          //std::map<int,std::string>* KeyMap=NULL;
          if(KeyOut[hkc_level].size()>1){
            KeyMap=KeyOut[hkc_level].back();
            KeyOut[hkc_level].pop_back();
          }
          
          int size=0;
          int tmpKey;
          int seq;
          int preseq;
        for(std::vector<std::string>::iterator it=KeyBuffer->begin();it!=KeyBuffer->end();it++){
          std::string sK=(*it).substr(0,it->find("d"));
          ParseLogInternalKey(sK,seq,tmpKey);
          std::map<int,std::string>::iterator iter=KeyMap.find(tmpKey);
          if(iter==KeyMap.end()){
            KeyMap[tmpKey]=*it;
            size++;
          }else{
            std::string Pre_sK=(*iter).second.substr(0,iter->second.find("d"));
            ParseLogInternalKey(Pre_sK,preseq,tmpKey);
            if (preseq>seq){
              oldData++;
            }else{
              iter->second=*it;
            }
          }
        }
        delete KeyBuffer;
        KeyBuffer=NULL;
        */
            Log(options_.info_log,"Log Compact Preperation.");
            status = FinishCompactionOutputFile(compact, input,HotDegree,0,stb);
            if (!status.ok()) {
              break;
            }
          for(std::map<int,std::string>::iterator iter=KeyMap.begin();iter!=KeyMap.end();iter++){
            Slice log_key=Slice(iter->second.c_str(),24);
            Slice log_value=Slice(iter->second.c_str()+25,iter->second.size()-25);
            if (compact->builder == NULL) {
              status = OpenCompactionOutputFile(compact);
              // build filter for Log Tables
              //log_stb=new SSTBloom_Opt_2(3000,0.001);
              HotDegree_log=0;
              //HotDegree=0;
              if (!status.ok()) {
                break;
              }
            }
            if (compact->builder->NumEntries() == 0) {
              compact->current_output()->smallest.DecodeFrom(log_key);
            }
            compact->current_output()->largest.DecodeFrom(log_key);
            compact->builder->LogAdd(log_key, log_value);
            HotDegree_log+=CalcHotNess(iter->second.substr(0,16));
            //log_stb->insert(iter->second.substr(0,16));
          }
          LogFilterBuffer[compact->current_output()->number]=log_stb;
          Log(options_.info_log,"Filter generated.");
          //delete KeyMap;
          KeyMap.clear();
          // Close output file if it is big enough
          Log(options_.info_log,"Log Compact Result.");
          status = FinishCompactionOutputFile(compact, input,HotDegree_log, 1,log_stb);
          if (!status.ok()) {
            break;
          } 
          
        }
      }else{
        
        // Open output file if necessary
        if (compact->builder == NULL) {
          status = OpenCompactionOutputFile(compact);
          stb=new SSTBloom_Opt_2(8000,0.001);
          HotDegree=0;
          HotSeed=0;
          if (!status.ok()) {
            break;
          }
        }
        if (compact->builder->NumEntries() == 0) {
          compact->current_output()->smallest.DecodeFrom(key);
        }
          compact->current_output()->largest.DecodeFrom(key);
          compact->builder->Add(key, input->value());   
          //stb->insert(ikey.user_key.ToString());   
          HotSeed++;
          
          /* just simu leveldb with our filter*/
          if(HotSeed%100==0){
            HotDegree+=CalcHotNess(ikey.user_key.ToString());
          }
          
            
          
          
          //HotDegree++;//just for speed testing


        // Close output file if it is big enough
        if (compact->builder->FileSize() >=
            compact->compaction->MaxOutputFileSize()) {
            status = FinishCompactionOutputFile(compact, input,HotDegree,0,stb);
          if (!status.ok()) {
            break;
          }
        }
      }
    }
    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input,HotDegree,0,stb);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  

  get0++;
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();//ref是引用计数
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();

  
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {

  write0++;
  //calculate the timedif of write operation 


  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }


  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  l0++;
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
         
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      l8++;
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      l12++;
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}



// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
 
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  //time_dif reocrd file
  
  //open_timedif_file.open(, std::ios::app);
  
  *dbptr = NULL;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
