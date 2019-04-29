#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <cstdlib>
#include "db/nvmtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "util/debug.h"
#include "util/multi_bloomfilter.h"
#include "port/cache_flush.h"

#define BIT_BLOOM_SIZE 1024 * 1024
#define BIT_BLOOM_HASH 4
namespace leveldb{

    Iterator* chunkTable::NewIterator(){
        return table_->NewIterator();
    }

    chunkTable::chunkTable(const InternalKeyComparator& comparator, 
            ArenaNVM* arena, bool recovery)
        :refs_(0),
        arena_(arena)
        //bbf_(new BitBloomFilter(BIT_BLOOM_HASH, BIT_BLOOM_SIZE))
        {
        table_ = new MemTable(comparator, *arena, recovery);
        table_->isNVMMemtable = true;
        table_->Ref();
    }
    chunkTable::~chunkTable(){
        //delete bbf_;
        table_->Unref();
        arena_ = nullptr;
        assert(refs_ == 0);
    }
    void chunkTable::Add(const char* kvitem){
        table_->Add(kvitem);
    }

    bool chunkTable::Get(const LookupKey& key, std::string* value, Status* s){
        return table_->Get(key, value, s);
    }
    NVMTable::NVMTable(const InternalKeyComparator& comparator, 
            std::vector<ArenaNVM*>& arenas,
            bool recovery):
            refs_(0){
                comparator_ = &comparator;
                assert(arenas.size() == kNumChunkTable);
                for(int i = 0; i < kNumChunkTable; i++){
                    chunkTable* ckTbl = new chunkTable(comparator, arenas[i], recovery);
                    cktables_[i] = ckTbl;
                }
    }

    NVMTable::NVMTable(const InternalKeyComparator& comparator)
        :refs_(0){
        comparator_ = &comparator;
        for(int i = 0; i < kNumChunkTable; i++){
            cktables_[i] = NULL;
        }
    } 

    NVMTable::~NVMTable(){ 
        for(int i = 0; i < kNumChunkTable; i++){
            if(cktables_[i])
                delete cktables_[i];
        }
    }

    void NVMTable::Add(const char* kvitem, const Slice& key){
       const uint32_t hash = chunkTableHash(key);
       cktables_[chunkTableIndex(hash)]->Add(kvitem);
    }
    
    int NVMTable::GetChunkTableIndex(const Slice& key){
       const uint32_t hash = chunkTableHash(key);
       return chunkTableIndex(hash);
    }
   
    bool NVMTable::NeedsCompaction(size_t chunk_thresh){
        for(int i = 0; i < kNumChunkTable; i++){
            if(cktables_[i] && (cktables_[i]->ApproximateNVMUsage() >= chunk_thresh))
                return true;   
        }
        return false;
    }
    
    bool NVMTable::Get(const LookupKey& key, std::string* value, Status* s){ 
       Slice memkey = key.memtable_key();
       uint32_t key_length;
       const char* key_ptr = GetVarint32Ptr(memkey.data(), memkey.data() + 5, &key_length);
       Slice user_key = Slice(key_ptr, key_length - 8);
       const uint32_t hash = chunkTableHash(user_key);
       //DEBUG_T("nvmtable get user_key:%s, index:%d\n", user_key.ToString().c_str(), 
         //      chunkTableIndex(hash));
       chunkTable* cktbl = cktables_[chunkTableIndex(hash)];
       /*if(!cktbl->CheckPredictIndex(user_key)){
           DEBUG_T("not in nvmtable\n");
           return false;
       }
       else */
           return cktbl->Get(key, value, s);
    }

    bool NVMTable::MaybeContains(const Slice& user_key){
       const uint32_t hash = chunkTableHash(user_key);
       chunkTable* cktbl = cktables_[chunkTableIndex(hash)];
       /*if(!cktbl->CheckPredictIndex(user_key))
           return false;
       else */
           return true;
    }


    void NVMTable::CheckAndAddToCompactionList(std::map<int, chunkTable*>& toCompactionList, size_t chunk_thresh){
        for(int i = 0; i < kNumChunkTable; i++){
            if(cktables_[i] && (cktables_[i]->ApproximateNVMUsage() >= chunk_thresh)){
                toCompactionList.insert(std::make_pair(i, cktables_[i]));
            }
        }
    }

    void NVMTable::UpdateChunkTables(std::map<int, chunkTable*>& update_chunks){
        std::map<int, chunkTable*>::iterator iter;
        for(iter = update_chunks.begin(); iter != update_chunks.end(); iter++){
            int index = iter->first;
            if(cktables_[index])
                delete cktables_[index];
            cktables_[index] = iter->second;
        }
    }
    
    void NVMTable::AddAllToCompactionList(std::map<int, chunkTable*>& toCompactionList){
        for(int i = 0; i < kNumChunkTable; i++){
                toCompactionList.insert(std::make_pair(i, cktables_[i]));
        } 
    }
    
    Iterator* NVMTable::NewIterator(){
        std::vector<Iterator*> list;
        for(int i = 0; i < kNumChunkTable; i++){
            list.push_back(cktables_[i]->NewIterator());
        }
        return NewMergingIterator(comparator_, &list[0], list.size());
    }

    Iterator* NVMTable::GetMergeIterator(std::vector<chunkTable*>& toCompactionList){
        std::vector<Iterator*> list;
        for(int i = 0; i < toCompactionList.size(); i++){
            list.push_back(toCompactionList[i]->NewIterator());
        }
        return NewMergingIterator(comparator_, &list[0], list.size());
    }

    Iterator* NVMTable::getchunkTableIterator(int index){
        return cktables_[index]->NewIterator();
    } 

    void NVMTable::PrintInfo(){
        DEBUG_T("---------------PRINT_NVMTABLE-------------------\n");
        for(int i = 0; i < kNumChunkTable; i++){
            DEBUG_T("index:%d, indexUsage:%zu\n", 
                    i, cktables_[i]->ApproximateNVMUsage());
        }
        DEBUG_T("-------------END PRINT_NVMTABLE-----------------\n");
    }

    void NVMTable::SaveMetadata(std::string metfile){
        DEBUG_T("save metafile:%s\n", metfile.c_str());
        int fd = open(metfile.c_str(), O_RDWR);
        if(fd == -1){
            fd = open(metfile.c_str(), O_RDWR | O_CREAT, 0644);
            if(fd == -1)
                perror("create_metfile_failed\n");
        }
        
        size_t bytes = ((BIT_BLOOM_SIZE + 7) / 8);
        size_t metfile_size = (bytes + 1) * kNumChunkTable;  
        
        DEBUG_T("BIT_BLOOM_SIZE + 7:%d, bytes:%zu, kNumChunkTable:%d, metfile_size:%zu\n", BIT_BLOOM_SIZE + 7, bytes, kNumChunkTable, metfile_size);
        
        if(ftruncate(fd, metfile_size) != 0){
            perror("ftruncate_failed\n");
        }
        char* meta_map_start = (char*)mmap(NULL, metfile_size, PROT_READ | PROT_WRITE, 
                        MAP_SHARED, fd, 0);

        for(int i = 0; i < kNumChunkTable; i++){
            if(!cktables_[i])
                continue;
            char* start = meta_map_start + (bytes + 1) * i;
            //cktables_[i]->SaveBloomFilter(start);
        }
        munmap(meta_map_start, metfile_size);
    }
    
    void NVMTable::RecoverMetadata(std::map<int, chunkTable*> update_chunks, 
            std::string metafile){
        //DEBUG_T("recover metafile:%s\n", metafile.c_str());
        int fd = open(metafile.c_str(), O_RDWR);
        if(fd == -1){
            fd = open(metafile.c_str(), O_RDWR | O_CREAT, 0644);
            if(fd == -1)
                perror("create_metfile_failed\n");
        }
        size_t bytes = ((BIT_BLOOM_SIZE + 7) / 8) ;
        size_t metfile_size = (bytes + 1) * kNumChunkTable;  
        if(ftruncate(fd, metfile_size) != 0){
            perror("ftruncate_failed\n");
        }
        char* meta_map_start = (char*)mmap(NULL, metfile_size, PROT_READ | PROT_WRITE, 
                        MAP_SHARED, fd, 0); 
        //DEBUG_T("BIT_BLOOM_SIZE:%d, bytes:%zu, kNumChunkTable:%d, metfile_size:%zu\n", BIT_BLOOM_SIZE, bytes, kNumChunkTable, metfile_size);
        for(auto iter = update_chunks.begin(); iter != update_chunks.end(); iter++){
            //DEBUG_T("iter->first:%d\n", iter->first);
            char* start = meta_map_start + (bytes + 1) * (iter->first);
            //iter->second->RecoverBloomFilter(start);
        }
    }
    
}
