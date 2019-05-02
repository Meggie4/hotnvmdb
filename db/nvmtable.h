/*************************************************************************
	> File Name: nvmtable.h
	> Author: Meggie
	> Mail: 1224642332@qq.com 
	> Created Time: Sat 19 Jan 2019 04:48:08 PM CST
 ************************************************************************/
#ifndef STORAGE_LEVELDB_DB_NVMTABLE_H_
#define STORAGE_LEVELDB_DB_NVMTABLE_H_

#include <string>
#include <assert.h>
#include <vector>
#include <map>
#include "leveldb/db.h"
#include "db/dbformat.h"
#include "util/arena.h"
#include "port/port.h"
#include "util/hash.h"
#include "table/merger.h"
#include "db/memtable.h"

#define kNumChunkTableBits 2
#define kNumChunkTable (1 << kNumChunkTableBits)

namespace leveldb{
class InternalKeyComparator;
class BitBloomFilter;
class chunkTable{
    public:
        chunkTable(const InternalKeyComparator& comparator, 
                ArenaNVM* arena, bool recovery = false);
        ~chunkTable();
        void Add(const char* kvitem);
        bool Get(const LookupKey& key, std::string* value, Status* s);
        Iterator* NewIterator();
        size_t ApproximateNVMUsage() {return arena_->MemoryUsage(); };
        
        void SetChunkNumber(uint64_t chunk_number){chunk_number_ = chunk_number;}
        uint64_t GetChunkNumber(){return chunk_number_;}
        

        void Ref(){++refs_;}
        void Unref(){
            --refs_;
            assert(refs_ >= 0);
            if(refs_ <= 0){
                delete this;
            }
        }
        /*
        void* operator new(std::size_t sz){
            return malloc(sz);
        }
        void operator delete(void* ptr){
            free(ptr);
        }
        void* operator new[](std::size_t sz){
            return malloc(sz);
        }*/
        ////(TODO)move to private, to debug
        //chunkLog* cklog_;
        //typedef NVMSkipList<const char*, KeyComparator> Table;
        //Table table_;
    private:
        friend class NVMTable;
        MemTable* table_;
        int refs_;
        //BitBloomFilter* bbf_;
        ArenaNVM* arena_;

        uint64_t chunk_number_;

        chunkTable(const chunkTable&);
        void operator=(const chunkTable&);
};



class NVMTable {
    public:
        NVMTable(const InternalKeyComparator& comparator, 
            std::vector<ArenaNVM*>& arenas,
            bool recovery);
        NVMTable(const InternalKeyComparator& comparator); 
        void Add(const char* kvitem, const Slice& key);
        bool Get(const LookupKey& key, std::string* value, Status* s);
        bool MaybeContains(const Slice& user_key);
        void CheckAndAddToCompactionList(std::map<int, chunkTable*>& toCompactionList, size_t chunk_thresh);
        void AddAllToCompactionList(std::map<int, chunkTable*>& toCompactionList);
        Iterator* NewIterator();
        Iterator* GetMergeIterator(std::vector<chunkTable*>& toCompactionList);
        Iterator* getchunkTableIterator(int index);
        const InternalKeyComparator* GetComparator(){return comparator_;}
        chunkTable* GetNewChunkTable(ArenaNVM* arena, bool recovery){
            return new chunkTable(*comparator_, arena, recovery);
        }
        void UpdateChunkTables(std::map<int, chunkTable*>& update_chunks);
        void PrintInfo(); 
        
        void SaveMetadata(std::string metfile);
        void RecoverMetadata(std::map<int, chunkTable*> update_chunks, 
                std::string metafile);

        static int GetChunkTableIndex(const Slice& key);
        bool NeedsCompaction(size_t chunk_thresh);
        
        chunkTable* cktables_[kNumChunkTable];
        void Ref(){
            ++refs_; 
            DEBUG_T("ref, refs:%d\n", refs_);
        }
        void Unref(){
            --refs_;
            DEBUG_T("unref, refs:%d\n", refs_);
            assert(refs_ >= 0);
            if(refs_ <= 0){
                delete this;
            }
        }
    private:
        ~NVMTable();
        //chunkTable* cktables_[kNumChunkTable];
        const InternalKeyComparator* comparator_;
        int refs_;
        static inline uint32_t chunkTableHash(const Slice& key){
            return Hash(key.data(), key.size(), 0);
        }
        static uint32_t chunkTableIndex(uint32_t hash){
           return hash >> (32 - kNumChunkTableBits);
        }
        
        
        NVMTable(const NVMTable&);
        void operator=(const NVMTable&);
};
}

#endif
