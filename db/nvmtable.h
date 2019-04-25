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
#include "db/nvm_skiplist.h"
#include "db/chunklog.h"
#include "util/arena.h"
#include "port/port.h"
#include "util/hash.h"
#include "table/merger.h"

#define kNumChunkTableBits 3
#define kNumChunkTable (1 << kNumChunkTableBits)

namespace leveldb{
class InternalKeyComparator;
class chunkTableIterator;
class BitBloomFilter;
class chunkTable{
    public:
        chunkTable(const InternalKeyComparator& comparator, 
                ArenaNVM* arena, chunkLog* cklog, bool recovery = false);
        ~chunkTable();
        void Add(const char* kvitem);
        void AddPredictIndex(const Slice& user_key);
        bool CheckPredictIndex(const Slice& user_key);
        bool Get(const Slice& key, std::string* value, Status* s, SequenceNumber sequence);
        bool Contains(const Slice& user_key, SequenceNumber sequence);
        size_t getKeyNum()const {return table_.GetNodeNum();} 
        Iterator* NewIterator();
        size_t ApproximateLogNVMUsage() {return cklog_->NVMUsage(); };
        size_t ApproximateIndexNVMUsage() {return arena_->MemoryUsage(); };
        void SetChunkindexNumber(uint64_t chunkindex_number){chunkindex_number_ = chunkindex_number;}
        void SetChunklogNumber(uint64_t chunklog_number){chunklog_number_ = chunklog_number;}
        uint64_t GetChunklogNumber(){return chunklog_number_;}
        uint64_t GetChunkindexNumber(){return chunkindex_number_;}

        void SaveBloomFilter(char* start);
        void RecoverBloomFilter(char* start);

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
        struct KeyComparator{
            const InternalKeyComparator comparator;
            explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
            int operator()(const char* a, const char* b) const;
            int user_compare(const char* a, const char* b) const;
        };

        friend class chunkTableIterator;
        friend class NVMTable;
        KeyComparator comparator_;
        ArenaNVM* arena_;
        chunkLog* cklog_;
        typedef NVMSkipList<const char*, KeyComparator> Table;
        Table table_;
        int refs_;
        BitBloomFilter* bbf_;

        uint64_t chunkindex_number_;
        uint64_t chunklog_number_;

        chunkTable(const chunkTable&);
        void operator=(const chunkTable&);
};

class NVMTable {
    public:
        NVMTable(const InternalKeyComparator& comparator, 
            std::vector<ArenaNVM*>& arenas,
            std::vector<chunkLog*>& cklogs,  
            bool recovery);
        NVMTable(const InternalKeyComparator& comparator); 
        void Add(const char* kvitem, const Slice& key);
        bool Get(const Slice& key, std::string* value, Status* s, SequenceNumber sequence);
        bool Contains(const Slice& user_key, SequenceNumber sequence);
        bool MaybeContains(const Slice& user_key);
        bool CheckAndAddToCompactionList(std::map<int, chunkTable*>& toCompactionList,
                size_t index_thresh,
                size_t log_thresh);
        void AddAllToCompactionList(std::map<int, chunkTable*>& toCompactionList);
        Iterator* NewIterator();
        Iterator* GetMergeIterator(std::vector<chunkTable*>& toCompactionList);
        Iterator* getchunkTableIterator(int index);
        const InternalKeyComparator* GetComparator(){return comparator_;}
        chunkTable* GetNewChunkTable(ArenaNVM* arena, chunkLog* ckg, bool recovery){
            return new chunkTable(*comparator_, arena, ckg, recovery);
        }
        void UpdateChunkTables(std::map<int, chunkTable*>& update_chunks);
        void PrintInfo(); 
        
        void SaveMetadata(std::string metfile);
        void RecoverMetadata(std::map<int, chunkTable*> update_chunks, 
                std::string metafile);
        
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
        chunkTable* cktables_[kNumChunkTable];
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
