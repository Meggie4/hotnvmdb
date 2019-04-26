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

    static Slice GetNVMLengthPrefixedSlice(const char* data) {
        uint32_t len;
        const char* p = data;
        p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        //DEBUG_T("GetNVMLengthPrefixedSlice, user_len:%d\n", len);
        return Slice(p, len);
    }
    static const char* EncodeNVMKey(std::string* scratch, const Slice& target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

    int chunkTable::KeyComparator::operator()(const char* aptr, const char* bptr)
        const {
        // Internal keys are encoded as length-prefixed strings.
        Slice a = GetNVMLengthPrefixedSlice(aptr);
        Slice b = GetNVMLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

    int chunkTable::KeyComparator::user_compare(const char* aptr, const char* bptr) const
    {
        Slice a = GetNVMLengthPrefixedSlice(aptr);
        Slice b = GetNVMLengthPrefixedSlice(bptr);
        Slice user_a = Slice(a.data(), a.size() - 8);
        Slice user_b = Slice(b.data(), b.size() - 8);
        //DEBUG_T("user_a:%s,len:%d, user_b:%s,len:%d\n", user_a.ToString().c_str(),
          //      user_a.size(),user_b.ToString().c_str(),user_b.size());
        return comparator.user_comparator()->Compare(user_a, user_b);
    }

    class chunkTableIterator: public Iterator{
        public:
            chunkTableIterator(chunkTable::Table* table, chunkLog* cklog) : 
                iter_(table, cklog), cklog_(cklog){  }
    
            virtual bool Valid() const { 
                //DEBUG_T("test chunktable if valid\n"); 
                return iter_.Valid(); 
            }
            virtual void Seek(const Slice& k) { iter_.Seek(EncodeNVMKey(&tmp_, k)); }
            virtual void SeekToFirst() { iter_.SeekToFirst(); }
            virtual void SeekToLast() { iter_.SeekToLast(); }
            virtual void Next() { iter_.Next(); }
            virtual void Prev() { iter_.Prev(); }
            virtual const char* GetNodeKey(){return cklog_->getKV(iter_.key_offset());}
            virtual Slice key() const { return GetNVMLengthPrefixedSlice(cklog_->getKV(iter_.key_offset())); }
            virtual Slice value() const {
                Slice key_slice = GetNVMLengthPrefixedSlice(cklog_->getKV(iter_.key_offset()));
                return GetNVMLengthPrefixedSlice(key_slice.data() + key_slice.size());
            }

            virtual Status status() const { return Status::OK(); }

            /*void* operator new(std::size_t sz){
                return malloc(sz);
            }

            void* operator new[](std::size_t sz){
                return malloc(sz);
            }

            void operator delete(void* ptr){
                free(ptr);
            }*/
        private:
            chunkTable::Table::Iterator iter_;
            chunkLog* cklog_;
            std::string tmp_;
            
            chunkTableIterator(const chunkTableIterator&);
            void operator=(const chunkTableIterator&);
    };

    Iterator* chunkTable::NewIterator(){
        return new chunkTableIterator(&table_, cklog_);
    }

    chunkTable::chunkTable(const InternalKeyComparator& comparator, 
            ArenaNVM* arena, chunkLog* cklog, bool recovery):
        comparator_(comparator),
        arena_(arena),
        cklog_(cklog),
        table_(comparator_, arena, cklog_, recovery),
        refs_(0),
        bbf_(new BitBloomFilter(BIT_BLOOM_HASH, BIT_BLOOM_SIZE)){
    }
    chunkTable::~chunkTable(){
        delete bbf_;
        delete arena_;
        delete cklog_;
        assert(refs_ == 0);
    }
    void chunkTable::Add(const char* kvitem){
        const char* key_ptr;
        uint32_t key_length;
        size_t kv_length;
        uint32_t key_length1;
        const char* key_ptr1 = GetVarint32Ptr(kvitem, kvitem + 5, &key_length1);
        DEBUG_T("chuntable Add, user_key:%s, len:%d\n", Slice(key_ptr1, key_length1 - 8).ToString().c_str(), 
               key_length1 - 8);
        key_ptr = GetKVLength(kvitem, &key_length, &kv_length);
        //Slice nvmkey = Slice(key_ptr, key_length); 
        
        uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
        //ValueType type = static_cast<ValueType>(tag & 0xff);
        SequenceNumber s = tag >> 8;
        DEBUG_T("before add to chunklog\n");
        const char* kv_offset = reinterpret_cast<char*>(cklog_->insert(kvitem, kv_length));
        DEBUG_T("chunktable,add kvoffset：%p\n", kv_offset);
        table_.Add(kvitem, kv_offset, s); 
        DEBUG_T("after add to chunktable\n");
        bbf_->Add(Slice(key_ptr1, key_length1 - 8));
    }

    bool chunkTable::Get(const Slice& user_key, std::string* value, Status* s, SequenceNumber sequence){
        Table::Iterator iter(&table_, cklog_);
        ///////create lookupkey 
        LookupKey lkey(user_key, sequence);
        Slice memkey = lkey.memtable_key();
        iter.Seek(memkey.data());
        //uint32_t userkey_length;
        //const char* userkey_ptr = GetVarint32Ptr(key.data(), key.data() + 5, &userkey_length);
        //Slice user_key = Slice(userkey_ptr, userkey_length - 8);
        if(iter.Valid()){
            const char* key_offset = iter.key_offset();
            //DEBUG_T("get, key_offset%p\n", key_offset);
            const char* entry = cklog_->getKV(key_offset);
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
            //DEBUG_T("user_key:%s\n", Slice(key_ptr, key_length - 8).ToString().c_str());
            if(comparator_.comparator.user_comparator()->Compare(
                        Slice(key_ptr, key_length - 8), user_key) == 0){
                //DEBUG_T("user_key,is equal\n");
                const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
                switch(static_cast<ValueType>(tag & 0xff)){
                    case kTypeValue: {
                        //DEBUG_T("get, kTypeValue\n");
                        Slice v = GetNVMLengthPrefixedSlice(key_ptr + key_length);
                        value->assign(v.data(), v.size());
                        return true;
                    }
                    case kTypeDeletion: {
                        //DEBUG_T("get, kTypeDeletion\n");
                        *s = Status::NotFound(Slice());
                        return true;
                    }
                }
            }
        }
        return false;
    }
   
    void chunkTable::AddPredictIndex(const Slice& user_key){
        bbf_->Add(user_key);
    }

    bool chunkTable::CheckPredictIndex(const Slice& user_key){
        if(user_key.ToString() == "key000259"){
            if(bbf_->Query(user_key)){
                DEBUG_T("key000259 is in nvmtable\n");
            }
            else  
                DEBUG_T("key000259 is not in nvmtable\n");
        }
        return bbf_->Query(user_key);
    }

    bool chunkTable::Contains(const Slice& user_key, SequenceNumber sequence){
        LookupKey lkey(user_key, sequence);
        Slice memkey = lkey.memtable_key();
        return table_.Contains(memkey.data());
    }

    void chunkTable::SaveBloomFilter(char* start){
        memset(start, 0, bbf_->bytes_);
        memcpy_persist(start, bbf_->bits_, bbf_->bytes_); 
        DEBUG_T("start:%p, bbf_->bytes_:%zu, bbf_->bits_:%p\n",
                start, bbf_->bytes_, bbf_->bits_);
    }

    void chunkTable::RecoverBloomFilter(char* start){
        memset(bbf_->bits_, 0, bbf_->bytes_);
        memcpy(bbf_->bits_, start, bbf_->bytes_); 
    }

    NVMTable::NVMTable(const InternalKeyComparator& comparator, 
            std::vector<ArenaNVM*>& arenas,
            std::vector<chunkLog*>& cklogs,
            bool recovery):
            refs_(0){
                comparator_ = &comparator;
                assert(arenas.size() == kNumChunkTable);
                for(int i = 0; i < kNumChunkTable; i++){
                    chunkTable* ckTbl = new chunkTable(comparator, arenas[i], cklogs[i], recovery);
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
       //DEBUG_T("add, index:%d\n", chunkTableIndex(hash));
       cktables_[chunkTableIndex(hash)]->Add(kvitem);
    }

    bool NVMTable::Get(const Slice& key, std::string* value, Status* s, SequenceNumber sequence){
       const uint32_t hash = chunkTableHash(key);
       DEBUG_T("nvmtable get user_key:%s, index:%d\n", key.ToString().c_str(), 
               chunkTableIndex(hash));
       chunkTable* cktbl = cktables_[chunkTableIndex(hash)];
       if(!cktbl->CheckPredictIndex(key)){
           DEBUG_T("not in nvmtable\n");
           return false;
       }
       else 
           return cktbl->Get(key, value, s, sequence);
    }

    bool NVMTable::Contains(const Slice& user_key, SequenceNumber sequence){
       const uint32_t hash = chunkTableHash(user_key);
       chunkTable* cktbl = cktables_[chunkTableIndex(hash)];
       if(!cktbl->CheckPredictIndex(user_key))
           return false;
       else 
           return cktbl->Contains(user_key, sequence);
    }
    
    bool NVMTable::MaybeContains(const Slice& user_key){
       const uint32_t hash = chunkTableHash(user_key);
       chunkTable* cktbl = cktables_[chunkTableIndex(hash)];
       if(!cktbl->CheckPredictIndex(user_key))
           return false;
       else 
           return true;
    }


    bool NVMTable::CheckAndAddToCompactionList(std::map<int, chunkTable*>& toCompactionList,
                                               size_t index_thresh,
                                               size_t log_thresh){
        for(int i = 0; i < kNumChunkTable; i++){
            if(cktables_[i] && (cktables_[i]->ApproximateIndexNVMUsage() >= index_thresh ||
                    cktables_[i]->ApproximateLogNVMUsage() >= log_thresh)){
                toCompactionList.insert(std::make_pair(i, cktables_[i]));
            }
        }
        if(toCompactionList.empty())
            return false;
        else 
            return true;
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
            DEBUG_T("index:%d, indexUsage:%zu, logUsage:%zu, numofEntries:%d\n", 
                    i, cktables_[i]->ApproximateIndexNVMUsage(),
                    cktables_[i]->ApproximateLogNVMUsage(), cktables_[i]->getKeyNum());
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
            cktables_[i]->SaveBloomFilter(start);
        }
        munmap(meta_map_start, metfile_size);
    }
    
    void NVMTable::RecoverMetadata(std::map<int, chunkTable*> update_chunks, 
            std::string metafile){
        DEBUG_T("recover metafile:%s\n", metafile.c_str());
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
        DEBUG_T("BIT_BLOOM_SIZE:%d, bytes:%zu, kNumChunkTable:%d, metfile_size:%zu\n", BIT_BLOOM_SIZE, bytes, kNumChunkTable, metfile_size);
        for(auto iter = update_chunks.begin(); iter != update_chunks.end(); iter++){
            DEBUG_T("iter->first:%d\n", iter->first);
            char* start = meta_map_start + (bytes + 1) * (iter->first);
            iter->second->RecoverBloomFilter(start);
        }
    }
}
