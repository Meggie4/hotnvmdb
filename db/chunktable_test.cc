// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/nvmtable.h"
#include <set>
#include <iostream>
#include <stdio.h>
#include "leveldb/env.h"
#include "leveldb/comparator.h"
#include "leveldb/status.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/arena.h"
#include "util/hash.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/coding.h"
#include "db/filename.h"
#include "db/dbformat.h"
#include <cstring>
#include "util/testutil.h"
//#include "db/write_batch_internal.h"

namespace leveldb {

class chunkTableTest { };

namespace{
// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, 0.5, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};


class KVGenerator{
    private:
        int kv_num_;
        int value_size_;
        SequenceNumber sequence;
        Random rand;
        int pos_;
        int len_;
        RandomGenerator gen;
    public:
        std::string data_;
        KVGenerator(int num, int value_size, int index):
            kv_num_(num), value_size_(value_size), 
            sequence(0), rand(1000 + index), pos_(0), len_(0){  }

        int GenerateKV(ValueType type, size_t* length){
            const int k =  (sequence + 1) % kv_num_;
            int value_size =  (sequence + 4) % value_size_;
            char key[100];
            snprintf(key, sizeof(key), "%016d", k);
            //fprintf(stderr, "\nkey:%s", key);
            if(type == kTypeValue){
                Slice value = gen.Generate(value_size);
                //std::cout<<", value:"<<value.ToString()<<std::endl;
                size_t key_size = strlen(key);
                size_t val_size = value.size();
                size_t internal_key_size = key_size + 8;
                const size_t encoded_len =
                    VarintLength(internal_key_size) + internal_key_size +
                    VarintLength(val_size) + val_size;
                len_ += encoded_len;
                char buf[encoded_len];
                char* p = EncodeVarint32(buf, internal_key_size);
                memcpy(p, key, key_size);
                p += key_size;
                EncodeFixed64(p, (sequence << 8) | type);
                sequence++;
                p += 8;
                p = EncodeVarint32(p, val_size);
                memcpy(p, value.data(), val_size);
                assert(p + val_size == buf + encoded_len);
                data_.append(buf, encoded_len);
                int tmp_pos = pos_;
                pos_ += encoded_len;
                *length = encoded_len;
                const char* kv_result = data_.c_str() + tmp_pos;
                uint32_t key_length;
                const char* key_ptr = GetVarint32Ptr(kv_result, kv_result + 5, &key_length);
                //fprintf(stderr, "before insert, user_key:%s, len:%d\n", 
                //        Slice(key_ptr, key_length - 8).ToString().c_str(), 
                //        key_length - 8);
                return tmp_pos;
            }
            else if(type == kTypeDeletion){
                size_t key_size = strlen(key);
                size_t internal_key_size = key_size + 8;
                const size_t encoded_len =
                    VarintLength(internal_key_size) + internal_key_size;
                len_ += encoded_len;
                char buf[encoded_len];
                char* p = EncodeVarint32(buf, internal_key_size);
                memcpy(p, key, key_size);
                EncodeFixed64(p, (sequence << 8) | type);
                sequence++;
                p += 8;
                assert(p == buf + encoded_len);
                data_.append(buf, encoded_len);
                int tmp_pos = pos_;
                pos_ += encoded_len;
                fprintf(stderr, "get kv:%s\n", data_.substr(tmp_pos, encoded_len).c_str());
                *length = encoded_len;
                return tmp_pos;
            }
            else{ 
                fprintf(stderr, "get kv failed\n");
                return -1;
            }
        }
};

void GenerateKVByself(std::string& kv , Slice& key, Slice &value, 
        SequenceNumber sequence, uint32_t tag){
                //std::cout<<", value:"<<value.ToString()<<std::endl;
                size_t key_size = key.size();
                size_t val_size = value.size();
                size_t internal_key_size = key_size + 8;
                size_t encoded_len = 0;
                if(tag == kTypeValue){
                    DEBUG_T("get, kTypeValue\n");
                    encoded_len = VarintLength(internal_key_size) + internal_key_size +
                    VarintLength(val_size) + val_size;
                }
                else {
                    DEBUG_T("get, kTypeDeletion\n");
                    encoded_len = VarintLength(internal_key_size) + internal_key_size;
                }
                char buf[encoded_len];
                char* p = EncodeVarint32(buf, internal_key_size);
                memcpy(p, key.data(), key_size);
                p += key_size;
                EncodeFixed64(p, (sequence << 8) | tag);
                p += 8;
                if(tag == kTypeValue){
                    p = EncodeVarint32(p, val_size);
                    memcpy(p, value.data(), val_size);
                    assert(p + val_size == buf + encoded_len);
                }
                kv.append(buf, encoded_len);
}

}///namespace

TEST(chunkTableTest, Simple) {

    InternalKeyComparator cmp(BytewiseComparator());
    
    ////////chunklog
    Env* env = Env::Default();
    std::string nvm_path;
    env->GetMEMDirectory(&nvm_path);
    uint64_t chunklog_number = 1;
    std::string chunklogfile = chunkLogFileName(nvm_path, chunklog_number);
    fprintf(stderr, "chunklogfile:%s", chunklogfile.c_str());
    size_t filesize = (64 << 10)<< 10;
    
    KVGenerator kvgen(1000, 10, 0);
    chunkLog ckg(&chunklogfile, filesize, false);
   
    //const char* kvoffset = reinterpret_cast<char*>(ckg.insert(kv1, kvlen));
    //fprintf(stderr, "offset:%p\n", kvoffset);
    
    //Slice user_key = ckg.getUserKey(kvoffset); 
    
    ////////arenanvm
    uint64_t chunkindex_number = 2;
    std::string chunkindexfile = chunkIndexFileName(nvm_path, chunkindex_number);
    size_t chunkindexfile_size = (4 << 10) << 10;
    ArenaNVM arena(&chunkindexfile, chunkindexfile_size, false);
    
   
    ////chunktable
    chunkTable* ckTbl = new chunkTable(cmp, &arena, &ckg, false);
    ckTbl->Ref();
    
    int num = 100;
    for(int i = 0; i < num; i++){
        size_t kvlen;
        int pos = kvgen.GenerateKV(kTypeValue, &kvlen);
        char kv[kvlen];
        memcpy(kv, kvgen.data_.c_str() + pos, kvlen);
        uint32_t key_length, val_length;
        const char* key_ptr = GetVarint32Ptr(kv, kv + 5, &key_length);
        const char* val_ptr = GetVarint32Ptr(key_ptr + key_length, key_ptr + key_length + 5, &val_length);
        std::string old_value;
        old_value.append(val_ptr, val_length);
        //fprintf(stderr, "kv1:%p, before table add, user_key:%s, len:%d\n", 
          //      kv, Slice(key_ptr, key_length - 8).ToString().c_str(), 
            //    key_length - 8);
        ckTbl->Add(kv);
        
        ///////////GET
        char key[100];
        int key_len = snprintf(key, sizeof(key), "%016d", i+1);
        //fprintf(stderr, "----------------\nkey:%s\n", key);
        std::string value;
        Status s;
        ckTbl->Get(Slice(key, key_len), &value, &s, 10000);
        ASSERT_TRUE(cmp.user_comparator()->Compare(old_value, value) == 0);
        std::cout<<"index_usage: "<<ckTbl->ApproximateIndexNVMUsage()<<", log_usage_: "
            <<ckTbl->ApproximateLogNVMUsage()<<std::endl;
        //std::cout<<"old_value:"<<old_value<<", value:"<<value<<std::endl;

        //////////////contains 
        //ASSERT_TRUE(ckTbl->Contains(Slice(key, key_len), 10000));
        

        /////update
        /*
        if(i >= 0){
            std::string newkv;
            Slice newvalue("I'm Meggie");
            Slice newkey(key, key_len);
            GenerateKVByself(newkv, newkey, newvalue, 10000, kTypeValue);
            
            std::cout<<"before update, key_num:"<<ckTbl->getKeyNum()<<std::endl;
            ckTbl->Add(newkv.data());
            std::cout<<"after update, key_num:"<<ckTbl->getKeyNum()<<std::endl;
            ckTbl->Get(newkey, &value, &s, 100001);
            std::cout<<"new_value:"<<value<<std::endl;
            
            /////delete 
            std::string deletekv;
            Slice deleteval;
            std::cout<<"delete,newkey:"<<newkey.ToString()<<std::endl;
            GenerateKVByself(deletekv, newkey, deleteval, 10002, kTypeDeletion);
            ckTbl->Add(deletekv.data());
            ckTbl->Get(newkey, &value, &s, 10003);
            ASSERT_TRUE(s.IsNotFound());
            std::cout<<"after delete,"<<std::endl;
            if(s.IsNotFound()){
                std::cout<<"NOT_FOUND"<<std::endl;
            }
            else 
                std::cout<<"new_value:"<<value<<std::endl;
        }
        */
    }
    Iterator* iter = ckTbl->NewIterator();
    ckTbl->Ref();
    iter->SeekToFirst();
    while(iter->Valid()){
        std::cout<<"key: "<<ExtractUserKey(iter->key()).ToString()<<", value: "<<iter->value().ToString()<<std::endl;
        iter->Next();
    }
    ckTbl->Unref();
    ckTbl->Unref();
}

/*
TEST(SkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  Comparator cmp;
  SkipList<Key, Comparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      list.Insert(key);
    }
  }

  for (int i = 0; i < R; i++) {
    if (list.Contains(i)) {
      ASSERT_EQ(keys.count(i), 1);
    } else {
      ASSERT_EQ(keys.count(i), 0);
    }
  }

  // Simple iterator tests
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    iter.Seek(0);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), iter.key());

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), iter.key());
  }

  // Forward iteration test
  for (int i = 0; i < R; i++) {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.Seek(i);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  {
    SkipList<Key, Comparator>::Iterator iter(&list);
    iter.SeekToLast();

    // Compare against model iterator
    for (std::set<Key>::reverse_iterator model_iter = keys.rbegin();
         model_iter != keys.rend();
         ++model_iter) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, iter.key());
      iter.Prev();
    }
    ASSERT_TRUE(!iter.Valid());
  }
}
*/
// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructor.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
/*
class ConcurrentTest {
 private:
  static const uint32_t K = 4;

  static uint64_t key(Key key) { return (key >> 40); }
  static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
  static uint64_t hash(Key key) { return key & 0xff; }

  static uint64_t HashNumbers(uint64_t k, uint64_t g) {
    uint64_t data[2] = { k, g };
    return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
  }

  static Key MakeKey(uint64_t k, uint64_t g) {
    assert(sizeof(Key) == sizeof(uint64_t));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }

  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
  }

  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }

  // Per-key generation
  struct State {
    port::AtomicPointer generation[K];
    void Set(int k, intptr_t v) {
      generation[k].Release_Store(reinterpret_cast<void*>(v));
    }
    intptr_t Get(int k) {
      return reinterpret_cast<intptr_t>(generation[k].Acquire_Load());
    }

    State() {
      for (int k = 0; k < K; k++) {
        Set(k, 0);
      }
    }
  };

  // Current state of the test
  State current_;

  Arena arena_;

  // SkipList is not protected by mu_.  We just use a single writer
  // thread to modify it.
  SkipList<Key, Comparator> list_;

 public:
  ConcurrentTest() : list_(Comparator(), &arena_) { }

  // REQUIRES: External synchronization
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const intptr_t g = current_.Get(k) + 1;
    const Key key = MakeKey(k, g);
    list_.Insert(key);
    current_.Set(k, g);
  }

  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    State initial_state;
    for (int k = 0; k < K; k++) {
      initial_state.Set(k, current_.Get(k));
    }

    Key pos = RandomTarget(rnd);
    SkipList<Key, Comparator>::Iterator iter(&list_);
    iter.Seek(pos);
    while (true) {
      Key current;
      if (!iter.Valid()) {
        current = MakeKey(K, 0);
      } else {
        current = iter.key();
        ASSERT_TRUE(IsValidKey(current)) << current;
      }
      ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state.
      while (pos < current) {
        ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        ASSERT_TRUE((gen(pos) == 0) ||
                    (gen(pos) > static_cast<Key>(initial_state.Get(key(pos))))
                    ) << "key: " << key(pos)
                      << "; gen: " << gen(pos)
                      << "; initgen: "
                      << initial_state.Get(key(pos));

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          pos = MakeKey(key(pos) + 1, 0);
        } else {
          pos = MakeKey(key(pos), gen(pos) + 1);
        }
      }

      if (!iter.Valid()) {
        break;
      }

      if (rnd->Next() % 2) {
        iter.Next();
        pos = MakeKey(key(pos), gen(pos) + 1);
      } else {
        Key new_target = RandomTarget(rnd);
        if (new_target > pos) {
          pos = new_target;
          iter.Seek(new_target);
        }
      }
    }
  }
};
const uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST(SkipTest, ConcurrentWithoutThreads) {
  ConcurrentTest test;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 10000; i++) {
    test.ReadStep(&rnd);
    test.WriteStep(&rnd);
  }
}

class TestState {
 public:
  ConcurrentTest t_;
  int seed_;
  port::AtomicPointer quit_flag_;

  enum ReaderState {
    STARTING,
    RUNNING,
    DONE
  };

  explicit TestState(int s)
      : seed_(s),
        quit_flag_(nullptr),
        state_(STARTING),
        state_cv_(&mu_) {}

  void Wait(ReaderState s) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    while (state_ != s) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  void Change(ReaderState s) LOCKS_EXCLUDED(mu_) {
    mu_.Lock();
    state_ = s;
    state_cv_.Signal();
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  ReaderState state_ GUARDED_BY(mu_);
  port::CondVar state_cv_ GUARDED_BY(mu_);
};

static void ConcurrentReader(void* arg) {
  TestState* state = reinterpret_cast<TestState*>(arg);
  Random rnd(state->seed_);
  int64_t reads = 0;
  state->Change(TestState::RUNNING);
  while (!state->quit_flag_.Acquire_Load()) {
    state->t_.ReadStep(&rnd);
    ++reads;
  }
  state->Change(TestState::DONE);
}

static void RunConcurrent(int run) {
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestState state(seed + 1);
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int i = 0; i < kSize; i++) {
      state.t_.WriteStep(&rnd);
    }
    state.quit_flag_.Release_Store(&state);  // Any non-null arg will do
    state.Wait(TestState::DONE);
  }
}

TEST(SkipTest, Concurrent1) { RunConcurrent(1); }
TEST(SkipTest, Concurrent2) { RunConcurrent(2); }
TEST(SkipTest, Concurrent3) { RunConcurrent(3); }
TEST(SkipTest, Concurrent4) { RunConcurrent(4); }
TEST(SkipTest, Concurrent5) { RunConcurrent(5); }

*/
}  // namespace leveldb
int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
