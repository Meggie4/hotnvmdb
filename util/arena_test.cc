// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <stdio.h>
#include <iostream>
#include "util/random.h"
#include "util/testharness.h"
#include "leveldb/env.h"
#include "db/filename.h"

namespace leveldb {

class ArenaTest { };

TEST(ArenaTest, Empty) {
  Arena arena;
}

TEST(ArenaTest, Simple) {
  std::vector<std::pair<size_t, char*> > allocated;
  Arena arena;
  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    //以1/10的几率按照对齐分配，9/10的几率普通分配。
    if (rnd.OneIn(10)) {
      r = arena.AllocateAligned(s);
    } else {
      r = arena.Allocate(s);
    }

    for (size_t b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      r[b] = i % 256;
    }
    bytes += s;
    allocated.push_back(std::make_pair(s, r));
    ASSERT_GE(arena.MemoryUsage(), bytes);
    if (i > N/10) {
      ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
    }
  }
  for (size_t i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;
    for (size_t b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }
}

TEST(ArenaTest, Complicated) {
  std::vector<std::pair<size_t, char*> > allocated;

  Env* env = Env::Default();
  std::string nvm_path;
  env->GetMEMDirectory(&nvm_path);
  uint64_t chunkindex_number = 1;
  std::string chunkindexfile = chunkIndexFileName(nvm_path, chunkindex_number);
  size_t chunkindexfile_size = (4 << 10) << 10;
  
  /*uint64_t chunklog_number = 2;
  std::string chunklogfile = chunkLogFileName(nvm_path, chunklog_number);
  size_t filesize = (4 << 10)<< 10;
  chunkLog ckg(&chunklogfile, filesize, false);*/
 
  fprintf(stderr, "index file:%s\n", chunkindexfile.c_str());
  ArenaNVM arena(&chunkindexfile, chunkindexfile_size, false);

  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {////如果为10000,20000,30000等等，那就直接赋值给s
      s = i;
    } else {
      s = rnd.OneIn(4000) ? rnd.Uniform(6000) :
          (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));//否则以1/4000的几率，得到6000范围内的均匀数;
      //另外的情况，以1/10几率返回100范围内的均匀数，其他情况获得20范围内的均匀数。
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    r = arena.AllocateAlignedNVM(s);

    //根据分配的字节数，进行填充。一个字节的最大数为256
    for (size_t b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      r[b] = i % 256;
    }
    //键值对所占的内存开销
    bytes += s;
    /////保存了字节数以及相应的内容
    allocated.push_back(std::make_pair(s, r));

    /////包括跳表节点总共占的内存开销。
    ASSERT_GE(arena.MemoryUsage(), bytes);
    /*if (i > N/10) {
      ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
    }*/
    //std::cout<<"MemoryUsage: "<<arena.MemoryUsage()<<", remaining: "<<arena.getAllocRem()<<std::endl;
    if(arena.getAllocRem() <= 0){
        fprintf(stderr, "remaining bytes <= 0\n");
        break;
    }
  }
  for (size_t i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;///字节数
    const char* p = allocated[i].second;////内容
    //fprintf(stderr, "contents:%s,i:%d\n", p, i); 
    for (size_t b = 0; b < num_bytes; b++) {///核对字节内容
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }
}



}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
