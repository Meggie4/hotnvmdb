// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/db.h"

#include "db/nvmtable.h"
#include "db/nvmwrite_batch_internal.h"
#include "leveldb/env.h"
#include "util/logging.h"
#include "util/testharness.h"

namespace leveldb {
/*
static std::string PrintContents(NVMWriteBatch* b) {
  InternalKeyComparator cmp(BytewiseComparator());
  MemTable* mem = new MemTable(cmp);
  mem->Ref();
  std::string state;
  Status s = NVMWriteBatchInternal::InsertInto(b, mem);
  int count = 0;
  Iterator* iter = mem->NewIterator();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    switch (ikey.type) {
      case kTypeValue:
        state.append("Put(");
        state.append(ikey.user_key.ToString());
        state.append(", ");
        state.append(iter->value().ToString());
        state.append(")");
        count++;
        break;
      case kTypeDeletion:
        state.append("Delete(");
        state.append(ikey.user_key.ToString());
        state.append(")");
        count++;
        break;
    }
    state.append("@");
    state.append(NumberToString(ikey.sequence));
  }
  delete iter;
  if (!s.ok()) {
    state.append("ParseError()");
  } else if (count != NVMWriteBatchInternal::Count(b)) {
    state.append("CountMismatch()");
  }
  mem->Unref();
  return state;
}
*/
class NVMWriteBatchTest { };

TEST(NVMWriteBatchTest, Empty) {
  NVMWriteBatch batch;
  //ASSERT_EQ("", PrintContents(&batch));
  ASSERT_EQ(0, NVMWriteBatchInternal::Count(&batch));
}

TEST(NVMWriteBatchTest, Multiple) {
  NVMWriteBatch batch;
  char* data1 = "foo,bar";
  char* data2 = "baz,boo";
  batch.Put(data1);
  batch.Put(data2);
  ASSERT_EQ(2, NVMWriteBatchInternal::Count(&batch));
  /*ASSERT_EQ("Put(baz, boo)@102"
            "Delete(box)@101"
            "Put(foo, bar)@100",
            PrintContents(&batch));*/
}

/*
TEST(NVMWriteBatchTest, Corruption) {
  NVMWriteBatch batch;
  batch.Put(Slice("foo"), Slice("bar"));
  batch.Delete(Slice("box"));
  NVMWriteBatchInternal::SetSequence(&batch, 200);
  Slice contents = NVMWriteBatchInternal::Contents(&batch);
  NVMWriteBatchInternal::SetContents(&batch,
                                  Slice(contents.data(),contents.size()-1));
  ASSERT_EQ("Put(foo, bar)@200"
            "ParseError()",
            PrintContents(&batch));
}

TEST(NVMWriteBatchTest, Append) {
  NVMWriteBatch b1, b2;
  NVMWriteBatchInternal::SetSequence(&b1, 200);
  NVMWriteBatchInternal::SetSequence(&b2, 300);
  b1.Append(b2);
  ASSERT_EQ("",
            PrintContents(&b1));
  b2.Put("a", "va");
  b1.Append(b2);
  ASSERT_EQ("Put(a, va)@200",
            PrintContents(&b1));
  b2.Clear();
  b2.Put("b", "vb");
  b1.Append(b2);
  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@201",
            PrintContents(&b1));
  b2.Delete("foo");
  b1.Append(b2);
  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@202"
            "Put(b, vb)@201"
            "Delete(foo)@203",
            PrintContents(&b1));
}

TEST(NVMWriteBatchTest, ApproximateSize) {
  NVMWriteBatch batch;
  size_t empty_size = batch.ApproximateSize();

  batch.Put(Slice("foo"), Slice("bar"));
  size_t one_key_size = batch.ApproximateSize();
  ASSERT_LT(empty_size, one_key_size);

  batch.Put(Slice("baz"), Slice("boo"));
  size_t two_keys_size = batch.ApproximateSize();
  ASSERT_LT(one_key_size, two_keys_size);

  batch.Delete(Slice("box"));
  size_t post_delete_size = batch.ApproximateSize();
  ASSERT_LT(two_keys_size, post_delete_size);
}
*/
}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
