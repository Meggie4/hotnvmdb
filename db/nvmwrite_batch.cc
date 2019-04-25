// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// NVMWriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/nvmwrite_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/nvmtable.h"
#include "db/nvmwrite_batch_internal.h"
#include "util/coding.h"
#include <cstring>

namespace leveldb {

// NVMWriteBatch header has a 4-byte count.
static const size_t kHeader = 4;

NVMWriteBatch::NVMWriteBatch() {
  Clear();
}

NVMWriteBatch::~NVMWriteBatch() { }

NVMWriteBatch::Handler::~Handler() { }

void NVMWriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t NVMWriteBatch::ApproximateSize() const {
  return rep_.size();
}

Status NVMWriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed NVMWriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
            std::string kvitem;
            kvitem.append(key.data(), key.size());
            kvitem.append(value.data(), value.size());
            handler->Put(kvitem.data());
        } else {
          return Status::Corruption("bad NVMWriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) { 
          handler->Put(key.data());
        } else {
          return Status::Corruption("bad NVMWriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown NVMWriteBatch tag");
    }
  }
  if (found != NVMWriteBatchInternal::Count(this)) {
    return Status::Corruption("NVMWriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int NVMWriteBatchInternal::Count(const NVMWriteBatch* b) {
  return DecodeFixed32(b->rep_.data());
}

void NVMWriteBatchInternal::SetCount(NVMWriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[0], n);
}


void NVMWriteBatch::Put(const char* kvitem) {
  NVMWriteBatchInternal::SetCount(this, NVMWriteBatchInternal::Count(this) + 1);
  size_t kv_length;
  uint32_t key_length;
  const char* key_ptr = GetKVLength(kvitem, &key_length, &kv_length);
  uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
  ValueType type = static_cast<ValueType>(tag & 0xff);
  rep_.push_back(static_cast<char>(type)); 
  rep_.append(kvitem, kv_length);
}

void NVMWriteBatch::Append(const NVMWriteBatch &source) {
  NVMWriteBatchInternal::Append(this, &source);
}

namespace {
class chunkTableInserter : public NVMWriteBatch::Handler {
 public:
  chunkTable* ckTbl_;

  virtual void Put(const char* kvitem) {
    ckTbl_->Add(kvitem);
  }
};
}  // namespace

Status NVMWriteBatchInternal::InsertInto(const NVMWriteBatch* b,
                                      chunkTable* ckTbl) {
  chunkTableInserter inserter;
  inserter.ckTbl_ = ckTbl;
  return b->Iterate(&inserter);
}

void NVMWriteBatchInternal::SetContents(NVMWriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void NVMWriteBatchInternal::Append(NVMWriteBatch* dst, const NVMWriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
