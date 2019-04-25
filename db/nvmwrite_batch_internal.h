// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_NVMWRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_NVMWRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/nvmwrite_batch.h"

namespace leveldb {

class chunkTable;

// NVMWriteBatchInternal provides static methods for manipulating a
// NVMWriteBatch that we don't want in the public WriteBatch interface.
class NVMWriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  static int Count(const NVMWriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(NVMWriteBatch* batch, int n);

  static Slice Contents(const NVMWriteBatch* batch) {
    return Slice(batch->rep_);
  }

  static size_t ByteSize(const NVMWriteBatch* batch) {
    return batch->rep_.size();
  }

  static void SetContents(NVMWriteBatch* batch, const Slice& contents);

  static Status InsertInto(const NVMWriteBatch* batch, chunkTable* ckTbl);

  static void Append(NVMWriteBatch* dst, const NVMWriteBatch* src);
};

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
