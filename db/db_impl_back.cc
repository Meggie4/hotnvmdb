// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <set>
#include <string>
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
//////////////////meggie
#include "util/multi_bloomfilter.h"
#include "util/threadpool.h"
#include "db/nvmtable.h"
#include "util/debug.h"
//////////////////meggie

////////////meggie
#ifdef TIMER_LOG
	#define start_timer(s) timer->StartTimer(s)
	#define record_timer(s) timer->Record(s)
#else
	#define start_timer(s)
	#define record_timer(s)
#endif
////////////meggie

namespace leveldb {

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
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {
  }
};


////////////////////meggie
struct DBImpl::NVMTableCompactionState{
    struct Output{
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };
    NVMTableCompactionState()
        :builder(nullptr),
        outfile(nullptr),
        total_bytes(0){}
    std::vector<Output> outputs;
    WritableFile* outfile;
    TableBuilder* builder;
    uint64_t total_bytes;
    Output* current_output() {return &outputs[outputs.size() - 1];}
};
struct DBImpl::nvmcompact_struct{
    int index;
    chunkTable* cktbl;
    chunkTable* new_cktbl;
    DBImpl *db; 
    FileMetaData meta; 
};
////////////////////meggie

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src, 
                        const std::string& dbname_nvm) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ///memtable size限定在10KB~1GB之间 
  ClipToRange(&result.write_buffer_size, 10<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  /////////////////////meggie
  //限定在1KB~1GB之间
  ClipToRange(&result.chunk_index_size, 1<<10, 1<<30);
  //限定在10KB~1GB之间
  ClipToRange(&result.chunk_log_size, 10<<10, 1<<30);
  /////////////////////meggie
  
  DEBUG_T("write_buffer_size:%zu, chunk_index_size:%zu, chunk_log_size:%zu\n", 
          result.write_buffer_size, result.chunk_index_size, result.chunk_log_size);
  if (result.info_log == nullptr){ 
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    ////////////meggie
    src.env->CreateDir(dbname_nvm);  // In case it does not exist
    ////////////meggie
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

//////////////////meggie
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname, const std::string& dbname_nvm)
//////////////////meggie
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, 
                               raw_options, dbname_nvm)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      ///////////////meggie
      dbname_nvm_(dbname_nvm),
      ///////////////meggie
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(nullptr),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      ///////////////////////meggie
      nvmtbl_(new NVMTable(internal_comparator_)),
      chunk_been_allocated_(false),
      hot_bf_(new MultiHotBloomFilter()),
      ///////////////////////meggie
      versions_(new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_)) {
  has_imm_.Release_Store(nullptr);
  //////////////////meggie
  nvmtbl_->Ref();
  thpool_ = new ThreadPool(4);
  chunk_index_files_.resize(kNumChunkTable);
  chunk_log_files_.resize(kNumChunkTable);
  timer = new Timer();
  //////////////////meggie
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-null value is ok
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }
  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  ///////////////meggie
  DEBUG_T("before delete nvmtbl_\n");
  nvmtbl_->PrintInfo();
  std::string metafilename = chunkMetaFileName(dbname_nvm_, chunk_meta_file_); 
  nvmtbl_->SaveMetadata(metafilename);
  nvmtbl_->Unref();
  delete hot_bf_;
  delete thpool_;
  delete timer;
  ///////////////meggie

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
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  std::vector<std::string> filenames_nvm;
  
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  ///////////////meggie
  env_->GetChildren(dbname_nvm_, &filenames_nvm);
  filenames.insert(filenames.end(), filenames_nvm.begin(), 
          filenames_nvm.end());
  ///////////////meggie
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
        /////////////////////meggie
        case kCkgFile:
          keep = (live.find(number) != live.end());
          break;
        case kIdxFile:
          keep = (live.find(number) != live.end());
          break;
        /////////////////////meggie
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
            static_cast<int>(type),
            static_cast<unsigned long long>(number));
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
  /////////////////meggie
  env_->CreateDir(dbname_nvm_);
  /////////////////meggie
  assert(db_lock_ == nullptr);
  DEBUG_T("dbname:%s,lock:%s\n", dbname_.c_str(), LockFileName(dbname_).c_str());
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

  DEBUG_T("before VersionSet->recover\n");
  s = versions_->Recover(save_manifest);
  DEBUG_T("after VersionSet->recover\n");
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
  /////////////////meggie
  std::vector<uint64_t> chunkindex_files;
  std::vector<uint64_t> chunklog_files;
  uint64_t chunkmeta_file;
  chunkindex_files.resize(kNumChunkTable);
  chunklog_files.resize(kNumChunkTable);
  versions_->AddChunkFiles(&chunkindex_files, &chunklog_files, 
          &chunkmeta_file);
  /////////////////meggie
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

  ///////////////////meggie
  s = RecoverChunkFile(chunkindex_files, chunklog_files, chunkmeta_file);
  if(!s.ok()){
      return s;
  }
  ///////////////////meggie
  
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

///////////////////meggie
Status DBImpl::RecoverChunkFile(std::vector<uint64_t>& chunkindex_files, 
                                std::vector<uint64_t>& chunklog_files,
                                uint64_t chunkmeta_file){
    std::map<int, chunkTable*> update_chunks;
    for(int i = 0; i < kNumChunkTable; i++){
        if(chunkindex_files[i] != 0 &&
                chunklog_files[i] != 0){
            chunk_been_allocated_ = true;
            versions_->MarkFileNumberUsed(chunkindex_files[i]);
            versions_->MarkFileNumberUsed(chunklog_files[i]);
           
            chunk_index_files_[i] = chunkindex_files[i];
            chunk_log_files_[i] = chunklog_files[i];
            
            std::string indexfilename = chunkIndexFileName(dbname_nvm_, chunkindex_files[i]);
            std::string logfilename = chunkLogFileName(dbname_nvm_, chunklog_files[i]);
            ArenaNVM* arena = new ArenaNVM(&indexfilename, options_.chunk_index_size, true);
            chunkLog* ckg =new chunkLog(&logfilename, options_.chunk_log_size, true);
            chunkTable* cktbl = nvmtbl_->GetNewChunkTable(arena, ckg, true);
            cktbl->SetChunklogNumber(chunklog_files[i]);
            cktbl->SetChunkindexNumber(chunkindex_files[i]);
            //DEBUG_T("version chunkindex_filenumber:%lu, chunkLogFilenumber:%lu\n",
              //      chunkindex_files[i], chunklog_files[i]);
            update_chunks.insert(std::make_pair(i, cktbl));
        }
    } 
    DEBUG_T("after recovery, chunkmetafile:%lu\n", chunkmeta_file);
    if(chunkmeta_file){
        std::string metafilename = chunkMetaFileName(dbname_nvm_, chunkmeta_file);
        chunk_meta_file_ = chunkmeta_file;
        nvmtbl_->RecoverMetadata(update_chunks, metafilename);
    }
    
    DEBUG_T("after recover metadata\n");
    
    UpdateNVMTable(update_chunks, true);

    printChunkFileNumbers();

    bool exceedThresh = nvmtbl_->CheckAndAddToCompactionList(
                    to_compaction_list_,
                    options_.chunk_index_size, 
                    options_.chunk_log_size);
    //DEBUG_T("before exceedThresh\n");
    if(exceedThresh){
        //bg_nvmtable_cv_.Signal(); 
        //Log(options_.info_log, "wait nvmtable compaction finished.....\n");
        //bg_fg_cv_.Wait();
    } 
    return Status::OK();
}
///////////////////meggie

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();
  // Open the log file
  Status status;
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
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
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem, hot_bf_);
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
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
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
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
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
    /////////////meggie
    start_timer(BUILD_LEVEL0_TABLES);
    std::map<std::string, uint32_t> hotkeys = mem->SelectHotKeysWithMean();
    s = BuildTable(dbname_, env_, options_, table_cache_, 
            iter, &meta, hotkeys, nvmtbl_, hot_bf_);
    record_timer(BUILD_LEVEL0_TABLES);
    start_timer(GET_LOCK_AFTER_BUILD_LEVEL0_TABLES);
    /////////////meggie
    mutex_.Lock();
    record_timer(GET_LOCK_AFTER_BUILD_LEVEL0_TABLES);
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
  
  //////////////meggie
  if(s.ok() && meta.file_size == 0){
    DEBUG_T("all datas in the memtable are hot\n");
    return s;
  } 
  //////////////meggie

  start_timer(ADD_LEVEL0_FILES_TO_EDIT);
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != nullptr) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }
  record_timer(ADD_LEVEL0_FILES_TO_EDIT);

  DEBUG_T("finish addfile\n");
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);
  
  start_timer(TOTAL_MEMTABLE_COMPACTION);
  ////////////////meggie
  start_timer(MAKE_ROOM_FOR_IMMUTABLE);
  MakeRoomForImmu(false);
  record_timer(MAKE_ROOM_FOR_IMMUTABLE);
  ////////////////meggie

  //DEBUG_T("after MakeRoomForImmu\n");
  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  start_timer(WRITE_LEVEL0_TABLE);
  Status s = WriteLevel0Table(imm_, &edit, base);
  record_timer(WRITE_LEVEL0_TABLE);
  base->Unref();
  //DEBUG_T("after base->unref\n");

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    start_timer(COMPACT_MEMTABLE_LOGANDAPPLY);
    s = versions_->LogAndApply(&edit, &mutex_);
    record_timer(COMPACT_MEMTABLE_LOGANDAPPLY);
  }

  DEBUG_T("after logandapply\n");
  if (s.ok()) {
    // Commit to the new state
    start_timer(COMPACT_MEMTABLE_DELETE_OBSOLETE_FILES);
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.Release_Store(nullptr);
    DeleteObsoleteFiles();
    record_timer(COMPACT_MEMTABLE_DELETE_OBSOLETE_FILES);
  } else {
    RecordBackgroundError(s);
  }
  record_timer(TOTAL_MEMTABLE_COMPACTION);
  DEBUG_T("finish CompactMemTable\n");
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
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr &&
             manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } 
  else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    //DEBUG_T("after CompactMemTable\n");
    return;
  }

  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    assert(c->num_input_files(0) == 1);
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
    manual_compaction_ = nullptr;
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
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
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
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

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
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
    if (has_imm_.NoBarrier_Load() != nullptr) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
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
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

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
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) { }
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

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
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  /////////meggie
  nvmtbl_->Ref();
  /////////meggie
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
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } 
    ///////////////////meggie
    else if(nvmtbl_->Get(key, value, &s, snapshot)){
    // Done
    }
    ///////////////////meggie
    else {
      DEBUG_T("to find in sstables\n");
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  /////////meggie
  nvmtbl_->Unref();
  /////////meggie
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != nullptr
       ? static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number()
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

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

//////////////////meggie
void DBImpl::PrintTimerAudit(){
    printf("--------timer information--------\n");
    timer->DebugString();
    printf("%s\n", timer->DebugString().c_str());
    printf("-----end timer information-------\n");
}
//////////////////meggie

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
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
  Status status = MakeRoomForWrite(my_batch == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != nullptr) {  // nullptr batch is for compactions
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
        status = WriteBatchInternal::InsertInto(updates, mem_, hot_bf_);
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
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

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

    if (w->batch != nullptr) {
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
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = nullptr;
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
////////////////////////meggie
void DBImpl::printChunkFileNumbers(){
    DEBUG_T("---------printChunkFileNumbers--------\n");
    for(int i = 0; i < chunk_index_files_.size(); i++){
        DEBUG_T("chunk%d: index number:%lu, log number:%lu\n", 
                i, chunk_index_files_[i], chunk_log_files_[i]);
    }
    DEBUG_T("---------printChunkFileNumbers--------\n");
}


Status DBImpl::UpdateNVMTable(std::map<int, chunkTable*>& update_chunks, bool recovery){
    if(!recovery){
        std::map<int, chunkTable*>::iterator iter;
        for(iter = update_chunks.begin(); iter != update_chunks.end(); iter++){
            chunk_index_files_[iter->first] = iter->second->GetChunkindexNumber();
            chunk_log_files_[iter->first] = iter->second->GetChunklogNumber();
        }
    }
    nvmtbl_->UpdateChunkTables(update_chunks);
    return Status::OK();
}

chunkTable* DBImpl::CreateNewchunkTable(){
    uint64_t new_chunkindex_number = versions_->NewFileNumber();
    uint64_t new_chunklog_number = versions_->NewFileNumber();
    std::string indexfilename = chunkIndexFileName(dbname_nvm_, new_chunkindex_number);
    std::string logfilename = chunkLogFileName(dbname_nvm_, new_chunklog_number);
    ArenaNVM* arena = new ArenaNVM(&indexfilename, options_.chunk_index_size, false);
    chunkLog* ckg =new chunkLog(&logfilename, options_.chunk_log_size, false);
    chunkTable* cktbl = nvmtbl_->GetNewChunkTable(arena, ckg, false);
    cktbl->SetChunklogNumber(new_chunklog_number);
    cktbl->SetChunkindexNumber(new_chunkindex_number);
    return cktbl;
}

void DBImpl::InitNVMCompact(std::map<int, chunkTable*>& to_compaction_list, 
        nvmcompact_struct* nvmcompact){
    std::map<int, chunkTable*>::iterator iter = to_compaction_list.begin();
    int i = 0;
    for(; iter != to_compaction_list.end(); iter++, i++){
        nvmcompact[i].index = iter->first;
        nvmcompact[i].cktbl = iter->second;
        nvmcompact[i].new_cktbl = CreateNewchunkTable();
        nvmcompact[i].db = this;
        nvmcompact[i].meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(nvmcompact[i].meta.number);
    }
}

Status DBImpl::FinishNVMTableCompaction(nvmcompact_struct* nvmcompact, 
                                    int size,
                                    Version* base){
    Status s;
    VersionEdit edit;
    std::map<int, chunkTable*> update_chunks;
    for(int i = 0; i < size; i++){
       pending_outputs_.erase(nvmcompact[i].meta.number);
       if(nvmcompact[i].meta.file_size > 0){
           const Slice min_user_key = nvmcompact[i].meta.smallest.user_key();
           const Slice max_user_key = nvmcompact[i].meta.largest.user_key();
           int level = 0;
           if(base != nullptr)
               level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
           edit.AddFile(level, nvmcompact[i].meta.number, 
                   nvmcompact[i].meta.file_size, nvmcompact[i].meta.smallest,
                   nvmcompact[i].meta.largest);
       } 
       update_chunks.insert(std::make_pair(nvmcompact[i].index, nvmcompact[i].new_cktbl));
    }
    UpdateNVMTable(update_chunks, false);
    edit.update_chunkfiles(chunk_index_files_, chunk_log_files_);
    s = versions_->LogAndApply(&edit, &mutex_);
    if(s.ok()){
        DeleteObsoleteFiles();
        std::map<int, chunkTable*>().swap(to_compaction_list_);
    }
    return s;
}

Status DBImpl::MakeRoomForImmu(bool force){
   mutex_.AssertHeld();
   Status s;
   bool exceedThresh;
   DEBUG_T("in MakeRoomForImmu, check\n");
   start_timer(CHECK_ADD_COMPACTION_LIST);
   exceedThresh = nvmtbl_->CheckAndAddToCompactionList(
                        to_compaction_list_,
                        options_.chunk_index_size, 
                        options_.chunk_log_size);
   record_timer(CHECK_ADD_COMPACTION_LIST);
   DEBUG_T("after check\n");
   while(true){
       if(!bg_error_.ok()){
            s = bg_error_;
            break;
       }else if(!force && !exceedThresh){
            break;
       }else{
            if(!exceedThresh){
                nvmtbl_->AddAllToCompactionList(to_compaction_list_);
            } 
            start_timer(TOTAL_NVMTABLE_COMPACTION);
            int sz = to_compaction_list_.size();
            nvmcompact_struct nvmcompact[sz];
            start_timer(INIT_NVM_COMPACT);
            InitNVMCompact(to_compaction_list_, nvmcompact);
            record_timer(INIT_NVM_COMPACT);
            
            DEBUG_T("before add job, sz:%d\n", sz);
            start_timer(THPOOL_HANDLE_JOB);
            for(int i = 0; i < sz; i++){
                DEBUG_T("have add job\n");
                thpool_->AddJob(CompactNVMTable, &nvmcompact[i]);
            }
            DEBUG_T("before wait\n");
            thpool_->WaitAll();
            DEBUG_T("after wait\n");
            record_timer(THPOOL_HANDLE_JOB);
           
            start_timer(FINISH_NVMTABLE_COMPACTION);
            Version* base = versions_->current();
            base->Ref();
            FinishNVMTableCompaction(nvmcompact, sz, base);
            base->Unref();
            record_timer(FINISH_NVMTABLE_COMPACTION);
            
            record_timer(TOTAL_NVMTABLE_COMPACTION);
            exceedThresh = nvmtbl_->CheckAndAddToCompactionList(
                                to_compaction_list_,
                                options_.chunk_index_size, 
                                options_.chunk_log_size);
            force = false;
       }
   }
   //DEBUG_T("finish MakeRoomForImmu\n");
   return s;
}

static const int kLevel0FileSize = (2 << 10) << 10;

Status DBImpl::WriteNVMTableToLevel0(chunkTable* cktbl, 
                        chunkTable* new_cktbl, 
                        FileMetaData* meta){
    const uint64_t start_micros = env_->NowMicros();
    Status s;
    TableBuilder* builder = NULL;
    WritableFile* file;
    int sst_num = 0;

    start_timer(TOTAL_WRITE_NVMTABLE_TO_LEVEL0);
    Iterator* iter = cktbl->NewIterator();
    iter->SeekToFirst();
    //DEBUG_T("test valid\n");
    if(iter->Valid()){
        //DEBUG_T("is valid\n");
        std::string fname = TableFileName(dbname_, meta->number);
        s = env_->NewWritableFile(fname, &file);               
        if(!s.ok()){
            return s;
        }
        builder = new TableBuilder(options_, file);
        bool first_entry = true;
        for(; iter->Valid(); iter->Next()) {
            Slice key = iter->key();
            Slice user_key(key.data(), key.size() - 8);
            char* number = const_cast<char*>(user_key.ToString().c_str()) + 3;
            if(hot_bf_->CheckHot(user_key)){
                DEBUG_T("in WriteNVMTableToLevel0, user_key:%s is check hot\n",
                        user_key.ToString().c_str());
                new_cktbl->Add(iter->GetNodeKey());
                if(atoi(number) == 259)
                    DEBUG_T("259 has been added from nvmtable to new_nvmtable\n");
            }
            else{
                sst_num++;
                if(first_entry){
                    meta->smallest.DecodeFrom(iter->key());
                    first_entry = false;
                }
                meta->largest.DecodeFrom(key);
                builder->Add(key, iter->value());
                if(atoi(number) == 259)
                    DEBUG_T("259 has been added from nvmtable to sstable\n");
            }
        }

        if(!sst_num)
            meta->file_size = 0;
        else{
            s = builder->Finish();
            meta->file_size = builder->FileSize();
            if(s.ok()){
                file->Sync();
            }
            if(s.ok()){
                file->Close();
            }
        }
        delete builder;
        builder = NULL;
        delete file;
        file = NULL;
       
        if(sst_num){
            Iterator* iterator = table_cache_->NewIterator(ReadOptions(), 
                    meta->number, meta->file_size);
            s = iterator->status();
            delete iterator;
        }
        if(s.ok()){
            DEBUG_T("NVMTable compaction Generated table #%lu, %lu bytes\n",
                    meta->number, meta->file_size);
        }
    }

    if(s.ok() && shutting_down_.Acquire_Load()){
        s = Status::IOError("Deleting DB during Compactnvmtable.....\n");
    }
    delete iter;
    record_timer(TOTAL_WRITE_NVMTABLE_TO_LEVEL0);
    
    return s;
}

void DBImpl::CompactNVMTable(void* args){
    nvmcompact_struct* nvmcompact = reinterpret_cast<nvmcompact_struct*>(args);
    DBImpl* db = nvmcompact->db;
    DEBUG_T("before WriteNVMTableToLevel0\n");
    db->WriteNVMTableToLevel0(nvmcompact->cktbl, nvmcompact->new_cktbl, &nvmcompact->meta);
    DEBUG_T("finish WriteNVMTableToLevel0\n");
}

Status DBImpl::TEST_CompactNVMTable(){
   
    return Status::OK();
}

////////////////////////meggie


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

////////////meggie
Status DB::Open(const Options& options, const std::string& dbname, 
                DB** dbptr,
                const std::string& dbname_nvm){
////////////meggie
  *dbptr = nullptr;
  
  ///////////////meggie
  DBImpl* impl = new DBImpl(options, dbname, dbname_nvm);
  ///////////////meggie
  impl->mutex_.Lock();
  VersionEdit edit;
  
  
  // Recover handles create_if_missing, error_if_exists
  DEBUG_T("before impl::recover\n");
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  DEBUG_T("after impl::recover\n");
  if (s.ok() && impl->mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    DEBUG_T("before env->NewWritableFile\n");
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    DEBUG_T("after env->NewWritableFile\n");
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  else{
      if(!s.ok())
        DEBUG_T("after recover is not ok\n");
  }
  
  ///////////////meggie
  if(!impl->chunk_been_allocated_){
      std::map<int, chunkTable*> update_chunks;
      for(int i = 0; i < kNumChunkTable; i++){
          chunkTable* cktbl = impl->CreateNewchunkTable();
          update_chunks.insert(std::make_pair(i, cktbl));
      }
      impl->UpdateNVMTable(update_chunks, false);
      edit.update_chunkfiles(impl->chunk_index_files_, impl->chunk_log_files_);

      uint64_t new_meta_number = impl->versions_->NewFileNumber();
      std::string metafilename = chunkMetaFileName(impl->dbname_nvm_, new_meta_number);
      impl->chunk_meta_file_ = new_meta_number;
      edit.SetMetaNumber(new_meta_number);

      if(!s.ok())
          return s;
  }
  //////////////meggie
 
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
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options, 
                const std::string& dbname_nvm) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  //////////////meggie
  std::vector<std::string> filenames_nvm;
  //////////////meggie
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  //////////////meggie
  result = env->GetChildren(dbname_nvm, &filenames_nvm);
  if(!result.ok())
        return Status::OK();
  filenames.insert(filenames.end(), filenames_nvm.begin(), 
          filenames_nvm.end());
  //////////////meggie
  
  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        /////////////////meggie
        if(find(filenames_nvm.begin(), filenames_nvm.end(), filenames[i]) != filenames_nvm.end())
            del = env->DeleteFile(dbname_nvm + "/" + filenames[i]);
        else 
        /////////////////meggie
            del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    ///////////////meggie
    env->DeleteDir(dbname_nvm);
    ///////////////meggie
  }
  return result;
}

}  // namespace leveldb
