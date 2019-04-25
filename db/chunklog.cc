#include <string>
#include <stdint.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "port/port.h"
#include "db/chunklog.h"
#include "db/dbformat.h"
#include "port/cache_flush.h"
#include "util/debug.h"

namespace leveldb{
chunkLog::chunkLog(std::string* logfile, 
        size_t filesize, bool recovery){
    //DEBUG_T("log_file:%s\n", logfile->c_str());
    fd = open(logfile->c_str(), O_RDWR);
    if(fd == -1){
        //perror("open_logfile_failed\n");
        fd = open(logfile->c_str(), O_RDWR | O_CREAT, 0664); 
        if(fd == -1)
            perror("create_logfile_failed\n");
        /*else
            perror("create_logfile_success\n");*/
    }
    if(ftruncate(fd, LOG_THRESH * filesize) != 0){
        perror("ftruncate_failed\n");
    }
    /*else{
        perror("ftruncate_success\n");
    }*/
    log_map_start_ = (void*)mmap(NULL, LOG_THRESH * filesize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if(recovery){  
        log_bytes_remaining_ = *((size_t*)log_map_start_);
        log_current_ptr_ = (char*)log_map_start_ + (filesize - log_bytes_remaining_);
        nvm_usage_.NoBarrier_Store(reinterpret_cast<void*>(filesize - log_bytes_remaining_));
    }
    else{
        //log_bytes_ = 0;
        log_bytes_remaining_ = filesize;
        *(size_t*)log_map_start_ = log_bytes_remaining_;
        flush_cache((void*)log_map_start_, CACHE_LINE_SIZE);
        log_current_ptr_ =  (char*)log_map_start_;
        nvm_usage_.NoBarrier_Store(reinterpret_cast<void*>(0));
    }
}

void* chunkLog::insert(const char* kvitem, size_t len){
    void* kvstart;
    uint32_t key_length1;
    uint32_t key_length;
    //DEBUG_T("log_current_ptr_:%p, log_map_start_:%p\n", log_current_ptr_, log_map_start_);
    const char* key_ptr1 = GetVarint32Ptr(kvitem, kvitem + 5, &key_length1);
    //DEBUG_T("before insert, user_key:%s, len:%d\n", Slice(key_ptr1, key_length1 - 8).ToString().c_str(), 
     //       key_length1 - 8);
    if(log_current_ptr_ == log_map_start_){
        kvstart = log_current_ptr_ + sizeof(size_t);
        memcpy_persist(log_current_ptr_ + sizeof(size_t), kvitem, len);
        log_current_ptr_ += sizeof(size_t) + len;
        //log_bytes_ += sizeof(size_t) + len;
        log_bytes_remaining_ -= sizeof(size_t) + len;
        //DEBUG_T("before insert, NVMUsage:%zu\n", NVMUsage());
        nvm_usage_.NoBarrier_Store(reinterpret_cast<void*>(NVMUsage() + sizeof(size_t) + len));
        //DEBUG_T("after insert, NVMUsage:%zu\n", NVMUsage());
    }
    else{
        kvstart = log_current_ptr_;
        memcpy_persist(log_current_ptr_, kvitem, len);
        log_current_ptr_ += len;
        //log_bytes_ += len;
        log_bytes_remaining_ -= len;
        nvm_usage_.NoBarrier_Store(reinterpret_cast<void*>(NVMUsage() + len));
    }
    *(size_t*)log_map_start_ = log_bytes_remaining_;
    flush_cache((void*)log_map_start_, CACHE_LINE_SIZE);
    
    //DEBUG_T("insert， kvstart：%p\n", kvstart);
    const char* key_ptr = GetVarint32Ptr(reinterpret_cast<char*>(kvstart), 
            reinterpret_cast<char*>(kvstart)+5, &key_length);
    //DEBUG_T("after insert, user_key:%s, len:%d\n", Slice(key_ptr, key_length - 8).ToString().c_str(), key_length - 8);
    return reinterpret_cast<void*>((intptr_t)kvstart - (intptr_t)log_map_start_);
}

char* chunkLog::getKV(const void* kvpos_offset) const{
    char* entry =  reinterpret_cast<char*>((intptr_t)log_map_start_ + (intptr_t)kvpos_offset);
    return entry;
}

const Slice chunkLog::getUserKey(const void* kvpos_offset) const{
    char* entry =  reinterpret_cast<char*>((intptr_t)log_map_start_ + (intptr_t)kvpos_offset);
    //DEBUG_T("getKey， keystart：%p\n", entry);
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    //DEBUG_T("user_key:%s, len:%d\n", Slice(key_ptr, key_length - 8).ToString().c_str(), key_length - 8);
    return Slice(key_ptr, key_length);
}

const Slice chunkLog::getValue(const void* kvpos_offset) const{
    char* entry =  reinterpret_cast<char*>((intptr_t)log_map_start_ + (intptr_t)kvpos_offset);
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    if(static_cast<ValueType>(tag & 0xff) == kTypeDeletion)
        return Slice();
    else{
        uint32_t value_length;
        const char* val_ptr = GetVarint32Ptr(key_ptr + key_length, key_ptr + key_length + 5, &value_length);
        return Slice(val_ptr, value_length);
    } 
}
}
