// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cstdlib>
#include "util/arena.h"
#include <assert.h>
#include "hoard/heaplayers/wrappers/gnuwrapper.h"
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
//////////////////meggie
#include "util/debug.h"
//////////////////meggie


static const long kBlockSize = 4096;

namespace leveldb {
Arena::Arena()
: memory_usage_(0)
{
    nvmarena_ = false;
    alloc_ptr_ = NULL;  // First allocation will allocate a block
    alloc_bytes_remaining_ = 0;
    map_start_ = map_end_ = 0;
    is_largemap_set_ = 0;
    nvmarena_ = false;
    fd = -1;
    kSize = kBlockSize;
}


Arena::~Arena() {
    if(this->nvmarena_ == true){

    }
    else{
        for (size_t i = 0; i < blocks_.size(); i++) {
            if(this->nvmarena_ == true) {
                DEBUG_T("have_delete_ArenaNVM, in ~Arena\n");
                munmap(blocks_[i], kSize);
                blocks_[i] = NULL;
            }
            else 
                delete[] blocks_[i];
        }
        if (fd != -1)
            close(fd);
    }
}

void* Arena:: operator new(size_t size)
{
    return malloc(size);
}

void* Arena::operator new[](size_t size) {
    return malloc(size);
}

void Arena::operator delete(void* ptr)
{
    free(ptr);
}

void* Arena::CalculateOffset(void* ptr) {
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

void* Arena::getMapStart() {
    return map_start_;
}

void* ArenaNVM::CalculateOffset(void* ptr) {
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

char* Arena::AllocateFallback(size_t bytes) {

    char *result = NULL;
    if (bytes > kBlockSize / 4) {
        // Object is more than a quarter of our block size.  Allocate it separately
        // to avoid wasting too much space in leftover bytes.
        result = AllocateNewBlock(bytes);
        return result;
    }

    // We waste the remaining space in the current block.
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = NULL;
    result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.NoBarrier_Store(
            reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
    return result;
}


ArenaNVM::ArenaNVM(std::string *filename, 
        size_t indexfile_size, 
        bool recovery): kNVMBlockSize(indexfile_size) {
    //: memory_usage_(0)
    fd = -1;
    allocation = false;
    if (recovery) {
        mfile = *filename;
        map_start_ = (void *)AllocateNVMBlock(kNVMBlockSize);
        alloc_bytes_remaining_ = *((size_t *)map_start_);
        nvmarena_ = true;
        alloc_ptr_ = (char *)map_start_ + (kSize - alloc_bytes_remaining_);
        map_end_ = 0;
        memory_usage_.NoBarrier_Store(
                reinterpret_cast<void *>(kNVMBlockSize - alloc_bytes_remaining_));
    }
    else {
        //memory_usage_=0;
        alloc_ptr_ = NULL;  // First allocation will allocate a block
        alloc_bytes_remaining_ = 0;
        map_start_ = map_end_ = 0;
        nvmarena_ = true;
        mfile = *filename;
    }
}

size_t Arena::getAllocRem() {
    return alloc_bytes_remaining_;
}

void* ArenaNVM::getMapStart() {
    return map_start_;
}

void* ArenaNVM:: operator new(size_t size)
{
#ifdef _USE_ARENA2_ALLOC
    return xxmalloc(size);
#else
    return malloc(size);
#endif
}

void* ArenaNVM::operator new[](size_t size) {
#ifdef _USE_ARENA2_ALLOC
    return xxmalloc(size);
#else
    return malloc(size);
#endif
}

ArenaNVM::~ArenaNVM() {
    for (size_t i = 0; i < blocks_.size(); i++) {
        assert(i <= 1);
        munmap(blocks_[i], kSize);
        //blocks_[i] = nullptr;
        DEBUG_T("have delete_ArenaNVM in ~ArenaNVM\n");
    }
    close(fd);
}

void ArenaNVM::operator delete(void* ptr)
{
#ifdef _USE_ARENA2_ALLOC
    xxfree(ptr);
#else
    free(ptr);
#endif
    ptr = NULL;
}


char* ArenaNVM::AllocateNVMBlock(size_t block_bytes) {
    //NoveLSM
    if(fd == -1)
        fd = open(mfile.c_str(), O_RDWR);
    
    if(fd == -1) {
        fd = open(mfile.c_str(), O_RDWR | O_CREAT, 0664);
        if (fd < 0)
            return NULL;
    }

    if(ftruncate(fd, MEM_THRESH * block_bytes) != 0){
        perror("ftruncate failed \n");
        return NULL;
    }

    char *result = (char *)mmap(NULL, MEM_THRESH * block_bytes, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

    /*if(result == NULL)
        DEBUG_T("after mmap, result is null\n");
    else 
        DEBUG_T("mmap success\n");*/
    kSize = MEM_THRESH * block_bytes;

    allocation = true;
    blocks_.push_back(result);
    assert(blocks_.size() <= 1);
    return result;
}

char* ArenaNVM::AllocateFallbackNVM(size_t bytes) {
    alloc_ptr_ = AllocateNVMBlock(kNVMBlockSize);
    map_start_ = (void *)alloc_ptr_;
    memory_usage_.NoBarrier_Store(
            reinterpret_cast<void*>(MemoryUsage() + bytes));
    alloc_bytes_remaining_ = kNVMBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* ArenaNVM::AllocateAlignedNVM(size_t bytes) {

    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;

    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
        memory_usage_.NoBarrier_Store(
                reinterpret_cast<void*>(MemoryUsage() + needed));
    } else {
        if(allocation){
            alloc_bytes_remaining_ = 0;
            result = alloc_ptr_ + slop;
            alloc_ptr_ += needed;
            memory_usage_.NoBarrier_Store(
                    reinterpret_cast<void*>(MemoryUsage() + needed));
        }
        else{
            result = this->AllocateFallbackNVM(bytes);
        }
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

//TODO: This method just implements virtual function
char* ArenaNVM::AllocateAligned(size_t bytes) {
    return NULL;
}

}  // namespace leveldb
