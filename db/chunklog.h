/*************************************************************************
	> File Name: chunklog.h
	> Author: Meggie
	> Mail: 1224642332@qq.com 
	> Created Time: Tue 22 Jan 2019 02:15:38 PM CST
 ************************************************************************/
#ifndef STORAGE_LEVELDB_DB_CHUNKLOG_H_
#define STORAGE_LEVELDB_DB_CHUNKLOG_H_

#include <string>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"
#include "leveldb/slice.h"

#define LOG_THRESH 1.5
namespace leveldb{
class chunkLog{
    public:
        chunkLog(std::string* logfile, size_t logfile_size, bool recovery);
        ~chunkLog();
        void* get_map_start(){return log_map_start_;}
        ///(TODO)offset
        void* get_current_ptr(){return log_current_ptr_;}
        size_t NVMUsage() const {return reinterpret_cast<uintptr_t>(nvm_usage_.NoBarrier_Load());}
        size_t get_bytes_remaining(){return log_bytes_remaining_;}
        void* insert(const char* kvitem, size_t len);
        char* getKV(const void* kvpos_offset) const;
        const Slice getUserKey(const void* kvpos_offset) const;
        const Slice getValue(const void* kvpos_offset) const;
    private:
        void* log_map_start_;
        char* log_current_ptr_;
        int fd;
        size_t filesize_;
        port::AtomicPointer nvm_usage_;
        size_t log_bytes_remaining_;
        chunkLog(const chunkLog&);
        void operator=(const chunkLog&);
};
}
#endif
