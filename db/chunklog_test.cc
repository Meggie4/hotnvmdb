/////unit test for chunklog.cc 

#include "db/chunklog.h"
#include <string>
#include <stdio.h>
#include <iostream>
#include "leveldb/env.h"
#include "db/filename.h"
#include "util/testharness.h"

namespace leveldb{
 
    class chunkLogTest {  };
    TEST(chunkLogTest, Insert) {
        Env* env = Env::Default();
        std::string nvm_path;
        env->GetMEMDirectory(&nvm_path);
        uint64_t chunklog_number = 1;
        std::string chunklogfile = chunkLogFileName(nvm_path, chunklog_number);
        std::cout<<"chunklogfile: "<<chunklogfile<<std::endl;

        size_t filesize = (4 << 10)<< 10;
        chunkLog ckg(&chunklogfile, filesize, false);

        fprintf(stderr, "map_start:%p\n", ckg.get_map_start());
        fprintf(stderr, "current_ptr:%p\n", ckg.get_current_ptr());
        fprintf(stderr, "bytes_remaining:%zu\n", ckg.get_bytes_remaining());

        char* kvitem = "my name is Meggie";
        size_t len = strlen(kvitem);
        void* offset = ckg.insert(kvitem, len);
        fprintf(stderr, "len:%zu, after insert, bytes_remaining:%zu, log_current_ptr:%p\n", 
                len, ckg.get_bytes_remaining(), ckg.get_current_ptr());
        
        std::cout<<"NVMUsage: "<<ckg.NVMUsage()<<", remaining: "<<ckg.get_bytes_remaining()<<std::endl;
        char* result = ckg.getKV(offset);
        fprintf(stderr, "get_result:%s\n", result);

    }

    /*TEST(chunkLogTest, GetKey) {

    }*/


}
int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
