/*************************************************************************
	> File Name: util/multi_bloomfilter_test.cc
	> Author: Meggie
	> Mail: 1224642332@qq.com 
	> Created Time: Thu 28 Feb 2019 03:10:14 PM CST
 ************************************************************************/
#include "util/testharness.h"
#include <iostream>
#include <stdio.h>
#include "util/multi_bloomfilter.h"

namespace leveldb{

    class MultiBloomFilterTest{  };
    TEST(MultiBloomFilterTest, Indentify){
        MultiHotBloomFilter mbf(4, 2, 4, 8, 2, 32);
        int key_num = 100;
        for(int i = 1; i <= key_num; i++){
            int k;
            char key[100];
            if((i % 4) == 0){
                k = 4;
            }
            else if((i % 3) == 0){
                k = 3;
            }
            else 
                k = i;
            snprintf(key, sizeof(key), "%16d", k);
            mbf.AddKey(Slice(key, strlen(key)));
        }
        for(int i = 1; i <= key_num; i++){
            int k;
            char key[100];
            if((i % 4) == 0){
                k = 4;
            }
            else if((i % 3) == 0){
                k = 3;
            }
            else 
                k = i;
            snprintf(key, sizeof(key), "%16d", k);
            if(mbf.CheckHot(Slice(key, strlen(key))))
                printf("%s is hot\n", key);
        }
    }

}
int main(int argc, char** argv) {
    return leveldb::test::RunAllTests();
}
