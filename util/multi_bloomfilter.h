/*************************************************************************
	> File Name: util/multi_bloomfilter.h
	> Author: Meggie
	> Mail: 1224642332@qq.com 
	> Created Time: Wed 27 Feb 2019 05:31:44 PM CST
 ************************************************************************/
#ifndef LEVELDB_MULTI_BLOOMFILTER_H
#define LEVELDB_MULTI_BLOOMFILTER_H

#include "util/hash.h"
#include "leveldb/slice.h"
#include <array>

namespace leveldb{

    class chunkTable;
    class BitBloomFilter{
        public:
            BitBloomFilter(int hash_num, size_t bit_size);
            ~BitBloomFilter();
            void Add(const Slice& key);
            bool Query(const Slice& key);
            void Reset();
            void DisplayFilter();
        private:
            friend class chunkTable;
            static uint32_t BitBloomHash(const Slice& key);
            static std::array<uint64_t, 2> BitMurHash(const Slice& key);
            inline uint64_t nthHash(uint8_t n, uint64_t hashA,
                    uint64_t hashB, uint64_t bit_size){
                return (hashA + n * hashB) % bit_size;
            }
            char* bits_;
            int hash_num_;
            size_t bit_size_;
            size_t bytes_;
    };
    class MultiHotBloomFilter{
        public:
            MultiHotBloomFilter(int bf_num = 4, double max_weight = 2, 
                    double hot_thresh = 2, int decay_window = 9216, 
                    int hash_num = 2, int bit_size_per_bf = 36864);
            ~MultiHotBloomFilter();
            void AddKey(const Slice& user_key);
            bool CheckHot(const Slice& user_key);
            size_t GetReqNum(){return req_num_;}
            
        private:
            void DecayBF();
            double CountWeight(const Slice& user_key);
            int bf_num_;
            double max_weight_;
            double hot_thresh_;
            int decay_window_;
            BitBloomFilter** bloom_filters_;
            int current_filter_index_;
            int decay_filter_index_;
            int* weights_;
            size_t req_num_;
    };
}//namespace leveldb

#endif
