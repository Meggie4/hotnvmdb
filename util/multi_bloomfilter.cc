/*************************************************************************
	> File Name: util/multi_bloomfilter.cc
	> Author: Meggie
	> Mail: 1224642332@qq.com 
	> Created Time: Wed 27 Feb 2019 08:16:52 PM CST
 ************************************************************************/
#include "util/multi_bloomfilter.h"
#include "util/MurmurHash3.h"
#include "util/debug.h"
#include <assert.h>

namespace leveldb{
    BitBloomFilter::BitBloomFilter(int hash_num, size_t bit_size)
    :hash_num_(hash_num){
        //byte align 
        bytes_ = (bit_size + 7) / 8;
        bit_size_ = bytes_ * 8;
        bits_ = new char[bytes_];
        for(int i = 0; i < bytes_; i++)
            bits_[i] &= 0x00;
    }
    
    BitBloomFilter::~BitBloomFilter(){
        delete []bits_;
    }
    
    uint32_t BitBloomFilter::BitBloomHash(const Slice& key){
        return Hash(key.data(), key.size(), 0xbc9f1d34);
    }

    std::array<uint64_t, 2> BitBloomFilter::BitMurHash(const Slice& key){
        std::array<uint64_t, 2> hashValue;
        MurmurHash3_x64_128(key.data(), key.size(), 0, hashValue.data());
        return hashValue;
    }
   
    /*
    void BitBloomFilter::Add(const Slice& key){
        uint32_t h = BitBloomFilter::BitBloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);
        //DEBUG_T("user_key:%s:", key.ToString().c_str());
        for(int j = 0; j < hash_num_; j++){
            const uint32_t bitpos = h % bit_size_;
            //DEBUG_T("bitpos%d:%d, ", j, bitpos);
            bits_[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
        //DEBUG_T("\n");
    }

    bool BitBloomFilter::Query(const Slice& key){
        uint32_t h = BitBloomFilter::BitBloomHash(key);
        const uint32_t delta = (h >> 17) | (h << 15);
        DEBUG_T("\n\nuser_key:%s:", key.ToString().c_str());
        for(int j = 0; j < hash_num_; j++){
            const uint32_t bitpos = h % bit_size_;
            DEBUG_T("bits_[bitpos/8]:%d, bitpos:%d; ", bits_[bitpos / 8], bitpos);
            if((bits_[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
                //DEBUG_T("false\n");
                return false;
            }
            h += delta;
        }
        return true;
    }
    */
    void BitBloomFilter::Add(const Slice& key){
        auto hashValues = BitBloomFilter::BitMurHash(key);
        //DEBUG_T("user_key:%s:", key.ToString().c_str());
        for(int j = 0; j < hash_num_; j++){
            const uint64_t bitpos = nthHash(j, hashValues[0], hashValues[1], bit_size_);
            //DEBUG_T("bitpos%d:%d, ", j, bitpos);
            bits_[bitpos / 8] |= (1 << (bitpos % 8));
        }
        //DEBUG_T("\n");
    }

    bool BitBloomFilter::Query(const Slice& key){
        auto hashValues = BitBloomFilter::BitMurHash(key);
        //DEBUG_T("\n\nuser_key:%s:", key.ToString().c_str());
        for(int j = 0; j < hash_num_; j++){
            const uint64_t bitpos = nthHash(j, hashValues[0], hashValues[1], bit_size_);
            //DEBUG_T("bits_[bitpos/8]:%d, bitpos:%d; ", bits_[bitpos / 8], bitpos);
            if((bits_[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
                //DEBUG_T("false\n");
                return false;
            }
        }
        return true;
    }

    void BitBloomFilter::Reset(){
        for(int i = 0; i < (bit_size_ / 8); i++){
            bits_[i] &= 0x00;
        }
    } 

    void BitBloomFilter::DisplayFilter(){
        DEBUG_T("byte_num:%d\n", bit_size_ / 8);
        for(int i = 0; i < (bit_size_ / 8); i++){
            DEBUG_T("byte%d:%d, ", i, bits_[i]);
        }
        DEBUG_T("\n");
    }
    
    MultiHotBloomFilter::MultiHotBloomFilter(int bf_num, double max_weight, 
            double hot_thresh, int decay_window, 
            int hash_num, int bit_size_per_bf)
        : bf_num_(bf_num),
        max_weight_(max_weight),
        hot_thresh_(hot_thresh),
        decay_window_(decay_window),
        current_filter_index_(0),
        decay_filter_index_(0),
        req_num_(0){
           bloom_filters_ = new BitBloomFilter* [bf_num_];
           weights_ = new int[bf_num_];
           for(int i = 0; i < bf_num_; i++){
               bloom_filters_[i] = new BitBloomFilter(hash_num, bit_size_per_bf);
               weights_[i] = i + 1;
           }
    }
    MultiHotBloomFilter::~MultiHotBloomFilter(){
        for(int i = 0; i < bf_num_; i++)
            delete bloom_filters_[i];
        delete []bloom_filters_;
        delete []weights_;
    }

    void MultiHotBloomFilter::DecayBF(){
        //reset weights
        for(int i = 0; i < bf_num_; i++){
            weights_[i] -= 1;
            if(weights_[i] == 0){
                assert(decay_filter_index_ == i);
                weights_[i] = bf_num_;
            }
        }
        //reset decaybf 
        bloom_filters_[decay_filter_index_]->Reset();
        //set next decaybf , circle right move
        decay_filter_index_ = (decay_filter_index_ + 1 + bf_num_) % bf_num_;
    }
 
    double MultiHotBloomFilter::CountWeight(const Slice& user_key){
        double keyWeight = 0;
        for(int i = 0; i < bf_num_; i++){
            if(bloom_filters_[i]->Query(user_key))
                keyWeight += weights_[i];
        }
        //DEBUG_T("user_key:%s\n: keyWeight:%f, result_weight:%f\n",
          //      user_key.ToString().c_str(), keyWeight, (keyWeight * max_weight_) / bf_num_);
        return (keyWeight * max_weight_) / bf_num_;
    }

    void MultiHotBloomFilter::AddKey(const Slice& user_key){
        req_num_++;
        if(req_num_ % decay_window_ == 0)
            DecayBF();
        if(bloom_filters_[current_filter_index_]->Query(user_key)){
            //DEBUG_T("after query\n");
            int next_filter_index = (current_filter_index_ + 1 + bf_num_) % bf_num_;
            while(next_filter_index != current_filter_index_){
                if(bloom_filters_[next_filter_index]->Query(user_key))
                    next_filter_index = (next_filter_index + 1 + bf_num_) % bf_num_;
                else{
                    //DEBUG_T("to_add_key1\n");
                    bloom_filters_[next_filter_index]->Add(user_key);
                    break;
                } 
            }
        }
        else{ 
            //DEBUG_T("to_add_key2\n");
            bloom_filters_[current_filter_index_]->Add(user_key);
        }
        current_filter_index_ = (current_filter_index_ + 1 + bf_num_) % bf_num_;
    }

    bool MultiHotBloomFilter::CheckHot(const Slice& user_key){
        //DEBUG_T("user_key:%s,CountWeight:%f\n", user_key.ToString().c_str(),
          //      CountWeight(user_key));
        if(CountWeight(user_key) >= hot_thresh_)
             return true;
        else 
             return false; 
    }
}

