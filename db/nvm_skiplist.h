#ifndef STORAGE_LEVELDB_DB_NVMSKIPLIST_H_
#define STORAGE_LEVELDB_DB_NVMSKIPLIST_H_

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...
#include <string>
#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"
#include "port/cache_flush.h"
#include "db/chunklog.h"
#include "util/debug.h"


namespace leveldb {

class Arena;

template<typename Key, class Comparator>
class NVMSkipList {

private:
    struct Node;

public:
    // Create a new SkipList object that will use "cmp" for comparing keys,
    // and will allocate memory using "*arena".  Objects allocated in the arena
    // must remain allocated for the lifetime of the skiplist object.
    NVMSkipList(Comparator cmp, ArenaNVM* arena, 
            chunkLog* cklog, bool recovery = false);

    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    
    ///(meggie)move to ptrivate
    //void Insert(const Key& key, uint64_t s, const Key& key_offset, void* node);

    ///////////////meggie
    void Add(const Key& nvmkey, const Key& key_offset, uint64_t s=0);
    ///////////////meggie

    // Returns true iff an entry that compares equal to key is in the list.
    bool Contains(const Key& key, Node** node, Node** prev) const;

    /////////////////////////meggie
    bool Contains(const Key& key) const;
    size_t GetNodeNum() const { return *node_num_; }
    /////////////////////////meggie

    void SetHead(void *ptr);

    // Iteration over the contents of a skip list
    class Iterator {
    public:
        // Initialize an iterator over the specified list.
        // The returned iterator is not valid.
        explicit Iterator(const NVMSkipList* list, const chunkLog* cklog);

        // Returns true iff the iterator is positioned at a valid node.
        bool Valid() const;

        // Returns the key at the current position.
        // REQUIRES: Valid()

        const Key& key_offset() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev();

        // Advance to the first entry with a key >= target
        void Seek(const Key& target);

        // Position at the first entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToFirst();

        // Position at the last entry in list.
        // Final state of iterator is Valid() iff list is not empty.
        void SeekToLast();

        //void SetHead(void *ptr);
        Node* node_;

    private:
        const NVMSkipList* list_;
        const chunkLog* cklog_;
        // Intentionally copyable
    };

private:
    enum { kMaxHeight = 12 };

    // Immutable after construction
    Comparator const compare_;
    ArenaNVM* const arena_;    // Arena used for allocations of nodes
    //////////meggie
    chunkLog* cklog_;
    size_t* node_num_;
    /////////meggie 

    //TODO: NoveLSM Make them private again
    //void* head_offset_;   // Head offset from map_start
    //Node* head_;

    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
    port::AtomicPointer max_height_;   // Height of the entire list

    inline int GetMaxHeight() const {
        return static_cast<int>(
                reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
    }

    // Read/written only by Insert().
    Random rnd_;

    Node* NewNode(const Key& key, int height, bool head_alloc);
    int RandomHeight();
    bool Equal(const Key& a, const Key& b) const { 
        return (compare_.user_compare(a, b) == 0); 
    }

    // Return true if key is greater than the data stored in "n"
    bool KeyIsAfterNode(const Key& key, Node* n) const;

    // Return the earliest node that comes at or after key.
    // Return NULL if there is no such node.
    //
    // If prev is non-NULL, fills prev[level] with pointer to previous
    // node at "level" for every level in [0..max_height_-1].
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    // Return the latest node with a key < key.
    // Return head_ if there is no such node.
    Node* FindLessThan(const Key& key) const;

    // Return the last node in the list.
    // Return head_ if list is empty.
    Node* FindLast() const;

    /////////////////meggie
    void Insert(const Key& key, uint64_t s, const Key& key_offset, Node* node, Node** prev);
    /////////////////meggie

    // No copying allowed
    NVMSkipList(const NVMSkipList&);
    void operator=(const NVMSkipList&);

public:
    //TODO: NoveLSM Make them private again
    void* head_offset_;   // Head offset from map_start
    Node* head_;
    size_t* alloc_rem;
    uint64_t *sequence;
    int* m_height;
};

// Implementation details follow
template<typename Key, class Comparator>
struct NVMSkipList<Key,Comparator>::Node {
    explicit Node(const Key& k) : key_offset(k){ }
    Key key_offset;
    // Accessors/mutators for links.  Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    Node* Next(int n) {
        assert(n >= 0);
        intptr_t offset = reinterpret_cast<intptr_t>( next_[n].Acquire_Load());
        return (offset != 0) ? reinterpret_cast<Node *>((intptr_t)this - offset) : NULL;
    }
    void SetNext(int n, Node* x) {
        assert(n >= 0);
        (x != NULL) ? next_[n].Release_Store(reinterpret_cast<void*>((intptr_t)this - (intptr_t)x)) : 
            next_[n].Release_Store(reinterpret_cast<void*>(0));
    }

    // No-barrier variants that can be safely used in a few locations.
    Node* NoBarrier_Next(int n) {
        assert(n >= 0);
        intptr_t offset = reinterpret_cast<intptr_t>(next_[n].NoBarrier_Load());
        return (offset != 0) ? reinterpret_cast<Node *>((intptr_t)this - offset) : NULL;
    }
    void NoBarrier_SetNext(int n, Node* x) {
        assert(n >= 0);
        (x != NULL) ? next_[n].NoBarrier_Store( reinterpret_cast<void*> ((intptr_t)this - (intptr_t)x)) : 
            next_[n].NoBarrier_Store( reinterpret_cast<void*> (0));
    }

private:
    // Array of length equal to the node height.  next_[0] is lowest level link.
    port::AtomicPointer next_[1];
};

    template<typename Key, class Comparator>
    typename NVMSkipList<Key,Comparator>::Node*
    NVMSkipList<Key,Comparator>::NewNode(const Key& key, int height, bool head_alloc) {
        char* mem;
        ArenaNVM *arena_nvm = (ArenaNVM*) arena_;
        if (head_alloc == true){
            mem = arena_nvm->AllocateAlignedNVM(
                    sizeof(size_t) + sizeof (uint64_t) + sizeof(int) + sizeof(size_t) + sizeof(Node) + 
                    sizeof(port::AtomicPointer) * (height - 1));
            //DEBUG_T("aligned:%lu\n", mem);
            char *offset_mem = mem + sizeof(size_t) + sizeof (uint64_t) + sizeof(int) + sizeof(size_t);
            return new (offset_mem) Node(key);
        }
        else{
            mem = arena_->AllocateAlignedNVM(
                    sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
            return new (mem) Node(key);
        }
    }

    template<typename Key, class Comparator>
    inline NVMSkipList<Key,Comparator>::Iterator::Iterator(const NVMSkipList* list, const chunkLog* cklog) {
        list_ = list;
        cklog_ = cklog;
        node_ = NULL;
    }

    template<typename Key, class Comparator>
    inline bool NVMSkipList<Key,Comparator>::Iterator::Valid() const {
        if(node_ == nullptr)
            ;//DEBUG_T("node_ is nullptr\n");
        return node_ != NULL;
    }

    template<typename Key, class Comparator>
    inline const Key& NVMSkipList<Key,Comparator>::Iterator::key_offset() const {
        assert(Valid());
        return node_->key_offset;
    }

    template<typename Key, class Comparator>
    inline void NVMSkipList<Key,Comparator>::Iterator::Next() {
        assert(Valid());
        node_ = node_->Next(0);
    }

    template<typename Key, class Comparator>
    inline void NVMSkipList<Key,Comparator>::Iterator::Prev() {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        assert(Valid());
        node_ = list_->FindLessThan(reinterpret_cast<Key>(cklog_->getKV(node_->key_offset)));
        if (node_ == list_->head_) {
            node_ = NULL;
        }
    }

    /*template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SetHead(void *ptr) {
  //list_->head_= (Node *)ptr;
}*/

    template<typename Key, class Comparator>
    inline void NVMSkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
        node_ = list_->FindGreaterOrEqual(target, NULL);
        if(node_ == NULL){
            ;//DEBUG_T("node is null\n");
        }
    }

    template<typename Key, class Comparator>
    inline void NVMSkipList<Key,Comparator>::Iterator::SeekToFirst() {
        node_ = list_->head_->Next(0);
    }

    template<typename Key, class Comparator>
    inline void NVMSkipList<Key,Comparator>::Iterator::SeekToLast() {
        node_ = list_->FindLast();
        if (node_ == list_->head_) {
            node_ = NULL;
        }
    }

    template<typename Key, class Comparator>
    int NVMSkipList<Key,Comparator>::RandomHeight() {
        // Increase height with probability 1 in kBranching
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
            height++;
        }
        assert(height > 0);
        assert(height <= kMaxHeight);
        return height;
    }

    template<typename Key, class Comparator>
    bool NVMSkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
        // NULL n is considered infinite
        if(n == NULL)
            return false;
        Key lkey = reinterpret_cast<Key>(cklog_->getKV(n->key_offset));
        /*if(compare_.user_compare(lkey, key) ==0){
            DEBUG_T("KeyIsAfterNode, equal\n");
            if(compare_(lkey, key) < 0){
                DEBUG_T("compare_, <\n");
            }
            else
                DEBUG_T("compare_, >\n");
        }
        else 
            DEBUG_T("KeyIsAfterNode, not equal\n");*/
        //////////////meggie
        return (n != NULL) && (compare_.user_compare(lkey, key) < 0);
        //////////////meggie
    }

    template<typename Key, class Comparator>
    typename NVMSkipList<Key,Comparator>::Node* NVMSkipList<Key,Comparator>::FindGreaterOrEqual(
            const Key& key, Node** prev)const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            Node* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) {
                // Keep searching in this list
                x = next;
            } else {
                if (prev != NULL) prev[level] = x;
                if (level == 0) {
                    return next;
                } else {
                    // Switch to next list
                    level--;
                }
            }
        }
    }

    template<typename Key, class Comparator>
    typename NVMSkipList<Key,Comparator>::Node*
    NVMSkipList<Key,Comparator>::FindLessThan(const Key& key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
            assert(x == head_ || compare_(reinterpret_cast<Key>(cklog_->getKV(x->key_offset)), key) < 0);
            Node* next = x->Next(level);
            if (next == NULL || compare_(reinterpret_cast<Key>(cklog_->getKV(next->key_offset)), key) >= 0) {
                    if (level == 0) {
                        return x;
                    } else {
                        // Switch to next list
                        level--;
                    }
            } else {
                    x = next;
            }
        }
    }

        template<typename Key, class Comparator>
        typename NVMSkipList<Key,Comparator>::Node* NVMSkipList<Key,Comparator>::FindLast()
        const {
            Node* x = head_;
            int level = GetMaxHeight() - 1;
            while (true) {
                Node* next = x->Next(level);
                if (next == NULL) {
                    if (level == 0) {
                        return x;
                    } else {
                        // Switch to next list
                        level--;
                    }
                } else {
                    x = next;
                }
            }
        }

        template<typename Key, class Comparator>
        NVMSkipList<Key,Comparator>::NVMSkipList(Comparator cmp, ArenaNVM* arena, 
                chunkLog* cklog, bool recovery)
        : compare_(cmp),
          arena_(arena),
          //head_(NewNode(0 /* any key will do */, kMaxHeight)),
          cklog_(cklog),
          max_height_(reinterpret_cast<void*>(1)),
          rnd_(0xdeadbeef),
          node_num_(0){
            //DEBUG_T("\nbefore NVMSkipList, arena_usage:%zu\n", arena_->MemoryUsage());
            if (recovery) {
                ArenaNVM *arena_nvm = (ArenaNVM*) arena_;
                head_ = (Node*)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + 
                        sizeof(uint64_t) + sizeof(int) + sizeof(size_t));
                
                void* head_offset = (reinterpret_cast<void*>(arena_nvm->CalculateOffset(
                                static_cast<void*>(head_))));
                //DEBUG_T("recover, head_offset:%p\n", head_offset);
                alloc_rem = (size_t *)arena_nvm->getMapStart();
                sequence = (uint64_t *)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t));
                m_height = (int *)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + sizeof(uint64_t));
                node_num_ = (size_t*)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + 
                        sizeof(uint64_t) + sizeof(int));
                max_height_.NoBarrier_Store(reinterpret_cast<void*>(*m_height));
            }
            else{
                ArenaNVM *arena_nvm = (ArenaNVM*) arena_;
                head_ = NewNode(0, kMaxHeight, true);
                void* head_offset = (reinterpret_cast<void*>(arena_nvm->CalculateOffset(
                                static_cast<void*>(head_))));
                //DEBUG_T("not recover, head_offset:%p\n", head_offset);
           
                Node* tmp_head = (Node*)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + 
                        sizeof(uint64_t) + sizeof(int) + sizeof(size_t));
                //DEBUG_T("getMapStart:%lu, tmp_head:%lu\n", arena_nvm->getMapStart(), tmp_head);

                head_offset = (reinterpret_cast<void*>(arena_nvm->CalculateOffset(
                                static_cast<void*>(tmp_head))));
                //DEBUG_T("guess, head_offset:%p\n", head_offset);
            }

            if (!recovery) {
                ArenaNVM *arena_nvm = (ArenaNVM*) arena;
                //DEBUG_T("arena_map_start:%p\n", arena_nvm->getMapStart());
                alloc_rem = (size_t *)arena_nvm->getMapStart();
                *alloc_rem = arena_->getAllocRem();
                flush_cache(alloc_rem, CACHE_LINE_SIZE);

                sequence = (uint64_t *)((uint8_t*)arena_->getMapStart() + sizeof(size_t));
                *sequence = 0;
                flush_cache(sequence, CACHE_LINE_SIZE);

                m_height = (int *)((uint8_t*)arena_->getMapStart() + sizeof(size_t) + sizeof(uint64_t));
                *m_height = GetMaxHeight();
		        flush_cache(m_height, CACHE_LINE_SIZE);
                
                node_num_ = (size_t*)((uint8_t*)arena_nvm->getMapStart() + sizeof(size_t) + 
                        sizeof(uint64_t) + sizeof(int));
                *node_num_ = 0;
                flush_cache(node_num_, CACHE_LINE_SIZE);
            }

            //NoveLSM: We find the offset from the starting address
            head_offset_ = (reinterpret_cast<void*>(arena_->CalculateOffset(static_cast<void*>(head_))));
            //head_offset_ = (size_t)(arena_->CalculateOffset(static_cast<void*>(head_)));
           
            if (!recovery) {
                for (int i = 0; i < kMaxHeight; i++) {
                    head_->SetNext(i, NULL);
                }
            }
            //DEBUG_T("after NVMSkipList, arena_usage:%zu\n", arena_->MemoryUsage());
        }

            template<typename Key, class Comparator>
            void NVMSkipList<Key,Comparator>::Insert(const Key& key, uint64_t s, 
                    const Key& key_offset, Node* x, Node** prev) {
                // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
                // here since Insert() is externally synchronized.

                // Update sequence number before updating data
                //DEBUG_T("before newnode, arena_usage:%zu\n", arena_->MemoryUsage());
                *sequence = s;

                // Our data structure does not allow duplicate insertion
                //assert(x == NULL || !Equal(key, reinterpret_cast<Key>(cklog_->getKV(x->key_offset))));

                int height = RandomHeight();
                if (height > GetMaxHeight()) {
                    for (int i = GetMaxHeight(); i < height; i++) {
                        prev[i] = head_;
                    }
                    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

                    // It is ok to mutate max_height_ without any synchronization
                    // with concurrent readers.  A concurrent reader that observes
                    // the new value of max_height_ will see either the old value of
                    // new level pointers from head_ (NULL), or a new value set in
                    // the loop below.  In the former case the reader will
                    // immediately drop to the next level since NULL sorts after all
                    // keys.  In the latter case the reader will use the new node.
                    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
                }
                
                ////////////////meggie
                //DEBUG_T("before new node\n");
                x = NewNode(key_offset, height, false);
                ///////meggie
                //DEBUG_T("before node num++\n");
                (*node_num_)++;
                //DEBUG_T("after node num++\n");
                ///////meggie
                ///////////////meggie
                for (int i = 0; i < height; i++) {
                    // NoBarrier_SetNext() suffices since we will add a barrier when
                    // we publish a pointer to "x" in prev[i].
                    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
                    prev[i]->SetNext(i, x);
                    flush_cache((void *)x, sizeof(Node));
                    flush_cache((void *)prev[i], sizeof(Node));
                }
                *alloc_rem = arena_->getAllocRem();
                flush_cache((void *)alloc_rem, CACHE_LINE_SIZE);
                // Set max_height after insertion to ensure correctness.
                // If NovelSM crashes before updating this, it would just
                // lead to inefficient lookups (O(n) vs O(logn)).
                *m_height = GetMaxHeight();
                //DEBUG_T("after newnode, arena_usage:%zu\n", arena_->MemoryUsage());
            }

            ///////////////meggie
            template<typename Key, class Comparator>
            void NVMSkipList<Key, Comparator>::Add(const Key& nvmkey, const Key& key_offset, uint64_t s){
                Node* x;
                Node* prev[kMaxHeight]; 
                if(Contains(nvmkey, &x, prev)){
                    //DEBUG_T("it's update\n");
                    x->key_offset = key_offset;
                }
                else{
                    //DEBUG_T("it's insert\n");
                    Insert(nvmkey, s, key_offset, x, prev); 
                }
            }
            template<typename Key, class Comparator>
            bool NVMSkipList<Key,Comparator>::Contains(const Key& key) const {
                Node* x = FindGreaterOrEqual(key, NULL);
                if (x != NULL && Equal(key, reinterpret_cast<Key>(cklog_->getKV(x->key_offset)))) {
                        return true;
                } else {
                        return false;
                }
            }
            template<typename Key, class Comparator>
            bool NVMSkipList<Key,Comparator>::Contains(const Key& key, Node** node, Node** prev) const {
                //DEBUG_T("nodenum:%d\n", *node_num_);
                Node* x = FindGreaterOrEqual(key, prev);
                //DEBUG_T("after FindGreaterOrEqual\n");
                if (x != NULL && Equal(key, reinterpret_cast<Key>(cklog_->getKV(x->key_offset)))) {
                        //DEBUG_T("contains\n");
                        *node = x;
                        //DEBUG_T("after contains\n");
                        return true;
                } else {
                        //DEBUG_T("not contains\n");
                        /*if(x == NULL)
                            DEBUG_T("x==null\n");
                        else 
                            DEBUG_T("x!=null\n");*/
                        return false;
                }
            }
            ///////////////meggie
            template<typename Key, class Comparator>
            void NVMSkipList<Key,Comparator>::SetHead(void *ptr){
                head_ = reinterpret_cast<Node *>(ptr);
                head_offset_ = (reinterpret_cast<void*>(arena_->CalculateOffset(static_cast<void*>(head_))));
            }

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_


