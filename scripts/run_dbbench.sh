#!/bin/bash
#set -x
cat /dev/null > $NOVELSMSRC/mylog.txt

NUMTHREAD=1
#BENCHMARKS="customed99hot1k_100k,\
#customed80hot1k_100k,\
#customedworkloaduniform1k_100k,\
#customed99hot4k_100k,\
#customed80hot4k_100k,\
#customedworkloaduniform4k_100k,\
#customed99hot1k_500k,\
#customed80hot1k_500k,\
#customedworkloaduniform1k_500k,\
#customed99hot4k_500k,\
#customed80hot4k_500k,\
#customedworkloaduniform4k_500k"

BENCHMARKS="customed99hot1k_100k"
#BENCHMARKS="customed80hot1k_100k"
#BENCHMARKS="customedworkloaduniform1k_100k"
#BENCHMARKS="customed99hot4k_100k"
#BENCHMARKS="customed80hot4k_100k"
#BENCHMARKS="customedworkloaduniform4k_100k"
#BENCHMARKS="customed99hot1k_500k"
#BENCHMARKS="customed80hot1k_500k"
#BENCHMARKS="customedworkloaduniform1k_500k"
#BENCHMARKS="customed99hot4k_500k"
#BENCHMARKS="customed80hot4k_500k"
#BENCHMARKS="customedworkloaduniform4k_500k"

#NoveLSM specific parameters
#NoveLSM uses memtable levels, always set to num_levels 2
#write_buffer_size DRAM memtable size in MBs
#write_buffer_size_2 specifies NVM memtable size; set it in few GBs for perfomance;
OTHERPARAMS="--write_buffer_size=$DRAMBUFFSZ --nvm_index_size=$NVMINDEXSZ --nvm_log_size=$NVMLOGSZ"

valgrind --verbose --log-file=valgrind --leak-check=full  --show-leak-kinds=all $DBBENCH/db_bench --threads=$NUMTHREAD --benchmarks=$BENCHMARKS $OTHERPARAMS
#$DBBENCH/db_bench --threads=$NUMTHREAD --benchmarks=$BENCHMARKS $OTHERPARAMS

#Run all benchmarks
#$APP_PRE#FIX $DBBENCH/db_bench --threads=$NUMTHREAD --num=$NUMKEYS --value_size=$VALUSESZ \
#$OTHERPARAMS --num_read_threads=$NUMREADTHREADS

