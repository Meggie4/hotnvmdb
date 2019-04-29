#!/bin/bash
export NOVELSMSRC=$PWD
export DBBENCH=$NOVELSMSRC/build
export TEST_TMPDIR=/mnt/ssd
export TEST_MEMDIR=/mnt/pmemdir
#DRAM buffer size in MB
export DRAMBUFFSZ=4
#NVM buffer size in MB
export NVMCHUNKSZ=32
