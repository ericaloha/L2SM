#!/bin/bash

# rm -rf /home/shunzi/l2sm/temp
# : > /home/shunzi/debug.log
# ./out-static/db_bench \
# --benchmarks='fill_Uniform' --use_existing_db=0 --num=1000000 --value_size=1008 \
# --read_ratio=0 --max_file_size=2097152 --write_buffer_size=67108864 --block_size=4096 \
# --cache_size=283115520 --bloom_bits=20 --open_files=1000 --db=/home/shunzi/l2sm/temp \
# --compression_ratio=1


# latest
# ./out-static/db_bench \
# --benchmarks='readAndzipK' --use_existing_db=1 --num=1000000 --value_size=1008 \
# --read_ratio=0 --max_file_size=2097152 --write_buffer_size=67108864 --block_size=4096 \
# --cache_size=283115520 --bloom_bits=20 --open_files=1000 --db=/home/shunzi/l2sm/temp \
# --compression_ratio=1 --reads=100000 --histogram=1

# echo "" > /home/shunzi/debug.log
# ./out-static/db_bench \
# --benchmarks='LoadAndRead' --use_existing_db=1 --ops=1000000 --num=1000000 --value_size=1008 \
# --read_ratio=5 --max_file_size=2097152 --write_buffer_size=67108864 --block_size=4096 \
# --cache_size=283115520 --bloom_bits=20 --open_files=1000 --db=/home/shunzi/l2sm/temp \
# --compression_ratio=1 --reads=100000 --histogram=1

rm -rf /home/shunzi/l2sm/test
cp -rf /home/shunzi/l2sm/temp /home/shunzi/l2sm/test
echo 1 > /proc/sys/vm/drop_caches
./out-static/db_bench \
--benchmarks='LoadAndRead' --use_existing_db=1 --ops=1000000 --num=1000000 --value_size=1008 \
--read_ratio=5 --max_file_size=2097152 --write_buffer_size=67108864 --block_size=4096 \
--cache_size=283115520 --bloom_bits=20 --open_files=1000 --db=/home/shunzi/l2sm/test \
--compression_ratio=1 --reads=100000 --histogram=1 --dist=zipf