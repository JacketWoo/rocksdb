mkdir -p build
cd build; cmake ../ -DCMAKE_INSTALL_PREFIX=/disk2/wxf/rocksdb/install -DGFLAGS_INCLUDE_DIR:PATH=/disk2/wxf/gflags/build/include; make install -j64
