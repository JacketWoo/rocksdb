// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/disk2/wxf/run_rocksdb/rocksdb_simple_example";

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  fprintf(stderr, "Key: key1, value: %s\n", value.data());
  assert(s.ok() || s.IsNotFound());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "valuehhhhhhhhhhhhhhhh");
  assert(s.ok());

  sleep(100000);
  delete db;

  return 0;
}
