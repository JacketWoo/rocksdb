#pragma once

#include <condition_variable>
#include <mutex>
#include <condition_variable>
#include <vector>

#include "io_posix.h"

namespace ROCKSDB_NAMESPACE {

struct PhysicalIO {
  off_t offset;	
  Slice data;
  std::unique_ptr<FSWritableFile> *file;
  IOStatus s;
};

struct PartWALWriter {
  static std::atomic<bool> exit;
  PartWALWriter();
  virtual ~PartWALWriter();
  volatile std::vector<PhysicalIO>* m_ios;
  std::condition_variable m_cv;
  std::mutex m_mutex;

  std::condition_variable m_result_cv;
  std::mutex m_result_mutex;
  volatile bool finished; 

  std::thread m_thr;
};

class PartWALWritableFile : public PosixWritableFile {
 public:
  // the number of partitioned WAL files
  static uint32_t part_num;
  // switch next parttion file for advancing every part_unit bytes.
  static uint32_t part_unit;
  static uint32_t writers_num;
  static std::vector<PartWALWriter> writers;

  PartWALWritableFile(const std::string& fname,
                      int fd,
  		      size_t logical_block_size,
  		      const EnvOptions& options);

  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& opts,
                                   IODebugContext* dbg) override;
 IOStatus NewPartWALWritableFile(uint32_t part_idx,
                                 FileSystem* fs,
				 const std::string& fname,
				 const FileOptions& options);
 private:
  std::vector<std::unique_ptr<FSWritableFile>> part_files;
};

void InitPartWAL();
IOStatus NewPartWALWritableFile(FileSystem* fs, const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                const FileOptions& options);
}
