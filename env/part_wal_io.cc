#include "part_wal_io.h"

#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

extern bool PosixPositionedWrite(
  int fd, const char* buf, size_t nbyte, off_t offset);
static void PartWALWriterThread(PartWALWriter* writer) {
  IOOptions options; // Only for call PositionedAppend
  std::vector<PhysicalIO> *ios = nullptr;
  
  while (true) {
    std::unique_lock<std::mutex> lock(writer->m_mutex);
    while (!PartWALWriter::exit && !writer->m_ios) {
      writer->m_cv.wait(lock);
    }
    if (PartWALWriter::exit) {
        break;
    }
    ios = (std::vector<PhysicalIO> *)writer->m_ios;
    assert(ios != nullptr);
    for (auto& io : *(ios)) {
      io.s = (*io.file)->PositionedAppend(io.data, io.offset, options, nullptr);
      if (io.s != IOStatus::OK()) {
        break;
      }
    }
    //std::unique_lock<std::mutex> result_lock(writer->m_result_mutex);
    writer->m_ios = nullptr;
    writer->finished = true;
    //writer->m_result_cv.notify_one();
  }
}

std::atomic<bool> PartWALWriter::exit(false);
PartWALWriter::PartWALWriter() :
  m_ios(nullptr),
  m_cv(),
  m_mutex(),
  m_result_cv(),
  m_result_mutex(),
  finished(true),
  m_thr(PartWALWriterThread, this) {}
  

IOStatus PartWALWritableFile::PositionedAppend(const Slice &data,
                                               uint64_t offset,
					       const IOOptions &,
					       IODebugContext *) {
   typedef std::vector<PhysicalIO> PartIOS; 
   std::vector<PartIOS> part_ios(writers_num);
   size_t left = data.size();
   const char* ptr = data.data();
   while (left > 0) {
     uint32_t part_idx = (offset % (part_num * part_unit)) / part_unit;
     uint32_t part_off = (offset / (part_num * part_unit)) * part_unit
                         + offset % part_unit;
     uint32_t part_len = part_unit - offset % part_unit;
     if (part_len > left) {
       part_len = left;
     }
     uint32_t writer_idx = part_idx % writers_num;
     part_ios[writer_idx].push_back({part_off,Slice(ptr, part_len),
		                     &(part_files[part_idx]), IOStatus::IOError()}); 
     ptr += part_len;
     left -= part_len;
   }
   for (uint32_t idx = 0; idx != writers_num; ++idx) {
     if (part_ios[idx].empty()) {
       continue;
     }
     std::unique_lock<std::mutex> tl(writers[idx].m_mutex);
     writers[idx].m_ios = &part_ios[idx];
     writers[idx].finished = false;
     writers[idx].m_cv.notify_one();
   }

   for (uint32_t idx = 0; idx != writers_num; ++idx) {
     //{
     //  std::unique_lock<std::mutex> tl(writers[idx].m_result_mutex);
     //  while (!writers[idx].finished) {
     //    writers[idx].m_result_cv.wait(tl);
     //  }
     //}
     while (!writers[idx].finished) {}

     assert(writers[idx].finished);
     for (auto& io : part_ios[idx]) {
       if (io.s != IOStatus::OK()) {
         return io.s;
       }
     }
   }
   return IOStatus::OK();
}

//void InitPartWAL() {
//  PartWALWritableFile::part_num = 4u;
//  PartWALWritableFile::part_unit = (128u*1024);
//  PartWALWritableFile::writers_num = 4u;
//  //for (uint32_t idx = 0; idx != PartWALWritableFile::writers_num; ++idx) {
//  //  PartWALWritableFile::writers.emplace_back();
//  //}
//  //PartWALWritableFile::writers.resize(PartWALWritableFile::writers_num);
//}

uint32_t PartWALWritableFile::part_num = 4u;
uint32_t PartWALWritableFile::part_unit = (128u*1024);
uint32_t PartWALWritableFile::writers_num = 4u;
std::vector<PartWALWriter> PartWALWritableFile::writers(part_num);

PartWALWritableFile::PartWALWritableFile(const std::string& fname,
                                         int fd,
					 size_t logical_block_size,
					 const EnvOptions& options) :
  PosixWritableFile(fname, fd, logical_block_size, options),
  part_files(part_num) {
}

IOStatus PartWALWritableFile::NewPartWALWritableFile(uint32_t part_idx,
                                                     FileSystem* fs,
                                                     const std::string& fname,
                                                     const FileOptions& options) {
  
  return fs->NewWritableFile(fname, options, &(part_files[part_idx]), nullptr);
}

IOStatus NewPartWALWritableFile(FileSystem* fs, const std::string& fname,
                                std::unique_ptr<FSWritableFile>* result,
                                const FileOptions& options) {
  PartWALWritableFile* file =
	  new PartWALWritableFile(fname, -1, kDefaultPageSize, options);
  assert(file != nullptr);
  result->reset(file);
  IOStatus s;
  char fbuf[1024];
  for (uint32_t idx = 0; idx != PartWALWritableFile::part_num; ++idx) {
     snprintf(fbuf, sizeof(fbuf), "%s:%u", fname.data(), idx);
     s = file->NewPartWALWritableFile(idx, fs, fbuf, options);
     if (s != IOStatus::OK()) {
       result->reset();
       return s;
     }
  }
  return s;
}

}
