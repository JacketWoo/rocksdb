#include "part_wal_io.h"

#include "util/string_util.h"
#include <strings.h>

namespace ROCKSDB_NAMESPACE {

extern bool PosixPositionedWrite(
  int fd, const char* buf, size_t nbyte, off_t offset);


void static set_cpu_affinity() {
  static volatile int cpu_id = 0;
  int local_cpu_id = ++cpu_id;
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(local_cpu_id, &cpu_set);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set);
}

static void PartWALWriterThread(PartWALWriter* writer) {
  set_cpu_affinity();
  IOOptions options; // Only for call PositionedAppend
  std::vector<PhysicalIO> *ios = nullptr;
 
  char* wbuf_ptr = (char*)malloc(50*1024*1024);
  //char* wbuf = (char *)(((uint64_t)wbuf_ptr + 4*1024ul) & (~(uint64_t)(4*1024ul-1))); 
  while (true) {
    //std::unique_lock<std::mutex> lock(writer->m_mutex);
    //while (!PartWALWriter::exit && !writer->m_ios) {
    //  writer->m_cv.wait(lock);
    //}
    while (!PartWALWriter::exit && !writer->m_ios) {}
    if (PartWALWriter::exit) {
        break;
    }
    ios = (std::vector<PhysicalIO> *)writer->m_ios;
    assert(ios != nullptr);
#if 1
    for (auto& io : *(ios)) {
      io.s = (*io.file)->PositionedAppend(io.data, io.offset, options, nullptr);
      if (io.s != IOStatus::OK()) {
        break;
      }
    }
#else
    if (ios->size() > 1) {
      char* ptr = wbuf;
      uint32_t psize = 0;
      for (auto& io : *(ios)) {
        memcpy(ptr, io.data.data(), io.data.size());
        ptr += io.data.size();
        psize += io.data.size();
      }
      auto& fio = ios->front();
      IOStatus s = (*fio.file)->PositionedAppend(Slice(wbuf, psize), fio.offset, options, nullptr);
      for (auto& io : *(ios)) {
        io.s = s;
        if (s != IOStatus::OK()) {
          break;
        }
      }
    } else {
      assert(ios->size() == 1);
      auto& fio = ios->front();
      fio.s = (*fio.file)->PositionedAppend(fio.data, fio.offset, options, nullptr);
    }
#endif

    //std::unique_lock<std::mutex> result_lock(writer->m_result_mutex);
    writer->m_ios = nullptr;
    writer->finished = true;
    //writer->m_result_cv.notify_one();
  }
  free(wbuf_ptr);
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
 
PartWALWriter::~PartWALWriter() {
  if (!PartWALWriter::exit.load()) {
    PartWALWriter::exit.store(true);
  }
  m_thr.join();
} 

IOStatus PartWALWritableFile::PositionedAppend(const Slice &data,
                                               uint64_t offset,
					       const IOOptions &,
					       IODebugContext *) {
   typedef std::vector<PhysicalIO> PartIOS; 
   std::vector<PartIOS> part_ios(writers_num);
   size_t left = data.size();
   const char* ptr = data.data();
   //fprintf(stderr, "PartWALWritableFile::PositionedAppend io size: %lu, offset: %lu\n", data.size(), offset);
   uint32_t start_writer = writers_num; 
   while (left > 0) {
     uint32_t part_idx = (offset % (part_num * part_unit)) / part_unit;
     uint32_t part_off = (offset / (part_num * part_unit)) * part_unit
                         + offset % part_unit;
     uint32_t part_len = part_unit - offset % part_unit;
     if (part_len > left) {
       part_len = left;
     }
     uint32_t writer_idx = part_idx % writers_num;
     if (start_writer == writers_num) {
       start_writer = writer_idx;
     }
     part_ios[writer_idx].push_back({part_off,Slice(ptr, part_len),
		                     &(part_files[part_idx]), IOStatus::IOError()}); 
     ptr += part_len;
     left -= part_len;
     offset += part_len;
   }
   for (uint32_t i = 0; i != writers_num; ++i) {
     uint32_t idx = (start_writer + i) % writers_num;
     if (part_ios[idx].empty()) {
       break;
     }
     //std::unique_lock<std::mutex> tl(writers[idx].m_mutex);
     //writers[idx].m_ios = &part_ios[idx];
     //writers[idx].finished = false;
     //writers[idx].m_cv.notify_one();
     writers[idx].m_ios = &part_ios[idx];
     writers[idx].finished = false;
   }

   for (uint32_t i = 0; i != writers_num; ++i) {
     uint32_t idx = (start_writer + i) % writers_num;
     //{
     //  std::unique_lock<std::mutex> tl(writers[idx].m_result_mutex);
     //  while (!writers[idx].finished) {
     //    writers[idx].m_result_cv.wait(tl);
     //  }
     //}
     if (part_ios[idx].empty()) {
       break;
     }
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
uint32_t PartWALWritableFile::part_unit = (1024u*128);
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
