#include "rocksdb/utilities/env_xdb.h"
#include <stdio.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include "cpprest/filestream.h"
#include "was/blob.h"
#include "was/common.h"
#include "was/queue.h"
#include "was/storage_account.h"
#include "was/table.h"

using namespace azure::storage;

namespace rocksdb {

const char* default_conn = "XDB_WAS_CONN";

Status err_to_status(int r) {
  switch (r) {
    case 0:
      return Status::OK();
    case -ENOENT:
      return Status::IOError();
    case -ENODATA:
    case -ENOTDIR:
      return Status::NotFound(Status::kNone);
    case -EINVAL:
      return Status::InvalidArgument(Status::kNone);
    case -EIO:
      return Status::IOError(Status::kNone);
    default:
      // FIXME :(
      assert(0 == "unrecognized error code");
      return Status::NotSupported(Status::kNone);
  }
}

class XdbWritableFile : public WritableFile {
 public:
  XdbWritableFile() {}

  ~XdbWritableFile() {}

  Status Append(const Slice& data) { return err_to_status(0); }

  Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) {
    return Status::NotSupported();
  }

  Status Truncate(uint64_t size) { return Status::NotSupported(); }

  Status Close() { return err_to_status(0); }

  Status Flush() { return err_to_status(0); }
};

class XdbSequentialFile : public SequentialFile {
 public:
  XdbSequentialFile() {}
  ~XdbSequentialFile() {}
  Status Read(size_t n, Slice* result, char* scratch) {
    return err_to_status(0);
  }
};

EnvXdb::EnvXdb(Env* env, const std::string& connect_string)
    //: EnvWrapper(Env::Default()) {
    : EnvWrapper(env) {
  printf("boot from EnvXdb\n");
}

Status EnvXdb::NewWritableFile(const std::string& fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  std::cout << "new write file:" << fname << std::endl;
  return EnvWrapper::NewWritableFile(fname, result, options);
}

Status EnvXdb::NewSequentialFile(const std::string& fname,
                                 std::unique_ptr<SequentialFile>* result,
                                 const EnvOptions& options) {
  std::cout << "new read file:" << fname << std::endl;
  return EnvWrapper::NewSequentialFile(fname, result, options);
}
Status EnvXdb::NewDirectory(const std::string& name,
                            unique_ptr<Directory>* result) {
  return EnvWrapper::NewDirectory(name, result);
}

Status EnvXdb::GetAbsolutePath(const std::string& db_path,
                               std::string* output_path) {
  return EnvWrapper::GetAbsolutePath(db_path, output_path);
}

Status EnvXdb::RenameFile(const std::string& src, const std::string& target) {
  return EnvWrapper::RenameFile(src, target);
}

EnvXdb* EnvXdb::Default(Env* env) {
  // static EnvXdb default_env(env, std::getenv(default_conn));
  static EnvXdb default_env(env, "acme");
  return &default_env;
}
}
