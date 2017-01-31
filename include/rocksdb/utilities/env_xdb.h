#pragma once
#include <stdio.h>
#include <time.h>
#include <algorithm>
#include <iostream>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include "util/io_posix.h"
#include "was/blob.h"
#include "was/common.h"
#include "was/queue.h"
#include "was/storage_account.h"
#include "was/table.h"

namespace rocksdb {

class XdbWritableFile;

class EnvXdb : public EnvWrapper {
  friend class XdbWritableFile;

 public:
  explicit EnvXdb(Env* env);

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override;

  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) override;

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual ~EnvXdb() {}

  static EnvXdb* Default(Env* env);

 private:
  azure::storage::cloud_blob_client _blob_client;
  azure::storage::cloud_blob_container _container;
};
}
