#pragma once
#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <iostream>
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

#include "util/io_posix.h"

namespace rocksdb {

  class EnvXdb : public EnvWrapper {
  public:

    explicit EnvXdb(Env* env, const std::string& connect_string);

    virtual Status NewWritableFile(const std::string& fname,
                                   unique_ptr<WritableFile>* result,
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

  };
}
