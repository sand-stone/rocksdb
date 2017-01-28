#include "rocksdb/utilities/env_xdb.h"
#include <iostream>

namespace rocksdb {

const char* default_conn = "XDB_WAS_CONN";

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
