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
const char* default_container = "XDB_WAS_CONTAINER";
const char* was_store = "was";

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
  XdbWritableFile(cloud_page_blob& page_blob) : _page_blob(page_blob) {
    _page_blob.create(64 * 1024 * 1024);
  }

  ~XdbWritableFile() {}

  Status Append(const Slice& data) {
    std::cout << "append data: " << data.size() << std::endl;
    const int page_size = 4 * 1024;
    std::vector<char> vector_buffer;
    const char* src = data.data();
    int index = 0;
    vector_buffer.reserve(page_size);
    size_t rc = data.size();
    while (rc > 0) {
      if (rc >= page_size) {
        vector_buffer.insert(vector_buffer.end(), src, src + page_size);
        rc -= page_size;
        src += page_size;
      } else {
        vector_buffer.insert(vector_buffer.end(), src, src + rc);
        rc = page_size - rc;
        while(rc-- > 0) {
          vector_buffer.insert(vector_buffer.end(), 0);
        }
        //std::fill(vector_buffer.end(), vector_buffer.end() + page_size - rc, 0);
        rc = 0;
      }
      concurrency::streams::istream page_stream =
          concurrency::streams::bytestream::open_istream(vector_buffer);
      azure::storage::page_range range(index * page_size,
                                       index * page_size + page_size - 1);
      try {
        // std::cout << "upload page range: " << range.start_offset() <<
        // std::endl;
        _page_blob.upload_pages(page_stream, range.start_offset(),
                                utility::string_t(U("")));
      } catch (const azure::storage::storage_exception& e) {
        std::cout << "append error:" << e.what() << std::endl;
      }
      vector_buffer.clear();
      index++;
    }
    return err_to_status(0);
  }

  Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) {
    return Status::NotSupported();
  }

  Status Truncate(uint64_t size) { return Status::NotSupported(); }

  Status Close() { return err_to_status(0); }

  Status Flush() { return err_to_status(0); }

  Status Sync() { return err_to_status(0); }

 private:
  cloud_page_blob _page_blob;
};

class XdbSequentialFile : public SequentialFile {
 public:
  XdbSequentialFile() {}
  ~XdbSequentialFile() {}
  Status Read(size_t n, Slice* result, char* scratch) {
    return err_to_status(0);
  }
};

EnvXdb::EnvXdb(Env* env) : EnvWrapper(env) {
  // static EnvXdb default_env(env, std::getenv(default_conn));
  // char* connect_string =
  char* connect_string = std::getenv(default_conn);
  char* container_name = std::getenv(default_container);
  if (connect_string == NULL || container_name == NULL) {
    std::cout << "connect_string or container_name is needed" << std::endl;
    exit(-1);
  }
  // std::cout << "connect_string: " << connect_string << std::endl;
  try {
    cloud_storage_account storage_account =
        cloud_storage_account::parse(connect_string);
    _blob_client = storage_account.create_cloud_blob_client();
    // std::cout << "container_name: " << container_name << std::endl;
    _container = _blob_client.get_container_reference(container_name);
    _container.create_if_not_exists();
  } catch (const azure::storage::storage_exception& e) {
    std::cout << e.what() << std::endl;
    exit(-1);
  } catch (...) {
    std::cout << "connect_string is invalid" << std::endl;
    exit(-1);
  }
}

Status EnvXdb::NewWritableFile(const std::string& fname,
                               unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  std::cout << "new write file:" << fname << std::endl;
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    result->reset(new XdbWritableFile(page_blob));
    return Status::OK();
  }
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
  std::cout << "rename from:" << src << " to:" << target << std::endl;
  return EnvWrapper::RenameFile(src, target);
}

EnvXdb* EnvXdb::Default(Env* env) {
  static EnvXdb default_env(env);
  return &default_env;
}
}
