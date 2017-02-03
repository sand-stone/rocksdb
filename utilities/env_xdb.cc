#include "rocksdb/utilities/env_xdb.h"
#include <stdio.h>
#include <chrono>
#include <ctime>
#include <iostream>
#include <thread>
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

class XdbSequentialFile : public SequentialFile {
 public:
  XdbSequentialFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob), _offset(0) {}

  ~XdbSequentialFile() { std::cout << "<<<close seq file " << std::endl; }

  Status Read(size_t n, Slice* result, char* scratch) {
    std::cout << "<<<read data: " << n << std::endl;
    _offset = (_offset >> 9) << 9;
    size_t nz = ((n >> 9) + 1) << 9;
    std::cout << "<<<download pages between " << _offset << " and:" << nz
              << std::endl;
    std::vector<page_range> pages =
        _page_blob.download_page_ranges(_offset, nz);
    if (pages.size() == 0) {
      *result = Slice(scratch, 0);
      std::cout << "<<<empty read " << std::endl;
      return Status::OK();
    }
    concurrency::streams::istream blobstream = _page_blob.open_read();
    char* target = scratch;
    size_t len = 0;
    size_t remain = n;
    for (std::vector<page_range>::iterator it = pages.begin(); it < pages.end();
         it++) {
      std::cout << "page start:" << it->start_offset()
                << "page end:" << it->end_offset() << std::endl;
      blobstream.seek(it->start_offset());
      concurrency::streams::stringstreambuf buffer;
      blobstream.read(buffer, it->end_offset() - it->start_offset());
      size_t bsize = buffer.size();
      std::cout << "read back size:" << bsize;
      if (bsize > remain) bsize = remain;
      buffer.scopy(target, bsize);
      remain -= bsize;
      target += bsize;
      len += bsize;
    }
    std::cout << ">>>> actual read data: " << len << std::endl;
    _offset += len;
    if (len == 0)
      *result = Slice(scratch, 0);
    else
      *result = Slice(scratch, len >= n ? n : len);
    return err_to_status(0);
  }

  Status Skip(uint64_t n) {
    std::cout << "Skip:" << n << std::endl;
    return Status::OK();
  }

 private:
  cloud_page_blob _page_blob;
  size_t _offset;
};

class XdbWritableFile : public WritableFile {
 public:
  XdbWritableFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob), _index(0), _offset(0) {
    _page_blob.create(64 * 1024 * 1024);
  }

  ~XdbWritableFile() {}

  Status Append2(const Slice& data) {
    std::cout << "append data: " << data.size() << std::endl;
    concurrency::streams::ostream blobstream = _page_blob.open_write();
    blobstream.seek(_offset);
    const char* src = data.data();
    size_t rc = data.size();
    while (rc > 0) {
      /*ssize_t n = blobstream.write(buffer, rc).get();
      if(n < 0) {
        break;
        }*/
      ssize_t n = blobstream.write(src).get();
      if (n < 0) {
        break;
      }
      rc -= 1;
      src += 1;
    }
    std::cout << "left : " << rc << std::endl;
    _offset += data.size();
    return err_to_status(0);
  }

  Status Append(const Slice& data) {
    std::cout << "append data: " << data.size() << std::endl;
    const int page_size = 1024 * 4;
    std::vector<char> vector_buffer;
    const char* src = data.data();
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
        while (rc-- > 0) {
          vector_buffer.insert(vector_buffer.end(), 0);
        }
        rc = 0;
      }
      concurrency::streams::istream page_stream =
          concurrency::streams::bytestream::open_istream(vector_buffer);
      azure::storage::page_range range(_index * page_size,
                                       _index * page_size + page_size - 1);
      try {
        _page_blob.upload_pages(page_stream, range.start_offset(),
                                utility::string_t(U("")));
      } catch (const azure::storage::storage_exception& e) {
        std::cout << "append error:" << e.what() << std::endl;
      }
      vector_buffer.clear();
      _index++;
    }
    std::cout << "page size: " << page_size << " append pages: " << _index
              << std::endl;
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
  size_t _index;
  size_t _offset;
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
  if (fname.find(was_store) == 0) {
    cloud_page_blob page_blob =
        _container.get_page_blob_reference(fname.substr(4));
    result->reset(new XdbSequentialFile(page_blob));
    return Status::OK();
  }
  return EnvWrapper::NewSequentialFile(fname, result, options);
}

Status EnvXdb::NewDirectory(const std::string& name,
                            unique_ptr<Directory>* result) {
  std::cout << "new dir:" << name << std::endl;
  if (name.find(was_store) == 0) {
    return Status::OK();
  }
  return EnvWrapper::NewDirectory(name, result);
}

Status EnvXdb::GetAbsolutePath(const std::string& db_path,
                               std::string* output_path) {
  std::cout << "abs path:" << db_path << std::endl;
  return EnvWrapper::GetAbsolutePath(db_path, output_path);
}

std::string lastname(const std::string& name) {
  std::size_t pos = name.find_last_of("/");
  return name.substr(pos + 1);
}

std::string firstname(const std::string& name) {
  std::string dirent(name);
  dirent.resize(dirent.size() - 1);
  std::size_t pos = dirent.find_last_of("/");
  return dirent.substr(pos + 1);
}

Status EnvXdb::GetChildren(const std::string& dir,
                           std::vector<std::string>* result) {
  std::cout << "GetChildren for: " << dir << std::endl;
  if (dir.find(was_store) == 0) {
    try {
      result->clear();
      list_blob_item_iterator end;
      for (list_blob_item_iterator it = _container.list_blobs(
               dir.substr(4), false, blob_listing_details::none, 0,
               blob_request_options(), operation_context());
           it != end; it++) {
        if (it->is_blob()) {
          // std::cout << "blob:" << it->as_blob().name() << std::endl;
        } else {
          list_blob_item_iterator bend;
          // std::cout << "enumerate folder:" << it->as_directory().prefix()
          //<< std::endl;
          for (list_blob_item_iterator bit = it->as_directory().list_blobs();
               bit != bend; bit++) {
            if (bit->is_blob()) {
              result->push_back(lastname(bit->as_blob().name()));
              std::cout << "blob:" << lastname(bit->as_blob().name())
                        << std::endl;
            } else {
              result->push_back(firstname(bit->as_directory().prefix()));
              std::cout << "dir:" << firstname(bit->as_directory().prefix())
                        << std::endl;
            }
          }
        }
      }
    } catch (const azure::storage::storage_exception& e) {
      std::cout << "get children for " << dir.substr(4) << e.what()
                << std::endl;
    }
    return Status::OK();
  }
  return EnvWrapper::GetChildren(dir, result);
}

void fixname(std::string& name) {
  std::size_t pos = name.find_first_of("//");
  if (pos != std::string::npos) {
    name.erase(pos, 1);
  }
}

int EnvXdb::WASRename(const std::string& source, const std::string& target) {
  try {
    std::string src(source);
    fixname(src);
    std::cout << "src: " << src << " dst: " << target << std::endl;
    cloud_page_blob target_blob = _container.get_page_blob_reference(target);
    target_blob.create(64 * 1024 * 1024);
    cloud_page_blob src_blob = _container.get_page_blob_reference(src);
    utility::string_t copy_id = target_blob.start_copy(src_blob);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    target_blob.download_attributes();
    copy_state state = target_blob.copy_state();
    if (state.status() == copy_status::success) {
      src_blob.delete_blob();
      return 0;
    } else {
      utility::string_t state_description;
      switch (state.status()) {
        case copy_status::aborted:
          state_description = "aborted";
          break;
        case copy_status::failed:
          state_description = "failed";
          break;
        case copy_status::invalid:
          state_description = "invalid";
          break;
        case copy_status::pending:
          state_description = "pending";
          break;
        case copy_status::success:
          state_description = "success";
          break;
      }
      std::cout << "ErrorX:" << state_description << std::endl
                << "The blob could not be copied." << std::endl;
    }
  } catch (const azure::storage::storage_exception& e) {
    std::cout << "Error:" << e.what() << std::endl
              << "The blob could not be copied." << std::endl;
  }
  return -1;
}

Status EnvXdb::RenameFile(const std::string& src, const std::string& target) {
  std::cout << "rename from:" << src << " to:" << target << std::endl;
  if (src.find(was_store) == 0 && target.find(was_store) == 0) {
    WASRename(src.substr(4), target.substr(4));
    return Status::OK();
  }
  return EnvWrapper::RenameFile(src, target);
}

Status EnvXdb::FileExists(const std::string& fname) {
  std::cout << "file exists: " << fname << std::endl;
  if (fname.find(was_store) == 0) {
    try {
      cloud_page_blob page_blob =
          _container.get_page_blob_reference(fname.substr(4));
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      return Status::NotFound();
    }
  }
  return EnvWrapper::FileExists(fname);
}

Status EnvXdb::GetFileSize(const std::string& f, uint64_t* s) {
  std::cout << "GetFileSize for name:" << f << std::endl;
  if (f.find(was_store) == 0) {
    cloud_page_blob page_blob = _container.get_page_blob_reference(f.substr(4));
    std::vector<page_range> pages = page_blob.download_page_ranges();
    if (pages.size() == 0) {
      *s = 0;
    } else {
      *s = pages[pages.size() - 1].end_offset();
    }
    std::cout << "GetFileSize size" << *s << std::endl;
    return Status::OK();
  }
  return EnvWrapper::GetFileSize(f, s);
}

Status EnvXdb::DeleteFile(const std::string& f) {
  if (f.find(was_store) == 0) {
    return Status::OK();
  }
  return EnvWrapper::DeleteFile(f);
}

Status EnvXdb::CreateDir(const std::string& d) {
  if (d.find(was_store) == 0) {
    return Status::OK();
  }
  return EnvWrapper::CreateDir(d);
}

Status EnvXdb::CreateDirIfMissing(const std::string& d) {
  if (d.find(was_store) == 0) {
    return Status::OK();
  }
  return EnvWrapper::CreateDirIfMissing(d);
}

Status EnvXdb::DeleteDir(const std::string& d) {
  if (d.find(was_store) == 0) {
    return Status::OK();
  }
  return EnvWrapper::DeleteDir(d);
}

EnvXdb* EnvXdb::Default(Env* env) {
  static EnvXdb default_env(env);
  return &default_env;
}
}
