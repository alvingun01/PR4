#include <map>
#include <vector>
#include <cstring>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::FileChunk;
using dfs_service::FileRequest;
using dfs_service::FileStatus;
using dfs_service::StoreResponse;
using dfs_service::DeleteResponse;
using dfs_service::ListRequest;
using dfs_service::ListResponse;


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    /**
     * Store a file streamed from the client.
     *
     * The first chunk must carry the file name. Data is written to a
     * temporary file and only renamed into place once the whole stream
     * has arrived, so an interrupted upload never leaves a partial file.
     */
    Status StoreFile(ServerContext* context,
                     ServerReader<FileChunk>* reader,
                     StoreResponse* response) override {

        FileChunk chunk;
        if (!reader->Read(&chunk)) {
            if (context->IsCancelled()) {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            return Status(StatusCode::INVALID_ARGUMENT, "Empty store stream");
        }

        const std::string name = chunk.name();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }

        const std::string path = WrapPath(name);
        const std::string tmp_path = WrapPath("." + name + ".dfs-upload");
        dfs_log(LL_DEBUG) << "StoreFile: receiving " << name;

        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            dfs_log(LL_ERROR) << "StoreFile: cannot open " << tmp_path << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to open file for writing");
        }

        size_t bytes_written = 0;
        do {
            if (context->IsCancelled()) {
                out.close();
                ::remove(tmp_path.c_str());
                dfs_log(LL_ERROR) << "StoreFile: deadline exceeded/cancelled for " << name;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            const std::string& data = chunk.data();
            if (!data.empty()) {
                out.write(data.data(), data.size());
                if (!out) {
                    out.close();
                    ::remove(tmp_path.c_str());
                    return Status(StatusCode::INTERNAL, "Error writing file");
                }
                bytes_written += data.size();
            }
        } while (reader->Read(&chunk));

        out.close();

        // Read() also returns false when the call is cancelled mid-stream.
        if (context->IsCancelled()) {
            ::remove(tmp_path.c_str());
            dfs_log(LL_ERROR) << "StoreFile: deadline exceeded/cancelled for " << name;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }

        if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
            ::remove(tmp_path.c_str());
            dfs_log(LL_ERROR) << "StoreFile: rename failed for " << name << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to finalize stored file");
        }

        FileStatus* status = response->mutable_status();
        if (!FillStatus(name, status)) {
            return Status(StatusCode::INTERNAL, "Unable to stat stored file");
        }

        dfs_log(LL_DEBUG) << "StoreFile: stored " << name << " (" << bytes_written << " bytes)";
        return Status::OK;
    }

    /**
     * Stream a file's contents back to the client.
     */
    Status FetchFile(ServerContext* context,
                     const FileRequest* request,
                     ServerWriter<FileChunk>* writer) override {

        const std::string& name = request->name();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }

        const std::string path = WrapPath(name);
        struct stat st;
        if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            dfs_log(LL_DEBUG) << "FetchFile: not found " << name;
            return Status(StatusCode::NOT_FOUND, "File not found: " + name);
        }

        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            return Status(StatusCode::NOT_FOUND, "Unable to open file: " + name);
        }

        dfs_log(LL_DEBUG) << "FetchFile: sending " << name << " (" << st.st_size << " bytes)";

        std::vector<char> buffer(DFS_CHUNK_SIZE);
        FileChunk chunk;
        chunk.set_name(name);
        while (in) {
            if (context->IsCancelled()) {
                dfs_log(LL_ERROR) << "FetchFile: deadline exceeded/cancelled for " << name;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            in.read(buffer.data(), buffer.size());
            std::streamsize count = in.gcount();
            if (count <= 0) {
                break;
            }
            chunk.set_data(buffer.data(), static_cast<size_t>(count));
            if (!writer->Write(chunk)) {
                // Stream broken: the client went away or the deadline passed.
                if (context->IsCancelled()) {
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
                }
                return Status(StatusCode::CANCELLED, "Stream closed by client");
            }
            chunk.clear_name();
        }

        if (in.bad()) {
            return Status(StatusCode::INTERNAL, "Error reading file: " + name);
        }

        return Status::OK;
    }

    /**
     * Delete a file from the server's mount path.
     */
    Status DeleteFile(ServerContext* context,
                      const FileRequest* request,
                      DeleteResponse* response) override {

        const std::string& name = request->name();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }

        const std::string path = WrapPath(name);
        struct stat st;
        if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return Status(StatusCode::NOT_FOUND, "File not found: " + name);
        }

        if (::remove(path.c_str()) != 0) {
            if (errno == ENOENT) {
                return Status(StatusCode::NOT_FOUND, "File not found: " + name);
            }
            dfs_log(LL_ERROR) << "DeleteFile: failed for " << name << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to delete file: " + name);
        }

        response->set_name(name);
        dfs_log(LL_DEBUG) << "DeleteFile: deleted " << name;
        return Status::OK;
    }

    /**
     * List every regular file in the mount path with its mtime.
     */
    Status ListFiles(ServerContext* context,
                     const ListRequest* request,
                     ListResponse* response) override {

        DIR* dir = ::opendir(this->mount_path.c_str());
        if (dir == nullptr) {
            dfs_log(LL_ERROR) << "ListFiles: cannot open " << this->mount_path << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to open mount directory");
        }

        struct dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            if (context->IsCancelled()) {
                ::closedir(dir);
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            const std::string name(entry->d_name);
            if (!dfs_is_valid_filename(name) || IsUploadTempFile(name)) {
                continue;
            }
            FileStatus status;
            if (FillStatus(name, &status)) {
                *response->add_files() = status;
            }
        }
        ::closedir(dir);

        dfs_log(LL_DEBUG) << "ListFiles: " << response->files_size() << " files";
        return Status::OK;
    }

    /**
     * Return size and time attributes for a single file.
     */
    Status GetFileStatus(ServerContext* context,
                         const FileRequest* request,
                         FileStatus* response) override {

        const std::string& name = request->name();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }
        if (!FillStatus(name, response)) {
            return Status(StatusCode::NOT_FOUND, "File not found: " + name);
        }
        return Status::OK;
    }

private:

    /**
     * Fill a FileStatus message for a regular file in the mount path.
     *
     * @return false if the file does not exist or is not a regular file
     */
    bool FillStatus(const std::string& name, FileStatus* status) {
        struct stat st;
        const std::string path = WrapPath(name);
        if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
            return false;
        }
        status->set_name(name);
        status->set_size(static_cast<uint64_t>(st.st_size));
        status->set_mtime(static_cast<int64_t>(st.st_mtime));
        status->set_ctime(static_cast<int64_t>(st.st_ctime));
        return true;
    }

    /** True for the hidden temp files StoreFile writes while an upload is in flight. **/
    static bool IsUploadTempFile(const std::string& name) {
        static const std::string suffix = ".dfs-upload";
        return name.size() > suffix.size() + 1 && name[0] == '.' &&
               name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
