#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <cstring>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
using dfs_service::FileChunk;
using dfs_service::FileRequest;
using dfs_service::FileStatus;
using dfs_service::StoreResponse;
using dfs_service::DeleteResponse;
using dfs_service::ListRequest;
using dfs_service::ListResponse;


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //

    const std::string path = WrapPath(filename);

    struct stat st;
    if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        dfs_log(LL_ERROR) << "Store: file not found on client: " << path;
        return StatusCode::NOT_FOUND;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        dfs_log(LL_ERROR) << "Store: unable to open " << path << ": " << strerror(errno);
        return StatusCode::NOT_FOUND;
    }

    ClientContext context;
    SetDeadline(context);

    StoreResponse response;
    std::unique_ptr<ClientWriter<FileChunk>> writer(
            service_stub->StoreFile(&context, &response));

    dfs_log(LL_DEBUG) << "Store: sending " << filename << " (" << st.st_size << " bytes)";

    // The first chunk always goes out (even for an empty file) so the
    // server learns the file name.
    std::vector<char> buffer(DFS_CHUNK_SIZE);
    FileChunk chunk;
    chunk.set_name(filename);
    bool first = true;
    bool stream_ok = true;

    while (true) {
        in.read(buffer.data(), buffer.size());
        std::streamsize count = in.gcount();
        if (count <= 0 && !first) {
            break;
        }
        chunk.set_data(buffer.data(), count > 0 ? static_cast<size_t>(count) : 0);
        if (!writer->Write(chunk)) {
            // Stream broken (deadline, server error, ...). Finish() reports why.
            stream_ok = false;
            break;
        }
        chunk.clear_name();
        first = false;
        if (!in) {
            break;
        }
    }

    if (in.bad()) {
        dfs_log(LL_ERROR) << "Store: error reading " << path;
        context.TryCancel();
    }

    if (stream_ok) {
        writer->WritesDone();
    }
    Status status = writer->Finish();

    if (in.bad() && status.ok()) {
        return StatusCode::CANCELLED;
    }
    if (status.ok()) {
        dfs_log(LL_DEBUG) << "Store: stored " << filename << " (server mtime "
                          << response.status().mtime() << ")";
    }
    return HandleStatus("Store", filename, status, false);
}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //

    const std::string path = WrapPath(filename);
    // Download into a temp file so a failed fetch never clobbers an existing local copy.
    const std::string tmp_path = WrapPath("." + filename + ".dfs-download");

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);

    std::unique_ptr<ClientReader<FileChunk>> reader(
            service_stub->FetchFile(&context, request));

    std::ofstream out;
    bool write_error = false;
    size_t bytes_received = 0;
    FileChunk chunk;

    while (reader->Read(&chunk)) {
        if (!out.is_open()) {
            out.open(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out.is_open()) {
                dfs_log(LL_ERROR) << "Fetch: unable to open " << tmp_path << ": " << strerror(errno);
                write_error = true;
                context.TryCancel();
                break;
            }
        }
        const std::string& data = chunk.data();
        out.write(data.data(), data.size());
        if (!out) {
            dfs_log(LL_ERROR) << "Fetch: error writing " << tmp_path;
            write_error = true;
            context.TryCancel();
            break;
        }
        bytes_received += data.size();
    }

    if (write_error) {
        // Drain whatever is left so Finish() does not block.
        while (reader->Read(&chunk)) {}
    }

    Status status = reader->Finish();

    if (out.is_open()) {
        out.close();
    }

    if (!status.ok() || write_error) {
        ::remove(tmp_path.c_str());
        if (write_error && status.error_code() != StatusCode::DEADLINE_EXCEEDED) {
            return StatusCode::CANCELLED;
        }
        return HandleStatus("Fetch", filename, status, true);
    }

    if (bytes_received == 0) {
        // An empty file sends no data chunks; create the empty file now.
        std::ofstream empty(tmp_path, std::ios::binary | std::ios::trunc);
        if (!empty.is_open()) {
            dfs_log(LL_ERROR) << "Fetch: unable to create " << tmp_path;
            return StatusCode::CANCELLED;
        }
    }

    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        dfs_log(LL_ERROR) << "Fetch: rename to " << path << " failed: " << strerror(errno);
        ::remove(tmp_path.c_str());
        return StatusCode::CANCELLED;
    }

    dfs_log(LL_DEBUG) << "Fetch: received " << filename << " (" << bytes_received << " bytes)";
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);
    DeleteResponse response;

    Status status = service_stub->DeleteFile(&context, request, &response);
    if (status.ok()) {
        dfs_log(LL_DEBUG) << "Delete: deleted " << filename;
    }
    return HandleStatus("Delete", filename, status, true);
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //

    ClientContext context;
    SetDeadline(context);

    ListRequest request;
    ListResponse response;

    Status status = service_stub->ListFiles(&context, request, &response);
    if (!status.ok()) {
        return HandleStatus("List", "", status, false);
    }

    for (const FileStatus& file : response.files()) {
        if (file_map != nullptr) {
            (*file_map)[file.name()] = static_cast<int>(file.mtime());
        }
        if (display) {
            std::cout << std::left << std::setw(40) << file.name()
                      << " mtime: " << file.mtime()
                      << "  size: " << file.size() << std::endl;
        }
    }

    dfs_log(LL_DEBUG) << "List: " << response.files_size() << " files";
    return StatusCode::OK;
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // The file_status is used to pass back a dfs_service::FileStatus message.
    // When it is NULL the status details are printed to stdout instead.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);
    FileStatus response;

    Status status = service_stub->GetFileStatus(&context, request, &response);
    if (!status.ok()) {
        return HandleStatus("Stat", filename, status, true);
    }

    if (file_status != nullptr) {
        static_cast<FileStatus*>(file_status)->CopyFrom(response);
    } else {
        std::cout << "name:  " << response.name() << std::endl
                  << "size:  " << response.size() << std::endl
                  << "mtime: " << response.mtime() << std::endl
                  << "ctime: " << response.ctime() << std::endl;
    }
    return StatusCode::OK;
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//

void DFSClientNodeP1::SetDeadline(ClientContext& context) const {
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(this->deadline_timeout));
}

StatusCode DFSClientNodeP1::HandleStatus(const std::string& operation,
                                         const std::string& filename,
                                         const Status& status,
                                         bool allow_not_found) const {
    switch (status.error_code()) {
        case StatusCode::OK:
            return StatusCode::OK;
        case StatusCode::DEADLINE_EXCEEDED:
            dfs_log(LL_ERROR) << operation << ": deadline exceeded ("
                              << this->deadline_timeout << "ms) " << filename;
            return StatusCode::DEADLINE_EXCEEDED;
        case StatusCode::NOT_FOUND:
            if (allow_not_found) {
                dfs_log(LL_ERROR) << operation << ": not found on server: " << filename;
                return StatusCode::NOT_FOUND;
            }
            break;
        default:
            break;
    }
    dfs_log(LL_ERROR) << operation << " failed for " << filename << ": ["
                      << status.error_code() << "] " << status.error_message();
    return StatusCode::CANCELLED;
}
