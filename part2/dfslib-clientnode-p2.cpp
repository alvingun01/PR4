#include <regex>
#include <mutex>
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
#include <dirent.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using dfs_service::FileRequest;
using dfs_service::FileChunk;
using dfs_service::FileStatus;
using dfs_service::FileList;
using dfs_service::DeletedFile;
using dfs_service::WriteLockResponse;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);
    request.set_client_id(this->client_id);
    WriteLockResponse response;

    Status status = service_stub->RequestWriteLock(&context, request, &response);
    if (status.ok()) {
        dfs_log(LL_DEBUG2) << "RequestWriteAccess: lock granted for " << filename;
    }
    return HandleStatus("RequestWriteAccess", filename, status, {StatusCode::RESOURCE_EXHAUSTED});
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    const std::string path = WrapPath(filename);

    struct stat st;
    std::uint32_t crc = 0;
    if (!this->crc_cache.Get(path, &this->crc_table, &crc, &st)) {
        dfs_log(LL_ERROR) << "Store: file not found on client: " << path;
        return StatusCode::NOT_FOUND;
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        dfs_log(LL_ERROR) << "Store: unable to open " << path << ": " << strerror(errno);
        return StatusCode::NOT_FOUND;
    }

    StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != StatusCode::OK) {
        dfs_log(LL_DEBUG) << "Store: no write lock for " << filename << " (" << lock_status << ")";
        return lock_status;
    }

    ClientContext context;
    SetDeadline(context);

    FileStatus response;
    std::unique_ptr<ClientWriter<FileChunk>> writer(
            service_stub->StoreFile(&context, &response));

    // Message 1: metadata only. The server either answers ALREADY_EXISTS
    // straight away, or sends initial metadata marked "proceed".
    FileChunk chunk;
    chunk.set_name(filename);
    chunk.set_client_id(this->client_id);
    chunk.set_crc(crc);
    chunk.set_mtime(static_cast<std::int64_t>(st.st_mtime));

    bool stream_ok = writer->Write(chunk);
    bool proceed = false;
    if (stream_ok) {
        writer->WaitForInitialMetadata();
        const auto& metadata = context.GetServerInitialMetadata();
        proceed = metadata.find(DFS_STORE_PROCEED_KEY) != metadata.end();
    }

    size_t bytes_sent = 0;
    if (stream_ok && proceed) {
        dfs_log(LL_DEBUG) << "Store: sending " << filename << " (" << st.st_size << " bytes)";
        std::vector<char> buffer(DFS_CHUNK_SIZE);
        FileChunk data_chunk;
        while (in) {
            in.read(buffer.data(), buffer.size());
            std::streamsize count = in.gcount();
            if (count <= 0) {
                break;
            }
            data_chunk.set_data(buffer.data(), static_cast<size_t>(count));
            if (!writer->Write(data_chunk)) {
                // Stream broken (deadline, server error, ...). Finish() reports why.
                stream_ok = false;
                break;
            }
            bytes_sent += static_cast<size_t>(count);
        }
        if (in.bad()) {
            dfs_log(LL_ERROR) << "Store: error reading " << path;
            context.TryCancel();
        }
    }

    if (stream_ok) {
        writer->WritesDone();
    }
    Status status = writer->Finish();

    if (in.bad() && status.ok()) {
        return StatusCode::CANCELLED;
    }
    if (status.ok()) {
        dfs_log(LL_DEBUG) << "Store: stored " << filename << " (" << bytes_sent << " bytes, mtime "
                          << response.mtime() << ")";
    } else if (status.error_code() == StatusCode::ALREADY_EXISTS) {
        dfs_log(LL_DEBUG) << "Store: " << filename << " unchanged on server";
    }
    return HandleStatus("Store", filename, status,
                        {StatusCode::ALREADY_EXISTS, StatusCode::RESOURCE_EXHAUSTED});
}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //

    const std::string path = WrapPath(filename);
    // Download into a hidden temp file (ignored by the watcher) and rename it
    // into place: a failed fetch never clobbers the local copy, and the rename
    // raises IN_MOVED_TO, which the watcher does not treat as a local edit.
    const std::string tmp_path = dfs_temp_path(this->mount_path, filename, DFS_DOWNLOAD_SUFFIX);

    FileRequest request;
    request.set_name(filename);
    request.set_client_id(this->client_id);
    std::uint32_t local_crc = 0;
    if (this->crc_cache.Get(path, &this->crc_table, &local_crc)) {
        request.set_crc(local_crc);
        request.set_has_crc(true);
    }

    ClientContext context;
    SetDeadline(context);

    std::unique_ptr<ClientReader<FileChunk>> reader(
            service_stub->FetchFile(&context, request));

    std::ofstream out;
    bool write_error = false;
    bool received_any = false;
    std::int64_t server_mtime = 0;
    size_t bytes_received = 0;
    FileChunk chunk;

    while (reader->Read(&chunk)) {
        if (!received_any) {
            received_any = true;
            server_mtime = chunk.mtime();
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

    if (!status.ok() || write_error || !received_any) {
        ::remove(tmp_path.c_str());
        if (status.error_code() == StatusCode::ALREADY_EXISTS) {
            dfs_log(LL_DEBUG) << "Fetch: " << filename << " unchanged on server";
            return StatusCode::ALREADY_EXISTS;
        }
        if (write_error && status.error_code() != StatusCode::DEADLINE_EXCEEDED) {
            return StatusCode::CANCELLED;
        }
        if (status.ok()) {
            dfs_log(LL_ERROR) << "Fetch: empty response for " << filename;
            return StatusCode::CANCELLED;
        }
        return HandleStatus("Fetch", filename, status, {StatusCode::NOT_FOUND});
    }

    // Match the server's mtime, so both sides agree the copies are in sync.
    if (server_mtime > 0 && !dfs_set_mtime(tmp_path, server_mtime)) {
        dfs_log(LL_ERROR) << "Fetch: unable to set mtime on " << tmp_path << ": " << strerror(errno);
    }

    if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
        dfs_log(LL_ERROR) << "Fetch: rename to " << path << " failed: " << strerror(errno);
        ::remove(tmp_path.c_str());
        return StatusCode::CANCELLED;
    }

    dfs_log(LL_DEBUG) << "Fetch: received " << filename << " (" << bytes_received << " bytes)";
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    {
        // A deletion this client made itself because the server deleted the
        // file: the server is already up to date.
        std::lock_guard<std::mutex> lock(this->suppressed_mutex);
        auto it = this->suppressed_deletes.find(filename);
        if (it != this->suppressed_deletes.end()) {
            this->suppressed_deletes.erase(it);
            dfs_log(LL_DEBUG2) << "Delete: " << filename << " was removed by sync; not sent to server";
            return StatusCode::OK;
        }
    }

    StatusCode lock_status = RequestWriteAccess(filename);
    if (lock_status != StatusCode::OK) {
        dfs_log(LL_DEBUG) << "Delete: no write lock for " << filename << " (" << lock_status << ")";
        return lock_status;
    }

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);
    request.set_client_id(this->client_id);
    FileStatus response;

    Status status = service_stub->DeleteFile(&context, request, &response);
    if (status.ok()) {
        dfs_log(LL_DEBUG) << "Delete: deleted " << filename;
    }
    return HandleStatus("Delete", filename, status,
                        {StatusCode::NOT_FOUND, StatusCode::RESOURCE_EXHAUSTED});
}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_client_id(this->client_id);
    FileList response;

    Status status = service_stub->ListFiles(&context, request, &response);
    if (!status.ok()) {
        return HandleStatus("List", "", status, {});
    }

    for (const FileStatus& file : response.files()) {
        if (file_map != nullptr) {
            (*file_map)[file.name()] = static_cast<int>(file.mtime());
        }
        if (display) {
            std::cout << std::left << std::setw(40) << file.name()
                      << " mtime: " << file.mtime()
                      << "  size: " << file.size()
                      << "  crc: " << file.crc() << std::endl;
        }
    }

    dfs_log(LL_DEBUG) << "List: " << response.files_size() << " files";
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    SetDeadline(context);

    FileRequest request;
    request.set_name(filename);
    request.set_client_id(this->client_id);
    FileStatus response;

    Status status = service_stub->GetFileStatus(&context, request, &response);
    if (!status.ok()) {
        return HandleStatus("Stat", filename, status, {StatusCode::NOT_FOUND});
    }

    if (file_status != nullptr) {
        static_cast<FileStatus*>(file_status)->CopyFrom(response);
    } else {
        std::cout << "name:  " << response.name() << std::endl
                  << "size:  " << response.size() << std::endl
                  << "mtime: " << response.mtime() << std::endl
                  << "ctime: " << response.ctime() << std::endl
                  << "crc:   " << response.crc() << std::endl;
    }
    return StatusCode::OK;
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    // The callback stores/deletes files; never let it run while the async
    // thread is reconciling the mount, and vice versa.
    std::lock_guard<std::mutex> lock(this->sync_mutex);
    callback();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //

                // Critical section: the watcher thread cannot store/delete
                // while this thread reconciles the mount.
                std::lock_guard<std::mutex> lock(this->sync_mutex);
                SyncWithServer(call_data->reply);

            } else {
                // Not holding sync_mutex here, so the watcher keeps working while we back off.
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }

        if (this->Unmounting()) {
            break;
        }

        // The server answers every CallbackList immediately, so pace the
        // polling instead of hammering it in a tight loop.
        std::this_thread::sleep_for(std::chrono::milliseconds(DFS_CALLBACK_INTERVAL_MS));

        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

void DFSClientNodeP2::SetDeadline(ClientContext& context) const {
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(this->deadline_timeout));
}

StatusCode DFSClientNodeP2::HandleStatus(const std::string& operation,
                                         const std::string& filename,
                                         const Status& status,
                                         std::initializer_list<StatusCode> allowed) const {
    const StatusCode code = status.error_code();
    if (code == StatusCode::OK) {
        return StatusCode::OK;
    }
    if (code == StatusCode::DEADLINE_EXCEEDED) {
        dfs_log(LL_ERROR) << operation << ": deadline exceeded ("
                          << this->deadline_timeout << "ms) " << filename;
        return StatusCode::DEADLINE_EXCEEDED;
    }
    for (StatusCode allowed_code : allowed) {
        if (code == allowed_code) {
            // Expected outcomes (lock contention, not found, unchanged) are not errors.
            dfs_log(LL_DEBUG) << operation << " " << filename << ": [" << code << "] "
                              << status.error_message();
            return code;
        }
    }
    dfs_log(LL_ERROR) << operation << " failed for " << filename << ": ["
                      << code << "] " << status.error_message();
    return StatusCode::CANCELLED;
}

void DFSClientNodeP2::DeleteLocal(const std::string& filename) {
    const std::string path = WrapPath(filename);
    {
        std::lock_guard<std::mutex> lock(this->suppressed_mutex);
        this->suppressed_deletes.insert(filename);
    }
    if (::remove(path.c_str()) != 0) {
        // No IN_DELETE will arrive, so don't leave the suppression behind.
        std::lock_guard<std::mutex> lock(this->suppressed_mutex);
        this->suppressed_deletes.erase(filename);
        if (errno != ENOENT) {
            dfs_log(LL_ERROR) << "Sync: unable to delete " << path << ": " << strerror(errno);
        }
        return;
    }
    this->crc_cache.Forget(path);
    dfs_log(LL_DEBUG) << "Sync: deleted local " << filename << " (deleted on server)";
}

void DFSClientNodeP2::SyncWithServer(const FileList& listing) {

    std::map<std::string, const FileStatus*> server_files;
    for (const FileStatus& file : listing.files()) {
        if (dfs_is_syncable_filename(file.name())) {
            server_files[file.name()] = &file;
        }
    }

    std::map<std::string, std::int64_t> tombstones;
    for (const DeletedFile& deleted : listing.deleted()) {
        tombstones[deleted.name()] = deleted.deleted_at();
    }

    // Every name present on either side.
    std::set<std::string> names;
    for (const auto& entry : server_files) {
        names.insert(entry.first);
    }
    DIR* dir = ::opendir(this->mount_path.c_str());
    if (dir == nullptr) {
        dfs_log(LL_ERROR) << "Sync: cannot open mount " << this->mount_path << ": " << strerror(errno);
        return;
    }
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        const std::string name(entry->d_name);
        if (dfs_is_syncable_filename(name)) {
            names.insert(name);
        }
    }
    ::closedir(dir);

    for (const std::string& name : names) {
        if (this->Unmounting()) {
            return;
        }

        const std::string path = WrapPath(name);
        struct stat st;
        const bool local_exists = ::stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
        auto server_it = server_files.find(name);
        const FileStatus* server = server_it != server_files.end() ? server_it->second : nullptr;

        if (server != nullptr && local_exists) {
            const std::int64_t local_mtime = static_cast<std::int64_t>(st.st_mtime);
            std::uint32_t local_crc = 0;
            if (!this->crc_cache.Get(path, &this->crc_table, &local_crc)) {
                continue;  // vanished since the stat; the watcher will report it
            }
            if (local_crc == server->crc()) {
                continue;  // same contents (timestamps may differ): nothing to do
            }
            // Contents differ: the newer modification wins. mtimes have
            // one-second resolution, so two clients writing within the same
            // second tie; the server's copy wins ties, otherwise the losing
            // client would stay out of sync forever.
            if (server->mtime() >= local_mtime) {
                dfs_log(LL_DEBUG) << "Sync: server copy of " << name << " is newer; fetching";
                Fetch(name);
            } else {
                dfs_log(LL_DEBUG) << "Sync: local copy of " << name << " is newer; storing";
                Store(name);
            }
        } else if (server != nullptr) {
            dfs_log(LL_DEBUG) << "Sync: " << name << " only on server; fetching";
            Fetch(name);
        } else if (local_exists) {
            auto tomb_it = tombstones.find(name);
            if (tomb_it != tombstones.end() &&
                    tomb_it->second >= static_cast<std::int64_t>(st.st_mtime)) {
                // Deleted on the server after our copy was last modified.
                DeleteLocal(name);
            } else {
                // New local file (including files present before mounting).
                dfs_log(LL_DEBUG) << "Sync: " << name << " only on client; storing";
                Store(name);
            }
        }
    }
}
