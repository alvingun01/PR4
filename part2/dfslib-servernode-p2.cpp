#include <map>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>
#include <ctime>
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

#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::FileRequest;
using dfs_service::FileChunk;
using dfs_service::FileStatus;
using dfs_service::FileList;
using dfs_service::DeletedFile;
using dfs_service::WriteLockResponse;


//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = FileRequest;
using FileListResponseType = FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    /** Signalled when a callback request is queued, so the queue thread need not spin **/
    std::condition_variable queue_cv;

    /** Guards write_locks **/
    std::mutex lock_mutex;

    /** filename -> client id of the client currently holding its write lock **/
    std::map<std::string, std::string> write_locks;

    /**
     * Guards the visible state of the mount directory together with `tombstones`.
     * Listing/stat/fetch-open take it shared; the final rename of a store and a
     * delete take it exclusive, so a listing never sees a half-applied change.
     */
    std::shared_mutex fs_mutex;

    /** filename -> server time it was deleted; cleared when the file is stored again **/
    std::map<std::string, std::int64_t> tombstones;

    /** Checksums of server files, reused while a file is unchanged **/
    DFSChecksumCache crc_cache;

    /** Makes concurrent upload temp files unique **/
    std::atomic<std::uint64_t> upload_sequence{0};

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            this->queued_tags.emplace_back(context, request, response, cq, tag);
        }
        this->queue_cv.notify_one();

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //

        // The reply is the full current state: every file (name, size, mtime,
        // crc) plus tombstones for deleted files. Each client diffs that
        // against its own mount and fetches/stores/deletes as needed.
        if (!BuildFileList(response)) {
            dfs_log(LL_ERROR) << "ProcessCallback: unable to list " << this->mount_path;
        }

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //

            // Sleep until a callback request is queued instead of busy-spinning
            // on the mutex. The timeout is only a safety net.
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                this->queue_cv.wait_for(lock, std::chrono::milliseconds(500),
                                        [this] { return !this->queued_tags.empty(); });
            }


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //

    /**
     * Grant the write lock for a file to the requesting client.
     *
     * Idempotent for the current holder; RESOURCE_EXHAUSTED when another
     * client holds it. The lock is released when that client's store or
     * delete of the file finishes.
     */
    Status RequestWriteLock(ServerContext* context,
                            const FileRequest* request,
                            WriteLockResponse* response) override {

        const std::string& name = request->name();
        const std::string& client_id = request->client_id();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }
        if (client_id.empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "Missing client id");
        }
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }

        std::string holder;
        if (!AcquireLock(name, client_id, &holder)) {
            dfs_log(LL_DEBUG) << "RequestWriteLock: " << name << " held by " << holder
                              << ", denied to " << client_id;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Write lock for " + name + " is held by another client");
        }

        response->set_name(name);
        response->set_holder(client_id);
        dfs_log(LL_DEBUG) << "RequestWriteLock: " << name << " granted to " << client_id;
        return Status::OK;
    }

    /**
     * Store a file streamed from the client.
     *
     * Message 1 carries only metadata (name, client id, crc, mtime). If the
     * server already has identical content it answers ALREADY_EXISTS before
     * any data is sent; otherwise it sends initial metadata as a "go ahead",
     * and the data is written to a temp file that is renamed into place only
     * once the whole stream has arrived. The client's mtime is preserved so
     * "newest modification wins" compares the times the edits were made.
     *
     * The write lock is released when this returns, whatever the outcome.
     */
    Status StoreFile(ServerContext* context,
                     ServerReader<FileChunk>* reader,
                     FileStatus* response) override {

        FileChunk chunk;
        if (!reader->Read(&chunk)) {
            if (context->IsCancelled()) {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            return Status(StatusCode::INVALID_ARGUMENT, "Empty store stream");
        }

        const std::string name = chunk.name();
        const std::string client_id = chunk.client_id();
        const std::uint32_t client_crc = chunk.crc();
        const std::int64_t client_mtime = chunk.mtime();

        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }
        if (client_id.empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "Missing client id");
        }

        // A client is expected to call RequestWriteLock first; if the file is
        // unlocked we take the lock on its behalf for the duration of the store.
        std::string holder;
        if (!AcquireLock(name, client_id, &holder)) {
            dfs_log(LL_DEBUG) << "StoreFile: " << name << " locked by " << holder << ", rejecting " << client_id;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Write lock for " + name + " is held by another client");
        }
        LockReleaser release(this, name, client_id);

        const std::string path = WrapPath(name);
        std::uint32_t server_crc = 0;
        {
            std::shared_lock<std::shared_mutex> fs_lock(this->fs_mutex);
            if (this->crc_cache.Get(path, &this->crc_table, &server_crc) && server_crc == client_crc) {
                dfs_log(LL_DEBUG) << "StoreFile: " << name << " unchanged (crc " << server_crc << ")";
                return Status(StatusCode::ALREADY_EXISTS, "File already exists with identical contents: " + name);
            }
        }

        // Tell the client to start streaming data.
        context->AddInitialMetadata(DFS_STORE_PROCEED_KEY, "1");
        reader->SendInitialMetadata();

        const std::string tmp_path = WrapPath("." + name + "." +
                std::to_string(this->upload_sequence.fetch_add(1)) + DFS_UPLOAD_SUFFIX);
        std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            dfs_log(LL_ERROR) << "StoreFile: cannot open " << tmp_path << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to open file for writing");
        }

        size_t bytes_written = 0;
        while (reader->Read(&chunk)) {
            if (context->IsCancelled()) {
                break;
            }
            const std::string& data = chunk.data();
            if (!data.empty()) {
                out.write(data.data(), data.size());
                if (!out) {
                    out.close();
                    ::remove(tmp_path.c_str());
                    dfs_log(LL_ERROR) << "StoreFile: write error for " << name;
                    return Status(StatusCode::INTERNAL, "Error writing file");
                }
                bytes_written += data.size();
            }
        }
        out.close();

        // Read() also returns false when the call is cancelled mid-stream.
        if (context->IsCancelled()) {
            ::remove(tmp_path.c_str());
            dfs_log(LL_ERROR) << "StoreFile: deadline exceeded/cancelled for " << name;
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }

        if (client_mtime > 0 && !dfs_set_mtime(tmp_path, client_mtime)) {
            dfs_log(LL_ERROR) << "StoreFile: unable to set mtime on " << tmp_path << ": " << strerror(errno);
        }

        {
            std::unique_lock<std::shared_mutex> fs_lock(this->fs_mutex);
            if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
                ::remove(tmp_path.c_str());
                dfs_log(LL_ERROR) << "StoreFile: rename failed for " << name << ": " << strerror(errno);
                return Status(StatusCode::INTERNAL, "Unable to finalize stored file");
            }
            this->tombstones.erase(name);
            if (!FillStatus(name, response)) {
                return Status(StatusCode::INTERNAL, "Unable to stat stored file");
            }
        }

        dfs_log(LL_DEBUG) << "StoreFile: stored " << name << " (" << bytes_written << " bytes) from " << client_id;
        return Status::OK;
    }

    /**
     * Stream a file's contents back to the client.
     *
     * If the request carries the client's checksum and it matches, answers
     * ALREADY_EXISTS without sending data. The first chunk carries the
     * server's mtime (and is sent even for an empty file).
     */
    Status FetchFile(ServerContext* context,
                     const FileRequest* request,
                     ServerWriter<FileChunk>* writer) override {

        const std::string& name = request->name();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }

        const std::string path = WrapPath(name);
        FileStatus status;
        std::ifstream in;
        {
            // Stat, checksum and open under one shared lock so the mtime we
            // report belongs to the same file we stream (renames are exclusive).
            std::shared_lock<std::shared_mutex> fs_lock(this->fs_mutex);
            if (!FillStatus(name, &status)) {
                dfs_log(LL_DEBUG) << "FetchFile: not found " << name;
                return Status(StatusCode::NOT_FOUND, "File not found: " + name);
            }
            if (request->has_crc() && request->crc() == status.crc()) {
                dfs_log(LL_DEBUG) << "FetchFile: " << name << " unchanged (crc " << status.crc() << ")";
                return Status(StatusCode::ALREADY_EXISTS, "Client already has identical contents: " + name);
            }
            in.open(path, std::ios::binary);
            if (!in.is_open()) {
                return Status(StatusCode::NOT_FOUND, "Unable to open file: " + name);
            }
        }

        dfs_log(LL_DEBUG) << "FetchFile: sending " << name << " (" << status.size() << " bytes)";

        std::vector<char> buffer(DFS_CHUNK_SIZE);
        FileChunk chunk;
        chunk.set_name(name);
        chunk.set_mtime(status.mtime());
        chunk.set_crc(status.crc());
        bool first = true;

        while (true) {
            if (context->IsCancelled()) {
                dfs_log(LL_ERROR) << "FetchFile: deadline exceeded/cancelled for " << name;
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
            }
            in.read(buffer.data(), buffer.size());
            std::streamsize count = in.gcount();
            if (count <= 0 && !first) {
                break;
            }
            chunk.set_data(buffer.data(), count > 0 ? static_cast<size_t>(count) : 0);
            if (!writer->Write(chunk)) {
                if (context->IsCancelled()) {
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
                }
                return Status(StatusCode::CANCELLED, "Stream closed by client");
            }
            if (first) {
                chunk.clear_name();
                chunk.clear_mtime();
                chunk.clear_crc();
                first = false;
            }
            if (!in) {
                break;
            }
        }

        if (in.bad()) {
            return Status(StatusCode::INTERNAL, "Error reading file: " + name);
        }
        return Status::OK;
    }

    /**
     * Delete a file and record a tombstone so other clients delete their copies.
     * Requires the write lock (taken on the caller's behalf if free); the
     * lock is released when this returns.
     */
    Status DeleteFile(ServerContext* context,
                      const FileRequest* request,
                      FileStatus* response) override {

        const std::string& name = request->name();
        const std::string& client_id = request->client_id();
        if (!dfs_is_valid_filename(name)) {
            return Status(StatusCode::INVALID_ARGUMENT, "Invalid file name: " + name);
        }
        if (client_id.empty()) {
            return Status(StatusCode::INVALID_ARGUMENT, "Missing client id");
        }
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }

        std::string holder;
        if (!AcquireLock(name, client_id, &holder)) {
            dfs_log(LL_DEBUG) << "DeleteFile: " << name << " locked by " << holder << ", rejecting " << client_id;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "Write lock for " + name + " is held by another client");
        }
        LockReleaser release(this, name, client_id);

        const std::string path = WrapPath(name);
        std::unique_lock<std::shared_mutex> fs_lock(this->fs_mutex);
        if (!FillStatus(name, response)) {
            return Status(StatusCode::NOT_FOUND, "File not found: " + name);
        }
        if (::remove(path.c_str()) != 0) {
            if (errno == ENOENT) {
                return Status(StatusCode::NOT_FOUND, "File not found: " + name);
            }
            dfs_log(LL_ERROR) << "DeleteFile: failed for " << name << ": " << strerror(errno);
            return Status(StatusCode::INTERNAL, "Unable to delete file: " + name);
        }
        this->tombstones[name] = static_cast<std::int64_t>(::time(nullptr));
        this->crc_cache.Forget(path);

        dfs_log(LL_DEBUG) << "DeleteFile: deleted " << name << " for " << client_id;
        return Status::OK;
    }

    /** List every file on the server with its attributes, plus tombstones. **/
    Status ListFiles(ServerContext* context,
                     const FileRequest* request,
                     FileList* response) override {
        if (context->IsCancelled()) {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or client cancelled");
        }
        if (!BuildFileList(response)) {
            return Status(StatusCode::INTERNAL, "Unable to open mount directory");
        }
        dfs_log(LL_DEBUG) << "ListFiles: " << response->files_size() << " files";
        return Status::OK;
    }

    /** Return size, time and checksum attributes for a single file. **/
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
        std::shared_lock<std::shared_mutex> fs_lock(this->fs_mutex);
        if (!FillStatus(name, response)) {
            return Status(StatusCode::NOT_FOUND, "File not found: " + name);
        }
        return Status::OK;
    }

private:

    /** Releases a client's write lock when it goes out of scope. **/
    struct LockReleaser {
        DFSServiceImpl* service;
        std::string name;
        std::string client_id;
        LockReleaser(DFSServiceImpl* service, const std::string& name, const std::string& client_id) :
            service(service), name(name), client_id(client_id) {}
        ~LockReleaser() { service->ReleaseLock(name, client_id); }
    };

    /**
     * Give the write lock for `name` to `client_id` if it is free or already theirs.
     * On failure `holder` receives the current holder.
     */
    bool AcquireLock(const std::string& name, const std::string& client_id, std::string* holder) {
        std::lock_guard<std::mutex> lock(this->lock_mutex);
        auto it = this->write_locks.find(name);
        if (it == this->write_locks.end()) {
            this->write_locks.emplace(name, client_id);
            return true;
        }
        if (it->second == client_id) {
            return true;
        }
        *holder = it->second;
        return false;
    }

    /** Release the write lock for `name` if `client_id` holds it. **/
    void ReleaseLock(const std::string& name, const std::string& client_id) {
        std::lock_guard<std::mutex> lock(this->lock_mutex);
        auto it = this->write_locks.find(name);
        if (it != this->write_locks.end() && it->second == client_id) {
            this->write_locks.erase(it);
            dfs_log(LL_DEBUG2) << "Released write lock for " << name << " from " << client_id;
        }
    }

    /**
     * Fill a FileStatus for a regular file in the mount path.
     * Caller holds fs_mutex (shared or exclusive).
     *
     * @return false if the file does not exist or is not a regular file
     */
    bool FillStatus(const std::string& name, FileStatus* status) {
        struct stat st;
        std::uint32_t crc = 0;
        if (!this->crc_cache.Get(WrapPath(name), &this->crc_table, &crc, &st)) {
            return false;
        }
        status->set_name(name);
        status->set_size(static_cast<std::uint64_t>(st.st_size));
        status->set_mtime(static_cast<std::int64_t>(st.st_mtime));
        status->set_ctime(static_cast<std::int64_t>(st.st_ctime));
        status->set_crc(crc);
        return true;
    }

    /** Fill `list` with every file in the mount and every tombstone. **/
    bool BuildFileList(FileList* list) {
        std::shared_lock<std::shared_mutex> fs_lock(this->fs_mutex);

        DIR* dir = ::opendir(this->mount_path.c_str());
        if (dir == nullptr) {
            dfs_log(LL_ERROR) << "BuildFileList: cannot open " << this->mount_path << ": " << strerror(errno);
            return false;
        }
        struct dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            const std::string name(entry->d_name);
            if (!dfs_is_valid_filename(name) || dfs_is_transfer_temp_file(name)) {
                continue;
            }
            FileStatus status;
            if (FillStatus(name, &status)) {
                *list->add_files() = status;
            }
        }
        ::closedir(dir);

        for (const auto& tombstone : this->tombstones) {
            DeletedFile* deleted = list->add_deleted();
            deleted->set_name(tombstone.first);
            deleted->set_deleted_at(tombstone.second);
        }
        return true;
    }


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
