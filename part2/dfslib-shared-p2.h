#ifndef PR4_DFSLIB_SHARED_H
#define PR4_DFSLIB_SHARED_H

#include <algorithm>
#include <cctype>
#include <locale>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <thread>
#include <sys/stat.h>

#include "src/dfs-utils.h"
#include "proto-src/dfs-service.grpc.pb.h"


//
// STUDENT INSTRUCTION
//
// The following defines and methods must be left as-is for the
// structural components provided to work.
//
#define DFS_RESET_TIMEOUT 3000
#define DFS_I_EVENT_SIZE (sizeof(struct inotify_event))
#define DFS_I_BUFFER_SIZE (1024 * (DFS_I_EVENT_SIZE + 16))

/** A file descriptor type **/
typedef int FileDescriptor;

/** A watch descriptor type **/
typedef int WatchDescriptor;

/** An inotify callback method **/
typedef void (*InotifyCallback)(uint, const std::string&, void*);

/** The expected struct for the inotify setup in this project **/
struct NotifyStruct {
    FileDescriptor fd;
    WatchDescriptor wd;
    uint event_type;
    std::thread * thread;
    InotifyCallback callback;
};

/** The expected event type for the event method in this project **/
struct EventStruct {
    void* event;
    void* instance;
};

//
// STUDENT INSTRUCTION:
//
// Add any additional shared code here
//

#include <map>
#include <mutex>
#include <cstdint>

/** Size of each data chunk streamed over gRPC (well under the 4MB message limit) **/
#define DFS_CHUNK_SIZE (64 * 1024)

/** Delay between CallbackList polls on the client (ms) **/
#define DFS_CALLBACK_INTERVAL_MS 100

/** Suffixes of the hidden temp files used while a transfer is in flight **/
#define DFS_UPLOAD_SUFFIX ".dfs-upload"
#define DFS_DOWNLOAD_SUFFIX ".dfs-download"

/**
 * Initial-metadata key the server sets on StoreFile to tell the client to
 * stream the file's data. Its absence means the server answered early
 * (e.g. ALREADY_EXISTS) and the client should go straight to Finish().
 */
#define DFS_STORE_PROCEED_KEY "dfs-store-proceed"

/**
 * True when `name` is a plain file name that is safe to use inside a mount
 * directory: non-empty, not "." or "..", and free of path separators or NUL
 * bytes (so it can never escape the mount path).
 */
bool dfs_is_valid_filename(const std::string& name);

/**
 * True when `name` is a file the client keeps in sync: a valid, non-hidden
 * file name with one of the extensions the inotify watcher accepts
 * (jpg|png|gif|txt|xlsx|docx|md|psd, case sensitive).
 */
bool dfs_is_syncable_filename(const std::string& name);

/** True for a hidden in-flight transfer temp file (".<name>.dfs-upload" etc). **/
bool dfs_is_transfer_temp_file(const std::string& name);

/** Path of the hidden temp file for `name` inside `mount_path`. **/
std::string dfs_temp_path(const std::string& mount_path, const std::string& name, const char* suffix);

/** Set a file's access and modification time to `mtime` (seconds). **/
bool dfs_set_mtime(const std::string& path, std::int64_t mtime);

/**
 * Thread-safe cache of file checksums.
 *
 * Computing a CRC reads the whole file, and the sync loop compares files
 * many times per second. An entry is reused only while the file's inode,
 * size and modification time (to the nanosecond) are unchanged, so any
 * rewrite or replacement of the file invalidates it.
 */
class DFSChecksumCache {
public:
    /**
     * Stat `path` and return its checksum.
     *
     * @param path   full path of the file
     * @param table  CRC table used when the checksum must be recomputed
     * @param crc    receives the checksum
     * @param st     optional, receives the stat result
     * @return false if the file does not exist or is not a regular file
     */
    bool Get(const std::string& path, CRC::Table<std::uint32_t, 32>* table,
             std::uint32_t* crc, struct stat* st = nullptr);

    /** Drop any entry for `path`. **/
    void Forget(const std::string& path);

private:
    struct Entry {
        ino_t ino;
        off_t size;
        time_t mtime_sec;
        long mtime_nsec;
        std::uint32_t crc;
    };
    std::mutex mutex;
    std::map<std::string, Entry> entries;
};


#endif

