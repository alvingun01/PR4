#include <string>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <sys/stat.h>

#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

// Global log level used throughout the system
// Note: this may be adjusted from the CLI in
// both the client and server executables.
// This shouldn't be changed at the file level.
dfs_log_level_e DFS_LOG_LEVEL = LL_ERROR;

//
// STUDENT INSTRUCTION:
//
// Add your additional code here. You may use
// the shared files for anything you need to add outside
// of the main structure, but you don't have to use them.
//
// Just be aware they are always submitted, so they should
// be compilable.
//

#include <regex>
#include <utime.h>

bool dfs_is_valid_filename(const std::string& name) {
    if (name.empty() || name == "." || name == "..") {
        return false;
    }
    for (char c : name) {
        if (c == '/' || c == '\\' || c == '\0') {
            return false;
        }
    }
    return true;
}

bool dfs_is_syncable_filename(const std::string& name) {
    // Same filter as DFSClient::InotifyEventCallback, so the sync loop and
    // the watcher agree on which files belong to the DFS.
    static const std::regex pattern(".*\\.(jpg|png|gif|txt|xlsx|docx|md|psd)$");
    return dfs_is_valid_filename(name) && name[0] != '.' && std::regex_match(name, pattern);
}

static bool dfs_ends_with(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool dfs_is_transfer_temp_file(const std::string& name) {
    return !name.empty() && name[0] == '.' &&
           (dfs_ends_with(name, DFS_UPLOAD_SUFFIX) || dfs_ends_with(name, DFS_DOWNLOAD_SUFFIX));
}

std::string dfs_temp_path(const std::string& mount_path, const std::string& name, const char* suffix) {
    // Leading '.' keeps the inotify watcher from reacting to the temp file.
    return mount_path + "." + name + suffix;
}

bool dfs_set_mtime(const std::string& path, std::int64_t mtime) {
    struct utimbuf times;
    times.actime = static_cast<time_t>(mtime);
    times.modtime = static_cast<time_t>(mtime);
    return ::utime(path.c_str(), &times) == 0;
}

bool DFSChecksumCache::Get(const std::string& path, CRC::Table<std::uint32_t, 32>* table,
                           std::uint32_t* crc, struct stat* st_out) {
    struct stat st;
    if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        Forget(path);
        return false;
    }
    if (st_out != nullptr) {
        *st_out = st;
    }

    {
        std::lock_guard<std::mutex> lock(this->mutex);
        auto it = this->entries.find(path);
        if (it != this->entries.end() &&
                it->second.ino == st.st_ino &&
                it->second.size == st.st_size &&
                it->second.mtime_sec == st.st_mtim.tv_sec &&
                it->second.mtime_nsec == st.st_mtim.tv_nsec) {
            *crc = it->second.crc;
            return true;
        }
    }

    // Computed outside the lock: hashing a large file must not stall other threads.
    std::uint32_t value = dfs_file_checksum(path, table);

    std::lock_guard<std::mutex> lock(this->mutex);
    this->entries[path] = Entry{st.st_ino, st.st_size, st.st_mtim.tv_sec, st.st_mtim.tv_nsec, value};
    *crc = value;
    return true;
}

void DFSChecksumCache::Forget(const std::string& path) {
    std::lock_guard<std::mutex> lock(this->mutex);
    this->entries.erase(path);
}
