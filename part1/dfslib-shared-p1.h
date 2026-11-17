#ifndef _DFSLIB_SHARED_H
#define _DFSLIB_SHARED_H

#include <algorithm>
#include <cctype>
#include <locale>
#include <cstddef>
#include <iostream>
#include <fstream>
#include <sys/stat.h>

#include "src/dfs-utils.h"
#include "proto-src/dfs-service.grpc.pb.h"

#define DFS_RESET_TIMEOUT 3000

//
// STUDENT INSTRUCTION:
//
// Add your additional code here
//

#include <string>

/** Size of each data chunk streamed over gRPC (well under the 4MB message limit) **/
#define DFS_CHUNK_SIZE (64 * 1024)

/**
 * Returns true when `name` is a plain file name that is safe to use
 * inside a mount directory: non-empty, not "." or "..", and free of
 * path separators or NUL bytes (so it can never escape the mount path).
 */
bool dfs_is_valid_filename(const std::string& name);


#endif

