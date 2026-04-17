// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED FUNCTIONS ───────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out)
{
    const char *type_str;

    if (type == OBJ_BLOB)
        type_str = "blob";
    else if (type == OBJ_TREE)
        type_str = "tree";
    else
        type_str = "commit";

    // Build header: "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    if (header_len <= 0 || (size_t)header_len >= sizeof(header)) {
        return -1;
    }

    size_t total_len = (size_t)header_len + len;

    // Allocate buffer for full object (header + data)
    char *full = malloc(total_len);
    if (!full) {
        return -1;
    }

    memcpy(full, header, (size_t)header_len);
    memcpy(full + header_len, data, len);

    // Compute SHA-256 of the entire object
    ObjectID id;
    compute_hash(full, total_len, &id);

    // Deduplication: if object already exists, return success
    if (object_exists(&id)) {
        *id_out = id;
        free(full);
        return 0;
    }

    // Build final storage path
    char path[512];
    object_path(&id, path, sizeof(path));

    // Create shard directory if it doesn't exist (.pes/objects/XX/)
    char dir[512];
    strcpy(dir, path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        mkdir(dir, 0755);           // ignore error if directory already exists
    }

    // Write to temporary file first (atomic write pattern)
    char tmp_path[520];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full);
        return -1;
    }

    if (write(fd, full, total_len) != (ssize_t)total_len) {
        close(fd);
        unlink(tmp_path);
        free(full);
        return -1;
    }

    fsync(fd);
    close(fd);

    // Atomically rename temp file to final path
    if (rename(tmp_path, path) != 0) {
        unlink(tmp_path);
        free(full);
        return -1;
    }

    // Optional but good practice: fsync the parent directory
    int dfd = open(dir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }

    *id_out = id;
    free(full);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out)
{
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) {
        return -1;
    }

    // Get file size
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    rewind(f);

    if (size == 0) {
        fclose(f);
        return -1;
    }

    // Read entire object into memory
    char *buf = malloc(size);
    if (!buf) {
        fclose(f);
        return -1;
    }

    if (fread(buf, 1, size, f) != size) {
        fclose(f);
        free(buf);
        return -1;
    }
    fclose(f);

    // Find the null terminator separating header and data
    char *null_pos = memchr(buf, '\0', size);
    if (!null_pos) {
        free(buf);
        return -1;
    }

    // Parse object type
    if (strncmp(buf, "blob", 4) == 0) {
        *type_out = OBJ_BLOB;
    }
    else if (strncmp(buf, "tree", 4) == 0) {
        *type_out = OBJ_TREE;
    }
    else if (strncmp(buf, "commit", 6) == 0) {
        *type_out = OBJ_COMMIT;
    }
    else {
        free(buf);
        return -1;
    }

    // Calculate lengths
    size_t header_len = (size_t)(null_pos - buf) + 1;
    size_t data_len = size - header_len;

    // Allocate and copy the data portion
    void *data = malloc(data_len);
    if (!data) {
        free(buf);
        return -1;
    }

    memcpy(data, buf + header_len, data_len);

    // Verify integrity by recomputing hash
    ObjectID check;
    compute_hash(buf, size, &check);

    if (memcmp(&check, id, sizeof(ObjectID)) != 0) {
        free(buf);
        free(data);
        return -1;  // hash mismatch → corrupted object
    }

    // Success
    *data_out = data;
    *len_out = data_len;

    free(buf);
    return 0;
}