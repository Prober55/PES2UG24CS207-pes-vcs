#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declaration
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED FUNCTIONS (UNCHANGED) ───

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    if (index->count == 0) printf("  (nothing to show)\n");
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
    }
    printf("\n");

    printf("Unstaged changes:\n");
    printf("  (nothing to show)\n\n");

    printf("Untracked files:\n");
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_name[0] == '.') continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;

            int tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    tracked = 1;
                    break;
                }
            }

            if (!tracked) {
                printf("  untracked:  %s\n", ent->d_name);
            }
        }
        closedir(dir);
    }
    printf("\n");
    return 0;
}

// ─── YOUR FUNCTIONS (FIXED) ───

int index_load(Index *index) {
    if (!index) return -1;

    index->count = 0;

    FILE *f = fopen(".pes/index", "r");
    if (!f) return 0;

    char hex[HASH_HEX_SIZE + 1];

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &index->entries[index->count];

        int rc = fscanf(f, "%o %64s %llu %u %511s\n",
                        &e->mode, hex,
                        (unsigned long long *)&e->mtime_sec,
                        &e->size, e->path);

        if (rc != 5) break;

        if (hex_to_hash(hex, &e->hash) != 0) {
            fclose(f);
            return -1;
        }

        index->count++;
    }

    fclose(f);
    return 0;
}

int index_save(const Index *index) {
    if (!index) return -1;

    mkdir(".pes", 0755);

    FILE *f = fopen(".pes/index", "w");
    if (!f) return -1;

    for (int i = 0; i < index->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&index->entries[i].hash, hex);

        fprintf(f, "%o %s %llu %u %s\n",
                index->entries[i].mode,
                hex,
                (unsigned long long)index->entries[i].mtime_sec,
                index->entries[i].size,
                index->entries[i].path);
    }

    fclose(f);
    return 0;
}

int index_add(Index *index, const char *path) {
    if (!index || !path) return -1;

    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (size < 0) {
        fclose(f);
        return -1;
    }

    void *buffer = malloc(size);
    if (!buffer) {
        fclose(f);
        return -1;
    }

    fread(buffer, 1, size, f);
    fclose(f);

    ObjectID id;
    if (object_write(OBJ_BLOB, buffer, size, &id) != 0) {
        free(buffer);
        return -1;
    }

    free(buffer);

    struct stat st;
    if (stat(path, &st) != 0) return -1;

    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        entry = &index->entries[index->count++];
        strcpy(entry->path, path);
    }

    entry->hash = id;
    entry->mtime_sec = st.st_mtime;
    entry->size = st.st_size;
    entry->mode = 0100644;

    return index_save(index);
}
// index module implementation