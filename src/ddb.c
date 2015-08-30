/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "lzf.h"    /* LZF compression library */
#include "endianconv.h"
#include "counter.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#define DDB_LOAD_NONE   0
#define DDB_LOAD_ENC    (1<<0)
#define DDB_LOAD_PLAIN  (1<<1)

#define ddbExitReportCorruptDDB(reason) ddbCheckThenExit(reason, __LINE__);

void ddbCheckThenExit(char *reason, int where) {
    serverLog(LL_WARNING, "Corrupt DDB detected at ddb.c:%d (%s). ",
        where, reason);
    exit(1);
}

static int ddbWriteRaw(rio *ddb, void *p, size_t len) {
    if (ddb && rioWrite(ddb,p,len) == 0)
        return -1;
    return len;
}

int ddbSaveType(rio *ddb, unsigned char type) {
    return ddbWriteRaw(ddb,&type,1);
}

/* Load a "type" in DDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. */
int ddbLoadType(rio *ddb) {
    unsigned char type;
    if (rioRead(ddb,&type,1) == 0) return -1;
    return type;
}

int ddbSaveLongLong(rio *ddb, long long t) {
    int64_t t64 = (int64_t) t;
    return ddbWriteRaw(ddb,&t64,8);
}

int ddbLoadLongLong(rio *ddb, long long* t) {
    int64_t t64;
    if (rioRead(ddb,&t64,8) == 0) return -1;
    *t = (long long)t64;
    return 0;
}

int ddbSaveLen(rio *ddb, uint32_t len) {
    return ddbWriteRaw(ddb,&len,4);
}

uint32_t ddbLoadLen(rio *ddb) {
    uint32_t len;
    if (rioRead(ddb,&len,4) == 0) return DDB_LENERR;
    return len;
}

ssize_t ddbSaveRawString(rio *ddb, unsigned char *s, size_t len) {
    ssize_t n, nwritten = 0;

    if ((n = ddbSaveLen(ddb,len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (ddbWriteRaw(ddb,s,len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

/* Load a string into an sds.
 * Reusing the space already available in s. */
sds ddbLoadSds(rio *ddb, sds s) {
    uint32_t len;

    len = ddbLoadLen(ddb);

    if (len == DDB_LENERR) {
        if (s) sdsfree(s);
        return NULL;
    }

    if (s == NULL) {
        s = sdsnewlen(NULL,len);
    } else {
        sdsclear(s);
        s = sdsgrowzero(s, len);
    }
    if (len && rioRead(ddb,s,len) == 0) {
        sdsfree(s);
        return NULL;
    }
    sdssetlen(s,len);
    return s;
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int ddbSaveLongDouble(rio *ddb, long double val) {
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
        snprintf((char*)buf+1,sizeof(buf)-1,"%.17Lg",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return ddbWriteRaw(ddb,buf,len);
}

/* For information about double serialization check ddbSaveLongDouble() */
int ddbLoadLongDouble(rio *ddb, long double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(ddb,&len,1) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (rioRead(ddb,buf,len) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%Lg", val);
        return 0;
    }
}

int ddbLoadDouble(rio *ddb, double *val) {
    int r;
    long double d;
    if ((r = ddbLoadLongDouble(ddb,&d)) == -1) return -1;
    *val = d;
    return 0;
}

int ddbSaveCounter(rio *ddb, const counter* cntr) {
    int n;
    size_t nwritten = 0;
    listNode *ln;
    listIter li;

    if ((n = ddbSaveType(ddb,DDB_TYPE_COUNTER)) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveRawString(ddb,(unsigned char*)cntr->name,sdslen(cntr->name))) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveLen(ddb,cntr->revision)) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveLongDouble(ddb,cntr->precision)) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveLen(ddb,cntr->hits)) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveLen(ddb,cntr->misses)) == -1) return -1;
    nwritten += n;

    if ((n = ddbSaveLen(ddb,listLength(cntr->shards))) == -1) return -1;
    nwritten += n;

    listRewind(cntr->shards,&li);
    while ((ln = listNext(&li)) != NULL) {
        shard *shrd = listNodeValue(ln);

        /* We save the exact same information independent if it's our or another shard. */

        if ((n = ddbSaveRawString(ddb,(unsigned char*)shrd->node_name, CLUSTER_NAMELEN)) == -1) return -1;
        nwritten += n;

        if ((n = ddbSaveLongDouble(ddb,shrd->value)) == -1) return -1;
        nwritten += n;

        if ((n = ddbSaveLongDouble(ddb,shrd->predict_value)) == -1) return -1;
        nwritten += n;

        if ((n = ddbSaveLongDouble(ddb,shrd->predict_change)) == -1) return -1;
        nwritten += n;
    }

    return nwritten;
}

/* Produces a dump of the database in DDB format sending it to the specified
 * Redis I/O channel. On success C_OK is returned, otherwise C_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns C_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. */
int ddbSaveRio(rio *ddb, int *error) {
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    uint64_t cksum;

    if (server.ddb_checksum)
        ddb->update_cksum = rioGenericUpdateChecksum;
    snprintf(magic,sizeof(magic),"DISCNT%03d",DDB_VERSION);
    if (ddbWriteRaw(ddb,magic,9) == -1) goto werr;

    if (dictSize(server.counters) == 0) goto done;
    di = dictGetIterator(server.counters);
    if (!di) return C_ERR;

    /* Write the RESIZE DB opcode. We trim the size to UINT32_MAX, which
     * is currently the largest type we are able to represent in DDB sizes.
     * However this does not limit the actual size of the DB to load since
     * these sizes are just hints to resize the hash tables. */
    uint32_t db_size;
    db_size = (dictSize(server.counters) <= UINT32_MAX) ?
        dictSize(server.counters) : UINT32_MAX;
    if (ddbSaveType(ddb,DDB_OPCODE_RESIZEDB) == -1) goto werr;
    if (ddbSaveLen(ddb,db_size) == -1) goto werr;

    /* Iterate this DB writing every entry */
    while((de = dictNext(di)) != NULL) {
        counter* cntr = dictGetVal(de);

        if (ddbSaveCounter(ddb,cntr) == -1) goto werr;
    }
    dictReleaseIterator(di);
    
done:
    di = NULL; /* So that we don't release it again on error. */

    /* EOF opcode */
    if (ddbSaveType(ddb,DDB_OPCODE_EOF) == -1) goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the
     * loading code skips the check in this case. */
    cksum = ddb->cksum;
    memrev64ifbe(&cksum);
    if (rioWrite(ddb,&cksum,8) == 0) goto werr;
    return C_OK;

werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
int ddbSave(char *filename) {
    char tmpfile[256];
    FILE *fp;
    rio ddb;
    int error = 0;

    snprintf(tmpfile,256,"temp-%d.ddb", (int) getpid());
    fp = fopen(tmpfile,"w");
    if (!fp) {
        serverLog(LL_WARNING, "Failed opening .ddb for saving: %s",
            strerror(errno));
        return C_ERR;
    }

    rioInitWithFile(&ddb,fp);
    if (ddbSaveRio(&ddb,&error) == C_ERR) {
        errno = error;
        goto werr;
    }

    /* Make sure data will not remain on the OS's output buffers */
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    if (rename(tmpfile,filename) == -1) {
        serverLog(LL_WARNING,"Error moving temp DB file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return C_ERR;
    }
    serverLog(LL_NOTICE,"DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = C_OK;
    return C_OK;

werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
}

int ddbSaveBackground(char *filename) {
    pid_t childpid;
    long long start;

    if (server.ddb_child_pid != -1) return C_ERR;

    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = time(NULL);

    start = ustime();
    if ((childpid = fork()) == 0) {
        int retval;

        /* Child */
        closeListeningSockets(0);
        serverSetProcTitle("discnt-ddb-bgsave");
        retval = ddbSave(filename);
        if (retval == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty();

            if (private_dirty) {
                serverLog(LL_NOTICE,
                    "DDB: %zu MB of memory used by copy-on-write",
                    private_dirty/(1024*1024));
            }
        }
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            server.lastbgsave_status = C_ERR;
            serverLog(LL_WARNING,"Can't save in background: fork: %s",
                strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,"Background saving started by pid %d",childpid);
        server.ddb_save_time_start = time(NULL);
        server.ddb_child_pid = childpid;
        server.ddb_child_type = DDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    }
    return C_OK; /* unreached */
}

void ddbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,sizeof(tmpfile),"temp-%d.ddb", (int) childpid);
    unlink(tmpfile);
}

int ddbLoadCounter(rio *ddb) {
    uint32_t n, i;
    sds name = NULL;
    counter *cntr = NULL;

    if ((name = ddbLoadSds(ddb,name)) == NULL) return -1;

    cntr = counterCreate(name);

    if ((cntr->revision = ddbLoadLen(ddb)) == DDB_LENERR) goto rerr;

    if (ddbLoadDouble(ddb,&cntr->precision) == -1) goto rerr;

    if ((cntr->hits = ddbLoadLen(ddb)) == DDB_LENERR) goto rerr;

    if ((cntr->misses = ddbLoadLen(ddb)) == DDB_LENERR) goto rerr;

    if ((n = ddbLoadLen(ddb)) == DDB_LENERR) goto rerr;

    for (i = 0; i < n; i++) {
        clusterNode *node;
        shard* shrd;

        if ((name = ddbLoadSds(ddb,name)) == NULL) goto rerr;

        if (sdslen(name) != CLUSTER_NAMELEN) {
            goto rerr;
        }

        node = clusterLookupNode(name);
        shrd = counterAddShard(cntr, node, name);

        if (ddbLoadLongDouble(ddb,&shrd->value) == -1) goto rerr;

        if (ddbLoadLongDouble(ddb,&shrd->predict_value) == -1) goto rerr;

        if (ddbLoadLongDouble(ddb,&shrd->predict_change) == -1) goto rerr;
    }

    sdsfree(name);

    return 0;

rerr:
    sdsfree(name);
    dictDelete(server.counters,cntr->name);

    return -1;
}

/* Mark that we are loading in the global state and setup the fields
 * needed to provide loading stats. */
void startLoading(FILE *fp) {
    struct stat sb;

    /* Load the DB */
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 0;
    } else {
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading progress info */
void loadingProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* Loading finished */
void stopLoading(void) {
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate ddb checksum  */
void ddbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.ddb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

int ddbLoad(char *filename) {
    int type, ddbver;
    char buf[1024];
    FILE *fp;
    rio ddb;

    if ((fp = fopen(filename,"r")) == NULL) return C_ERR;

    rioInitWithFile(&ddb,fp);
    ddb.update_cksum = ddbLoadProgressCallback;
    ddb.max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(&ddb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"DISCNT",6) != 0) {
        fclose(fp);
        serverLog(LL_WARNING,"Wrong signature trying to load DB from file");
        errno = EINVAL;
        return C_ERR;
    }
    ddbver = atoi(buf+6);
    if (ddbver < 1 || ddbver > DDB_VERSION) {
        fclose(fp);
        serverLog(LL_WARNING,"Can't handle DDB format version %d",ddbver);
        errno = EINVAL;
        return C_ERR;
    }

    startLoading(fp);
    while(1) {
        /* Read type. */
        if ((type = ddbLoadType(&ddb)) == -1) goto eoferr;

        if (type == DDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == DDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently
             * selected data base, in order to avoid useless rehashing. */
            uint32_t db_size;
            if ((db_size = ddbLoadLen(&ddb)) == DDB_LENERR)
                goto eoferr;
            dictExpand(server.counters,db_size);
            continue; /* Read type again. */
        }

        if (ddbLoadCounter(&ddb) == -1) goto eoferr;
    }
    /* Verify the checksum if DDB version is >= 5 */
    if (ddbver >= 5 && server.ddb_checksum) {
        uint64_t cksum, expected = ddb.cksum;

        if (rioRead(&ddb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            serverLog(LL_WARNING,"DDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            serverLog(LL_WARNING,"Wrong DDB checksum. Aborting now.");
            ddbExitReportCorruptDDB("DDB CRC error");
        }
    }

    fclose(fp);

    countersUpdateValues();

    stopLoading();
    return C_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    serverLog(LL_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
    ddbExitReportCorruptDDB("Unexpected EOF reading DDB file");
    return C_ERR; /* Just to avoid warning */
}

/* A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of actual BGSAVEs. */
void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        server.lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;

        serverLog(LL_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        ddbRemoveTempFile(server.ddb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("ddb-unlink-temp-file",latency);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = C_ERR;
    }
    server.ddb_child_pid = -1;
    server.ddb_child_type = DDB_CHILD_TYPE_NONE;
    server.ddb_save_time_last = time(NULL)-server.ddb_save_time_start;
    server.ddb_save_time_start = -1;
}

/* When a background DDB saving/transfer terminates, call the right handler. */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    switch(server.ddb_child_type) {
    case DDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal);
        break;
    default:
        serverPanic("Unknown DDB child type.");
        break;
    }
}

void saveCommand(client *c) {
    if (server.ddb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
        return;
    }
    if (ddbSave(server.ddb_filename) == C_OK) {
        addReply(c,shared.ok);
    } else {
        addReply(c,shared.err);
    }
}

void bgsaveCommand(client *c) {
    if (server.ddb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
    } else if (ddbSaveBackground(server.ddb_filename) == C_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}
