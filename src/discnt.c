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
 *   * Neither the name of Discnt nor the names of its contributors may be used
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

#include "discnt.h"
#include "cluster.h"
#include "bio.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct discntServer server; /* server global state */

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name: a string representing the command name.
 * function: pointer to the C function implementing the command.
 * arity: number of arguments, it is possible to use -N to say >= N
 * sflags: command flags as string. See below for a table of flags.
 * flags: flags as bitmask. Computed by Discnt using the 'sflags' field.
 * get_keys_proc: an optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 * first_key_index: first argument that is a key
 * last_key_index: last argument that is a key
 * key_step: step to get all the keys from first to last argument. For instance
 *           in MSET the step is two since arguments are key,val,key,val,...
 * microseconds: microseconds of total execution time for this command.
 * calls: total number of calls of this command.
 *
 * The flags, microseconds and calls fields are computed by Discnt and should
 * always be set to zero.
 *
 * Command flags are expressed using strings where every character represents
 * a flag. Later the populateCommandTable() function will take care of
 * populating the real 'flags' field using this characters.
 *
 * This is the meaning of the flags:
 *
 * w: write command (may modify data).
 * r: read command  (will never modify data).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * l: Allow command while loading the database.
 * M: Do not automatically propagate the command on MONITOR.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 */
struct serverCommand serverCommandTable[] = {
    /* Server commands. */
    {"auth",authCommand,2,"rlF",0,NULL,0,0,0,0,0},
    {"ping",pingCommand,-1,"rF",0,NULL,0,0,0,0,0},
    {"info",infoCommand,-1,"rl",0,NULL,0,0,0,0,0},
    {"shutdown",shutdownCommand,-1,"arl",0,NULL,0,0,0,0,0},
    {"lastsave",lastsaveCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"monitor",monitorCommand,1,"ar",0,NULL,0,0,0,0,0},
    {"debug",debugCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"config",configCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"cluster",clusterCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"ar",0,NULL,0,0,0,0,0},
    {"time",timeCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"command",commandCommand,0,"rl",0,NULL,0,0,0,0,0},
    {"latency",latencyCommand,-2,"arl",0,NULL,0,0,0,0,0},
    {"hello",helloCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"save",saveCommand,1,"ar",0,NULL,0,0,0,0,0},
    {"bgsave",bgsaveCommand,1,"ar",0,NULL,0,0,0,0,0},

    /* Counter commands. */
    {"incr",incrCommand,3,"wmF",0,NULL,0,0,0,0,0},
    {"get",getCommand,2,"rF",0,NULL,0,0,0,0,0},
    {"counters",countersCommand,2,"r",0,NULL,0,0,0,0,0},
    {"precision",precisionCommand,-2,"wmF",0,NULL,0,0,0,0,0},
};

/*============================ Utility functions ============================ */

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#";
    FILE *fp;
    char buf[64];
    int rawmode = (level & DISCNT_LOG_RAW);
    int log_to_stdout = server.logfile[0] == '\0';

    level &= 0xff; /* clear flags */
    if (level < server.verbosity) return;

    fp = log_to_stdout ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();

        gettimeofday(&tv,NULL);
        off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        if (pid != server.pid) {
            role_char = 'C'; /* DDB writing child. */
        } else {
            role_char = 'P'; /* Parent child. */
        }
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[DISCNT_MAX_LOGMSG_LEN];

    if ((level&0xff) < server.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Log a fixed message without printf-alike capabilities, in a way that is
 * safe to call from a signal handler.
 *
 * We actually use this only for signals that are not fatal from the point
 * of view of Discnt. Signals that are going to kill the server anyway and
 * where we need printf-alike features are served by serverLog(). */
void serverLogFromHandler(int level, const char *msg) {
    int fd;
    int log_to_stdout = server.logfile[0] == '\0';
    char buf[64];

    if ((level&0xff) < server.verbosity || (log_to_stdout && server.daemonize))
        return;
    fd = log_to_stdout ? STDOUT_FILENO :
                         open(server.logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    if (fd == -1) return;
    ll2string(buf,sizeof(buf),getpid());
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,":signal-handler (",17) == -1) goto err;
    ll2string(buf,sizeof(buf),time(NULL));
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,") ",2) == -1) goto err;
    if (write(fd,msg,strlen(msg)) == -1) goto err;
    if (write(fd,"\n",1) == -1) goto err;
err:
    if (!log_to_stdout) close(fd);
}

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/* Return a random time error between -(milliseconds/2) and +(milliseconds/2).
 * This is useful in order to desynchronize multiple nodes competing for the
 * same operation in a best-effort way. */
mstime_t randomTimeError(mstime_t milliseconds) {
    return rand()%milliseconds - milliseconds/2;
}

/* After a DDB dump we exit from children using _exit() instead of
 * exit(), because the latter may interact with the same file objects used by
 * the parent process. However if we are testing the coverage normal exit() is
 * used in order to obtain the right coverage information. */
void exitFromChild(int retcode) {
#ifdef COVERAGE_TEST
    exit(retcode);
#else
    _exit(retcode);
#endif
}

/*====================== Hash table type implementation  ==================== */

/* This is a hash table type that uses the SDS dynamic strings library as
 * keys and discnt objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictDiscntObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == DISCNT_ENCODING_INT &&
        o2->encoding == DISCNT_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == DISCNT_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type hash table */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictDiscntObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:5262 to
 * clusterNode structures. */
unsigned int dictClusterNodeHash(const void *key) {
    return dictGenHashFunction(key,DISCNT_CLUSTER_NAMELEN);
}

int dictClusterNodeKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    DICT_NOTUSED(privdata);
    return memcmp(key1, key2, DISCNT_CLUSTER_NAMELEN) == 0;
}

dictType clusterNodesDictType = {
    dictClusterNodeHash,        /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictClusterNodeKeyCompare,  /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType clusterNodesBlackListDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */

};

dictType counterDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictCounterDestructor       /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < DISCNT_HT_MINFILL));
}

void tryResizeHashTables(void) {
    if (htNeedsResize(server.counters)) dictResize(server.counters);
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some rehahsing.
 *
 * The function returns 1 if some rehashing was performed, otherwise 0
 * is returned. */
int incrementallyRehash(void) {
    int workdone = 0;

    if (dictIsRehashing(server.counters)) {
        dictRehashMilliseconds(server.counters,1);
        workdone = 1;
    }

    return workdone;
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(void) {
    if (server.ddb_child_pid == -1)
        dictEnableResize();
    else
        dictDisableResize();
}

/* Remove all the server state and everything else to start
 * like a fresh instance just rebooted. */
void flushServerData(void) {
    dictEmpty(server.counters, NULL);
}

/* ======================= Cron: called every 100 ms ======================== */

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long t = mstime() - server.inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    server.inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    server.inst_metric[metric].samples[server.inst_metric[metric].idx] =
        ops_sec;
    server.inst_metric[metric].idx++;
    server.inst_metric[metric].idx %= DISCNT_METRIC_SAMPLES;
    server.inst_metric[metric].last_sample_time = mstime();
    server.inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < DISCNT_METRIC_SAMPLES; j++)
        sum += server.inst_metric[metric].samples[j];
    return sum / DISCNT_METRIC_SAMPLES;
}

/* Check for timeouts. Returns non-zero if the client was terminated */
int clientsCronHandleTimeout(client *c) {
    time_t now = server.unixtime;

    if (server.maxidletime &&
        !(c->flags & DISCNT_BLOCKED) &&  /* no timeout for blocked clients. */
        (now - c->lastinteraction > server.maxidletime))
    {
        serverLog(LL_VERBOSE,"Closing idle client");
        freeClient(c);
        return 1;
    } else if (c->flags & DISCNT_BLOCKED) {
        /* Blocked OPS timeout is handled with milliseconds resolution.
         * However note that the actual resolution is limited by
         * server.hz. */
        mstime_t now_ms = mstime();

        if (c->bpop.timeout != 0 && c->bpop.timeout < now_ms) {
            replyToBlockedClientTimedOut(c);
            unblockClient(c);
        }
    }
    return 0;
}

/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
int clientsCronResizeQueryBuffer(client *c) {
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = server.unixtime - c->lastinteraction;

    /* There are two conditions to resize the query buffer:
     * 1) Query buffer is > BIG_ARG and too big for latest peak.
     * 2) Client is inactive and the buffer is bigger than 1k. */
    if (((querybuf_size > DISCNT_MBULK_BIG_ARG) &&
         (querybuf_size/(c->querybuf_peak+1)) > 2) ||
         (querybuf_size > 1024 && idletime > 2))
    {
        /* Only resize the query buffer if it is actually wasting space. */
        if (sdsavail(c->querybuf) > 1024) {
            c->querybuf = sdsRemoveFreeSpace(c->querybuf);
        }
    }
    /* Reset the peak again to capture the peak memory usage in the next
     * cycle. */
    c->querybuf_peak = 0;
    return 0;
}

void clientsCron(void) {
    /* Make sure to process at least 1/(server.hz*10) of clients per call.
     * Since this function is called server.hz times per second we are sure that
     * in the worst case we process all the clients in 10 seconds.
     * In normal conditions (a reasonable number of clients) we process
     * all the clients in a shorter time. */
    int numclients = listLength(server.clients);
    int iterations = numclients/(server.hz*10);

    if (iterations < 50)
        iterations = (numclients < 50) ? numclients : 50;
    while(listLength(server.clients) && iterations--) {
        client *c;
        listNode *head;

        /* Rotate the list, take the current head, process.
         * This way if the client must be removed from the list it's the
         * first element and we don't incur into O(N) computation. */
        listRotate(server.clients);
        head = listFirst(server.clients);
        c = listNodeValue(head);
        /* The following functions do different service checks on the client.
         * The protocol is that they return non-zero if the client was
         * terminated. */
        if (clientsCronHandleTimeout(c)) continue;
        if (clientsCronResizeQueryBuffer(c)) continue;
    }
}

/* This function handles 'background' operations we are required to do
 * incrementally in the Discnt server, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(void) {
    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    if (server.ddb_child_pid == -1) {
        /* We use global counters so if we stop the computation at a given
         * DB we'll be able to start from the successive in the next
         * cron loop iteration. */

        tryResizeHashTables();
        incrementallyRehash();
    }
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL) */
void updateCachedTime(void) {
    server.unixtime = time(NULL);
    server.mstime = mstime();
}

/* This is our timer interrupt, called server.hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called server.hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    DISCNT_NOTUSED(eventLoop);
    DISCNT_NOTUSED(id);
    DISCNT_NOTUSED(clientData);
    int j;

    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    if (server.watchdog_period) watchdogScheduleSignal(server.watchdog_period);

    /* Update the time cache. */
    updateCachedTime();

    run_with_period(100) {
        trackInstantaneousMetric(DISCNT_METRIC_COMMAND,server.stat_numcommands);
        trackInstantaneousMetric(DISCNT_METRIC_NET_INPUT,
                server.stat_net_input_bytes);
        trackInstantaneousMetric(DISCNT_METRIC_NET_OUTPUT,
                server.stat_net_output_bytes);
    }

    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used_memory();

    /* Sample the RSS here since this is a relatively slow call. */
    server.resident_set_size = zmalloc_get_rss();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (server.shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS) == DISCNT_OK) exit(0);
        serverLog(LL_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
        server.shutdown_asap = 0;
    }

    /* Show information, */
    run_with_period(5000) {
        serverLog(LL_VERBOSE,
            "%lu clients connected, %zu bytes in use",
            listLength(server.clients),
            zmalloc_used_memory());

        serverLog(LL_VERBOSE,"DB: %lu keys in %lu slots HT.",dictSize(server.counters),dictSlots(server.counters));
    }

    /* We need to do a few operations on clients asynchronously. */
    clientsCron();

    /* Handle background operations on Discnt. */
    databasesCron();

    run_with_period(1000) {
        countersHistoryCron();
    }

    run_with_period(100) {
        countersValueCron();
    }

    /* Check if a background saving in progress terminated. */
    if (server.ddb_child_pid != -1) {
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = 0;

            if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

            if (pid == server.ddb_child_pid) {
                backgroundSaveDoneHandler(exitcode,bysignal);
            } else {
                serverLog(LL_WARNING,
                    "Warning, detected child with unmatched pid: %ld",
                    (long)pid);
            }
            updateDictResizePolicy();
        }
    } else {
        /* If there is not a background saving/rewrite in progress check if
         * we have to save/rewrite now */
         for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;

            /* Save if we reached the given amount of changes,
             * the given amount of seconds, and if the latest bgsave was
             * successful or if, in case of an error, at least
             * DISCNT_BGSAVE_RETRY_DELAY seconds already elapsed. */
            if (server.dirty >= sp->changes &&
                server.unixtime-server.lastsave > sp->seconds &&
                (server.unixtime-server.lastbgsave_try >
                 DISCNT_BGSAVE_RETRY_DELAY ||
                 server.lastbgsave_status == DISCNT_OK))
            {
                serverLog(LL_NOTICE,"%d changes in %d seconds. Saving...",
                    sp->changes, (int)sp->seconds);
                ddbSaveBackground(server.ddb_filename);
                break;
            }
        }
    }

    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue();

    /* Clear the paused clients flag if needed. */
    clientsArePaused(); /* Don't check return value, just use the side effect.*/

    /* Run the Discnt Cluster cron. */
    run_with_period(100) {
        clusterCron();
    }

    server.cronloops++;
    return 1000/server.hz;
}

/* This function gets called every time Discnt is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    DISCNT_NOTUSED(eventLoop);

    /* Try to process pending commands for clients that were just unblocked. */
    if (listLength(server.unblocked_clients))
        processUnblockedClients();

    /* Call the Discnt Cluster before sleep function. */
    clusterBeforeSleep();
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    shared.crlf = createObject(DISCNT_STRING,sdsnew("\r\n"));
    shared.ok = createObject(DISCNT_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(DISCNT_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(DISCNT_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(DISCNT_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(DISCNT_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(DISCNT_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(DISCNT_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(DISCNT_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(DISCNT_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(DISCNT_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(DISCNT_STRING,sdsnew("+QUEUED\r\n"));
    shared.wrongtypeerr = createObject(DISCNT_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(DISCNT_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(DISCNT_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(DISCNT_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(DISCNT_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.noscripterr = createObject(DISCNT_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    shared.loadingerr = createObject(DISCNT_STRING,sdsnew(
        "-LOADING Discnt is loading the dataset in memory\r\n"));
    shared.slowscripterr = createObject(DISCNT_STRING,sdsnew(
        "-BUSY Discnt is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    shared.masterdownerr = createObject(DISCNT_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
    shared.bgsaveerr = createObject(DISCNT_STRING,sdsnew(
        "-MISCONF Discnt is configured to save DDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Discnt logs for details about the error.\r\n"));
    shared.roslaveerr = createObject(DISCNT_STRING,sdsnew(
        "-READONLY You can't write against a read only slave.\r\n"));
    shared.noautherr = createObject(DISCNT_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    shared.oomerr = createObject(DISCNT_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    shared.execaborterr = createObject(DISCNT_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    shared.noreplicaserr = createObject(DISCNT_STRING,sdsnew(
        "-NOREPLICAS Not enough good slaves to write.\r\n"));
    shared.busykeyerr = createObject(DISCNT_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));
    shared.space = createObject(DISCNT_STRING,sdsnew(" "));
    shared.colon = createObject(DISCNT_STRING,sdsnew(":"));
    shared.plus = createObject(DISCNT_STRING,sdsnew("+"));
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    for (j = 0; j < DISCNT_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(DISCNT_STRING,(void*)(long)j);
        shared.integers[j]->encoding = DISCNT_ENCODING_INT;
    }
    for (j = 0; j < DISCNT_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = createObject(DISCNT_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        shared.bulkhdr[j] = createObject(DISCNT_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = createStringObject("minstring",9);
    shared.maxstring = createStringObject("maxstring",9);
}

void initServerConfig(void) {
    int j;

    getRandomHexChars(server.runid,DISCNT_RUN_ID_SIZE);
    server.configfile = NULL;
    server.hz = DISCNT_DEFAULT_HZ;
    server.runid[DISCNT_RUN_ID_SIZE] = '\0';
    server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
    server.port = DISCNT_SERVERPORT;
    server.tcp_backlog = DISCNT_TCP_BACKLOG;
    server.bindaddr_count = 0;
    server.unixsocket = NULL;
    server.unixsocketperm = DISCNT_DEFAULT_UNIX_SOCKET_PERM;
    server.ipfd_count = 0;
    server.sofd = -1;
    server.dbnum = DISCNT_DEFAULT_DBNUM;
    server.verbosity = DISCNT_DEFAULT_VERBOSITY;
    server.maxidletime = DISCNT_MAXIDLETIME;
    server.tcpkeepalive = DISCNT_DEFAULT_TCP_KEEPALIVE;
    server.active_expire_enabled = 1;
    server.client_max_querybuf_len = DISCNT_MAX_QUERYBUF_LEN;
    server.saveparams = NULL;
    server.loading = 0;
    server.logfile = zstrdup(DISCNT_DEFAULT_LOGFILE);
    server.syslog_enabled = DISCNT_DEFAULT_SYSLOG_ENABLED;
    server.syslog_ident = zstrdup(DISCNT_DEFAULT_SYSLOG_IDENT);
    server.syslog_facility = LOG_LOCAL0;
    server.daemonize = DISCNT_DEFAULT_DAEMONIZE;
    server.pidfile = zstrdup(DISCNT_DEFAULT_PID_FILE);
    server.ddb_filename = zstrdup(DISCNT_DEFAULT_DDB_FILENAME);
    server.requirepass = NULL;
    server.ddb_compression = DISCNT_DEFAULT_DDB_COMPRESSION;
    server.ddb_checksum = DISCNT_DEFAULT_DDB_CHECKSUM;
    server.stop_writes_on_bgsave_err = DISCNT_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR;
    server.activerehashing = DISCNT_DEFAULT_ACTIVE_REHASHING;
    server.maxclients = DISCNT_MAX_CLIENTS;
    server.precision = DISCNT_PRECISION;
    server.history_size = DISCNT_HISTORY;
    server.history_index = 0;
    server.bpop_blocked_clients = 0;
    server.maxmemory = DISCNT_DEFAULT_MAXMEMORY;
    server.shutdown_asap = 0;
    server.cluster_node_timeout = DISCNT_CLUSTER_DEFAULT_NODE_TIMEOUT;
    server.cluster_configfile = zstrdup(DISCNT_DEFAULT_CLUSTER_CONFIG_FILE);
    server.next_client_id = 1; /* Client IDs, start from 1 .*/
    server.loading_process_events_interval_bytes = (1024*1024*2);

    resetServerSaveParams();
    appendServerSaveParams(60*60,1); /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100); /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* Client output buffer limits */
    for (j = 0; j < DISCNT_CLIENT_TYPE_COUNT; j++)
        server.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we initiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * discnt.conf using the rename-command directive. */
    server.commands = dictCreate(&commandTableDictType,NULL);
    server.orig_commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");
    server.lpushCommand = lookupCommandByCString("lpush");
    server.lpopCommand = lookupCommandByCString("lpop");
    server.rpopCommand = lookupCommandByCString("rpop");

    /* Latency monitor */
    server.latency_monitor_threshold = DISCNT_DEFAULT_LATENCY_MONITOR_THRESHOLD;

    /* Debugging */
    server.assert_failed = "<no assertion failed>";
    server.assert_file = "<no file>";
    server.assert_line = 0;
    server.bug_report_start = 0;
    server.watchdog_period = 0;
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (DISCNT_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * server.maxclients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
    rlim_t maxfiles = server.maxclients+DISCNT_MIN_RESERVED_FDS;
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        serverLog(LL_WARNING,"Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
            strerror(errno));
        server.maxclients = 1024-DISCNT_MIN_RESERVED_FDS;
    } else {
        rlim_t oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t f;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            f = maxfiles;
            while(f > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = f;
                limit.rlim_max = f;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'f'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (f < decr_step) break;
                f -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (f < oldlimit) f = oldlimit;

            if (f != maxfiles) {
                int old_maxclients = server.maxclients;
                server.maxclients = f-DISCNT_MIN_RESERVED_FDS;
                if (server.maxclients < 1) {
                    serverLog(LL_WARNING,"Your current 'ulimit -n' "
                        "of %llu is not enough for Discnt to start. "
                        "Please increase your open file limit to at least "
                        "%llu. Exiting.",
                        (unsigned long long) oldlimit,
                        (unsigned long long) maxfiles);
                    exit(1);
                }
                serverLog(LL_WARNING,"You requested maxclients of %d "
                    "requiring at least %llu max file descriptors.",
                    old_maxclients,
                    (unsigned long long) maxfiles);
                serverLog(LL_WARNING,"Discnt can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                serverLog(LL_WARNING,"Current maximum open files is %llu. "
                    "maxclients has been reduced to %d to compensate for "
                    "low ulimit. "
                    "If you need higher maxclients increase 'ulimit -n'.",
                    (unsigned long long) oldlimit, server.maxclients);
            } else {
                serverLog(LL_NOTICE,"Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        }
    }
}

/* Check that server.tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
void checkTcpBacklogSettings(void) {
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
    char buf[1024];
    if (!fp) return;
    if (fgets(buf,sizeof(buf),fp) != NULL) {
        int somaxconn = atoi(buf);
        if (somaxconn > 0 && somaxconn < server.tcp_backlog) {
            serverLog(LL_WARNING,"WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", server.tcp_backlog, somaxconn);
        }
    }
    fclose(fp);
#endif
}

/* Initialize a set of file descriptors to listen to the specified 'port'
 * binding the addresses specified in the Discnt server configuration.
 *
 * The listening file descriptors are stored in the integer array 'fds'
 * and their number is set in '*count'.
 *
 * The addresses to bind are specified in the global server.bindaddr array
 * and their number is server.bindaddr_count. If the server configuration
 * contains no specific addresses to bind, this function will try to
 * bind * (all addresses) for both the IPv4 and IPv6 protocols.
 *
 * On success the function returns DISCNT_OK.
 *
 * On error the function returns DISCNT_ERR. For the function to be on
 * error, at least one of the server.bindaddr addresses was
 * impossible to bind, or no bind addresses were specified in the server
 * configuration but the function is not able to bind * for at least
 * one of the IPv4 or IPv6 protocols. */
int listenToPort(int port, int *fds, int *count) {
    int j;

    /* Force binding of 0.0.0.0 if no bind address is specified, always
     * entering the loop if j == 0. */
    if (server.bindaddr_count == 0) server.bindaddr[0] = NULL;
    for (j = 0; j < server.bindaddr_count || j == 0; j++) {
        if (server.bindaddr[j] == NULL) {
            /* Bind * for both IPv6 and IPv4, we enter here only if
             * server.bindaddr_count == 0. */
            fds[*count] = anetTcp6Server(server.neterr,port,NULL,
                server.tcp_backlog);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            }
            fds[*count] = anetTcpServer(server.neterr,port,NULL,
                server.tcp_backlog);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            }
            /* Exit the loop if we were able to bind * on IPv4 or IPv6,
             * otherwise fds[*count] will be ANET_ERR and we'll print an
             * error and return to the caller with an error. */
            if (*count) break;
        } else if (strchr(server.bindaddr[j],':')) {
            /* Bind IPv6 address. */
            fds[*count] = anetTcp6Server(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        } else {
            /* Bind IPv4 address. */
            fds[*count] = anetTcpServer(server.neterr,port,server.bindaddr[j],
                server.tcp_backlog);
        }
        if (fds[*count] == ANET_ERR) {
            serverLog(LL_WARNING,
                "Creating Server TCP listening socket %s:%d: %s",
                server.bindaddr[j] ? server.bindaddr[j] : "*",
                port, server.neterr);
            return DISCNT_ERR;
        }
        anetNonBlock(NULL,fds[*count]);
        (*count)++;
    }
    return DISCNT_OK;
}

/* Resets the stats that we expose via INFO or other means that we want
 * to reset via CONFIG RESETSTAT. The function is also used in order to
 * initialize these fields in initServer() at server startup. */
void resetServerStats(void) {
    int j;

    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_fork_time = 0;
    server.stat_fork_rate = 0;
    server.stat_rejected_conn = 0;
    for (j = 0; j < DISCNT_METRIC_COUNT; j++) {
        server.inst_metric[j].idx = 0;
        server.inst_metric[j].last_sample_time = mstime();
        server.inst_metric[j].last_sample_count = 0;
        memset(server.inst_metric[j].samples,0,
            sizeof(server.inst_metric[j].samples));
    }
    server.stat_net_input_bytes = 0;
    server.stat_net_output_bytes = 0;
    server.stat_hits = 0;
    server.stat_misses = 0;
}

void initServer(void) {
    int j;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();

    if (server.syslog_enabled) {
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            server.syslog_facility);
    }

    server.pid = getpid();
    server.current_client = NULL;
    server.clients = listCreate();
    server.clients_to_close = listCreate();
    server.monitors = listCreate();
    server.unblocked_clients = listCreate();
    server.ready_keys = listCreate();
    server.clients_paused = 0;

    createSharedObjects();
    adjustOpenFilesLimit();
    server.el = aeCreateEventLoop(server.maxclients+DISCNT_EVENTLOOP_FDSET_INCR);
    /* Open the TCP listening socket for the user commands. */
    if (server.port != 0 &&
        listenToPort(server.port,server.ipfd,&server.ipfd_count) == DISCNT_ERR)
        exit(1);

    /* Open the listening Unix domain socket. */
    if (server.unixsocket != NULL) {
        unlink(server.unixsocket); /* don't care if this fails */
        server.sofd = anetUnixServer(server.neterr,server.unixsocket,
            server.unixsocketperm, server.tcp_backlog);
        if (server.sofd == ANET_ERR) {
            serverLog(LL_WARNING, "Opening socket: %s", server.neterr);
            exit(1);
        }
        anetNonBlock(NULL,server.sofd);
    }

    /* Abort if there are no listening sockets at all. */
    if (server.ipfd_count == 0 && server.sofd < 0) {
        serverLog(LL_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }

    /* Create the Discnt data structures, and init other internal state. */
    server.counters = dictCreate(&counterDictType,NULL);
    server.cronloops = 0;
    server.ddb_child_pid = -1;
    server.ddb_child_type = DDB_CHILD_TYPE_NONE;
    server.lastsave = time(NULL); /* At startup we consider the DB saved. */
    server.lastbgsave_try = 0;    /* At startup we never tried to BGSAVE. */
    server.ddb_save_time_last = -1;
    server.ddb_save_time_start = -1;
    server.dirty = 0;
    resetServerStats();

    /* A few stats we don't want to reset: server startup time, and peak mem. */
    server.stat_starttime = time(NULL);
    server.stat_peak_memory = 0;
    server.resident_set_size = 0;
    updateCachedTime();

    /* Create the serverCron() time event, that's our main way to process
     * background operations. */
    if (aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL) == AE_ERR) {
        serverPanic("Can't create the serverCron time event.");
        exit(1);
    }

    /* Create an event handler for accepting new connections in TCP and Unix
     * domain sockets. */
    for (j = 0; j < server.ipfd_count; j++) {
        if (aeCreateFileEvent(server.el, server.ipfd[j], AE_READABLE,
            acceptTcpHandler,NULL) == AE_ERR)
            {
                serverPanic(
                    "Unrecoverable error creating server.ipfd file event.");
            }
    }
    if (server.sofd > 0 && aeCreateFileEvent(server.el,server.sofd,AE_READABLE,
        acceptUnixHandler,NULL) == AE_ERR) serverPanic("Unrecoverable error creating server.sofd file event.");

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
     * useless crashes of the Discnt instance for out of memory. */
    if (server.arch_bits == 32 && server.maxmemory == 0) {
        serverLog(LL_WARNING,"Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
        server.maxmemory = 3072LL*(1024*1024); /* 3 GB */
    }

    clusterInit();
    latencyMonitorInit();
    bioInit();
}

/* Populates the Discnt Command Table starting from the hard coded list
 * we have on top of discnt.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);

    for (j = 0; j < numcommands; j++) {
        struct serverCommand *c = serverCommandTable+j;
        char *f = c->sflags;
        int retval1, retval2;

        while(*f != '\0') {
            switch(*f) {
            case 'w': c->flags |= DISCNT_CMD_WRITE; break;
            case 'r': c->flags |= DISCNT_CMD_READONLY; break;
            case 'm': c->flags |= DISCNT_CMD_DENYOOM; break;
            case 'a': c->flags |= DISCNT_CMD_ADMIN; break;
            case 'l': c->flags |= DISCNT_CMD_LOADING; break;
            case 'M': c->flags |= DISCNT_CMD_SKIP_MONITOR; break;
            case 'F': c->flags |= DISCNT_CMD_FAST; break;
            default: serverPanic("Unsupported command flag"); break;
            }
            f++;
        }

        retval1 = dictAdd(server.commands, sdsnew(c->name), c);
        /* Populate an additional dictionary that will be unaffected
         * by rename-command statements in discnt.conf. */
        retval2 = dictAdd(server.orig_commands, sdsnew(c->name), c);
        serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
    }
}

void resetCommandTableStats(void) {
    int numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);
    int j;

    for (j = 0; j < numcommands; j++) {
        struct serverCommand *c = serverCommandTable+j;

        c->microseconds = 0;
        c->calls = 0;
    }
}

/* ====================== Commands lookup and execution ===================== */

struct serverCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}

struct serverCommand *lookupCommandByCString(char *s) {
    struct serverCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}

/* Lookup the command in the current table, if not found also check in
 * the original table containing the original command names unaffected by
 * discnt.conf rename-command statement.
 *
 * This is used by functions rewriting the argument vector such as
 * rewriteClientCommandVector() in order to set client->cmd pointer
 * correctly even if the command was renamed. */
struct serverCommand *lookupCommandOrOriginal(sds name) {
    struct serverCommand *cmd = dictFetchValue(server.commands, name);

    if (!cmd) cmd = dictFetchValue(server.orig_commands,name);
    return cmd;
}

/* Send commands to the list of clients in monitor state. */
void replicationFeedMonitors(client *c, list *monitors, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & DISCNT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[unix:%s] ",server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%s] ",getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == DISCNT_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(DISCNT_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Call() is the core of Discnt execution of a command */
void call(client *c, int flags) {
    long long start, duration;

    /* Sent the command to clients in MONITOR mode, only if the commands are
     * not generated from reading an AOF. */
    if (listLength(server.monitors) &&
        !server.loading &&
        !(c->cmd->flags & DISCNT_CMD_SKIP_MONITOR))
    {
        replicationFeedMonitors(c,server.monitors,c->argv,c->argc);
    }

    /* Call the command. */
    start = ustime();
    c->cmd->proc(c);
    duration = ustime()-start;

    /* Populate the per-command statistics that we show in INFO commandstats. */
    if (flags & DISCNT_CALL_STATS) {
        c->cmd->microseconds += duration;
        c->cmd->calls++;
    }
    server.stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroyed (i.e. after QUIT). */
int processCommand(client *c) {
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        addReply(c,shared.ok);
        c->flags |= DISCNT_CLOSE_AFTER_REPLY;
        return DISCNT_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        addReplyErrorFormat(c,"unknown command '%s'",
            (char*)c->argv[0]->ptr);
        return DISCNT_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return DISCNT_OK;
    }

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    {
        addReply(c,shared.noautherr);
        return DISCNT_OK;
    }

    /* Handle the maxmemory directive. */
    if (server.maxmemory) {
        if (getMemoryWarningLevel() && c->cmd->flags & DISCNT_CMD_DENYOOM) {
            addReply(c, shared.oomerr);
            return DISCNT_OK;
        }
    }

    /* Don't accept write commands if there are problems persisting on disk
     * and if this is a master instance. */
    if ((server.stop_writes_on_bgsave_err &&
        server.saveparamslen > 0 &&
        server.lastbgsave_status == DISCNT_ERR) &&
        (c->cmd->flags & DISCNT_CMD_WRITE ||
         c->cmd->proc == pingCommand))
    {
        addReply(c, shared.bgsaveerr);
        return DISCNT_OK;
    }

    /* Loading DB? Return an error if the command has not the
     * DISCNT_CMD_LOADING flag. */
    if (server.loading && !(c->cmd->flags & DISCNT_CMD_LOADING)) {
        addReply(c, shared.loadingerr);
        return DISCNT_OK;
    }

    call(c,DISCNT_CALL_FULL);
    return DISCNT_OK;
}

/*================================== Shutdown =============================== */

/* Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. */
void closeListeningSockets(int unlink_unix_socket) {
    int j;

    for (j = 0; j < server.ipfd_count; j++) close(server.ipfd[j]);
    if (server.sofd != -1) close(server.sofd);
    for (j = 0; j < server.cfd_count; j++) close(server.cfd[j]);
    if (unlink_unix_socket && server.unixsocket) {
        serverLog(LL_NOTICE,"Removing the unix socket file.");
        unlink(server.unixsocket); /* don't care if this fails */
    }
}

/* Performs the operations needed to shutdown correctly the server */
int prepareForShutdown(int flags) {
    int save = flags & SHUTDOWN_SAVE;
    int nosave = flags & SHUTDOWN_NOSAVE;

    serverLog(LL_WARNING,"User requested shutdown...");

    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.ddb_child_pid != -1) {
        serverLog(LL_WARNING,"There is a child saving an .ddb. Killing it!");
        kill(server.ddb_child_pid,SIGUSR1);
        ddbRemoveTempFile(server.ddb_child_pid);
    }
    if ((server.saveparamslen > 0 && !nosave) || save) {
        serverLog(LL_NOTICE,"Saving the final DDB snapshot before exiting.");
        /* Snapshotting. Perform a SYNC SAVE and exit */
        if (ddbSave(server.ddb_filename) != DISCNT_OK) {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            serverLog(LL_WARNING,"Error trying to save the DB, can't exit.");
            return DISCNT_ERR;
        }
    }
    if (server.daemonize) {
        serverLog(LL_NOTICE,"Removing the pid file.");
        unlink(server.pidfile);
    }

    /* Close the listening sockets. Apparently this allows faster restarts. */
    closeListeningSockets(1);
    serverLog(LL_WARNING,"Discnt is now ready to exit, bye bye...");
    return DISCNT_OK;
}

/*================================== Commands =============================== */

/* Return zero if strings are the same, non-zero if they are not.
 * The comparison is performed in a way that prevents an attacker to obtain
 * information about the nature of the strings just monitoring the execution
 * time of the function.
 *
 * Note that limiting the comparison length to strings up to 512 bytes we
 * can avoid leaking any information about the password length and any
 * possible branch misprediction related leak.
 */
int time_independent_strcmp(char *a, char *b) {
    char bufa[DISCNT_AUTHPASS_MAX_LEN], bufb[DISCNT_AUTHPASS_MAX_LEN];
    /* The above two strlen perform len(a) + len(b) operations where either
     * a or b are fixed (our password) length, and the difference is only
     * relative to the length of the user provided string, so no information
     * leak is possible in the following two lines of code. */
    unsigned int alen = strlen(a);
    unsigned int blen = strlen(b);
    unsigned int j;
    int diff = 0;

    /* We can't compare strings longer than our static buffers.
     * Note that this will never pass the first test in practical circumstances
     * so there is no info leak. */
    if (alen > sizeof(bufa) || blen > sizeof(bufb)) return 1;

    memset(bufa,0,sizeof(bufa));        /* Constant time. */
    memset(bufb,0,sizeof(bufb));        /* Constant time. */
    /* Again the time of the following two copies is proportional to
     * len(a) + len(b) so no info is leaked. */
    memcpy(bufa,a,alen);
    memcpy(bufb,b,blen);

    /* Always compare all the chars in the two buffers without
     * conditional expressions. */
    for (j = 0; j < sizeof(bufa); j++) {
        diff |= (bufa[j] ^ bufb[j]);
    }
    /* Length must be equal as well. */
    diff |= alen ^ blen;
    return diff; /* If zero strings are the same. */
}

void authCommand(client *c) {
    if (!server.requirepass) {
        addReplyError(c,"Client sent AUTH, but no password is set");
    } else if (!time_independent_strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplyError(c,"invalid password");
    }
}

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
    /* The command takes zero or one arguments. */
    if (c->argc > 2) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return;
    }

    if (c->argc == 1)
        addReply(c,shared.pong);
    else
        addReplyBulk(c,c->argv[1]);
}

void echoCommand(client *c) {
    addReplyBulk(c,c->argv[1]);
}

void timeCommand(client *c) {
    struct timeval tv;

    /* gettimeofday() can only fail if &tv is a bad address so we
     * don't check for errors. */
    gettimeofday(&tv,NULL);
    addReplyMultiBulkLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
}

void shutdownCommand(client *c) {
    int flags = 0;

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"nosave")) {
            flags |= SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[1]->ptr,"save")) {
            flags |= SHUTDOWN_SAVE;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to save
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data).
     */
    if (server.loading)
        flags = (flags & ~SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE;
    if (prepareForShutdown(flags) == DISCNT_OK) exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

void lastsaveCommand(client *c) {
    addReplyLongLong(c,server.lastsave);
}

/* Helper function for addReplyCommand() to output flags. */
int addReplyCommandFlag(client *c, struct serverCommand *cmd, int f, char *reply) {
    if (cmd->flags & f) {
        addReplyStatus(c, reply);
        return 1;
    }
    return 0;
}

/* Output the representation of a Discnt command. Used by the COMMAND command.*/
void addReplyCommand(client *c, struct serverCommand *cmd) {
    if (!cmd) {
        addReply(c, shared.nullbulk);
    } else {
        /* We are adding: command name, arg count, flags, first, last, offset */
        addReplyMultiBulkLen(c, 6);
        addReplyBulkCString(c, cmd->name);
        addReplyLongLong(c, cmd->arity);

        int flagcount = 0;
        void *flaglen = addDeferredMultiBulkLength(c);
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_WRITE, "write");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_READONLY, "readonly");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_DENYOOM, "denyoom");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_ADMIN, "admin");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_RANDOM, "random");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_LOADING, "loading");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_SKIP_MONITOR, "skip_monitor");
        flagcount += addReplyCommandFlag(c,cmd,DISCNT_CMD_FAST, "fast");
        setDeferredMultiBulkLength(c, flaglen, flagcount);

        addReplyLongLong(c, cmd->firstkey);
        addReplyLongLong(c, cmd->lastkey);
        addReplyLongLong(c, cmd->keystep);
    }
}

/* COMMAND <subcommand> <args> */
void commandCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    if (c->argc == 1) {
        addReplyMultiBulkLen(c, dictSize(server.commands));
        di = dictGetIterator(server.commands);
        while ((de = dictNext(di)) != NULL) {
            addReplyCommand(c, dictGetVal(de));
        }
        dictReleaseIterator(di);
    } else if (!strcasecmp(c->argv[1]->ptr, "info")) {
        int i;
        addReplyMultiBulkLen(c, c->argc-2);
        for (i = 2; i < c->argc; i++) {
            addReplyCommand(c, dictFetchValue(server.commands, c->argv[i]->ptr));
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "count") && c->argc == 2) {
        addReplyLongLong(c, dictSize(server.commands));
    } else {
        addReplyError(c, "Unknown subcommand or wrong number of arguments.");
        return;
    }
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        sprintf(s,"%.2fT",d);
    } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024*1024);
        sprintf(s,"%.2fP",d);
    } else {
        /* Let's hope we never need this */
        sprintf(s,"%lluB",n);
    }
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genDiscntInfoString(char *section) {
    sds info = sdsempty();
    time_t uptime = server.unixtime-server.stat_starttime;
    int j, numcommands;
    struct rusage self_ru, c_ru;
    unsigned long lol, bib;
    int allsections = 0, defsections = 0;
    int sections = 0;

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);
    getClientsMaxBuffers(&lol,&bib);

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        info = sdscatprintf(info,
            "# Server\r\n"
            "discnt_version:%s\r\n"
            "discnt_git_sha1:%s\r\n"
            "discnt_git_dirty:%d\r\n"
            "discnt_build_id:%llx\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "hz:%d\r\n"
            "config_file:%s\r\n",
            DISCNT_VERSION,
            discntGitSHA1(),
            strtol(discntGitDirty(),NULL,10) > 0,
            (unsigned long long) discntBuildId(),
            name.sysname, name.release, name.machine,
            server.arch_bits,
            aeGetApiName(),
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            server.runid,
            server.port,
            (intmax_t)uptime,
            (intmax_t)(uptime/(3600*24)),
            server.hz,
            server.configfile ? server.configfile : "");
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section,"clients")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Clients\r\n"
            "connected_clients:%lu\r\n"
            "client_longest_output_list:%lu\r\n"
            "client_biggest_input_buf:%lu\r\n"
            "blocked_clients:%d\r\n",
            listLength(server.clients),
            lol, bib,
            server.bpop_blocked_clients);
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section,"memory")) {
        char hmem[64];
        char peak_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();

        /* Peak memory is updated from time to time by serverCron() so it
         * may happen that the instantaneous value is slightly bigger than
         * the peak value. This may confuse users, so we update the peak
         * if found smaller than the current memory usage. */
        if (zmalloc_used > server.stat_peak_memory)
            server.stat_peak_memory = zmalloc_used;

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(peak_hmem,server.stat_peak_memory);
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_allocator:%s\r\n",
            zmalloc_used,
            hmem,
            server.resident_set_size,
            server.stat_peak_memory,
            peak_hmem,
            zmalloc_get_fragmentation_ratio(server.resident_set_size),
            ZMALLOC_LIB
            );
    }

    /* Counters */
    if (allsections || defsections || !strcasecmp(section,"counters")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Counters\r\n"
            "counters:%lu\r\n"
            "hits:%lld\r\n"
            "misses:%lld\r\n",
            dictSize(server.counters),
            server.stat_hits,
            server.stat_misses);
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section,"persistence")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Persistence\r\n"
            "loading:%d\r\n"
            "ddb_changes_since_last_save:%lld\r\n"
            "ddb_bgsave_in_progress:%d\r\n"
            "ddb_last_save_time:%jd\r\n"
            "ddb_last_bgsave_status:%s\r\n"
            "ddb_last_bgsave_time_sec:%jd\r\n"
            "ddb_current_bgsave_time_sec:%jd\r\n",
            server.loading,
            server.dirty,
            server.ddb_child_pid != -1,
            (intmax_t)server.lastsave,
            (server.lastbgsave_status == DISCNT_OK) ? "ok" : "err",
            (intmax_t)server.ddb_save_time_last,
            (intmax_t)((server.ddb_child_pid == -1) ?
                -1 : time(NULL)-server.ddb_save_time_start));

        if (server.loading) {
            double perc;
            time_t eta, elapsed;
            off_t remaining_bytes = server.loading_total_bytes-
                                    server.loading_loaded_bytes;

            perc = ((double)server.loading_loaded_bytes /
                   server.loading_total_bytes) * 100;

            elapsed = server.unixtime-server.loading_start_time;
            if (elapsed == 0) {
                eta = 1; /* A fake 1 second figure if we don't have
                            enough info */
            } else {
                eta = (elapsed*remaining_bytes)/server.loading_loaded_bytes;
            }

            info = sdscatprintf(info,
                "loading_start_time:%jd\r\n"
                "loading_total_bytes:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) server.loading_start_time,
                (unsigned long long) server.loading_total_bytes,
                (unsigned long long) server.loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
        }
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "latest_fork_usec:%lld\r\n",
            server.stat_numconnections,
            server.stat_numcommands,
            getInstantaneousMetric(DISCNT_METRIC_COMMAND),
            server.stat_net_input_bytes,
            server.stat_net_output_bytes,
            (float)getInstantaneousMetric(DISCNT_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(DISCNT_METRIC_NET_OUTPUT)/1024,
            server.stat_rejected_conn,
            server.stat_fork_time);
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section,"cpu")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%.2f\r\n"
        "used_cpu_user:%.2f\r\n"
        "used_cpu_sys_children:%.2f\r\n"
        "used_cpu_user_children:%.2f\r\n",
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000);
    }

    /* cmdtime */
    if (allsections || !strcasecmp(section,"commandstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Commandstats\r\n");
        numcommands = sizeof(serverCommandTable)/sizeof(struct serverCommand);
        for (j = 0; j < numcommands; j++) {
            struct serverCommand *c = serverCommandTable+j;

            if (!c->calls) continue;
            info = sdscatprintf(info,
                "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f\r\n",
                c->name, c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls));
        }
    }

    return info;
}

void infoCommand(client *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    addReplyBulkSds(c, genDiscntInfoString(section));
}

void monitorCommand(client *c) {
    /* ignore MONITOR if already slave or in monitor mode */
    if (c->flags & DISCNT_MONITOR) return;

    c->flags |= DISCNT_MONITOR;
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
}

/* ============================ Maxmemory directive  ======================== */

/* Get the memory warning level:
 *
 * 0 - No memory warning.
 * 1 - Over 75% of maxmemory setting.
 * 2 - Over 95% of maxmemory setting.
 * 3 - Over 95% of maxmemory setting and RSS over 100% of maxmemory setting.
 *
 * The server may change behavior depending on the memory warning level.
 * For example at warning >= 1, new jobs are only replicated to
 * external nodes and a localy copy is not retained. */
int getMemoryWarningLevel(void) {
    size_t mem_used = zmalloc_used_memory();

    if (mem_used > server.maxmemory / 100 * 95 &&
        mem_used > server.resident_set_size) return 3;
    if (mem_used > server.maxmemory / 100 * 95) return 2;
    if (mem_used > server.maxmemory / 100 * 75) return 1;
    return 0;
}

/* =================================== Main! ================================ */

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxOvercommitMemoryWarning(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        serverLog(LL_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
}
#endif /* __linux__ */

void createPidFile(void) {
    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Discnt is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version(void) {
    printf("Discnt server v=%s sha=%s:%d malloc=%s bits=%d build=%llx\n",
        DISCNT_VERSION,
        discntGitSHA1(),
        atoi(discntGitDirty()) > 0,
        ZMALLOC_LIB,
        sizeof(long) == 4 ? 32 : 64,
        (unsigned long long) discntBuildId());
    exit(0);
}

void usage(void) {
    fprintf(stderr,"Usage: ./discnt-server [/path/to/discnt.conf] [options]\n");
    fprintf(stderr,"       ./discnt-server - (read config from stdin)\n");
    fprintf(stderr,"       ./discnt-server -v or --version\n");
    fprintf(stderr,"       ./discnt-server -h or --help\n");
    fprintf(stderr,"       ./discnt-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./discnt-server (run the server with default conf)\n");
    fprintf(stderr,"       ./discnt-server /etc/discnt/5262.conf\n");
    fprintf(stderr,"       ./discnt-server --port 5555\n");
    fprintf(stderr,"       ./discnt-server --port 5555 --slaveof 127.0.0.1 6666\n");
    fprintf(stderr,"       ./discnt-server /etc/mydiscnt.conf --loglevel verbose\n\n");
    exit(1);
}

void discntAsciiArt(void) {
#include "asciilogo.h"
    char *buf = zmalloc(1024*16);

    if (server.syslog_enabled) {
        serverLog(LL_NOTICE,
            "Discnt %s (%s/%d) %s bit, port %d, pid %ld ready to start.",
            DISCNT_VERSION,
            discntGitSHA1(),
            strtol(discntGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            server.port,
            (long) getpid()
        );
    } else {
        snprintf(buf,1024*16,ascii_logo,
            DISCNT_VERSION,
            discntGitSHA1(),
            strtol(discntGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            server.port,
            (long) getpid()
        );
        serverLogRaw(LL_NOTICE|DISCNT_LOG_RAW,buf);
    }
    zfree(buf);
}

static void sigShutdownHandler(int sig) {
    char *msg;

    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT scheduling shutdown...";
        break;
    case SIGTERM:
        msg = "Received SIGTERM scheduling shutdown...";
        break;
    default:
        msg = "Received shutdown signal, scheduling shutdown...";
    };

    /* SIGINT is often delivered via Ctrl+C in an interactive session.
     * If we receive the signal the second time, we interpret this as
     * the user really wanting to quit ASAP without waiting to persist
     * on disk. */
    if (server.shutdown_asap && sig == SIGINT) {
        serverLogFromHandler(LL_WARNING, "You insist... exiting now.");
        ddbRemoveTempFile(getpid());
        exit(1); /* Exit with an error since this was not a clean shutdown. */
    } else if (server.loading) {
        exit(0);
    }

    serverLogFromHandler(LL_WARNING, msg);
    server.shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

void memtest(size_t megabytes, int passes);

void loadDataFromDisk(void) {
  long long start = ustime();
  if (ddbLoad(server.ddb_filename) == DISCNT_OK) {
    serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",
        (float)(ustime()-start)/1000000);
  } else if (errno != ENOENT) {
    serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
    exit(1);
  }
}

void serverOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("Discnt aborting for OUT OF MEMORY");
}

void serverSetProcTitle(char *title) {
#ifdef USE_SETPROCTITLE
    setproctitle("%s %s:%d",
        title,
        server.bindaddr_count ? server.bindaddr[0] : "*",
        server.port);
#else
    DISCNT_NOTUSED(title);
#endif
}

int main(int argc, char **argv) {
    struct timeval tv;

    /* We need to initialize our libraries, and the server configuration. */
#ifdef INIT_SETPROCTITLE_REPLACEMENT
    spt_init(argc, argv);
#endif
    setlocale(LC_COLLATE,"");
    zmalloc_enable_thread_safeness();
    zmalloc_set_oom_handler(serverOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    dictSetHashFunctionSeed(tv.tv_sec^tv.tv_usec^getpid());
    initServerConfig();

    if (argc >= 2) {
        int j = 1; /* First option to parse in argv[] */
        sds options = sdsempty();
        char *configfile = NULL;

        /* Handle special options --help and --version */
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0 ||
            strcmp(argv[1], "-h") == 0) usage();
        if (strcmp(argv[1], "--test-memory") == 0) {
            if (argc == 3) {
                memtest(atoi(argv[2]),50);
                exit(0);
            } else {
                fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n");
                fprintf(stderr,"Example: ./discnt-server --test-memory 4096\n\n");
                exit(1);
            }
        }

        /* First argument is the config file name? */
        if (argv[j][0] != '-' || argv[j][1] != '-')
            configfile = argv[j++];
        /* All the other options are parsed and conceptually appended to the
         * configuration file. For instance --port 6380 will generate the
         * string "port 6380\n" to be parsed after the actual file name
         * is parsed, if any. */
        while(j != argc) {
            if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (sdslen(options)) options = sdscat(options,"\n");
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            } else {
                /* Option argument */
                options = sdscatrepr(options,argv[j],strlen(argv[j]));
                options = sdscat(options," ");
            }
            j++;
        }
        if (configfile) server.configfile = getAbsolutePath(configfile);
        loadServerConfig(configfile,options);
        sdsfree(options);
    } else {
        serverLog(LL_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/discnt.conf", argv[0]);
    }
    if (server.daemonize) daemonize();
    initServer();
    if (server.daemonize) createPidFile();
    serverSetProcTitle(argv[0]);
    discntAsciiArt();

    /* Things not needed when running in Sentinel mode. */
    serverLog(LL_WARNING,"Server started, Discnt version " DISCNT_VERSION);
#ifdef __linux__
    linuxOvercommitMemoryWarning();
#endif
    checkTcpBacklogSettings();
    loadDataFromDisk();
    if (server.ipfd_count > 0)
        serverLog(LL_NOTICE,"The server is now ready to accept connections on port %d", server.port);
    if (server.sofd > 0)
        serverLog(LL_NOTICE,"The server is now ready to accept connections at %s", server.unixsocket);

    /* Warning the user about suspicious maxmemory setting. */
    if (server.maxmemory > 0 && server.maxmemory < 1024*1024) {
        serverLog(LL_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory);
    }

    aeSetBeforeSleepProc(server.el,beforeSleep);
    /* Make sure to do a cron run before 1st client is served, so that we
     * can do periodic tasks there that we can assume to always be executed
     * at least once before the server starts serving queries. */
    serverCron(NULL,0,NULL);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;
}

/* The End */
