/*
 * Copyright (c) 2014, Erik Dubbelboer <erik at dubbelboer dot com>
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
#include "counter.h"
#include "endianconv.h"

#include <math.h>


void counterUpdateHistory(counter *cntr) {
    cntr->history[server.history_index] = cntr->myshard->value;
}

void counterPredict(counter *cntr, mstime_t now, int history_last_index) {
    long double change = 0, last = cntr->myshard->value;
    int i;

    for (i = history_last_index; i >= 0; i--) {
        change += (last - cntr->history[i]);
        last    = cntr->history[i];
    }
    for (i = server.history_size - 1; i > history_last_index; i--) {
        change += (last - cntr->history[i]);
        last    = cntr->history[i];
    }

    cntr->myshard->predict_time   = now;
    cntr->myshard->predict_value  = cntr->myshard->value;
    cntr->myshard->predict_change = change / ((long double)(server.history_size) * 1000.0);
}

void counterClearAcks(counter *cntr) {
    if (cntr->acks == NULL) {
        cntr->acks = dictCreate(&clusterNodesDictType, NULL);
    } else {
        dictEmpty(cntr->acks, NULL);
    }
}

void counterAddAck(counter *cntr, clusterNode *node) {
    int retval = dictAdd(cntr->acks, node->name, NULL);
    serverAssert(retval == 0);
}

void counterFree(counter *cntr) {
    listNode *ln;
    listIter li;

    listRewind(cntr->shards,&li);
    while ((ln = listNext(&li)) != NULL) {
        shard *shrd = listNodeValue(ln);

        zfree(shrd);
    }

    listRelease(cntr->shards);

    if (cntr->acks) dictRelease(cntr->acks);

    /* cntr->name will be freed by the dict code. */

    zfree(cntr);
}

void dictCounterDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */

    counterFree(val);
}

counter *counterLookup(sds name) {
    dictEntry *de = dictFind(server.counters, name);
    if (de) {
        return dictGetVal(de);
    } else {
        return NULL;
    }
}

counter *counterCreate(sds name) {
    counter *cntr = zcalloc(sizeof(counter) + (sizeof(long double) * server.history_size));

    cntr->name    = name;
    cntr->shards  = listCreate();
    cntr->history = (long double*)(cntr + 1);

    return cntr;
}

void counterAdd(counter *cntr) {
    int retval = dictAdd(server.counters, cntr->name, cntr);
    serverAssert(retval == 0);
}

void incrCommand(client *c) {
    long double increment;
    counter *cntr;

    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != DISCNT_OK)
        return;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(sdsdup(c->argv[1]->ptr));
        counterAdd(cntr);
    }

    if (cntr->myshard == NULL) {
        cntr->myshard = zcalloc(sizeof(shard));
        cntr->myshard->node = myself;
        cntr->myshard->value = increment;

        memcpy(cntr->myshard->node_name,myself->name,DISCNT_CLUSTER_NAMELEN);

        listAddNodeTail(cntr->shards, cntr->myshard);
    } else {
        cntr->myshard->value += increment;
    }

    cntr->value += increment;

    server.dirty++;

    addReplyLongDouble(c, cntr->value);
}

void getCommand(client *c) {
    long double value = 0;
    counter *cntr;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr) {
        value = cntr->value;
    }

    addReplyLongDouble(c, value);
}

void countersCommand(client *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    di = dictGetSafeIterator(server.counters);
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            addReplyBulk(c,keyobj);
            numkeys++;
            decrRefCount(keyobj);
        }
    }
    dictReleaseIterator(di);
    setDeferredMultiBulkLength(c,replylen,numkeys);
}

void countersAddNode(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;
    
    if (nodeInHandshake(node)) return;

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        counter *cntr;
        listNode *ln;
        listIter li;
        shard *shrd;

        cntr = dictGetVal(de);

        if (cntr->myshard) {
            clusterSendShardToNode(cntr, node);
        }

        listRewind(cntr->shards,&li);
        while ((ln = listNext(&li)) != NULL) {
            shrd = listNodeValue(ln);

            if (memcmp(shrd->node_name, node->name, DISCNT_CLUSTER_NAMELEN) == 0) {
                shrd->node = node;
            }
        }
    }
    dictReleaseIterator(it);
}

void countersNodeFail(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        counter *cntr;
        listNode *ln;
        listIter li;
        shard *shrd;

        cntr = dictGetVal(de);

        listRewind(cntr->shards,&li);
        while ((ln = listNext(&li)) != NULL) {
            shrd = listNodeValue(ln);

            if (shrd->node == node) {
                /* Remove prediction, don't update our prediction
                   for this shard anymore. */
                shrd->predict_time = 0;
            }
        }
    }
    dictReleaseIterator(it);
}

void counterMaybeResend(counter *cntr) {
    int i;
    clusterNode *node;

    if (cntr->acks == NULL || dictSize(cntr->acks) == 0) {
        return;
    }

    if (cntr->updated > mstime()-5000) {
        return;
    }

    for (i = 0; i < server.cluster->reachable_nodes_count; i++) {
        node = server.cluster->reachable_nodes[i];

        if (node->link == NULL) continue;

        if (dictFind(cntr->acks, node->name) != NULL) {
            clusterSendShardToNode(cntr, node);
        }
    }
}

/* -----------------------------------------------------------------------------
 * COUNTERS cron job
 * -------------------------------------------------------------------------- */

/* This is executed every second */
void countersHistoryCron(void) {
    int history_last_index;
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    history_last_index = server.history_index;
    server.history_index = (server.history_index + 1) % server.history_size;

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double rvalue, elapsed;
        counter *cntr;

        cntr = dictGetVal(de);

        if (cntr->myshard == NULL) {
            continue;
        }

        if (cntr->myshard->predict_time > 0) {
            elapsed = (now - cntr->myshard->predict_time);
            rvalue = cntr->myshard->predict_value + (elapsed * cntr->myshard->predict_change);

            if (fabs((double)(rvalue - cntr->myshard->value)) <= server.precision) {
                /* It's still up to date, check if we need to resend our last prediction. */
                counterMaybeResend(cntr);

                /* Only count hits for when something actually changed but
                 * is still within prediction. */
                if (cntr->history[history_last_index] != cntr->myshard->value) {
                    server.stat_hits++;
                    cntr->hits++;
                }

                counterUpdateHistory(cntr);

                continue;
            }
        }

        /* New prediction. */
        cntr->revision++;

        server.stat_misses++;
        cntr->misses++;

        counterPredict(cntr, now, history_last_index);
        clusterSendShard(cntr);

        counterUpdateHistory(cntr);

        cntr->updated = mstime();

        server.dirty++;
    }
    dictReleaseIterator(it);
}

void countersValueCron(void) {
    countersUpdateValues();
}

void countersUpdateValues(void) {
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double elapsed, value = 0;
        counter *cntr;
        listNode *ln;
        listIter li;
        shard *shrd;

        cntr = dictGetVal(de);

        listRewind(cntr->shards,&li);
        while ((ln = listNext(&li)) != NULL) {
            shrd = listNodeValue(ln);

            if (shrd == cntr->myshard) {
                value += shrd->value;
                continue;
            }

            if (shrd->predict_time > 0 && shrd->predict_value > 0) {
                elapsed = (now - shrd->predict_time);
                shrd->value = shrd->predict_value + (elapsed * shrd->predict_change);
            }

            value += shrd->value;
        }

        cntr->value = value;
    }
    dictReleaseIterator(it);
}

