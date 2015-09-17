/*
 * Copyright (c) 2015, Erik Dubbelboer <erik at dubbelboer dot com>
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

#include "server.h"
#include "counter.h"
#include "endianconv.h"

#include <math.h>


void counterUpdateHistory(counter *cntr) {
    cntr->history[server.history_index] = cntr->myshard->value;
}

/* Make a new prediction for our shard.
 * history_last_index is the newest item in the history.
 * server.history_index is the oldest item in the history.
 */
void counterPredict(counter *cntr, mstime_t now, int history_last_index) {
    UNUSED(history_last_index);
    long double change = 0, last = cntr->myshard->value;

    /* Sum the difference of the last server.history_size+1 seconds:
     *
     * for (i = history_last_index; i >= 0; i--) {
     *     change += (last - cntr->history[i]);
     *     last    = cntr->history[i];
     * }
     * for (i = server.history_size - 1; i > history_last_index; i--) {
     *     change += (last - cntr->history[i]);
     *     last    = cntr->history[i];
     * }
     *
     * This is the same as:
     */
    change = last - cntr->history[server.history_index];

    cntr->myshard->predict_time   = now;
    cntr->myshard->predict_value  = cntr->myshard->value;
    cntr->myshard->predict_change = change / ((long double)(server.history_size) * 1000.0);
}

void counterClearWantAcks(counter *cntr) {
    if (cntr->want_acks == NULL) {
        cntr->want_acks = dictCreate(&clusterNodesDictType, NULL);
    } else {
        dictEmpty(cntr->want_acks, NULL);
    }
}

void counterWantAck(counter *cntr, const clusterNode *node) {
    serverAssert(dictAdd(cntr->want_acks,(void*)node->name,NULL) == DICT_OK);
}

void counterGotAck(counter *cntr, const clusterNode *node) {
    dictDelete(cntr->want_acks, node->name);
    if (htNeedsResize(cntr->want_acks)) dictResize(cntr->want_acks);
}

void dictCounterDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    listNode *ln;
    listIter li;
    counter *cntr = val;
    pubsub *p;

    /* Unsubscribe everyone */
    if (cntr->subscribers != NULL) {
        while (listLength(cntr->subscribers)) {
            ln = listFirst(cntr->subscribers);
            p  = listNodeValue(ln);
            pubsubUnsubscribeCounter(p->c,cntr->name,1);
        }
    }

    /* Free all shards */
    listRewind(cntr->shards,&li);
    while ((ln = listNext(&li)) != NULL) {
        shard *shrd = listNodeValue(ln);
        zfree(shrd);
    }

    listRelease(cntr->shards);
    if (cntr->want_acks) dictRelease(cntr->want_acks);

    /* cntr->name will be freed by the dict code. */

    zfree(cntr);
}

counter *counterLookup(const sds name) {
    dictEntry *de = dictFind(server.counters, name);
    if (de) {
        return dictGetVal(de);
    } else {
        return NULL;
    }
}

/* Create a counter and add it to server.counters. */
counter *counterCreate(sds name) {
    counter *cntr = zcalloc(sizeof(counter) + (sizeof(long double) * server.history_size));
    cntr->name      = sdsdup(name);
    cntr->shards    = listCreate();
    cntr->history   = (long double*)(cntr + 1);
    cntr->precision = server.default_precision;

    serverAssert(dictAdd(server.counters, cntr->name, cntr) == DICT_OK);

    return cntr;
}

/* node can be NULL so node_name is a seperate parameter. */
shard *counterAddShard(counter *cntr, clusterNode* node, const char *node_name) {
    shard *shrd = zcalloc(sizeof(shard));
    shrd->node = node;
    memcpy(shrd->node_name,node_name,CLUSTER_NAMELEN);
    if (node == myself) {
        serverAssert(cntr->myshard == NULL);
        cntr->myshard = shrd;
    }
    listAddNodeTail(cntr->shards, shrd);
    return shrd;
}

/* Build the counter's cached response buffer. */
void counterCacheResponse(counter *cntr) {
    char dbuf[128];
    int dlen;

    if (isinf(cntr->value)) {
        if (cntr->value > 0) {
            strcpy(cntr->rbuf, OBJ_SHARED_INF); /* inf */
            cntr->rlen = sizeof(OBJ_SHARED_INF)-1;
        } else {
            strcpy(cntr->rbuf, OBJ_SHARED_NINF); /* -inf */
            cntr->rlen = sizeof(OBJ_SHARED_NINF)-1;
        }
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17Lg",cntr->value);
        cntr->rlen = snprintf(cntr->rbuf,sizeof(cntr->rbuf),"$%d\r\n%s\r\n",dlen,dbuf);
    }
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

            /* Don't do a prediction with our own shard. */
            if (shrd == cntr->myshard) {
                value += shrd->value;
                continue;
            }

            /* Don't update predictions for failing nodes. */
            if (shrd->node == NULL || nodeFailed(shrd->node)) {
                /*serverLog(LL_DEBUG,"Counter %s not using shard of %.40s",
                    cntr->name, shrd->node_name);*/
                continue;
            }

            if (shrd->predict_time > 0 && shrd->predict_value != 0) {
                elapsed = now - shrd->predict_time;
                shrd->value = shrd->predict_value + (elapsed * shrd->predict_change);

                /*serverLog(LL_DEBUG,"Counter %s new value %Lf for shard %.40s",
                    cntr->name, shrd->value, shrd->node_name);
            } else {
                serverLog(LL_DEBUG,"Counter %s not using shard of %.40s %llu %Lf",
                    cntr->name, shrd->node_name, shrd->predict_time, shrd->predict_value);*/
            }

            value += shrd->value;
        }

        if (cntr->value != value) {
            cntr->value = value;

            /* Make sure the cached response gets recalculated. */
            cntr->rlen = 0;
        }
    }
    dictReleaseIterator(it);
}

void counterPubSub(counter *cntr, mstime_t now) {
    listNode *ln;
    listIter li;

    if (cntr->subscribers == NULL)
        return;

    listRewind(cntr->subscribers,&li);
    while ((ln = listNext(&li)) != NULL) {
        pubsub *p = listNodeValue(ln);

        if (p->next > now) {
            continue;
        }
        if (p->lastvalue == cntr->value) {
            continue;
        }

        addReply(p->c,shared.mbulkhdr[3]);
        addReply(p->c,shared.messagebulk);
        addReplyBulkCBuffer(p->c,cntr->name,sdslen(cntr->name));
        addReplyLongDouble(p->c, cntr->value);

        p->next      = now + p->seconds*1000;
        p->lastvalue = cntr->value;
    }
}

/* -----------------------------------------------------------------------------
 * Counter related commands
 * -------------------------------------------------------------------------- */

void genericIncrCommand(client *c, long double increment) {
    counter *cntr;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    }

    if (cntr->myshard == NULL) {
        counterAddShard(cntr, myself, myself->name);
    }

    cntr->myshard->value += increment;
    cntr->value += increment;
    server.dirty++;
    counterCacheResponse(cntr);
    addReplyString(c,cntr->rbuf,cntr->rlen);
}

void incrCommand(client *c) {
    genericIncrCommand(c, 1.0);
}

void incrbyCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, increment);
}

void incrbyfloatCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, increment);
}

void decrCommand(client *c) {
    genericIncrCommand(c, -1.0);
}

void decrbyCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, -increment);
}

void getCommand(client *c) {
    counter *cntr;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        if (c->argc == 2) {
            addReplyString(c,OBJ_SHARED_0STR,sizeof(OBJ_SHARED_0STR)-1);
        } else if (!strcasecmp(c->argv[2]->ptr,"state")) {
            addReplyMultiBulkLen(c,2);
            addReplyString(c,OBJ_SHARED_0STR,sizeof(OBJ_SHARED_0STR)-1);
            if (server.cluster->failing_nodes_count > 0) {
                addReplyString(c, OBJ_SHARED_INCONSISTENT, sizeof(OBJ_SHARED_INCONSISTENT)-1);
            } else {
                addReplyString(c, OBJ_SHARED_CONSISTENT, sizeof(OBJ_SHARED_CONSISTENT)-1);
            }
        }
        return;
    }

    /* Do we need to recalculate the cached response? */
    if (cntr->rlen == 0) {
        counterCacheResponse(cntr);
    }

    if (c->argc == 2) {
        addReplyString(c,cntr->rbuf,cntr->rlen);
    } else if (!strcasecmp(c->argv[2]->ptr,"state")) {
        addReplyMultiBulkLen(c,2);
        addReplyString(c,cntr->rbuf,cntr->rlen);
        if (server.cluster->failing_nodes_count > 0) {
            addReplyString(c, OBJ_SHARED_INCONSISTENT, sizeof(OBJ_SHARED_INCONSISTENT)-1);
        } else {
            addReplyString(c, OBJ_SHARED_CONSISTENT, sizeof(OBJ_SHARED_CONSISTENT)-1);
        }
    } else {
        addReplyErrorFormat(c, "Unknown GET option '%s'",
            (char*)c->argv[2]->ptr);
    }
}

void setCommand(client *c) {
    counter *cntr;
    long double value;
    unsigned int i;

    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&value,NULL) != C_OK)
        return;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    }

    if (cntr->myshard == NULL) {
        counterAddShard(cntr,myself,myself->name);
    }

    /* myshard->value        = 4
     * cntr->value           = 10
     * value                 = 2
     * value in other shards = 6
     * new myshard->value    = 2 - (10 - 4) = -4
     */
    cntr->myshard->value = value - (cntr->value - cntr->myshard->value);

    /* Force a new prediction to be send. */
    cntr->myshard->predict_time = 0;

    /* Make sure the prediction is 0 so it doesn't change every second. */
    for (i = 0; i < server.history_size; i++) {
        cntr->history[i] = cntr->myshard->value;
    }

    cntr->value = value;
    server.dirty++;
    counterCacheResponse(cntr);
    addReplyString(c,cntr->rbuf,cntr->rlen);
}

void precisionCommand(client *c) {
    counter *cntr;
    double precision;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL && c->argc == 2) {
        /* Counter doesn't exist, return the default precision. */
        addReplyDouble(c, server.default_precision);
        return;
    }

    if (c->argc == 2) {
        addReplyDouble(c, cntr->precision);
        return;
    }

    if (getDoubleFromObjectOrReply(c, c->argv[2], &precision, NULL) != C_OK)
        return;

    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    }

    cntr->precision = precision;
    server.dirty++;
    addReplyDouble(c, cntr->precision);
}

void keysCommand(client *c) {
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

/* -----------------------------------------------------------------------------
 * Cluster related
 * -------------------------------------------------------------------------- */

void countersClusterAddNode(clusterNode *node) {
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

        /* If we have our own shard make sure to send it's prediction
         * to the new node. */
        if (cntr->myshard) {
            clusterSendShardToNode(cntr, node);
        }

        listRewind(cntr->shards,&li);
        while ((ln = listNext(&li)) != NULL) {
            shrd = listNodeValue(ln);

            if (memcmp(shrd->node_name, node->name, CLUSTER_NAMELEN) == 0) {
                shrd->node = node;
            }
        }
    }
    dictReleaseIterator(it);
}

/* Check if we need to resend our prediction of the counter to some nodes. */
void counterMaybeResend(counter *cntr) {
    clusterNode *node;
    int i;

    /* No acks to send? */
    if (cntr->want_acks == NULL || dictSize(cntr->want_acks) == 0) {
        return;
    }

    /* Too soon? */
    if (cntr->updated > mstime()-5000) {
        return;
    }

    /* Only check reachable nodes that have a valid link. */
    for (i = 0; i < server.cluster->reachable_nodes_count; i++) {
        node = server.cluster->reachable_nodes[i];

        if (node->link == NULL) continue;

        if (dictFind(cntr->want_acks, node->name) != NULL) {
            clusterSendShardToNode(cntr, node);
        }
    }
}

/* -----------------------------------------------------------------------------
 * COUNTERS cron job
 * -------------------------------------------------------------------------- */

/* This is executed every second */
void countersCron(void) {
    int history_last_index;
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    history_last_index = server.history_index;
    server.history_index = (server.history_index + 1) % server.history_size;

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double rvalue, elapsed;
        double diff;
        counter *cntr;

        cntr = dictGetVal(de);

        counterPubSub(cntr, now);

        if (cntr->myshard == NULL) {
            continue;
        }

        if (cntr->myshard->predict_time > 0) {
            elapsed = (now - cntr->myshard->predict_time);
            rvalue = cntr->myshard->predict_value + (elapsed * cntr->myshard->predict_change);
            diff = fabs((double)(rvalue - cntr->myshard->value));

            if (diff <= cntr->precision) {
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
            } else {
                serverLog(LL_DEBUG,"Counter %s needs new prediction (%f <= %f)",
                    cntr->name, diff, cntr->precision);
            }
        }

        /* Make a new prediction. */

        server.stat_misses++;
        cntr->misses++;
        cntr->revision++;

        counterPredict(cntr, now, history_last_index);
        clusterSendShard(cntr);

        counterUpdateHistory(cntr);

        cntr->updated = mstime();
    }
    dictReleaseIterator(it);
}

