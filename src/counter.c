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


void counterHistoryUpdate(counter *cntr) {
    cntr->history[server.history_index] = cntr->myshard->value;
}

/* Calculate the change for our shard.
 * history_last_index is the newest item in the history.
 * server.history_index is the oldest item in the history.
 */
long double counterHistoryChange(counter *cntr, mstime_t now, int history_last_index) {
    UNUSED(now);
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

    return change;
}

/* Make a new prediction for our shard. */
void counterPredict(counter *cntr, mstime_t now, long double change) {
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
    /* pubsubUnsubscribeCounter will set cntr->subscribers to NULL */
    while (cntr->subscribers != NULL) {
        ln = listFirst(cntr->subscribers);
        p  = listNodeValue(ln);
        pubsubUnsubscribeCounter(p->c,cntr->name,1);
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
counter *counterCreate(const sds name) {
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
void addReplyCounter(client *c, counter *cntr) {
    addReplyLongDouble(c, cntr->value);
}

void countersUpdateValue(counter *cntr) {
    mstime_t now = mstime();
    long double elapsed;
    listNode *ln;
    listIter li;
    shard *shrd;

    cntr->value = 0;

    listRewind(cntr->shards,&li);
    while ((ln = listNext(&li)) != NULL) {
        shrd = listNodeValue(ln);

        /* Don't do a prediction with our own shard. */
        if (shrd == cntr->myshard) {
            cntr->value += shrd->value;
            continue;
        }

        elapsed = now - shrd->predict_time;
        shrd->value = shrd->predict_value + (elapsed * shrd->predict_change);

        /*if (shrd->node == NULL || nodeFailed(shrd->node)) {
            serverLog(LL_DEBUG,"Counter %s not updating shard of %.40s",
                cntr->name, shrd->node_name);
        } else if (shrd->predict_time > 0 && shrd->predict_value != 0) {
            serverLog(LL_DEBUG,"Counter %s new value %Lf for shard %.40s",
                cntr->name, shrd->value, shrd->node_name);
        } else {
            serverLog(LL_DEBUG,"Counter %s not using shard of %.40s %llu %Lf",
                cntr->name, shrd->node_name, shrd->predict_time, shrd->predict_value);
        }*/

        cntr->value += shrd->value;
    }
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
        countersUpdateValue(cntr);
        if (p->lastvalue == cntr->value) {
            continue;
        }

        addReply(p->c,shared.mbulkhdr[3]);
        addReply(p->c,shared.messagebulk);
        addReplyBulkCBuffer(p->c,cntr->name,sdslen(cntr->name));
        addReplyCounter(p->c, cntr);

        p->next      = now + p->seconds*1000;
        p->lastvalue = cntr->value;
    }
}

void counterResetShard(counter *cntr, clusterNode *node) {
    listNode *ln;
    listIter li;

    listRewind(cntr->shards,&li);
    while ((ln = listNext(&li)) != NULL) {
        shard *shrd = listNodeValue(ln);

        if (shrd->node == node) {
            cntr->value -= shrd->value;

            shrd->value = 0;
            if (shrd == cntr->myshard) {
                shrd->predict_time = 0;
            } else {
                shrd->predict_time = mstime();
            }
            shrd->predict_value = 0;
            shrd->predict_change = 0;

            server.dirty++;
            return;
        }
    }
}

int counterShardsForNode(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;
    int count = 0;

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
                count++;
            }
        }
    }
    dictReleaseIterator(it);

    return count;
}

void counterDeleteShards(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;

    it = dictGetSafeIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        counter *cntr;
        listIter li;
        listNode *ln;

        cntr = dictGetVal(de);

        listRewind(cntr->shards, &li);
        while ((ln = listNext(&li)) != NULL) {
            shard *shrd = listNodeValue(ln);
            if (shrd->node == node) {
                if (shrd == cntr->myshard) {
                    cntr->myshard = NULL;
                }
                listDelNode(cntr->shards, ln);
            }
        }
    }
    dictReleaseIterator(it);
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of counters existing. */
void existsCommand(client *c) {
    long long count = 0;
    int j;

    for (j = 1; j < c->argc; j++) {
        if (counterLookup(c->argv[j]->ptr) != NULL) count++;
    }
    addReplyLongLong(c,count);
}

/* -----------------------------------------------------------------------------
 * Counter related commands
 * -------------------------------------------------------------------------- */

void genericIncrCommand(client *c, const sds name, long double increment) {
    counter *cntr;

    cntr = counterLookup(name);
    if (cntr == NULL) {
        cntr = counterCreate(name);
    }

    if (cntr->myshard == NULL) {
        counterAddShard(cntr, myself, myself->name);
    }

    long double newshardvalue = cntr->myshard->value + increment;
    if (newshardvalue > COUNTER_MAX) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    } else if (newshardvalue < COUNTER_MIN) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    }

    countersUpdateValue(cntr);

    long double newvalue = cntr->value + increment;
    if (newvalue > COUNTER_MAX) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    } else if (newvalue < COUNTER_MIN) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    }

    cntr->myshard->value = newshardvalue;
    cntr->value = newvalue;
    server.dirty++;
    addReplyCounter(c,cntr);
}

void incrCommand(client *c) {
    genericIncrCommand(c, c->argv[1]->ptr, 1.0);
}

void incrbyCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, c->argv[1]->ptr, increment);
}

void incrbyfloatCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, c->argv[1]->ptr, increment);
}

void decrCommand(client *c) {
    genericIncrCommand(c, c->argv[1]->ptr, -1.0);
}

void decrbyCommand(client *c) {
    long double increment;
    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != C_OK)
        return;
    genericIncrCommand(c, c->argv[1]->ptr, -increment);
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

    countersUpdateValue(cntr);

    if (c->argc == 2) {
        addReplyCounter(c,cntr);
    } else if (!strcasecmp(c->argv[2]->ptr,"state")) {
        addReplyMultiBulkLen(c,2);
        addReplyCounter(c,cntr);
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

void mgetCommand(client *c) {
    counter *cntr;
    int j;

    addReplyMultiBulkLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        cntr = counterLookup(c->argv[j]->ptr);

        if (cntr == NULL) {
            addReplyString(c,OBJ_SHARED_0STR,sizeof(OBJ_SHARED_0STR)-1);
        } else {
            addReplyCounter(c,cntr);
        }
    }
}

void setCommand(client *c) {
    counter *cntr;
    long double value;
    unsigned int i;

    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&value,NULL) != C_OK)
        return;

    if (value > COUNTER_MAX) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    } else if (value < COUNTER_MIN) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    }

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    }

    if (cntr->myshard == NULL) {
        counterAddShard(cntr,myself,myself->name);
    }

    countersUpdateValue(cntr);

    long double newshardvalue = value - (cntr->value - cntr->myshard->value);
    if (newshardvalue > COUNTER_MAX) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    } else if (newshardvalue < COUNTER_MIN) {
        addReplyErrorFormat(c, "Counter value out of allowed range");
        return;
    }

    /* myshard->value        = 4
     * cntr->value           = 10
     * value                 = 2
     * value in other shards = 6
     * new myshard->value    = 2 - (10 - 4) = -4
     */
    cntr->myshard->value = newshardvalue;

    /* Force a new prediction to be send. */
    cntr->myshard->predict_time = 0;

    /* Make sure the prediction is 0 so it doesn't change every second. */
    for (i = 0; i < server.history_size; i++) {
        cntr->history[i] = newshardvalue;
    }

    cntr->value = value;
    server.dirty++;
    addReplyCounter(c,cntr);
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

void mincrbyCommand(client *c) {
    int j;
    long double increment;

    if (getLongDoubleFromObjectOrReply(c,c->argv[1],&increment,NULL) != C_OK)
        return;

    addReplyMultiBulkLen(c,c->argc-2);
    for (j = 2; j < c->argc; j++) {
        genericIncrCommand(c, c->argv[j]->ptr, increment);
    }
}

/* -----------------------------------------------------------------------------
 * Cluster related
 * -------------------------------------------------------------------------- */

void countersSync(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;

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
        long double rvalue, elapsed, diff, change;
        counter *cntr;
        int miss;

        cntr = dictGetVal(de);

        counterPubSub(cntr, now);

        if (cntr->myshard == NULL) {
            continue;
        }

        change = counterHistoryChange(cntr, now, history_last_index);

        if (cntr->myshard->predict_time > 0) {
            miss = 0;

            /* When our shard doesn't change anymore we should make sure it's predicted value
             * matches the actual value otherwise we never have a mis-prediction and
             * other instances will keep using our wrongly predicted value */
            if (change == 0 && (cntr->myshard->predict_change != 0 || cntr->myshard->predict_value != cntr->myshard->value)) {
                miss = 1;

                serverLog(LL_DEBUG,"Counter %s needs a final prediction (%Lf != %Lf)",
                      cntr->name, cntr->myshard->predict_value, cntr->myshard->value);
            } else {
                elapsed = (now - cntr->myshard->predict_time);
                rvalue = cntr->myshard->predict_value + (elapsed * cntr->myshard->predict_change);
                diff = fabsl(rvalue - cntr->myshard->value);

                if (diff > cntr->precision) {
                    miss = 1;

                    serverLog(LL_DEBUG,"Counter %s needs new prediction (%Lf <= %f)",
                        cntr->name, diff, cntr->precision);
                }
            }

            if (miss == 0) {
                /* It's still up to date, check if we need to resend our last prediction. */
                counterMaybeResend(cntr);

                /* Only count hits for when something actually changed but
                 * is still within our last prediction. */
                if (cntr->history[history_last_index] != cntr->myshard->value) {
                    server.stat_hits++;
                    cntr->hits++;
                }

                counterHistoryUpdate(cntr);

                continue;
            }
        }

        /* Make a new prediction. */

        server.stat_misses++;
        cntr->misses++;
        cntr->revision++;

        counterPredict(cntr, now, change);
        clusterSendShard(cntr);

        counterHistoryUpdate(cntr);

        cntr->updated = mstime();
    }
    dictReleaseIterator(it);
}

/* This is executed every second */
void countersCleanupCron(void) {
    dictIterator *it;
    dictEntry *de;
    mstime_t notafter = mstime() - 2000; /* Don't delete counters that changed in the last N milliseconds. */

    it = dictGetSafeIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        counter *cntr;
        listIter li;
        listNode *ln;

        cntr = dictGetVal(de);

        // Don't delete counters that might haven't synced yet.
        if (cntr->want_acks != NULL && dictSize(cntr->want_acks) > 0) {
            continue;
        }

        listRewind(cntr->shards, &li);
        while ((ln = listNext(&li)) != NULL) {
            shard *shrd = listNodeValue(ln);
            if (shrd->predict_time == 0 || shrd->predict_time > notafter) {
                continue;
            }
            if (shrd->value == 0 && shrd->predict_value == 0 && shrd->predict_change == 0) {
                if (shrd == cntr->myshard) {
                    cntr->myshard = NULL;
                }
                listDelNode(cntr->shards, ln);
            }
        }

        if (listLength(cntr->shards) == 0) {
            /* Don't delete counters that have subscribers as they keep
             * a pointer to the counter.
             */
            if (cntr->subscribers == NULL) {
                dictDelete(server.counters, cntr->name);
            }
        }
    }
    dictReleaseIterator(it);
}

