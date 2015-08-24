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
    cntr->history[server.history_index] = cntr->myrepl->value;
}

void counterPredict(counter *cntr, mstime_t now) {
    cntr->myrepl->predict_time   = now;
    cntr->myrepl->predict_value  = cntr->myrepl->value;
    cntr->myrepl->predict_change = (cntr->myrepl->value - cntr->history[server.history_index]) / ((long double)server.history_size * 1000.0);
}

void dictCounterDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    counter* cntr = val;
    listNode *ln;
    listIter li;

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */

    listRewind(cntr->replicas,&li);
    while ((ln = listNext(&li)) != NULL) {
        replica *repl = listNodeValue(ln);

        zfree(repl);
    }

    listRelease(cntr->replicas);

    if (cntr->acks) dictEmpty(cntr->acks,NULL);

    /* cntr->name will be freed by the dict code. */
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
    int retval;

    counter *cntr = zmalloc(sizeof(counter) + (sizeof(long double) * server.history_size));

    memset(cntr, 0, sizeof(counter) + (sizeof(long double) * server.history_size));

    cntr->name     = sdsdup(name);
    cntr->replicas = listCreate();

    cntr->history = (long double*)(cntr + 1);

    retval = dictAdd(server.counters, cntr->name, cntr);
    serverAssert(retval == 0);

    return cntr;
}

void incrCommand(client *c) {
    long double increment;
    counter *cntr;
    robj *new;

    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != DISCNT_OK)
        return;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    }

    if (cntr->myrepl == NULL) {
        cntr->myrepl = zmalloc(sizeof(replica));
        cntr->myrepl->node = myself;
        cntr->myrepl->value = increment;
        cntr->myrepl->predict_time   = 0;
        cntr->myrepl->predict_value  = 0;
        cntr->myrepl->predict_change = 0;

        memcpy(cntr->myrepl->node_name,myself->name,DISCNT_CLUSTER_NAMELEN);

        listAddNodeTail(cntr->replicas, cntr->myrepl);
    } else {
        cntr->myrepl->value += increment;
    }

    cntr->value += increment;

    new = createStringObjectFromLongDouble(cntr->value);

    addReplyBulk(c, new);
}

void getCommand(client *c) {
    long double value = 0;
    counter *cntr;
   
    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr) {
        value = cntr->value;
    }

    robj *new = createStringObjectFromLongDouble(value);

    addReplyBulk(c, new);
}

void countersAddNode(clusterNode *node) {
    dictIterator *it;
    dictEntry *de;
    
    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        counter *cntr;
        listNode *ln;
        listIter li;
        replica *repl;

        cntr = dictGetVal(de);

        listRewind(cntr->replicas,&li);
        while ((ln = listNext(&li)) != NULL) {
            repl = listNodeValue(ln);

            if (memcmp(repl->node_name, node->name, DISCNT_CLUSTER_NAMELEN) == 0) {
                repl->node = node;
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
        replica *repl;

        cntr = dictGetVal(de);

        listRewind(cntr->replicas,&li);
        while ((ln = listNext(&li)) != NULL) {
            repl = listNodeValue(ln);

            if (repl->node == node) {
                /* Remove prediction, don't update our prediction
                   for this replica anymore. */
                repl->predict_time = 0;
            }
        }
    }
    dictReleaseIterator(it);
}

void counterMaybeResend(counter *cntr) {
    int i;
    clusterNode *node;

    if (dictSize(cntr->acks) == 0) {
        return;
    }

    for (i = 0; i < server.cluster->reachable_nodes_count; i++) {
        node = server.cluster->reachable_nodes[i];

        if (node->link == NULL) continue;

        if (dictFind(cntr->acks, node->name) != NULL) {
            clusterSendReplicaToNode(cntr, node);
        }
    }
}

/* -----------------------------------------------------------------------------
 * COUNTERS cron job
 * -------------------------------------------------------------------------- */

/* This is executed every second */
void countersHistoryCron(void) {
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    server.history_index = (server.history_index + 1) % server.history_size;
    
    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double rvalue, elapsed;
        counter *cntr;

        cntr = dictGetVal(de);

        if (cntr->myrepl == NULL) {
            continue;
        }

        counterUpdateHistory(cntr);

        if (cntr->myrepl->predict_time > 0) {
            elapsed = (now - cntr->myrepl->predict_time);
            rvalue = cntr->myrepl->predict_value + (elapsed * cntr->myrepl->predict_change);

            if (fabs((double)(rvalue - cntr->myrepl->value)) <= server.precision) {
                /* It's still up to date, check if we need to resend our last prediction. */
                counterMaybeResend(cntr);

                cntr->hits++;
            }
        }

        /* New prediction. */
        cntr->revision++;

        cntr->misses++;

        counterPredict(cntr, now);

        clusterSendReplica(cntr);
    }
    dictReleaseIterator(it);
}


void countersValueCron(void) {
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double elapsed, value = 0;
        counter *cntr;
        listNode *ln;
        listIter li;
        replica *repl;

        cntr = dictGetVal(de);

        listRewind(cntr->replicas,&li);
        while ((ln = listNext(&li)) != NULL) {
            repl = listNodeValue(ln);

            if (repl == cntr->myrepl) {
                value += repl->value;
                continue;
            }

            if (repl->predict_time > 0 && repl->predict_value > 0) {
                elapsed = (now - repl->predict_time);
                repl->value = repl->predict_value + (elapsed * repl->predict_change);
            }

            value += repl->value;
        }
    
        cntr->value = value;
    }
    dictReleaseIterator(it);
}
