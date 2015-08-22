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


void replicaUpdateHistory(replica *repl) {
    repl->history[server.history_index] = repl->value;
}

void replicaPredict(replica *repl, mstime_t now) {
    repl->predict_time   = now;
    repl->predict_value  = repl->value;
    repl->predict_change = (repl->value - repl->history[server.history_index]) / ((long double)server.history_size * 1000.0);
}

void dictCounterDestructor(void *privdata, void *val) {
    DICT_NOTUSED(privdata);
    counter* cntr = val;
    listNode *ln;
    listIter li;
    replica* repl;

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */

    listRewind(cntr->replicas,&li);
    while ((ln = listNext(&li)) != NULL) {
        repl = listNodeValue(ln);

        if (repl->history != NULL) {
            zfree(repl->history);
        }

        zfree(repl);
    }

    listRelease(cntr->replicas);
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
    counter *cntr = zmalloc(sizeof(counter));
    cntr->name = sdsdup(name);
    cntr->replicas = listCreate();
    cntr->value = 0;

    int retval = dictAdd(server.counters, cntr->name, cntr);

    serverAssert(retval == 0);

    return cntr;
}

void incrCommand(client *c) {
    long double increment;
    listNode *ln;
    counter *cntr;
    replica *repl = NULL;
    robj *new;

    if (getLongDoubleFromObjectOrReply(c,c->argv[2],&increment,NULL) != DISCNT_OK)
        return;

    cntr = counterLookup(c->argv[1]->ptr);
    if (cntr == NULL) {
        cntr = counterCreate(c->argv[1]->ptr);
    } else if (listLength(cntr->replicas) > 0) {
        ln = listFirst(cntr->replicas);
        serverAssert(ln != NULL);
        repl = listNodeValue(ln);

        if (repl->node != myself) {
            repl = NULL;
        }
    }

    if (repl == NULL) {
        repl = zmalloc(sizeof(replica) + sizeof(long double)*server.history_size);
        repl->node = myself;
        repl->value = increment;
        repl->predict_time   = 0;
        repl->predict_value  = 0;
        repl->predict_change = 0;

        repl->history = (long double*)(repl + 1);

        memset(repl->history, 0, sizeof(long double)*server.history_size);

        memcpy(repl->node_name,myself->name,DISCNT_CLUSTER_NAMELEN);

        listAddNodeHead(cntr->replicas, repl);
    } else {
        repl->value += increment;
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

/* -----------------------------------------------------------------------------
 * COUNTERS cron job
 * -------------------------------------------------------------------------- */

/* This is executed every second */
void countersCron(void) {
    int predict;
    dictIterator *it;
    dictEntry *de;
    mstime_t now = mstime();

    server.history_index = (server.history_index + 1) % server.history_size;
    
    it = dictGetIterator(server.counters);
    while ((de = dictNext(it)) != NULL) {
        long double rvalue, elapsed, value = 0;
        counter *cntr;
        listNode *ln;
        listIter li;
        replica *repl;

        cntr = dictGetVal(de);

        listRewind(cntr->replicas,&li);
        while ((ln = listNext(&li)) != NULL) {
            repl = listNodeValue(ln);

            if (repl->node == myself) {
                predict = 0;

                if (repl->predict_time == 0) {
                    predict = 1;
                } else {
                    elapsed = (now - repl->predict_time);
                    rvalue = repl->predict_value + (elapsed * repl->predict_change);

                    if (fabs((double)(rvalue - repl->value)) > server.precision) {
                        predict = 1;
                    }
                }

                if (predict) {
                    replicaPredict(repl, now);

                    clusterSendReplica(cntr->name, repl);
                }

                replicaUpdateHistory(repl);
            } else {
                if (repl->predict_time > 0 && repl->predict_value > 0) {
                    elapsed = (now - repl->predict_time);
                    repl->value = repl->predict_value + (elapsed * repl->predict_change);
                }
            }

            value += repl->value;
        }
    
        cntr->value = value;
    }
    dictReleaseIterator(it);
}
