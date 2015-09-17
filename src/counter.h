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

#ifndef __DISCNT_COUNTER_H
#define __DISCNT_COUNTER_H

#include "adlist.h"
#include "cluster.h"


typedef struct shard {
    clusterNode *node;
    char         node_name[CLUSTER_NAMELEN];

    /* For our shard (node == myself) this is the actual value.
     * For other shards this is our last prediction of the value.
     */
    long double value;

    mstime_t    predict_time;   /* Time we made the last prediction. */
    long double predict_value;  /* Value at the last prediction. */
    long double predict_change; /* Change per micro second. */
} shard;

/* Counter representation in memory. */
typedef struct counter {
    sds   name;
    list  *shards;
    shard *myshard;

    long double value;    /* Cached value. */
    long double *history; /* History of this counter per second for server.history_size seconds. */

    uint32_t revision;
    dict     *want_acks;
    mstime_t updated;
    double precision;  /* Desired prediction precision. */

    uint32_t hits;
    uint32_t misses;

    /* Cached output buffer. */
    char rbuf[128];
    int rlen;

    list *subscribers; /* Map of clients subscribed to this counter. */
} counter;


/*-----------------------------------------------------------------------------
 * Exported API.
 *----------------------------------------------------------------------------*/

void counterClearWantAcks(counter *cntr);
void counterWantAck(counter *cntr, const clusterNode *node);
void counterGotAck(counter *cntr, const clusterNode *node);
counter *counterLookup(const sds name);
counter *counterCreate(sds name);
shard *counterAddShard(counter *cntr, clusterNode* node, const char *node_name);
void countersClusterAddNode(clusterNode *node);

#endif
