/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "counter.h"

/*-----------------------------------------------------------------------------
 * Sub low level API
 *----------------------------------------------------------------------------*/

/* Subscribe a client to a counter. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that counter. */
int pubsubSubscribeCounter(client *c, sds name) {
    int retval = 0;
    counter* cntr;

    cntr = counterLookup(name);
    if (cntr == NULL) {
        cntr = counterCreate(name);
    }

    if (c->sub_counters == NULL) {
        c->sub_counters = dictCreate(&counterLookupDictType,NULL);
    }

    /* Add the counter to the client -> counters hash table */
    if (dictAdd(c->sub_counters,cntr->name,cntr) == DICT_OK) {
        retval = 1;
        if (cntr->subscribers == NULL) {
            cntr->subscribers = listCreate();
        }
        serverAssert(listAddNodeTail(cntr->subscribers,c) != NULL);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulkCBuffer(c,name,sdslen(name));
    addReplyLongLong(c,dictSize(c->sub_counters));
    return retval;
}

/* Unsubscribe a client from a counter. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified counter. */
int pubsubUnsubscribeCounter(client *c, sds name, int notify) {
    dictEntry *de;
    listNode *ln;
    int retval = 0;
    counter *cntr;

    /* Remove the counter from the client -> counters hash table */
    if (c->sub_counters != NULL) {
        de = dictFind(c->sub_counters,name);
        if (de != NULL) {
            retval = 1;
            cntr = dictGetVal(de);
            dictDelete(c->sub_counters,name);
            /* Remove the client from the counter -> clients list hash table */
            ln = listSearchKey(cntr->subscribers,c);
            serverAssertWithInfo(c,NULL,ln != NULL);
            listDelNode(cntr->subscribers,ln);
            if (listLength(cntr->subscribers) == 0) {
                listRelease(cntr->subscribers);
                cntr->subscribers = NULL;
            }
        }
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReplyBulkCBuffer(c,name,sdslen(name));
        addReplyLongLong(c,dictSize(c->sub_counters));
    }
    return retval;
}

/* Unsubscribe from all the counters. Return the number of counters the
 * client was subscribed to. */
int pubsubUnsubscribeAllCounters(client *c, int notify) {
    dictIterator *di = dictGetSafeIterator(c->sub_counters);
    dictEntry *de;
    sds name;
    int count = 0;

    while((de = dictNext(di)) != NULL) {
        name = dictGetKey(de);

        count += pubsubUnsubscribeCounter(c,name,notify);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->sub_counters));
    }
    dictReleaseIterator(di);
    return count;
}

/*-----------------------------------------------------------------------------
 * Sub commands implementation
 *----------------------------------------------------------------------------*/

void subscribeCommand(client *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeCounter(c,c->argv[j]->ptr);
    c->flags |= CLIENT_PUBSUB;
}

void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllCounters(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeCounter(c,c->argv[j]->ptr,1);
    }
    if (dictSize(c->sub_counters) == 0) c->flags &= ~CLIENT_PUBSUB;
}

