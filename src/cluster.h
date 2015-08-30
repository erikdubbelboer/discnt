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

#ifndef __DISCNT_CLUSTER_H
#define __DISCNT_CLUSTER_H

/*-----------------------------------------------------------------------------
 * Discnt cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

#define CLUSTER_OK 0          /* Everything looks ok */
#define CLUSTER_FAIL 1        /* The cluster can't work */
#define CLUSTER_NAMELEN 40    /* sha1 hex length */
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */
#define CLUSTER_DEFAULT_NODE_TIMEOUT 15000
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */

struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
typedef struct clusterLink {
    mstime_t ctime;             /* Link creation time */
    int fd;                     /* TCP socket file descriptor */
    sds sndbuf;                 /* Packet send buffer */
    sds rcvbuf;                 /* Packet reception buffer */
    struct clusterNode *node;   /* Node related to this link if any, or NULL */
} clusterLink;

/* Cluster node flags and macros. */
#define CLUSTER_NODE_PFAIL     (1<<0) /* Failure? Need acknowledge */
#define CLUSTER_NODE_FAIL      (1<<1) /* The node is believed to be malfunctioning */
#define CLUSTER_NODE_MYSELF    (1<<2) /* This node is myself */
#define CLUSTER_NODE_HANDSHAKE (1<<3) /* Node in handshake state. */
#define CLUSTER_NODE_NOADDR    (1<<4) /* Node address unknown */
#define CLUSTER_NODE_MEET      (1<<5) /* Send a MEET message to this node */
#define CLUSTER_NODE_DELETED   (1<<6) /* Node no longer part of the cluster */
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeDeleted(n) ((n)->flags & CLUSTER_NODE_DELETED)

/* This structure represent elements of node->fail_reports. */
typedef struct clusterNodeFailReport {
    struct clusterNode *node;  /* Node reporting the failure condition. */
    mstime_t time;             /* Time of the last report from this node. */
} clusterNodeFailReport;

typedef struct clusterNode {
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */
    mstime_t ctime; /* Node object creation time. */
    int flags;      /* DISCNT_NODE_... */
    mstime_t ping_sent;      /* Unix time we sent latest ping */
    mstime_t pong_received;  /* Unix time we received the pong */
    mstime_t fail_time;      /* Unix time when FAIL flag was set */
    char ip[NET_IP_STR_LEN];  /* Latest known IP address of this node */
    int port;                   /* Latest known port of this node */
    clusterLink *link;          /* TCP/IP link with this node */
    list *fail_reports;         /* List of nodes signaling this as failing */
} clusterNode;

typedef struct clusterState {
    clusterNode *myself;  /* This node */
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ... */
    int size;             /* Num of known cluster nodes */
    dict *nodes;          /* Hash table of name -> clusterNode structures */
    dict *deleted_nodes;    /* Nodes removed from the cluster. */
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Reachable nodes array. This array only lists reachable nodes
     * excluding our own node, and is used in order to quickly select
     * random receivers of messages to populate. */
    clusterNode **reachable_nodes;
    int reachable_nodes_count;
    /* Statistics. */
    long long stats_bus_messages_sent;  /* Num of msg sent via cluster bus. */
    long long stats_bus_messages_received; /* Num of msg rcvd via cluster bus.*/
} clusterState;

/* clusterState todo_before_sleep flags. */
#define CLUSTER_TODO_UPDATE_STATE (1<<0)
#define CLUSTER_TODO_SAVE_CONFIG (1<<1)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<2)

/* Discnt cluster messages header */

/* Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
#define CLUSTERMSG_TYPE_PING 0     /* Ping. */
#define CLUSTERMSG_TYPE_PONG 1     /* Reply to Ping. */
#define CLUSTERMSG_TYPE_MEET 2     /* Meet "let's join" message. */
#define CLUSTERMSG_TYPE_FAIL 3     /* Mark node xxx as failing. */
#define CLUSTERMSG_TYPE_COUNTER 4  /* A counter. */
#define CLUSTERMSG_TYPE_ACK 5      /* Ack. */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
typedef struct {
    char nodename[CLUSTER_NAMELEN];
    uint32_t ping_sent;
    uint32_t pong_received;
    char ip[NET_IP_STR_LEN]; /* IP address last time it was seen */
    uint16_t port;              /* port last time it was seen */
    uint16_t flags;             /* node->flags copy */
    uint16_t notused1;          /* Some room for future improvements. */
    uint32_t notused2;
} clusterMsgDataGossip;

typedef struct {
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    uint32_t after;
} clusterMsgDataUpdate;

typedef struct {
    uint64_t predict_time;
    uint64_t predict_value;
    uint64_t predict_change;
    uint32_t revision;
    uint16_t name_length;  /* Length of name. */
    unsigned char name[2]; /* Defined as 2 bytes just for alignment. */
} clusterMsgDataCounter;

typedef struct {
    uint32_t revision;
    uint16_t name_length;  /* Length of name. */
    unsigned char name[2]; /* Defined as 2 bytes just for alignment. */
} clusterMsgDataAck;

union clusterMsgData {
    /* PING, MEET and PONG. */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL. */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* COUNTER. */
    struct {
        clusterMsgDataCounter shard;
    } counter;

    /* ACK. */
    struct {
        clusterMsgDataAck about;
    } ack;
};

#define CLUSTER_PROTO_VER 0 /* Cluster bus protocol version. */

typedef struct {
    char sig[4];        /* Siganture "DbuZ" (Discnt Cluster message bus). */
    uint32_t totlen;    /* Total length of this message */
    uint16_t ver;       /* Protocol version, currently set to 0. */
    uint16_t notused0;  /* 2 bytes not used. */
    uint16_t type;      /* Message type */
    uint16_t count;     /* Only used for some kind of messages. */
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */
    char notused1[32];  /* 32 bytes reserved for future usage. */
    uint16_t port;      /* Sender TCP base port */
    uint16_t flags;     /* Sender node flags */
    unsigned char state; /* Cluster state from the POV of the sender */
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/*-----------------------------------------------------------------------------
 * Exported API.
 *----------------------------------------------------------------------------*/

extern clusterNode *myself;

clusterNode *clusterLookupNode(char *name);
void clusterUpdateReachableNodes(void);

struct counter;

void clusterSendShardToNode(struct counter* cntr, clusterNode *node);
void clusterSendShard(struct counter* cntr); /* To all nodes. */

#endif /* __DISCNT_CLUSTER_H */
