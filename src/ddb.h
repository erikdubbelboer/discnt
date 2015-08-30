/*
 * Copyright (c) 2014-2015, Erik Dubbelboer <erik at dubbelboer dot com>
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

#ifndef __DDB_H
#define __DDB_H

#include <stdio.h>
#include "rio.h"

/* TBD: include only necessary headers. */
#include "server.h"

/* The current DDB version. When the format changes in a way that is no longer
 * backward compatible this number gets incremented. */
#define DDB_VERSION 1

#define DDB_LENERR UINT_MAX

#define DDB_TYPE_COUNTER 0

/* Special DDB opcodes (saved/loaded with ddbSaveType/ddbLoadType). */
#define DDB_OPCODE_RESIZEDB   251
#define DDB_OPCODE_EOF        255

int ddbLoad(char *filename);
int ddbSaveBackground(char *filename);
void ddbRemoveTempFile(pid_t childpid);
int ddbSave(char *filename);
void backgroundSaveDoneHandler(int exitcode, int bysignal);

#endif
