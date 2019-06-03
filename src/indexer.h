/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef INDEXER_H
#define INDEXER_H

#include <sys/types.h>
#include "redismodule.h"

typedef struct {
    RedisModuleString *key;
    RedisModuleString *value;
} Label;

typedef enum  {
    EQ,
    NEQ,
    // Contains a label
    CONTAINS,
    // Not Contains a label
    NCONTAINS,
    // REQ,
    // NREQ
} PredicateType;

typedef struct QueryPredicate {
    PredicateType type;
    Label label;
} QueryPredicate;
uint64_t GetTimeStamp();
void IndexInit();
void IndexMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
void RemoveIndexedMetric(RedisModuleCtx *ctx, RedisModuleString *ts_key, Label *labels, size_t labels_count);
RedisModuleString **QueryIndex(RedisModuleCtx *ctx, QueryPredicate *index_predicate, size_t predicate_count, int*len);
int parseLabel(RedisModuleCtx *ctx, RedisModuleString *label, Label *retLabel, const char *separator);
int CountPredicateType(QueryPredicate *queries, size_t query_count, PredicateType type);
#endif
