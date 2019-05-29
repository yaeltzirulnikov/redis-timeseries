
/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef REDISTIMESERIES_MODULE_VERSION

#define REDISTIMESERIES_VERSION_MAJOR 0
#define REDISTIMESERIES_VERSION_MINOR 9
#define REDISTIMESERIES_VERSION_PATCH 6

#define REDISTIMESERIES_SEMANTIC_VERSION(major, minor, patch) \
  (major * 10000 + minor * 100 + patch)

#define REDISTIMESERIES_MODULE_VERSION REDISTIMESERIES_SEMANTIC_VERSION(REDISTIMESERIES_VERSION_MAJOR, REDISTIMESERIES_VERSION_MINOR, REDISTIMESERIES_VERSION_PATCH)

#endif