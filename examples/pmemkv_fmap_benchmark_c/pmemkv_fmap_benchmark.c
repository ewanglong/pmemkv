// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */

/*
 * pmemkv_fmap_benchmark.c -- benchmark usage of fmap engine.
 */
//#define _GNU_SOURCE
//#include <sched.h>
//#undef _GNU_SOURCE

#include <assert.h>
//#define __cplusplus
#include <libpmemkv.h>
//#undef __cplusplus
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>

#define ASSERT(expr)                                                                     \
	do {                                                                             \
		if (!(expr))                                                             \
			puts(pmemkv_errormsg());                                         \
		assert(expr);                                                            \
	} while (0)

#define LOG(msg) puts(msg)
#define MAX_KEY_LEN 16
#define MAX_VAL_LEN 128
#define MAX_BEN_ITEM 10000000
//#define MAX_BEN_ITEM 3000000
#define MAX_INTERVAL_TIMES 100000

static bool sst_active = false;
static bool sst_trigger = false;
static pmemkv_db *db = NULL;
static const uint64_t SIZE = 16 * 1024UL * 1024UL * 1024UL;

int get_kv_callback(const char *k, size_t kb, const char *value, size_t value_bytes,
		    void *arg)
{
	printf("   visited: %s\n", k);

	return 0;
}

typedef struct thread_args {
	int thread_num;
	pmemkv_db *db;
	char *val_pool;
} thread_args;

#define INSTANT_OPS
//static char *valpool;
//static char valpool[MAX_VAL_LEN * 4];
//static __thread char valpool[MAX_VAL_LEN * 4];
static _Thread_local char valpool[MAX_VAL_LEN * 4];
static void *thread_ben(void *arg)
{
	int s;
	/* parameters */
	pmemkv_db *db = ((thread_args *)arg)->db;
	int tn = ((thread_args *)arg)->thread_num;

	printf("Starting benchmarking...: thread #%d\n", tn);

	for (int j = 0; j < MAX_VAL_LEN * 4; j++) {
		valpool[j] = (char)((j+1) % 255);
	}

	char curkey[MAX_KEY_LEN];
	char *curval = valpool;

#ifdef INSTANT_OPS
	long long cur_us;
	long long last_us = 0;
	struct timeval now;
	gettimeofday(&now, NULL);
	last_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);
#endif

#if 0
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(tn > 23 ? tn + 48 : tn + 24, &cpuset);
	pthread_t current_thread = pthread_self();
	pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
#endif

	srand(time(NULL));
	for (int i = 0, j = 0; i < MAX_BEN_ITEM; i++) {
		//snprintf(curkey, sizeof(curkey), "key%012d:", rand() % MAX_BEN_ITEM * tn);
		snprintf(curkey, sizeof(curkey), "key%012d:", i * tn);
		//snprintf(curkey, sizeof(curkey), "key%012d:", rand() % MAX_BEN_ITEM * tn);
		//s = pmemkv_put(db, curkey, strlen(curkey), curval + rand() % (MAX_VAL_LEN * 3), MAX_VAL_LEN);
		s = pmemkv_put(db, curkey, strlen(curkey), curval + i % (MAX_VAL_LEN * 3), MAX_VAL_LEN);
		//s = pmemkv_put(db, curkey, strlen(curkey), "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz012345678901", MAX_VAL_LEN);
		ASSERT(s == PMEMKV_STATUS_OK);
#ifdef INSTANT_OPS
		if (++j % MAX_INTERVAL_TIMES == 0) {
			//long long cur_us = mstime();
			gettimeofday(&now, NULL);
			cur_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);
			double inst_dt = (double)(cur_us - last_us);
			double inst_ops = (double)MAX_INTERVAL_TIMES/inst_dt * 1000000.0;
			fprintf(stderr, "Thread #%d WRITE: %.2f ops\n", tn, inst_ops);
			last_us = cur_us;
		}
#endif
	}

	return NULL;
}

void* create_shared_memory(size_t size) {
	int protection = PROT_READ | PROT_WRITE;

	int visibility = MAP_SHARED | MAP_ANONYMOUS;

	return mmap(NULL, size, protection, visibility, -1, 0);
}

void *do_snapshot(char *sst_path) {
	int childpid;
	void *shmem = create_shared_memory(64);
	char done_msg[] = "sst_done";

#if 1
	struct timeval now;
	long long last_us, cur_us;
	gettimeofday(&now, NULL);
	last_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);
#endif

	if ((childpid = fork()) == 0) {
		/* Child */
		pmemkv_snapshot(db, sst_path, true);
		memcpy(shmem, done_msg, sizeof(done_msg));
		exit(0);
	} else {
#if 1
		gettimeofday(&now, NULL);
		cur_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);
		fprintf(stderr, "fork() consumed %d microseconds\n", (cur_us - last_us)/1000);
#endif
		/* in parent process, set sst_active_ flag ON */
		pmemkv_snapshot(db, NULL, false);
		return shmem;
	}
}

int main(int argc, char *argv[])
{
	int ret, i, j;
	if (argc < 2) {
		fprintf(stderr, "Usage: %s file\n", argv[0]);
		exit(1);
	}

	/* See libpmemkv_config(3) for more detailed example of config creation */
	fprintf(stderr, "Creating config\n");
	pmemkv_config *cfg = pmemkv_config_new();
	ASSERT(cfg != NULL);

	int s = pmemkv_config_put_path(cfg, argv[1]);
	ASSERT(s == PMEMKV_STATUS_OK);

	long ts = 1;
	if (argc > 2) {
		ts = strtol(argv[2], NULL, 10);
	}

	s = pmemkv_config_put_size(cfg, SIZE);
	ASSERT(s == PMEMKV_STATUS_OK);
	s = pmemkv_config_put_force_create(cfg, 1/*true*/);
	ASSERT(s == PMEMKV_STATUS_OK);

	fprintf(stderr, "Opening pmemkv database with 'fmap' engine\n");
	s = pmemkv_open("fmap", cfg, &db);
	ASSERT(s == PMEMKV_STATUS_OK);
	ASSERT(db != NULL);

	pthread_t *p_threads = malloc(sizeof(pthread_t) * (unsigned int)ts);
	if (p_threads == NULL) {
		fprintf(stderr, "malloc failed");
		exit(-1);
	}

	thread_args *threads_args = malloc(sizeof(thread_args) * (unsigned int)ts);
	if (threads_args == NULL) {
		fprintf(stderr, "malloc failed");
		exit(-1);
	}

	//valpool = malloc(MAX_VAL_LEN * 4);
	for (int j = 0; j < MAX_VAL_LEN * 4; j++) {
		valpool[j] = (char)((j+1) % 255);
	}

	long long cur_us;
	long long last_us = 0;
	struct timeval now;
	gettimeofday(&now, NULL);
	last_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);

	if (ts > 1) {
		for (i = 0; i < ts; i++) {
			threads_args[i].thread_num = i;
			threads_args[i].db = db;
			if ((ret = pthread_create(&p_threads[i], NULL, thread_ben,
					&threads_args[i])) != 0) {
				fprintf(stderr, "Cannot start a thread #%d: %s\n",
					i, strerror(ret));
				return ret;
			}
		}

		for (i = ts - 1; i >= 0; i--)
			pthread_join(p_threads[i], NULL);
	} else {
		fprintf(stderr, "Starting benchmarking...: main thread\n");

#ifdef INSTANT_OPS
		long long cur_inst_us;
		long long last_inst_us = 0;
		struct timeval now_inst;
		gettimeofday(&now_inst, NULL);
		last_inst_us = (long long)(now_inst.tv_sec * 1000000 + now_inst.tv_usec);
#endif

		char curkey[MAX_KEY_LEN];
		char *curval = valpool;

#if 0
		char sst_path[50];
		//sprintf(sst_path, "%s-sst", argv[1]);
		char cwd[128];
		char *cwdp = getcwd(cwd,128);
		sprintf(sst_path, "%s/pmemkv.sst", cwdp);
#endif

		for (i = 0, j = 0; i < MAX_BEN_ITEM; i++) {
#if 1
			void *shmem;
			char done_msg[] = "sst_done";

			if (i == MAX_BEN_ITEM / 2 || sst_trigger == true) {
				//shmem = do_snapshot(sst_path);
				shmem = do_snapshot(NULL);
				sst_trigger = false;
				sst_active = true;
				fprintf(stderr, "snapshot is triggered!\n");
			}

			if (sst_active == true && !memcmp(shmem, done_msg, sizeof(done_msg))) {
				/* in parent process, set sst_active_ flag OFF */
				pmemkv_snapshot(db, NULL, false);
				sst_active = false;
				fprintf(stderr, "snapshot is done by child process!\n");
			}
#endif

			snprintf(curkey, sizeof(curkey), "key%012d:", i);
			//snprintf(curkey, sizeof(curkey), "key%012d:", rand() % MAX_BEN_ITEM);

			s = pmemkv_put(db, curkey, strlen(curkey), curval + rand() % (MAX_VAL_LEN * 3), MAX_VAL_LEN);
			//s = pmemkv_put(db, curkey, strlen(curkey), curval + i % (MAX_VAL_LEN * 3), MAX_VAL_LEN);
			ASSERT(s == PMEMKV_STATUS_OK);
#ifdef INSTANT_OPS
			if (++j % MAX_INTERVAL_TIMES == 0) {
				//long long cur_us = mstime();
				gettimeofday(&now_inst, NULL);
				cur_inst_us = (long long)(now_inst.tv_sec * 1000000 + now_inst.tv_usec);
				double inst_dt = (double)(cur_inst_us - last_inst_us);
				double inst_ops = (double)MAX_INTERVAL_TIMES/inst_dt * 1000000.0;
				fprintf(stderr, "Main Thread WRITE: %.2f ops\n", inst_ops);
				last_inst_us = cur_inst_us;
			}
#endif
		}
	}

	gettimeofday(&now, NULL);
	cur_us = (long long)(now.tv_sec * 1000000 + now.tv_usec);
	double ave_dt = (double)(cur_us - last_us);
	double ave_ops = (double)ts * MAX_BEN_ITEM / ave_dt * 1000000.0;
	fprintf(stderr, "%d threads average WRITE: %.2f ops\n", ts, ave_ops);

	fprintf(stderr, "Closing database\n");

	pmemkv_close(db);

	return 0;
}
