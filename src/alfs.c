// File: src/alfs.c
// ALFS (Anushiravan-Level Fair Scheduler) - user-space simulation
//
// Core idea (per project spec):
// - CFS-like fairness using vruntime + nice->weight mapping
// - Use Min-Heap instead of RB-Tree for runnable entities
// - Input events from Unix Domain Socket (JSON)
// - Output one SchedulerTick JSON per vtime
//
// Build: see Makefile
//
// NOTE: This is NOT Linux kernel code. It's a deterministic user-space simulator.

#define _POSIX_C_SOURCE 200809L

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "../third_party/jsmn.h"

#ifndef ARRAY_LEN
#define ARRAY_LEN(x) (sizeof(x)/sizeof((x)[0]))
#endif

// =========================
// Constants / CFS-like stuff
// =========================

// From Linux kernel's CFS nice table (prio_to_weight), nice -20..+19.
static const int PRIO_TO_WEIGHT[40] = {
    /* -20 */ 88761, 71755, 56483, 46273, 36291,
    /* -15 */ 29154, 23254, 18705, 14949, 11916,
    /* -10 */  9548,  7620,  6100,  4904,  3906,
    /*  -5 */  3121,  2501,  1991,  1586,  1277,
    /*   0 */  1024,   820,   655,   526,   423,
    /*  +5 */   335,   272,   215,   172,   137,
    /* +10 */   110,    87,    70,    56,    45,
    /* +15 */    36,    29,    23,    18,    15
};

#define NICE_MIN (-20)
#define NICE_MAX (+19)
#define NICE_0_WEIGHT 1024

static int clamp_nice(int nice) {
    if (nice < NICE_MIN) return NICE_MIN;
    if (nice > NICE_MAX) return NICE_MAX;
    return nice;
}

static int nice_to_weight(int nice) {
    nice = clamp_nice(nice);
    return PRIO_TO_WEIGHT[nice - NICE_MIN]; // shift by +20
}

// Use integer vruntime scaled like: vruntime += delta_exec * NICE_0_WEIGHT / weight
// We keep vruntime in "microseconds * NICE_0_WEIGHT" units to avoid floating points.
// That means:
//   task->vruntime += quanta_us * NICE_0_WEIGHT * NICE_0_WEIGHT / weight
// is too big. Instead, store vruntime as microseconds, but compute scaled delta:
//   delta_v = quanta_us * NICE_0_WEIGHT / weight
// so vruntime is in microseconds-ish "virtual time" units.
static int64_t vruntime_delta_us(int64_t exec_us, int weight) {
    // exec_us * NICE_0_WEIGHT / weight
    // careful: keep precision with int64.
    return (exec_us * (int64_t)NICE_0_WEIGHT) / (int64_t)weight;
}

// Group vruntime uses shares similarly: delta = exec_us * NICE_0_WEIGHT / shares
static int64_t group_vruntime_delta_us(int64_t exec_us, int shares) {
    if (shares <= 0) shares = 1;
    return (exec_us * (int64_t)NICE_0_WEIGHT) / (int64_t)shares;
}

// =========================
// Simple dynamic pointer vec
// =========================

typedef struct {
    void **data;
    size_t len;
    size_t cap;
} PtrVec;

static void vec_init(PtrVec *v) {
    v->data = NULL;
    v->len = 0;
    v->cap = 0;
}

static void vec_free(PtrVec *v) {
    free(v->data);
    v->data = NULL;
    v->len = 0;
    v->cap = 0;
}

static void vec_reserve(PtrVec *v, size_t cap) {
    if (cap <= v->cap) return;
    size_t newcap = v->cap ? v->cap : 8;
    while (newcap < cap) newcap *= 2;
    void **p = (void**)realloc(v->data, newcap * sizeof(void*));
    if (!p) {
        fprintf(stderr, "OOM in vec_reserve\n");
        exit(1);
    }
    v->data = p;
    v->cap = newcap;
}

static void vec_push(PtrVec *v, void *p) {
    vec_reserve(v, v->len + 1);
    v->data[v->len++] = p;
}

// Remove by pointer (linear). Returns true if removed.
static bool vec_remove_ptr(PtrVec *v, void *p) {
    for (size_t i = 0; i < v->len; i++) {
        if (v->data[i] == p) {
            v->data[i] = v->data[v->len - 1];
            v->len--;
            return true;
        }
    }
    return false;
}

// =========================
// Bitset for CPU masks
// =========================

typedef struct {
    uint64_t *w;
    size_t nwords;
} Bitset;

static void bitset_init(Bitset *b, size_t nbits) {
    b->nwords = (nbits + 63) / 64;
    b->w = (uint64_t*)calloc(b->nwords ? b->nwords : 1, sizeof(uint64_t));
    if (!b->w) {
        fprintf(stderr, "OOM in bitset_init\n");
        exit(1);
    }
}

static void bitset_free(Bitset *b) {
    free(b->w);
    b->w = NULL;
    b->nwords = 0;
}

static void bitset_set_all(Bitset *b, size_t nbits) {
    for (size_t i = 0; i < b->nwords; i++) b->w[i] = ~0ULL;
    // clear top unused bits
    size_t extra = b->nwords * 64 - nbits;
    if (extra && b->nwords) {
        uint64_t mask = ~0ULL >> extra;
        b->w[b->nwords - 1] &= mask;
    }
}

static void bitset_clear(Bitset *b) {
    for (size_t i = 0; i < b->nwords; i++) b->w[i] = 0ULL;
}

static void bitset_set(Bitset *b, int bit) {
    if (bit < 0) return;
    size_t i = (size_t)bit / 64;
    size_t o = (size_t)bit % 64;
    if (i >= b->nwords) return;
    b->w[i] |= (1ULL << o);
}

static bool bitset_test(const Bitset *b, int bit) {
    if (bit < 0) return false;
    size_t i = (size_t)bit / 64;
    size_t o = (size_t)bit % 64;
    if (i >= b->nwords) return false;
    return (b->w[i] >> o) & 1ULL;
}

// =========================
// Heap (binary min-heap)
// =========================

typedef int (*heap_cmp_fn)(const void *a, const void *b);
typedef void (*heap_set_index_fn)(void *item, int idx);
typedef int (*heap_get_index_fn)(const void *item);

typedef struct {
    void **a;
    size_t len;
    size_t cap;
    heap_cmp_fn cmp;
    heap_set_index_fn set_idx;
    heap_get_index_fn get_idx;
} Heap;

static void heap_init(Heap *h, heap_cmp_fn cmp, heap_set_index_fn set_idx, heap_get_index_fn get_idx) {
    h->a = NULL;
    h->len = 0;
    h->cap = 0;
    h->cmp = cmp;
    h->set_idx = set_idx;
    h->get_idx = get_idx;
}

static void heap_free(Heap *h) {
    free(h->a);
    h->a = NULL;
    h->len = 0;
    h->cap = 0;
}

static void heap_reserve(Heap *h, size_t cap) {
    if (cap <= h->cap) return;
    size_t newcap = h->cap ? h->cap : 8;
    while (newcap < cap) newcap *= 2;
    void **p = (void**)realloc(h->a, newcap * sizeof(void*));
    if (!p) {
        fprintf(stderr, "OOM in heap_reserve\n");
        exit(1);
    }
    h->a = p;
    h->cap = newcap;
}

static void heap_swap(Heap *h, size_t i, size_t j) {
    void *tmp = h->a[i];
    h->a[i] = h->a[j];
    h->a[j] = tmp;
    h->set_idx(h->a[i], (int)i);
    h->set_idx(h->a[j], (int)j);
}

static void heap_sift_up(Heap *h, size_t i) {
    while (i > 0) {
        size_t p = (i - 1) / 2;
        if (h->cmp(h->a[i], h->a[p]) >= 0) break;
        heap_swap(h, i, p);
        i = p;
    }
}

static void heap_sift_down(Heap *h, size_t i) {
    while (1) {
        size_t l = 2*i + 1;
        size_t r = 2*i + 2;
        size_t m = i;
        if (l < h->len && h->cmp(h->a[l], h->a[m]) < 0) m = l;
        if (r < h->len && h->cmp(h->a[r], h->a[m]) < 0) m = r;
        if (m == i) break;
        heap_swap(h, i, m);
        i = m;
    }
}

static void heap_push(Heap *h, void *item) {
    heap_reserve(h, h->len + 1);
    size_t i = h->len++;
    h->a[i] = item;
    h->set_idx(item, (int)i);
    heap_sift_up(h, i);
}

static void *heap_pop(Heap *h) {
    if (h->len == 0) return NULL;
    void *min = h->a[0];
    h->set_idx(min, -1);
    h->len--;
    if (h->len > 0) {
        h->a[0] = h->a[h->len];
        h->set_idx(h->a[0], 0);
        heap_sift_down(h, 0);
    }
    return min;
}

static bool heap_remove_at(Heap *h, size_t i) {
    if (i >= h->len) return false;
    void *item = h->a[i];
    h->set_idx(item, -1);
    h->len--;
    if (i != h->len) {
        h->a[i] = h->a[h->len];
        h->set_idx(h->a[i], (int)i);
        // fix both directions
        heap_sift_up(h, i);
        heap_sift_down(h, i);
    }
    return true;
}

static bool heap_remove_item(Heap *h, const void *item) {
    int idx = h->get_idx(item);
    if (idx < 0) return false;
    return heap_remove_at(h, (size_t)idx);
}

static void heap_fix(Heap *h, void *item) {
    int idx = h->get_idx(item);
    if (idx < 0) return;
    heap_sift_up(h, (size_t)idx);
    heap_sift_down(h, (size_t)idx);
}

// =========================
// Scheduler state
// =========================

typedef enum {
    TASK_RUNNABLE = 0,
    TASK_BLOCKED = 1,
    TASK_EXITED  = 2
} TaskState;

typedef enum {
    BURST_FREEZE_AND_PUSHBACK = 0, // default: push to back (max vruntime) then freeze updates for duration
    BURST_FREEZE_ONLY         = 1, // freeze vruntime updates immediately
    BURST_TRACK_VRUNTIME      = 2  // ignore hint, keep tracking like normal
} BurstMode;

struct Cgroup;

typedef struct Task {
    char *id;
    uint64_t seq;
    int nice;
    int weight;

    int64_t vruntime_us; // CFS-like virtual runtime (integer, in "virtual microseconds")
    TaskState state;

    Bitset aff; // per-task cpuMask

    struct Cgroup *cg; // owning group
    int heap_idx; // index in cg->task_heap if runnable else -1

    // For metadata:
    int last_cpu; // last tick's cpu, -1 if not running
    int current_cpu;

    // CPU_BURST:
    int burst_remaining;
    bool burst_freeze;
} Task;

typedef struct Cgroup {
    char *id;
    int cpu_shares;    // default 1024
    int cpu_quota_us;  // -1 unlimited
    int cpu_period_us; // default 100000

    int64_t vruntime_us; // group-level vruntime
    int64_t period_start_us;
    int64_t runtime_this_period_us;
    bool throttled;

    Bitset mask; // cgroup cpuMask
    int heap_idx; // index in scheduler.cg_heap

    Heap task_heap; // runnable tasks in this cgroup (min by task->vruntime_us)
} Cgroup;

typedef struct {
    int preemptions;
    int migrations;
    // optional extras
    PtrVec runnable_ids; // char*
    PtrVec blocked_ids;  // char*
} TickMeta;

typedef struct {
    int ncpus;
    int64_t quanta_us;
    int64_t now_vtime;
    int64_t now_us;

    BurstMode burst_mode;
    bool meta_extra;

    PtrVec tasks;   // Task*
    PtrVec cgroups; // Cgroup*

    Heap cg_heap;   // cgroup heap (min by cgroup->vruntime_us)

    Task **prev_cpu_task; // [ncpus]
    Task **cur_cpu_task;  // [ncpus]

    uint64_t task_seq_next;
    bool debug;
} Scheduler;

// -------------------------
// Heap callbacks/comparators
// -------------------------

static void task_set_idx(void *item, int idx) { ((Task*)item)->heap_idx = idx; }
static int  task_get_idx(const void *item) { return ((const Task*)item)->heap_idx; }

static int task_cmp(const void *a, const void *b) {
    const Task *x = (const Task*)a;
    const Task *y = (const Task*)b;
    if (x->vruntime_us < y->vruntime_us) return -1;
    if (x->vruntime_us > y->vruntime_us) return  1;
    if (x->seq < y->seq) return -1;
    if (x->seq > y->seq) return  1;
    return strcmp(x->id, y->id);
}

static void cgroup_set_idx(void *item, int idx) { ((Cgroup*)item)->heap_idx = idx; }
static int  cgroup_get_idx(const void *item) { return ((const Cgroup*)item)->heap_idx; }

static int cgroup_cmp(const void *a, const void *b) {
    const Cgroup *x = (const Cgroup*)a;
    const Cgroup *y = (const Cgroup*)b;
    if (x->vruntime_us < y->vruntime_us) return -1;
    if (x->vruntime_us > y->vruntime_us) return  1;
    return strcmp(x->id, y->id);
}

// -------------------------
// Lookup helpers (linear)
// -------------------------

static Task *sched_find_task(Scheduler *s, const char *id) {
    for (size_t i = 0; i < s->tasks.len; i++) {
        Task *t = (Task*)s->tasks.data[i];
        if (strcmp(t->id, id) == 0) return t;
    }
    return NULL;
}

static Cgroup *sched_find_cgroup(Scheduler *s, const char *id) {
    for (size_t i = 0; i < s->cgroups.len; i++) {
        Cgroup *cg = (Cgroup*)s->cgroups.data[i];
        if (strcmp(cg->id, id) == 0) return cg;
    }
    return NULL;
}

// -------------------------
// Tick meta helpers
// -------------------------

static void meta_init(TickMeta *m) {
    m->preemptions = 0;
    m->migrations = 0;
    vec_init(&m->runnable_ids);
    vec_init(&m->blocked_ids);
}

static void meta_free(TickMeta *m) {
    // The strings are task->id pointers; do NOT free.
    vec_free(&m->runnable_ids);
    vec_free(&m->blocked_ids);
}

static void meta_collect_task_lists(Scheduler *s, TickMeta *m) {
    for (size_t i = 0; i < s->tasks.len; i++) {
        Task *t = (Task*)s->tasks.data[i];
        if (t->state == TASK_RUNNABLE) vec_push(&m->runnable_ids, t->id);
        else if (t->state == TASK_BLOCKED) vec_push(&m->blocked_ids, t->id);
    }
}

// =========================
// Cgroup / Task lifecycle
// =========================

static Cgroup *cgroup_new(Scheduler *s, const char *id, int shares, int quota_us, int period_us, const int *cpu_mask, size_t cpu_mask_len) {
    if (sched_find_cgroup(s, id)) {
        fprintf(stderr, "[warn] CGROUP_CREATE ignored, already exists: %s\n", id);
        return sched_find_cgroup(s, id);
    }
    Cgroup *cg = (Cgroup*)calloc(1, sizeof(Cgroup));
    if (!cg) { fprintf(stderr, "OOM cgroup_new\n"); exit(1); }
    cg->id = strdup(id);
    cg->cpu_shares = (shares > 0) ? shares : 1024;
    cg->cpu_quota_us = quota_us;
    cg->cpu_period_us = (period_us > 0) ? period_us : 100000;
    cg->vruntime_us = 0;
    cg->period_start_us = 0;
    cg->runtime_this_period_us = 0;
    cg->throttled = false;
    cg->heap_idx = -1;

    bitset_init(&cg->mask, (size_t)s->ncpus);
    bitset_clear(&cg->mask);
    if (cpu_mask && cpu_mask_len > 0) {
        for (size_t i = 0; i < cpu_mask_len; i++) bitset_set(&cg->mask, cpu_mask[i]);
    } else {
        bitset_set_all(&cg->mask, (size_t)s->ncpus);
    }

    heap_init(&cg->task_heap, task_cmp, task_set_idx, task_get_idx);

    vec_push(&s->cgroups, cg);
    heap_push(&s->cg_heap, cg);

    if (s->debug) fprintf(stderr, "[dbg] cgroup_new id=%s shares=%d quota=%d period=%d\n",
                          cg->id, cg->cpu_shares, cg->cpu_quota_us, cg->cpu_period_us);
    return cg;
}

static void cgroup_update_period(Cgroup *cg, int64_t now_us) {
    if (cg->cpu_quota_us < 0) {
        cg->throttled = false;
        return;
    }
    if (cg->cpu_period_us <= 0) cg->cpu_period_us = 100000;
    // Advance period if needed (handle jumps).
    while (now_us - cg->period_start_us >= cg->cpu_period_us) {
        cg->period_start_us += cg->cpu_period_us;
        cg->runtime_this_period_us = 0;
        cg->throttled = false;
    }
    if (cg->runtime_this_period_us >= cg->cpu_quota_us) cg->throttled = true;
}

static void cgroup_account_runtime(Cgroup *cg, int64_t delta_us, int64_t now_us) {
    (void)now_us;
    if (cg->cpu_quota_us < 0) return; // unlimited
    cg->runtime_this_period_us += delta_us;
    if (cg->runtime_this_period_us >= cg->cpu_quota_us) cg->throttled = true;
}

static bool cgroup_cpu_allowed(Cgroup *cg, int cpu) {
    return bitset_test(&cg->mask, cpu);
}
static int64_t cgroup_max_vruntime(Cgroup *cg);

static Task *task_new(Scheduler *s, const char *task_id, int nice, const char *cgroup_id) {
    if (sched_find_task(s, task_id)) {
        fprintf(stderr, "[warn] TASK_CREATE ignored, already exists: %s\n", task_id);
        return sched_find_task(s, task_id);
    }
    Task *t = (Task*)calloc(1, sizeof(Task));
    if (!t) { fprintf(stderr, "OOM task_new\n"); exit(1); }
    t->id = strdup(task_id);
    t->seq = s->task_seq_next++;
    t->nice = clamp_nice(nice);
    t->weight = nice_to_weight(t->nice);
    t->state = TASK_RUNNABLE;
    t->heap_idx = -1;

    t->last_cpu = -1;
    t->current_cpu = -1;

    t->burst_remaining = 0;
    t->burst_freeze = false;

    bitset_init(&t->aff, (size_t)s->ncpus);
    bitset_set_all(&t->aff, (size_t)s->ncpus);

    // Find or create default cgroup.
    const char *cgid = (cgroup_id && cgroup_id[0]) ? cgroup_id : "0";
    Cgroup *cg = sched_find_cgroup(s, cgid);
    if (!cg) {
        // default: unlimited, all cpus
        cg = cgroup_new(s, cgid, 1024, -1, 100000, NULL, 0);
    }
    t->cg = cg;
    t->vruntime_us = cgroup_max_vruntime(cg);
    heap_push(&cg->task_heap, t);

    vec_push(&s->tasks, t);

    if (s->debug) fprintf(stderr, "[dbg] task_new id=%s nice=%d weight=%d cgroup=%s\n",
                          t->id, t->nice, t->weight, t->cg->id);
    return t;
}

static void task_set_nice(Task *t, int newNice) {
    t->nice = clamp_nice(newNice);
    t->weight = nice_to_weight(t->nice);
}

static bool task_cpu_allowed(Task *t, int cpu) {
    return bitset_test(&t->aff, cpu);
}

static void task_set_affinity(Scheduler *s, Task *t, const int *cpus, size_t ncpus) {
    (void)s;
    bitset_clear(&t->aff);
    for (size_t i = 0; i < ncpus; i++) bitset_set(&t->aff, cpus[i]);
}

static void task_block(Scheduler *s, Task *t) {
    (void)s;
    if (t->state != TASK_RUNNABLE) return;
    // remove from cgroup heap
    heap_remove_item(&t->cg->task_heap, t);
    t->state = TASK_BLOCKED;
}

static void task_unblock(Scheduler *s, Task *t) {
    (void)s;
    if (t->state != TASK_BLOCKED) return;
    t->state = TASK_RUNNABLE;
    heap_push(&t->cg->task_heap, t);
}

static void task_exit(Scheduler *s, Task *t) {
    (void)s;
    if (t->state == TASK_EXITED) return;
    if (t->state == TASK_RUNNABLE) {
        heap_remove_item(&t->cg->task_heap, t);
    }
    t->state = TASK_EXITED;
}

static int64_t cgroup_max_vruntime(Cgroup *cg) {
    int64_t mx = 0;
    bool has = false;
    for (size_t i = 0; i < cg->task_heap.len; i++) {
        Task *t = (Task*)cg->task_heap.a[i];
        if (!has || t->vruntime_us > mx) {
            mx = t->vruntime_us;
            has = true;
        }
    }
    if (!has) return 0;
    return mx;
}

static void task_yield(Scheduler *s, Task *t) {
    (void)s;
    if (t->state != TASK_RUNNABLE) return;
    // per spec: vruntime = max_vruntime in its runqueue (we use cgroup heap as runqueue)
    int64_t mx = cgroup_max_vruntime(t->cg);
    t->vruntime_us = mx;
    heap_fix(&t->cg->task_heap, t);
}

static void task_move_cgroup(Scheduler *s, Task *t, const char *new_cgid) {
    if (!new_cgid || !new_cgid[0]) new_cgid = "0";
    Cgroup *newcg = sched_find_cgroup(s, new_cgid);
    if (!newcg) {
        // create on demand (unlimited)
        newcg = cgroup_new(s, new_cgid, 1024, -1, 100000, NULL, 0);
    }
    if (t->cg == newcg) return;

    // detach from old runqueue if runnable
    if (t->state == TASK_RUNNABLE) heap_remove_item(&t->cg->task_heap, t);

    t->cg = newcg;

    if (t->state == TASK_RUNNABLE) heap_push(&t->cg->task_heap, t);
}

static void cgroup_modify(Scheduler *s, Cgroup *cg, bool has_shares, int shares, bool has_quota, int quota, bool has_period, int period, bool has_mask, const int *mask, size_t mask_len) {
    (void)s;
    if (has_shares && shares > 0) cg->cpu_shares = shares;
    if (has_quota) cg->cpu_quota_us = quota;
    if (has_period && period > 0) cg->cpu_period_us = period;
    if (has_mask) {
        bitset_clear(&cg->mask);
        for (size_t i = 0; i < mask_len; i++) bitset_set(&cg->mask, mask[i]);
    }
    // quota changes may affect throttled state; will be re-evaluated on next tick.
    heap_fix(&s->cg_heap, cg);
}

static void cgroup_delete(Scheduler *s, Cgroup *cg) {
    // To avoid dangling tasks, we disallow deleting non-empty cgroups.
    if (cg->task_heap.len > 0) {
        fprintf(stderr, "[warn] CGROUP_DELETE ignored (non-empty): %s\n", cg->id);
        return;
    }
    // also avoid deleting default "0"
    if (strcmp(cg->id, "0") == 0) {
        fprintf(stderr, "[warn] CGROUP_DELETE ignored (default cgroup): %s\n", cg->id);
        return;
    }
    heap_remove_item(&s->cg_heap, cg);
    // remove from vector
    vec_remove_ptr(&s->cgroups, cg);

    heap_free(&cg->task_heap);
    bitset_free(&cg->mask);
    free(cg->id);
    free(cg);
}

// =========================
// Scheduling
// =========================

static bool task_is_eligible(Scheduler *s, Task *t, int cpu) {
    (void)s;
    if (!t || t->state != TASK_RUNNABLE) return false;
    if (!task_cpu_allowed(t, cpu)) return false;
    if (!cgroup_cpu_allowed(t->cg, cpu)) return false;
    if (t->cg->throttled) return false;
    return true;
}

static Task *pick_task_from_cgroup(Scheduler *s, Cgroup *cg, int cpu) {
    // Pop from cg->task_heap until we find one eligible for this cpu.
    if (cg->task_heap.len == 0) return NULL;

    PtrVec skipped;
    vec_init(&skipped);

    Task *chosen = NULL;
    while (cg->task_heap.len > 0) {
        Task *t = (Task*)heap_pop(&cg->task_heap);
        if (task_is_eligible(s, t, cpu) && t->current_cpu == -1) {
            chosen = t;
            break;
        }
        vec_push(&skipped, t);
    }

    // Reinsert skipped tasks.
    for (size_t i = 0; i < skipped.len; i++) {
        heap_push(&cg->task_heap, skipped.data[i]);
    }
    vec_free(&skipped);

    return chosen;
}

static Task *pick_next_task(Scheduler *s, int cpu) {
    PtrVec skipped_groups;
    vec_init(&skipped_groups);

    Task *chosen = NULL;
    Cgroup *chosen_cg = NULL;

    while (s->cg_heap.len > 0) {
        Cgroup *cg = (Cgroup*)heap_pop(&s->cg_heap);

        // basic eligibility: not throttled, has runnable tasks, cpu allowed
        if (cg->throttled || cg->task_heap.len == 0 || !cgroup_cpu_allowed(cg, cpu)) {
            vec_push(&skipped_groups, cg);
            continue;
        }

        Task *t = pick_task_from_cgroup(s, cg, cpu);
        if (!t) {
            vec_push(&skipped_groups, cg);
            continue;
        }

        chosen = t;
        chosen_cg = cg;
        break;
    }

    // Put skipped groups back
    for (size_t i = 0; i < skipped_groups.len; i++) heap_push(&s->cg_heap, skipped_groups.data[i]);
    vec_free(&skipped_groups);

    if (!chosen) {
        // If we popped a chosen_cg? no
        return NULL;
    }

    // Account runtime & advance vruntimes BEFORE reinserting cgroup.
    cgroup_account_runtime(chosen_cg, s->quanta_us, s->now_us);
    chosen_cg->vruntime_us += group_vruntime_delta_us(s->quanta_us, chosen_cg->cpu_shares);

    // CPU_BURST handling:
    bool track = true;
    if (s->burst_mode == BURST_TRACK_VRUNTIME) track = true;
    else if (chosen->burst_remaining > 0) track = !chosen->burst_freeze;

    if (track) {
        chosen->vruntime_us += vruntime_delta_us(s->quanta_us, chosen->weight);
    }

    if (chosen->burst_remaining > 0) {
        chosen->burst_remaining--;
        if (chosen->burst_remaining <= 0) {
            chosen->burst_freeze = false;
        }
    }

    heap_push(&s->cg_heap, chosen_cg);

    if (s->debug) {
        fprintf(stderr, "[dbg] pick cpu=%d -> task=%s (cg=%s) t_vr=%" PRId64 " cg_vr=%" PRId64 "\n",
                cpu, chosen->id, chosen_cg->id, chosen->vruntime_us, chosen_cg->vruntime_us);
    }

    return chosen;
}

static void scheduler_begin_tick(Scheduler *s, int64_t vtime) {
    s->now_vtime = vtime;
    s->now_us = vtime * s->quanta_us;

    // clear per-tick assignments
    for (int cpu = 0; cpu < s->ncpus; cpu++) s->cur_cpu_task[cpu] = NULL;
    for (size_t i = 0; i < s->tasks.len; i++) {
        Task *t = (Task*)s->tasks.data[i];
        t->current_cpu = -1;
    }

    // update quota periods
    for (size_t i = 0; i < s->cgroups.len; i++) {
        Cgroup *cg = (Cgroup*)s->cgroups.data[i];
        cgroup_update_period(cg, s->now_us);
        heap_fix(&s->cg_heap, cg);
    }
}

static void scheduler_finish_tick(Scheduler *s, TickMeta *meta) {
    // preemptions: count CPUs where previous tick had a task, and this tick differs (including to idle).
    // This matches the example's behavior (idle->task doesn't count, task->idle counts). (See PDF example)
    for (int cpu = 0; cpu < s->ncpus; cpu++) {
        Task *prev = s->prev_cpu_task[cpu];
        Task *cur  = s->cur_cpu_task[cpu];
        if (prev != NULL && prev != cur) meta->preemptions++;
    }

    // migrations: count tasks that changed CPU AND their previous CPU became idle.
    // This matches the provided example where swaps are not counted as migration, but moves leaving an idle slot are. 
    for (int cpu = 0; cpu < s->ncpus; cpu++) {
        Task *cur = s->cur_cpu_task[cpu];
        if (!cur) continue;
        int prev_cpu = cur->last_cpu;
        if (prev_cpu >= 0 && prev_cpu != cpu) {
            if (prev_cpu < s->ncpus && s->cur_cpu_task[prev_cpu] == NULL) {
                meta->migrations++;
            }
        }
    }

    if (s->meta_extra) meta_collect_task_lists(s, meta);

    // update last_cpu for next tick
    for (size_t i = 0; i < s->tasks.len; i++) {
        Task *t = (Task*)s->tasks.data[i];
        t->last_cpu = -1;
    }
    for (int cpu = 0; cpu < s->ncpus; cpu++) {
        Task *cur = s->cur_cpu_task[cpu];
        if (cur) cur->last_cpu = cpu;
    }

    // rotate schedule
    for (int cpu = 0; cpu < s->ncpus; cpu++) s->prev_cpu_task[cpu] = s->cur_cpu_task[cpu];
}

static void scheduler_run_tick(Scheduler *s, int64_t vtime, TickMeta *meta) {
    scheduler_begin_tick(s, vtime);

    // Choose tasks for each CPU (sequential deterministic selection)
    PtrVec selected_tasks;
    vec_init(&selected_tasks);

    for (int cpu = 0; cpu < s->ncpus; cpu++) {
        Task *t = pick_next_task(s, cpu);
        if (t) {
            s->cur_cpu_task[cpu] = t;
            t->current_cpu = cpu;
            vec_push(&selected_tasks, t);
        } else {
            s->cur_cpu_task[cpu] = NULL; // idle
        }
    }

    // Reinsert chosen tasks back into their cgroup heaps (so they stay runnable next tick)
    for (size_t i = 0; i < selected_tasks.len; i++) {
        Task *t = (Task*)selected_tasks.data[i];
        if (t->state == TASK_RUNNABLE) {
            heap_push(&t->cg->task_heap, t);
        }
    }
    vec_free(&selected_tasks);

    scheduler_finish_tick(s, meta);
}

// =========================
// JSON utilities (jsmn)
// =========================

static bool tok_streq(const char *js, const jsmntok_t *tok, const char *s) {
    size_t len = (size_t)(tok->end - tok->start);
    return (tok->type == JSMN_STRING) &&
           (strlen(s) == len) &&
           (strncmp(js + tok->start, s, len) == 0);
}

static int tok_skip(const jsmntok_t *toks, int i) {
    // returns index after token i including its descendants
    int j = i + 1;
    if (toks[i].type == JSMN_OBJECT) {
        for (int k = 0; k < toks[i].size; k++) {
            j = tok_skip(toks, j); // key
            j = tok_skip(toks, j); // value
        }
    } else if (toks[i].type == JSMN_ARRAY) {
        for (int k = 0; k < toks[i].size; k++) {
            j = tok_skip(toks, j);
        }
    }
    return j;
}

static int json_obj_get(const char *js, const jsmntok_t *toks, int obj_i, const char *key) {
    if (toks[obj_i].type != JSMN_OBJECT) return -1;
    int i = obj_i + 1;
    for (int k = 0; k < toks[obj_i].size; k++) {
        const jsmntok_t *kt = &toks[i];
        int v = i + 1;
        if (tok_streq(js, kt, key)) return v;
        i = tok_skip(toks, v);
    }
    return -1;
}

static bool json_tok_is_null(const char *js, const jsmntok_t *tok) {
    if (tok->type != JSMN_PRIMITIVE) return false;
    int len = tok->end - tok->start;
    return len == 4 && strncmp(js + tok->start, "null", 4) == 0;
}

static bool json_tok_to_int64(const char *js, const jsmntok_t *tok, int64_t *out) {
    if (tok->type != JSMN_PRIMITIVE) return false;
    char tmp[64];
    int n = tok->end - tok->start;
    if (n <= 0 || n >= (int)sizeof(tmp)) return false;
    memcpy(tmp, js + tok->start, (size_t)n);
    tmp[n] = 0;
    char *endp = NULL;
    errno = 0;
    long long v = strtoll(tmp, &endp, 10);
    if (errno != 0 || endp == tmp) return false;
    *out = (int64_t)v;
    return true;
}

static char *json_tok_to_cstr(const char *js, const jsmntok_t *tok) {
    if (tok->type != JSMN_STRING) return NULL;
    int n = tok->end - tok->start;
    char *s = (char*)malloc((size_t)n + 1);
    if (!s) { fprintf(stderr, "OOM json_tok_to_cstr\n"); exit(1); }
    memcpy(s, js + tok->start, (size_t)n);
    s[n] = 0;
    return s;
}

static int *json_array_of_ints(const char *js, const jsmntok_t *toks, int arr_i, size_t *out_len) {
    if (toks[arr_i].type != JSMN_ARRAY) return NULL;
    size_t n = (size_t)toks[arr_i].size;
    int *a = (int*)calloc(n ? n : 1, sizeof(int));
    if (!a) { fprintf(stderr, "OOM json_array_of_ints\n"); exit(1); }
    int i = arr_i + 1;
    for (size_t k = 0; k < n; k++) {
        int64_t v = 0;
        if (!json_tok_to_int64(js, &toks[i], &v)) v = 0;
        a[k] = (int)v;
        i = tok_skip(toks, i);
    }
    *out_len = n;
    return a;
}

// =========================
// Event processing
// =========================

static void handle_event(Scheduler *s, const char *js, const jsmntok_t *toks, int ev_i) {
    int action_i = json_obj_get(js, toks, ev_i, "action");
    if (action_i < 0) return;
    char *action = json_tok_to_cstr(js, &toks[action_i]);
    if (!action) return;

    if (strcmp(action, "TASK_CREATE") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        if (tid_i < 0) { free(action); return; }
        char *tid = json_tok_to_cstr(js, &toks[tid_i]);
        int nice = 0;
        char *cgid = NULL;

        int nice_i = json_obj_get(js, toks, ev_i, "nice");
        if (nice_i >= 0 && !json_tok_is_null(js, &toks[nice_i])) {
            int64_t v;
            if (json_tok_to_int64(js, &toks[nice_i], &v)) nice = (int)v;
        }
        int cgid_i = json_obj_get(js, toks, ev_i, "cgroupId");
        if (cgid_i >= 0 && !json_tok_is_null(js, &toks[cgid_i])) cgid = json_tok_to_cstr(js, &toks[cgid_i]);

        task_new(s, tid, nice, cgid ? cgid : "0");

        free(tid);
        free(cgid);
    } else if (strcmp(action, "TASK_EXIT") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        if (tid_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            Task *t = sched_find_task(s, tid);
            if (t) task_exit(s, t);
            free(tid);
        }
    } else if (strcmp(action, "TASK_BLOCK") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        if (tid_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            Task *t = sched_find_task(s, tid);
            if (t) task_block(s, t);
            free(tid);
        }
    } else if (strcmp(action, "TASK_UNBLOCK") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        if (tid_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            Task *t = sched_find_task(s, tid);
            if (t) task_unblock(s, t);
            free(tid);
        }
    } else if (strcmp(action, "TASK_YIELD") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        if (tid_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            Task *t = sched_find_task(s, tid);
            if (t) task_yield(s, t);
            free(tid);
        }
    } else if (strcmp(action, "TASK_SETNICE") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        int nn_i  = json_obj_get(js, toks, ev_i, "newNice");
        if (tid_i >= 0 && nn_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            int64_t nn = 0;
            if (json_tok_to_int64(js, &toks[nn_i], &nn)) {
                Task *t = sched_find_task(s, tid);
                if (t) task_set_nice(t, (int)nn);
            }
            free(tid);
        }
    } else if (strcmp(action, "TASK_SET_AFFINITY") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        int m_i = json_obj_get(js, toks, ev_i, "cpuMask");
        if (tid_i >= 0 && m_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            size_t n = 0;
            int *a = json_array_of_ints(js, toks, m_i, &n);
            Task *t = sched_find_task(s, tid);
            if (t) task_set_affinity(s, t, a, n);
            free(a);
            free(tid);
        }
    } else if (strcmp(action, "CGROUP_CREATE") == 0) {
        int id_i = json_obj_get(js, toks, ev_i, "cgroupId");
        if (id_i < 0) { free(action); return; }
        char *cgid = json_tok_to_cstr(js, &toks[id_i]);

        int shares = 1024;
        int quota = -1;
        int period = 100000;
        int mask_i = json_obj_get(js, toks, ev_i, "cpuMask");
        int shares_i = json_obj_get(js, toks, ev_i, "cpuShares");
        int quota_i  = json_obj_get(js, toks, ev_i, "cpuQuotaUs");
        int period_i = json_obj_get(js, toks, ev_i, "cpuPeriodUs");

        if (shares_i >= 0 && !json_tok_is_null(js, &toks[shares_i])) {
            int64_t v;
            if (json_tok_to_int64(js, &toks[shares_i], &v)) shares = (int)v;
        }
        if (quota_i >= 0 && !json_tok_is_null(js, &toks[quota_i])) {
            int64_t v;
            if (json_tok_to_int64(js, &toks[quota_i], &v)) quota = (int)v;
        }
        if (period_i >= 0 && !json_tok_is_null(js, &toks[period_i])) {
            int64_t v;
            if (json_tok_to_int64(js, &toks[period_i], &v)) period = (int)v;
        }

        size_t mn = 0;
        int *mask = NULL;
        if (mask_i >= 0 && !json_tok_is_null(js, &toks[mask_i])) {
            mask = json_array_of_ints(js, toks, mask_i, &mn);
        }

        cgroup_new(s, cgid, shares, quota, period, mask, mn);

        free(mask);
        free(cgid);
    } else if (strcmp(action, "CGROUP_MODIFY") == 0) {
        int id_i = json_obj_get(js, toks, ev_i, "cgroupId");
        if (id_i >= 0) {
            char *cgid = json_tok_to_cstr(js, &toks[id_i]);
            Cgroup *cg = sched_find_cgroup(s, cgid);
            if (cg) {
                bool has_shares = false, has_quota = false, has_period = false, has_mask = false;
                int shares = cg->cpu_shares;
                int quota = cg->cpu_quota_us;
                int period = cg->cpu_period_us;
                int *mask = NULL; size_t mn = 0;

                int shares_i = json_obj_get(js, toks, ev_i, "cpuShares");
                int quota_i  = json_obj_get(js, toks, ev_i, "cpuQuotaUs");
                int period_i = json_obj_get(js, toks, ev_i, "cpuPeriodUs"); // not in schema example but safe
                int mask_i   = json_obj_get(js, toks, ev_i, "cpuMask");

                if (shares_i >= 0 && !json_tok_is_null(js, &toks[shares_i])) {
                    int64_t v;
                    if (json_tok_to_int64(js, &toks[shares_i], &v)) { has_shares = true; shares = (int)v; }
                }
                if (quota_i >= 0 && !json_tok_is_null(js, &toks[quota_i])) {
                    int64_t v;
                    if (json_tok_to_int64(js, &toks[quota_i], &v)) { has_quota = true; quota = (int)v; }
                }
                if (period_i >= 0 && !json_tok_is_null(js, &toks[period_i])) {
                    int64_t v;
                    if (json_tok_to_int64(js, &toks[period_i], &v)) { has_period = true; period = (int)v; }
                }
                if (mask_i >= 0 && !json_tok_is_null(js, &toks[mask_i])) {
                    mask = json_array_of_ints(js, toks, mask_i, &mn);
                    has_mask = true;
                }

                cgroup_modify(s, cg, has_shares, shares, has_quota, quota, has_period, period, has_mask, mask, mn);
                free(mask);
            }
            free(cgid);
        }
    } else if (strcmp(action, "CGROUP_DELETE") == 0) {
        int id_i = json_obj_get(js, toks, ev_i, "cgroupId");
        if (id_i >= 0) {
            char *cgid = json_tok_to_cstr(js, &toks[id_i]);
            Cgroup *cg = sched_find_cgroup(s, cgid);
            if (cg) cgroup_delete(s, cg);
            free(cgid);
        }
    } else if (strcmp(action, "TASK_MOVE_CGROUP") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        int ncg_i = json_obj_get(js, toks, ev_i, "newCgroupId");
        if (tid_i >= 0 && ncg_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            char *ncg = json_tok_to_cstr(js, &toks[ncg_i]);
            Task *t = sched_find_task(s, tid);
            if (t) task_move_cgroup(s, t, ncg);
            free(ncg);
            free(tid);
        }
    } else if (strcmp(action, "CPU_BURST") == 0) {
        int tid_i = json_obj_get(js, toks, ev_i, "taskId");
        int dur_i = json_obj_get(js, toks, ev_i, "duration");
        if (tid_i >= 0 && dur_i >= 0) {
            char *tid = json_tok_to_cstr(js, &toks[tid_i]);
            int64_t d = 0;
            if (json_tok_to_int64(js, &toks[dur_i], &d)) {
                Task *t = sched_find_task(s, tid);
                if (t) {
                    t->burst_remaining = (int)d;
                    if (s->burst_mode == BURST_FREEZE_AND_PUSHBACK) {
                        // push to back: set vruntime = max_vruntime then freeze
                        heap_fix(&t->cg->task_heap, t);
                        t->burst_freeze = true;
                    } else if (s->burst_mode == BURST_FREEZE_ONLY) {
                        t->burst_freeze = true;
                    } else {
                        t->burst_freeze = false;
                    }
                }
            }
            free(tid);
        }
    } else {
        fprintf(stderr, "[warn] unknown action: %s\n", action);
    }

    free(action);
}

// Process one timeframe object token index tf_i (object)
static bool process_timeframe(Scheduler *s, const char *js, const jsmntok_t *toks, int tf_i, int sock_fd);

// Parse a JSON payload which is either:
// - a TimeFrame object
// - OR an array of TimeFrame objects (for offline testing)
static void process_payload(Scheduler *s, const char *js, int sock_fd) {
    jsmn_parser p;
    jsmn_init(&p);

    int tokcap = 2048;
    jsmntok_t *toks = (jsmntok_t*)calloc((size_t)tokcap, sizeof(jsmntok_t));
    if (!toks) { fprintf(stderr, "OOM toks\n"); exit(1); }

    int r = jsmn_parse(&p, js, strlen(js), toks, (unsigned)tokcap);
    if (r == JSMN_ERROR_NOMEM) {
        // try bigger
        tokcap *= 4;
        toks = (jsmntok_t*)realloc(toks, (size_t)tokcap * sizeof(jsmntok_t));
        if (!toks) { fprintf(stderr, "OOM toks realloc\n"); exit(1); }
        memset(toks, 0, (size_t)tokcap * sizeof(jsmntok_t));
        jsmn_init(&p);
        r = jsmn_parse(&p, js, strlen(js), toks, (unsigned)tokcap);
    }
    if (r < 0) {
        fprintf(stderr, "[err] JSON parse error: %d\n", r);
        free(toks);
        return;
    }
    if (r < 1) { free(toks); return; }

    if (toks[0].type == JSMN_OBJECT) {
        process_timeframe(s, js, toks, 0, sock_fd);
    } else if (toks[0].type == JSMN_ARRAY) {
        int i = 1;
        for (int k = 0; k < toks[0].size; k++) {
            if (toks[i].type == JSMN_OBJECT) {
                process_timeframe(s, js, toks, i, sock_fd);
            }
            i = tok_skip(toks, i);
        }
    } else {
        fprintf(stderr, "[err] root JSON must be object or array\n");
    }

    free(toks);
}

// =========================
// Output JSON
// =========================

static void send_all(int fd, const char *buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t w = send(fd, buf + off, n - off, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) continue;
            perror("send");
            return;
        }
        off += (size_t)w;
    }
}

static void send_cstr(int fd, const char *s) {
    send_all(fd, s, strlen(s));
}

static void send_json_escaped_string(int fd, const char *s) {
    // Minimal escape: backslash and quote and control chars.
    send_cstr(fd, "\"");
    for (const unsigned char *p = (const unsigned char*)s; *p; p++) {
        unsigned char c = *p;
        if (c == '\\' || c == '\"') {
            char tmp[2] = {(char)'\\', (char)c};
            send_all(fd, tmp, 2);
        } else if (c == '\n') {
            send_cstr(fd, "\\n");
        } else if (c == '\r') {
            send_cstr(fd, "\\r");
        } else if (c == '\t') {
            send_cstr(fd, "\\t");
        } else if (c < 32) {
            char tmp[8];
            snprintf(tmp, sizeof(tmp), "\\u%04x", (unsigned)c);
            send_cstr(fd, tmp);
        } else {
            char tmp[1] = {(char)c};
            send_all(fd, tmp, 1);
        }
    }
    send_cstr(fd, "\"");
}

static void send_tick_json(int fd, Scheduler *s, int64_t vtime, const TickMeta *meta) {
    char tmp[128];

    send_cstr(fd, "{");
    snprintf(tmp, sizeof(tmp), "\"vtime\":%" PRId64 ",\"schedule\":[", vtime);
    send_cstr(fd, tmp);

    for (int cpu = 0; cpu < s->ncpus; cpu++) {
        if (cpu) send_cstr(fd, ",");
        const char *id = s->cur_cpu_task[cpu] ? s->cur_cpu_task[cpu]->id : "idle";
        send_json_escaped_string(fd, id);
    }
    send_cstr(fd, "]");

    // meta (optional but we always include as project asks for extra score)
    send_cstr(fd, ",\"meta\":{");
    snprintf(tmp, sizeof(tmp), "\"preemptions\":%d,\"migrations\":%d", meta->preemptions, meta->migrations);
    send_cstr(fd, tmp);

    if (s->meta_extra) {
        // runnableTasks
        send_cstr(fd, ",\"runnableTasks\":[");
        for (size_t i = 0; i < meta->runnable_ids.len; i++) {
            if (i) send_cstr(fd, ",");
            send_json_escaped_string(fd, (const char*)meta->runnable_ids.data[i]);
        }
        send_cstr(fd, "]");
        // blockedTasks
        send_cstr(fd, ",\"blockedTasks\":[");
        for (size_t i = 0; i < meta->blocked_ids.len; i++) {
            if (i) send_cstr(fd, ",");
            send_json_escaped_string(fd, (const char*)meta->blocked_ids.data[i]);
        }
        send_cstr(fd, "]");
    }

    send_cstr(fd, "}");

    send_cstr(fd, "}\n");
}

// =========================
// TimeFrame processing (apply events then schedule)
// =========================

static bool process_timeframe(Scheduler *s, const char *js, const jsmntok_t *toks, int tf_i, int sock_fd) {
    int vt_i = json_obj_get(js, toks, tf_i, "vtime");
    int evs_i = json_obj_get(js, toks, tf_i, "events");
    if (vt_i < 0 || evs_i < 0) {
        fprintf(stderr, "[warn] invalid timeframe (missing vtime/events)\n");
        return false;
    }
    int64_t vtime = 0;
    if (!json_tok_to_int64(js, &toks[vt_i], &vtime)) {
        fprintf(stderr, "[warn] invalid vtime\n");
        return false;
    }

    if (toks[evs_i].type != JSMN_ARRAY) {
        fprintf(stderr, "[warn] events must be array\n");
        return false;
    }

    // Apply events first
    int i = evs_i + 1;
    for (int k = 0; k < toks[evs_i].size; k++) {
        if (toks[i].type == JSMN_OBJECT) handle_event(s, js, toks, i);
        i = tok_skip(toks, i);
    }

    // Run scheduler for this tick and output result
    TickMeta meta;
    meta_init(&meta);
    scheduler_run_tick(s, vtime, &meta);
    if (sock_fd >= 0) send_tick_json(sock_fd, s, vtime, &meta);
    meta_free(&meta);

    return true;
}

// =========================
// Robust JSON framing over stream
// =========================

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} ByteBuf;

static void bb_init(ByteBuf *b) {
    b->buf = NULL;
    b->len = 0;
    b->cap = 0;
}

static void bb_free(ByteBuf *b) {
    free(b->buf);
    b->buf = NULL;
    b->len = 0;
    b->cap = 0;
}

static void bb_reserve(ByteBuf *b, size_t cap) {
    if (cap <= b->cap) return;
    size_t newcap = b->cap ? b->cap : 4096;
    while (newcap < cap) newcap *= 2;
    char *p = (char*)realloc(b->buf, newcap);
    if (!p) { fprintf(stderr, "OOM bb_reserve\n"); exit(1); }
    b->buf = p;
    b->cap = newcap;
}

static void bb_append(ByteBuf *b, const char *data, size_t n) {
    bb_reserve(b, b->len + n + 1);
    memcpy(b->buf + b->len, data, n);
    b->len += n;
    b->buf[b->len] = 0;
}

static void bb_consume(ByteBuf *b, size_t n) {
    if (n >= b->len) {
        b->len = 0;
        if (b->buf) b->buf[0] = 0;
        return;
    }
    memmove(b->buf, b->buf + n, b->len - n);
    b->len -= n;
    b->buf[b->len] = 0;
}

// Extract one complete JSON value from buffer using brace/bracket counting.
// Returns malloc'd json string if available, else NULL. Consumes extracted bytes.
static char *extract_one_json(ByteBuf *b) {
    size_t i = 0;
    while (i < b->len && isspace((unsigned char)b->buf[i])) i++;
    if (i >= b->len) {
        bb_consume(b, i);
        return NULL;
    }
    if (b->buf[i] != '{' && b->buf[i] != '[') {
        // skip garbage until next likely start
        while (i < b->len && b->buf[i] != '{' && b->buf[i] != '[') i++;
        bb_consume(b, i);
        return NULL;
    }

    size_t start = i;
    int depth = 0;
    bool in_str = false;
    bool esc = false;

    for (; i < b->len; i++) {
        char c = b->buf[i];
        if (in_str) {
            if (esc) esc = false;
            else if (c == '\\') esc = true;
            else if (c == '"') in_str = false;
            continue;
        } else {
            if (c == '"') { in_str = true; continue; }
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
            if (depth == 0) {
                size_t end = i + 1;
                size_t n = end - start;
                char *out = (char*)malloc(n + 1);
                if (!out) { fprintf(stderr, "OOM extract_one_json\n"); exit(1); }
                memcpy(out, b->buf + start, n);
                out[n] = 0;
                bb_consume(b, end);
                return out;
            }
        }
    }
    // need more data
    return NULL;
}

// =========================
// UDS client
// =========================

static int uds_connect(const char *path) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(addr.sun_path)) {
        fprintf(stderr, "socket path too long\n");
        close(fd);
        return -1;
    }
    strncpy(addr.sun_path, path, sizeof(addr.sun_path)-1);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

// =========================
// Scheduler init/free
// =========================

static void scheduler_init(Scheduler *s, int ncpus, int64_t quanta_us, BurstMode burst_mode, bool meta_extra, bool debug) {
    memset(s, 0, sizeof(*s));
    s->ncpus = ncpus;
    s->quanta_us = quanta_us;
    s->burst_mode = burst_mode;
    s->meta_extra = meta_extra;
    s->task_seq_next = 1;
    s->debug = debug;

    vec_init(&s->tasks);
    vec_init(&s->cgroups);
    heap_init(&s->cg_heap, cgroup_cmp, cgroup_set_idx, cgroup_get_idx);

    s->prev_cpu_task = (Task**)calloc((size_t)ncpus, sizeof(Task*));
    s->cur_cpu_task  = (Task**)calloc((size_t)ncpus, sizeof(Task*));
    if (!s->prev_cpu_task || !s->cur_cpu_task) {
        fprintf(stderr, "OOM cpu arrays\n");
        exit(1);
    }

    // default cgroup "0": all cpus, unlimited
    cgroup_new(s, "0", 1024, -1, 100000, NULL, 0);
}

static void scheduler_free(Scheduler *s) {
    // free tasks
    for (size_t i = 0; i < s->tasks.len; i++) {
        Task *t = (Task*)s->tasks.data[i];
        bitset_free(&t->aff);
        free(t->id);
        free(t);
    }
    vec_free(&s->tasks);

    // free cgroups
    for (size_t i = 0; i < s->cgroups.len; i++) {
        Cgroup *cg = (Cgroup*)s->cgroups.data[i];
        heap_free(&cg->task_heap);
        bitset_free(&cg->mask);
        free(cg->id);
        free(cg);
    }
    vec_free(&s->cgroups);

    heap_free(&s->cg_heap);

    free(s->prev_cpu_task);
    free(s->cur_cpu_task);
}

// =========================
// CLI parsing
// =========================

static void usage(const char *argv0) {
    fprintf(stderr,
        "Usage: %s --cpus N --quanta-us Q [--socket PATH] [--burst-mode MODE] [--meta-extra] [--meta-minimal] [--debug]\n"
        "\n"
        "  --cpus N           number of CPUs\n"
        "  --quanta-us Q      quanta per tick, in microseconds\n"
        "  --socket PATH      unix domain socket path (default: ./socket.event then ./event.socket)\n"
        "  --burst-mode MODE  freeze_pushback (default) | freeze | track\n"
        "  --meta-extra       include extra meta fields (runnableTasks, blockedTasks)\n"
        "  --meta-minimal     only output {preemptions,migrations} (default)\n"
        "  --debug            verbose debug logs to stderr\n",
        argv0);
}

static BurstMode parse_burst_mode(const char *s) {
    if (!s) return BURST_FREEZE_AND_PUSHBACK;
    if (strcmp(s, "freeze_pushback") == 0) return BURST_FREEZE_AND_PUSHBACK;
    if (strcmp(s, "freeze") == 0) return BURST_FREEZE_ONLY;
    if (strcmp(s, "track") == 0) return BURST_TRACK_VRUNTIME;
    return BURST_FREEZE_AND_PUSHBACK;
}

// =========================
// main
// =========================

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);

    int ncpus = -1;
    int64_t quanta_us = -1;
    const char *sock_path = NULL;
    const char *burst_mode_str = "freeze_pushback";
    bool meta_extra = false;
    bool debug = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--cpus") == 0 && i + 1 < argc) {
            ncpus = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--quanta-us") == 0 && i + 1 < argc) {
            quanta_us = (int64_t)atoll(argv[++i]);
        } else if (strcmp(argv[i], "--socket") == 0 && i + 1 < argc) {
            sock_path = argv[++i];
        } else if (strcmp(argv[i], "--burst-mode") == 0 && i + 1 < argc) {
            burst_mode_str = argv[++i];
        } else if (strcmp(argv[i], "--meta-minimal") == 0) {
            meta_extra = false;
        } else if (strcmp(argv[i], "--meta-extra") == 0) {
            meta_extra = true;
        } else if (strcmp(argv[i], "--debug") == 0) {
            debug = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown arg: %s\n", argv[i]);
            usage(argv[0]);
            return 2;
        }
    }

    if (ncpus <= 0 || quanta_us <= 0) {
        usage(argv[0]);
        return 2;
    }

    // Default socket paths (per PDF): try socket.event then event.socket
    const char *default1 = "./socket.event";
    const char *default2 = "./event.socket";


    int sock_fd = -1;
    if (sock_path) {
        sock_fd = uds_connect(sock_path);
        if (sock_fd < 0) return 1;
    } else {
        sock_fd = uds_connect(default1);
        if (sock_fd < 0) {
            sock_fd = uds_connect(default2);
            if (sock_fd < 0) {
                fprintf(stderr, "Failed to connect to default sockets: %s or %s\n", default1, default2);
                return 1;
            }
        }
    }

    Scheduler sched;
    scheduler_init(&sched, ncpus, quanta_us, parse_burst_mode(burst_mode_str), meta_extra, debug);

    if (debug) fprintf(stderr, "[dbg] connected. ncpus=%d quanta_us=%" PRId64 " meta_extra=%d burst_mode=%s\n",
                       ncpus, quanta_us, (int)meta_extra, burst_mode_str);

    ByteBuf bb;
    bb_init(&bb);

    char rbuf[4096];
    while (1) {
        ssize_t n = recv(sock_fd, rbuf, sizeof(rbuf), 0);
        if (n == 0) {
            if (debug) fprintf(stderr, "[dbg] socket closed by peer\n");
            break;
        } else if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv");
            break;
        }

        bb_append(&bb, rbuf, (size_t)n);

        while (1) {
            char *one = extract_one_json(&bb);
            if (!one) break;
            process_payload(&sched, one, sock_fd);
            free(one);
        }
    }

    bb_free(&bb);
    scheduler_free(&sched);
    close(sock_fd);
    return 0;
}
