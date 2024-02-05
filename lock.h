/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Spin/RW lock
 *
 * Hohai University
 */

#ifndef LOCK_H
#define LOCK_H

#include "atomic.h"

typedef struct {
    unsigned int slock;
} spinlock_t;

#define SPIN_INIT {0}
#define SPINLOCK_UNLOCK	0

#define __SPINLOCK_UNLOCKED	{ .slock = SPINLOCK_UNLOCK }

#define DEFINE_SPINLOCK(x)	spinlock_t x = __SPINLOCK_UNLOCKED

static inline spinlock_t* spin_lock_init(spinlock_t *lock) {
    lock->slock = 0;
    return lock;
}

static inline void spin_lock(spinlock_t *lock) {
    short inc = 0x0100;
    __asm__ __volatile__ ("  lock; xaddw %w0, %1;"
                          "1:"
                          "  cmpb %h0, %b0;"
                          "  je 2f;"
                          "  rep ; nop;"
                          "  movb %1, %b0;"
                          "  jmp 1b;"
                          "2:"
    : "+Q" (inc), "+m" (lock->slock)
    :: "memory", "cc");
}

static inline void spin_unlock(spinlock_t *lock) {
    __asm__ __volatile__("lock; incb %0;" : "+m" (lock->slock) :: "memory", "cc");
}

/*
 * Writer states & reader shift and bias
 */
#define	_QW_WAITING	1			/* A writer is waiting	   */
#define	_QW_LOCKED	0xff		/* A writer holds the lock */
#define	_QW_WMASK	0xff		/* Writer mask		   */
#define	_QR_SHIFT	8			/* Reader count shift	   */
#define _QR_BIAS	(1U << _QR_SHIFT)

/*
 * 8 bytes
 */
typedef struct {
    atomic_t 	cnts; // version for readers
    spinlock_t	slock;
} rwlock_t;

#define RWLOCK_UNLOCK {			\
	.cnts = ATOMIC_INIT(0),		\
	.slock = ATOMIC_INIT(0),	\
}

#define DEFINE_RWLOCK(x)	rwlock_t x = RWLOCK_UNLOCK

static inline void rwlock_init(rwlock_t *lock) {
    atomic_set(&lock->cnts, 0);
    spin_lock_init(&lock->slock);
}

static void __read_lock(rwlock_t* lock);
static void __read_unlock(rwlock_t* lock);
static void __write_lock(rwlock_t* lock);
static void __write_unlock(rwlock_t* lock);

#define read_lock(lock)			do{ barrier(); __read_lock(lock); } while (0)
#define read_unlock(lock)		do{ barrier(); __read_unlock(lock); } while (0)
#define write_lock(lock)		do{ barrier(); __write_lock(lock); } while (0)
#define write_unlock(lock)		do{ barrier(); __write_unlock(lock); } while (0)

#ifndef likely
#define likely(x)               __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x)				__builtin_expect(!!(x), 0)
#endif

#ifndef cpu_relax
#define cpu_relax() asm volatile("pause\n" : : : "memory")
#endif

/**
 * rspin_until_writer_unlock - inc reader count & spin until writer is gone
 * @lock  : Pointer to queue rwlock structure
 * @writer: Current queue rwlock writer status byte
 */
static inline void rspin_until_writer_unlock(rwlock_t *lock, unsigned int cnts)
{
    while ((cnts & _QW_WMASK) == _QW_LOCKED) {
        cpu_relax();
        memory_lfence();
        cnts = atomic_read(&lock->cnts);
    }
}

static void __read_lock_slowpath(rwlock_t *lock) {
    unsigned int cnts;

    /* slow path */
    atomic_sub(_QR_BIAS, &lock->cnts);

    /* Put the reader into the wait queue */
    spin_lock(&lock->slock);

    while (atomic_read(&lock->cnts) & _QW_WMASK)
        cpu_relax();

    cnts = atomic_add_return(_QR_BIAS, &lock->cnts) - _QR_BIAS;
    rspin_until_writer_unlock(lock, cnts);

    /* Signal the next one in queue to become queue bhead */
    spin_unlock(&lock->slock);
}

/**
 * __read_lock - acquire read lock of a queue rwlock
 * @lock: Pointer to queue rwlock structure
 */
static void __read_lock(rwlock_t* lock) {
    unsigned int cnts;

    /* fast path */
    cnts = atomic_add_return(_QR_BIAS, &lock->cnts);
    if (likely(!(cnts & _QW_WMASK)))
        return;

    __read_lock_slowpath(lock);
}

/**
 * __read_unlock - release read lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
static void __read_unlock(rwlock_t* lock) {
    memory_lfence();
    /* Atomically decrement the reader count */
    atomic_sub(_QR_BIAS, &lock->cnts);
}

static void __write_lock_slowpath(rwlock_t* lock) {
    int cnts;

    /* Put the writer into the wait queue */
    spin_lock(&lock->slock);

    /* Try to acquire the lock directly if no reader is present */
    if (!atomic_read(&lock->cnts) &&
        (atomic_cmpxchg(&lock->cnts, 0, _QW_LOCKED) == 0))
        goto unlock;

    /*
     * Set the waiting flag to notify readers that a writer is pending,
     * or wait for a previous writer to go away.
     */
    for (;;) {
        cnts = atomic_read(&lock->cnts);
        if (!(cnts & _QW_WMASK) &&
            (atomic_cmpxchg(&lock->cnts, cnts, cnts | _QW_WAITING) == cnts))
            break;

        cpu_relax();
    }

    /* When no more readers, set the lock flag */
    for (;;) {
        cnts = atomic_read(&lock->cnts);
        if ((cnts == _QW_WAITING) &&
            (atomic_cmpxchg(&lock->cnts, _QW_WAITING, _QW_LOCKED) == _QW_WAITING))
            break;

        cpu_relax();
    }

    unlock:
    spin_unlock(&lock->slock);
}

/**
 * __write_lock - acquire write lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
static void __write_lock(rwlock_t* lock) {
    /* fast path */
    if (atomic_cmpxchg(&lock->cnts, 0, _QW_LOCKED) == 0)
        return;

    /* slow path */
    __write_lock_slowpath(lock);
}

/**
 * __write_unlock - release write lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
static void __write_unlock(rwlock_t* lock) {
    memory_sfence();
    atomic_sub(_QW_LOCKED, &lock->cnts);
}

/*
 * Reader/writer consistent mechanism without starving writers. This type of
 * lock for data where the reader wants a consistent set of information
 * and is willing to retry if the information changes. There are two types
 * of readers:
 * 1. Sequence readers which never block a writer but they may have to retry
 *    if a writer is in progress by detecting change in sequence number.
 *    Writers do not wait for a sequence reader.
 * 2. Locking readers which will wait if a writer or another locking reader
 *    is in progress. A locking reader in progress will also block a writer
 *    from going forward. Unlike the regular rwlock, the read lock here is
 *    exclusive so that only one locking reader can get it.
 *
 * This is not as cache friendly as brlock. Also, this may not work well
 * for data that contains pointers, because any writer could
 * invalidate a pointer that a reader was following.
 *
 * Expected non-blocking reader usage:
 * 	do {
 *	    seq = read_seqbegin(&foo);
 * 	...
 *      } while (read_seqretry(&foo, seq));
 *
 *
 * On non-SMP the spin locks disappear but the writer still needs
 * to increment the sequence variables because an interrupt routine could
 * change the state of the data.
 *
 * Based on x86_64 vsyscall gettimeofday
 * by Keith Owens and Andrea Arcangeli
 */

/*
 * Version using sequence version only.
 * This can be used when code has its own mutex protecting the
 * updating starting before the write_seqcountbeqin() and ending
 * after the write_seqcount_end().
 */
typedef struct seqcount {
	unsigned sequence;
} seqcount_t;

static inline void seqcount_init(seqcount_t *s)
{
	s->sequence = 0;
}

#define SEQCNT_ZERO { .sequence = 0 }


/**
 * __read_seqcount_begin - begin a seq-read critical section (without barrier)
 * @s: pointer to seqcount_t
 * Returns: count to be passed to read_seqcount_retry
 *
 * __read_seqcount_begin is like read_seqcount_begin, but has no smp_rmb()
 * barrier. Callers should ensure that smp_rmb() or equivalent ordering is
 * provided before actually loading any of the variables that are to be
 * protected in this critical section.
 *
 * Use carefully, only in critical code, and comment how the barrier is
 * provided.
 */
static inline unsigned __read_seqcount_begin(const seqcount_t *s)
{
	unsigned ret;

repeat:
	ret = ACCESS_ONCE(s->sequence);
	if (unlikely(ret & 1)) {
		cpu_relax();
		goto repeat;
	}
	return ret;
}

/**
 * raw_read_seqcount - Read the raw seqcount
 * @s: pointer to seqcount_t
 * Returns: count to be passed to read_seqcount_retry
 *
 * raw_read_seqcount opens a read critical section of the given
 * seqcount without any lockdep checking and without checking or
 * masking the LSB. Calling code is responsible for handling that.
 */
static inline unsigned raw_read_seqcount(const seqcount_t *s)
{
	unsigned ret = ACCESS_ONCE(s->sequence);
	smp_rmb();
	return ret;
}

/**
 * raw_read_seqcount_begin - start seq-read critical section w/o lockdep
 * @s: pointer to seqcount_t
 * Returns: count to be passed to read_seqcount_retry
 *
 * raw_read_seqcount_begin opens a read critical section of the given
 * seqcount, but without any lockdep checking. Validity of the critical
 * section is tested by checking read_seqcount_retry function.
 */
static inline unsigned raw_read_seqcount_begin(const seqcount_t *s)
{
	unsigned ret = __read_seqcount_begin(s);
	smp_rmb();
	return ret;
}

/**
 * read_seqcount_begin - begin a seq-read critical section
 * @s: pointer to seqcount_t
 * Returns: count to be passed to read_seqcount_retry
 *
 * read_seqcount_begin opens a read critical section of the given seqcount.
 * Validity of the critical section is tested by checking read_seqcount_retry
 * function.
 */
static inline unsigned read_seqcount_begin(const seqcount_t *s)
{
	return raw_read_seqcount_begin(s);
}

/**
 * raw_seqcount_begin - begin a seq-read critical section
 * @s: pointer to seqcount_t
 * Returns: count to be passed to read_seqcount_retry
 *
 * raw_seqcount_begin opens a read critical section of the given seqcount.
 * Validity of the critical section is tested by checking read_seqcount_retry
 * function.
 *
 * Unlike read_seqcount_begin(), this function will not wait for the count
 * to stabilize. If a writer is active when we begin, we will fail the
 * read_seqcount_retry() instead of stabilizing at the beginning of the
 * critical section.
 */
static inline unsigned raw_seqcount_begin(const seqcount_t *s)
{
	unsigned ret = ACCESS_ONCE(s->sequence);
	smp_rmb();
	return ret & ~1;
}

/**
 * __read_seqcount_retry - end a seq-read critical section (without barrier)
 * @s: pointer to seqcount_t
 * @start: count, from read_seqcount_begin
 * Returns: 1 if retry is required, else 0
 *
 * __read_seqcount_retry is like read_seqcount_retry, but has no smp_rmb()
 * barrier. Callers should ensure that smp_rmb() or equivalent ordering is
 * provided before actually loading any of the variables that are to be
 * protected in this critical section.
 *
 * Use carefully, only in critical code, and comment how the barrier is
 * provided.
 */
static inline int __read_seqcount_retry(const seqcount_t *s, unsigned start)
{
	return unlikely(s->sequence != start);
}

/**
 * read_seqcount_retry - end a seq-read critical section
 * @s: pointer to seqcount_t
 * @start: count, from read_seqcount_begin
 * Returns: 1 if retry is required, else 0
 *
 * read_seqcount_retry closes a read critical section of the given seqcount.
 * If the critical section was invalid, it must be ignored (and typically
 * retried).
 */
static inline int read_seqcount_retry(const seqcount_t *s, unsigned start)
{
	smp_rmb();
	return __read_seqcount_retry(s, start);
}



static inline void raw_write_seqcount_begin(seqcount_t *s)
{
	s->sequence++;
	smp_wmb();
}

static inline void raw_write_seqcount_end(seqcount_t *s)
{
	smp_wmb();
	s->sequence++;
}

/**
 * raw_write_seqcount_barrier - do a seq write barrier
 * @s: pointer to seqcount_t
 *
 * This can be used to provide an ordering guarantee instead of the
 * usual consistency guarantee. It is one wmb cheaper, because we can
 * collapse the two back-to-back wmb()s.
 *
 *      seqcount_t seq;
 *      bool X = true, Y = false;
 *
 *      void read(void)
 *      {
 *              bool x, y;
 *
 *              do {
 *                      int s = read_seqcount_begin(&seq);
 *
 *                      x = X; y = Y;
 *
 *              } while (read_seqcount_retry(&seq, s));
 *
 *              BUG_ON(!x && !y);
 *      }
 *
 *      void write(void)
 *      {
 *              Y = true;
 *
 *              raw_write_seqcount_barrier(seq);
 *
 *              X = false;
 *      }
 */
static inline void raw_write_seqcount_barrier(seqcount_t *s)
{
	s->sequence++;
	smp_wmb();
	s->sequence++;
}

static inline int raw_read_seqcount_latch(seqcount_t *s)
{
	int seq = ACCESS_ONCE(s->sequence);
	/* Pairs with the first smp_wmb() in raw_write_seqcount_latch() */
	// smp_read_barrier_depends();
	return seq;
}

/**
 * raw_write_seqcount_latch - redirect readers to even/odd copy
 * @s: pointer to seqcount_t
 *
 * The latch technique is a multiversion concurrency control method that allows
 * queries during non-atomic modifications. If you can guarantee queries never
 * interrupt the modification -- e.g. the concurrency is strictly between CPUs
 * -- you most likely do not need this.
 *
 * Where the traditional RCU/lockless data structures rely on atomic
 * modifications to ensure queries observe either the old or the new state the
 * latch allows the same for non-atomic updates. The trade-nr is doubling the
 * cost of storage; we have to maintain two copies of the entire data
 * structure.
 *
 * Very simply put: we first modify one copy and then the other. This ensures
 * there is always one copy in a stable state, ready to give us an answer.
 *
 * The basic form is a data structure like:
 *
 * struct latch_struct {
 *	seqcount_t		seq;
 *	struct data_struct	data[2];
 * };
 *
 * Where a modification, which is assumed to be externally serialized, does the
 * following:
 *
 * void latch_modify(struct latch_struct *latch, ...)
 * {
 *	smp_wmb();	<- Ensure that the last data[1] update is visible
 *	latch->seq++;
 *	smp_wmb();	<- Ensure that the seqcount update is visible
 *
 *	modify(latch->data[0], ...);
 *
 *	smp_wmb();	<- Ensure that the data[0] update is visible
 *	latch->seq++;
 *	smp_wmb();	<- Ensure that the seqcount update is visible
 *
 *	modify(latch->data[1], ...);
 * }
 *
 * The query will have a form like:
 *
 * struct entry *latch_query(struct latch_struct *latch, ...)
 * {
 *	struct entry *entry;
 *	unsigned seq, idx;
 *
 *	do {
 *		seq = raw_read_seqcount_latch(&latch->seq);
 *
 *		idx = seq & 0x01;
 *		entry = data_query(latch->data[idx], ...);
 *
 *		smp_rmb();
 *	} while (seq != latch->seq);
 *
 *	return entry;
 * }
 *
 * So during the modification, queries are first redirected to data[1]. Then we
 * modify data[0]. When that is complete, we redirect queries back to data[0]
 * and we can modify data[1].
 *
 * NOTE: The non-requirement for atomic modifications does _NOT_ include
 *       the publishing of new entries in the case where data is a dynamic
 *       data structure.
 *
 *       An iteration might start in data[0] and get suspended long enough
 *       to miss an entire modification sequence, once it resumes it might
 *       observe the new entry.
 *
 * NOTE: When data is a dynamic data structure; one should use regular RCU
 *       patterns to manage the lifetimes of the objects within.
 */
static inline void raw_write_seqcount_latch(seqcount_t *s)
{
       smp_wmb();      /* prior stores before incrementing "sequence" */
       s->sequence++;
       smp_wmb();      /* increment "sequence" before following stores */
}

/*
 * Sequence version only version assumes that callers are using their
 * own mutexing.
 */
static inline void write_seqcount_begin_nested(seqcount_t *s, int subclass)
{
	raw_write_seqcount_begin(s);
}

static inline void write_seqcount_begin(seqcount_t *s)
{
	write_seqcount_begin_nested(s, 0);
}

static inline void write_seqcount_end(seqcount_t *s)
{
	raw_write_seqcount_end(s);
}

/**
 * write_seqcount_invalidate - invalidate in-progress read-side seq operations
 * @s: pointer to seqcount_t
 *
 * After write_seqcount_invalidate, no read-side seq operations will complete
 * successfully and see data older than this.
 */
static inline void write_seqcount_invalidate(seqcount_t *s)
{
	smp_wmb();
	s->sequence+=2;
}

#endif //LOCK_H
