/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Masstree-based index implementation
 *
 * Hohai University
 */

#include "utils.h"
#include "k.h"

#include "masstree/config.h"
#include "masstree/compiler.hh"
#include "masstree/kvthread.hh"
#include "masstree/masstree.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_print.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/masstree_stats.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/string.hh"

#include "masstree/string.cc"
#include "masstree/straccum.cc"
#include "masstree/kvthread.cc"

typedef void index_t;

struct table_params : Masstree::nodeparams<> {
    using value_type = void *;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = threadinfo;
    using key_unparse_type = Masstree::key_unparse_printable_string;
};

using Str = Masstree::Str;
using table_type = Masstree::basic_table<table_params>;
using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
using cursor_type = Masstree::tcursor<table_params>;
using leaf_type = Masstree::leaf<table_params>;
using internode_type = Masstree::internode<table_params>;

using node_type = Masstree::node_base<table_params>;

static std::atomic<int> thread_id(0);
static __thread table_params::threadinfo_type *ti = nullptr;

struct mt_index {
    table_type tab;
};

struct scanner {
    explicit scanner(void **val) : val(val) { }

    template <typename ScanStackElt, typename Key>
    void visit_leaf(const ScanStackElt &iter, const Key &key, threadinfo &) {
        /* do nothing */
    }

    bool visit_value(const Str key, void *val, threadinfo &) {
        *this->val = val;
        return false;
    }

private:
    void **val;
};

extern "C" {

index_t *index_create() {
    auto *mti = new mt_index;
    bonsai_assert(!ti);
    int cur = thread_id++;
    ti = threadinfo::make(threadinfo::TI_MAIN, cur);
    mti->tab.initialize(*ti);
    return mti;
}

void index_thread_init(index_t *index) {
    if (ti) {
        return;
    }
    int cur = thread_id++;
    ti = threadinfo::make(threadinfo::TI_PROCESS, cur);
    pr_debug(10, "masstree index thread init (thread id: %d)", cur);
}

void index_destroy(index_t *index) {
    delete static_cast<mt_index *>(index);
}

int index_upsert(index_t *index, k_t key, void *val) {
    auto *mti = static_cast<mt_index *>(index);
    cursor_type lp(mti->tab, key.key, key.len);
    lp.find_insert(*ti);
    lp.value() = val;
    fence();
    lp.finish(1, *ti);
    return 0;
}

int index_remove(index_t *index, k_t key) {
    auto *mti = static_cast<mt_index *>(index);
    cursor_type lp(mti->tab, key.key, key.len);
    if (unlikely(!lp.find_locked(*ti))) {
        lp.finish(0, *ti);
        return -ENOENT;
    }
    lp.finish(-1, *ti);
    return 0;
}

void *index_find_first_ge(index_t *index, k_t key) {
    auto *mti = static_cast<mt_index *>(index);
    void *val = ERR_PTR(-ENOENT);
    auto k = Str(key.key, key.len);
    scanner scanner(&val);
    mti->tab.scan(k, true, scanner, *ti);
    return val;
}

}
