/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "schema/schema_fwd.hh"

namespace data_dictionary {
struct storage_options;
}

namespace sstables {

class sstables_manager;

class table_storage;
using table_storage_ptr = seastar::lw_shared_ptr<table_storage>;

// A reference to the storage of a table, held on one shard.
//
// The storage of a table is the place where its sstables live. On local
// storage, it is the directory of the table in every data directory. The
// reference keeps the storage in place: the directory is removed only when
// no shard holds a reference, see storage_manager::drop_table_storage().
//
// The reference is also the only way to tell the sstables manager where an
// sstable lives: sstables_manager::make_sstable() takes a reference, and the
// sstable keeps it. So an sstable can't be made in a directory that nothing
// keeps in place, and its files can be opened by name for as long as the
// sstable object lives.
//
// The table object holds a reference from its construction until it stops.
// Whoever makes an sstable of the table gets the reference from the table,
// see replica::table::get_table_storage().
//
// sstables_manager::open_table_storage() makes a reference, and creates the
// storage if needed. The destructor releases the reference.
class table_storage : public seastar::enable_lw_shared_from_this<table_storage> {
    sstables_manager& _manager;
    table_id _id;
    seastar::lw_shared_ptr<const data_dictionary::storage_options> _options;
    // Whether the reference is counted by the storage manager, which then
    // removes the storage of a dropped table once the last reference is gone.
    // Only local storage that open_table_storage() opened is counted.
    bool _counted;
    // The reference that this one was derived from with with_options(). It
    // is held so that the storage stays in place.
    table_storage_ptr _parent;
public:
    table_storage(sstables_manager& manager, table_id id, seastar::lw_shared_ptr<const data_dictionary::storage_options> options, bool counted,
            table_storage_ptr parent = nullptr);
    table_storage(const table_storage&) = delete;
    table_storage& operator=(const table_storage&) = delete;
    ~table_storage();

    table_id id() const noexcept { return _id; }
    // The storage options of the table. For local storage, they name the
    // directory of the table.
    const seastar::lw_shared_ptr<const data_dictionary::storage_options>& options() const noexcept { return _options; }
    // Returns a reference to the same storage, with other storage options.
    // It keeps this reference alive. It is for sstables of the table that
    // live elsewhere than options() say: in the directory of the table in
    // another data directory, or under another object storage prefix.
    table_storage_ptr with_options(seastar::lw_shared_ptr<const data_dictionary::storage_options> options);
};

} // namespace sstables
