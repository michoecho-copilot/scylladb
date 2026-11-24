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
// The table object holds a reference from its construction until it stops.
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
public:
    table_storage(sstables_manager& manager, table_id id, seastar::lw_shared_ptr<const data_dictionary::storage_options> options, bool counted);
    table_storage(const table_storage&) = delete;
    table_storage& operator=(const table_storage&) = delete;
    ~table_storage();

    table_id id() const noexcept { return _id; }
    // The storage options of the table. For local storage, they name the
    // directory of the table.
    const seastar::lw_shared_ptr<const data_dictionary::storage_options>& options() const noexcept { return _options; }
};

} // namespace sstables
