/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <cctype>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include "utils/log.hh"
#include "utils/error_injection.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_registry.hh"
#include "sstables/partition_index_cache.hh"
#include "sstables/sstables.hh"
#include "object_storage_client.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/config.hh"
#include "db/object_storage_endpoint_param.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "utils/assert.hh"
#include "utils/http.hh"
#include "exceptions/exceptions.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

const storage& atomic_deletion::get_storage(const std::vector<shared_sstable>& ssts) {
    auto& storage = ssts.front()->get_storage();
    for (const auto& sst : ssts | std::views::drop(1)) {
        auto& other_storage = sst->get_storage();
        if (storage != other_storage) {
            on_internal_error(smlogger, fmt::format(
                    "atomic_deletion created with SSTables from different storage contexts: front={} front_storage={} offending={} offending_storage={}",
                    ssts.front()->get_filename(), storage.prefix(),
                    sst->get_filename(), other_storage.prefix()));
        }
    }
    return storage;
}

atomic_deletion::atomic_deletion(std::vector<shared_sstable> ssts)
    : _ssts(std::move(ssts))
    , _storage(_ssts.empty() ? nullptr : &get_storage(_ssts))
    , _impl(_storage ? _storage->make_atomic_deletion_impl() : nullptr) {
}

future<> atomic_deletion::commit() {
    if (empty()) {
        co_return;
    }
    if (utils::get_local_injector().enter("delete_atomically_before_prepare")) {
        throw std::runtime_error("delete_atomically_before_prepare");
    }
    co_await _impl->commit(_ssts);
    utils::get_local_injector().inject("delete_atomically_after_prepare",
            [] { throw std::runtime_error("delete_atomically_after_prepare"); });
}

future<> atomic_deletion::execute() noexcept {
    if (empty()) {
        co_return;
    }
    try {
        co_await coroutine::parallel_for_each(_ssts, [this] (shared_sstable sst) {
            return sst->unlink(this);
        });
        utils::get_local_injector().inject("delete_atomically_after_unlink",
                [] { throw std::runtime_error("delete_atomically_after_unlink"); });
        co_await _impl->finalize();
    } catch (...) {
        // After commit(), the SSTables will be deleted even after a crash.
        smlogger.warn("SSTables deletion failed after commit: {:t}. Will be retried on restart.", std::current_exception());
    }
}

sstables_manager::sstables_manager(
    sstring name, db::large_data_handler& large_data_handler, db::corrupt_data_handler& corrupt_data_handler, struct config cfg, gms::feature_service& feat, cache_tracker& ct, directory_semaphore& dir_sem,
    noncopyable_function<locator::host_id()>&& resolve_host_id, sstable_compressor_factory& compressor_factory, const abort_source& abort, std::vector<file_io_extension*> file_io_extensions, scheduling_group maintenance_sg, storage_manager* shared)
    : _storage(shared)
    , _large_data_handler(large_data_handler)
    , _corrupt_data_handler(corrupt_data_handler)
    , _config(std::move(cfg))
    , _file_io_extensions(std::move(file_io_extensions))
    , _features(feat)
    , _cache_tracker(ct)
    , _sstable_metadata_concurrency_sem(
        max_count_sstable_metadata_concurrent_reads,
        max_memory_sstable_metadata_concurrent_reads(_config.available_memory),
        fmt::format("sstables_manager_{}", name),
        std::numeric_limits<size_t>::max(),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        utils::updateable_value(std::numeric_limits<uint32_t>::max()),
        utils::updateable_value(uint32_t(1)),
        utils::updateable_value(0.0f),
        reader_concurrency_semaphore::register_metrics::no,
        reader_concurrency_semaphore_shared_pool::empty_pool())
    , _dir_semaphore(dir_sem)
    , _resolve_host_id(std::move(resolve_host_id))
    , _maintenance_sg(std::move(maintenance_sg))
    , _compressor_factory(compressor_factory)
    , _abort(abort)
    , _signal_gate("sstables_manager::signal")
{
    _components_reloader_status = components_reclaim_reload_fiber();
}

sstables_manager::~sstables_manager() {
    SCYLLA_ASSERT(_closing);
    SCYLLA_ASSERT(_active.empty());
    SCYLLA_ASSERT(_undergoing_close.empty());
}

void sstables_manager::subscribe(sstables_manager_event_handler& handler) {
    handler.subscribe(_signal_source.connect([this, &handler] (sstables::generation_type gen, notification_event_type event) mutable -> future<> {
        if (auto gh = _signal_gate.try_hold()) {
            // Note: the slot must not touch its captures after suspending.
            // The connection, and with it this closure, may be gone by the
            // time it is resumed.
            co_await handler.notify(gen, event);
        }
    }));
}

using osp = db::object_storage_endpoint_param;

storage_manager::object_storage_endpoint::object_storage_endpoint(db::object_storage_endpoint_param ep)
    : cfg(ep)
{}

storage_manager::storage_manager(const db::config& cfg, config stm_cfg)
    : _object_storage_clients_memory(stm_cfg.object_storage_clients_memory)
    , _connections_per_shard(cfg.object_storage_connections_per_shard())
    , _config_updater(std::make_unique<config_updater_sync>(cfg, *this))
    , _connections_updater(std::make_unique<connections_updater_sync>(cfg, *this))
{
    for (auto& e : cfg.object_storage_endpoints()) {
        _object_storage_endpoints.emplace(std::make_pair(e.key(), e));
    }

    if (!stm_cfg.skip_metrics_registration) {
        namespace sm = seastar::metrics;
        metrics.add_group("object_storage", {
            sm::make_gauge("memory_usage", [this, limit = stm_cfg.object_storage_clients_memory] { return limit - _object_storage_clients_memory.available_units(); },
                    sm::description("Total number of bytes consumed by object storage client"), {}),
        });
    }
}

future<> storage_manager::stop() {
    for (auto ep : _object_storage_endpoints) {
        if (ep.second.client != nullptr) {
            co_await ep.second.client->close();
        }
    }
}


auto storage_manager::use_table_storage(table_id id) -> table_storage_state_ptr {
    if (this_shard_id() != 0) {
        on_internal_error(smlogger, "The storage of tables is tracked on shard 0");
    }
    auto& ts = _table_storages[id];
    if (!ts) {
        ts = make_lw_shared<table_storage_state>();
    }
    ++ts->users;
    return ts;
}

future<> storage_manager::unuse_table_storage(table_id id, table_storage_state_ptr ts) noexcept {
    if (--ts->users != 0) {
        co_return;
    }
    auto units = co_await get_units(ts->lock, 1);
    if (ts->users != 0) {
        // A new table took the storage over while this waited for the lock.
        co_return;
    }
    if (ts->dropped) {
        co_await utils::get_local_injector().inject("storage_manager_remove_table_storage_wait", utils::wait_for_message(std::chrono::seconds(60)));
        for (const auto& dir : ts->dirs) {
            try {
                smlogger.info("Removing the directory {} of dropped table {}", dir, id);
                co_await remove_table_directory_if_has_no_snapshots(dir);
            } catch (...) {
                smlogger.warn("Failed to remove the directory {} of dropped table {}: {}", dir, id, std::current_exception());
            }
        }
        ts->created = false;
        ts->dropped = false;
    }
    // A user that came during the removal waits for the lock, and keeps the
    // state. It creates the directories again.
    if (ts->users == 0) {
        if (auto it = _table_storages.find(id); it != _table_storages.end() && it->second == ts) {
            _table_storages.erase(it);
            ts->unused.set_value();
        }
    }
}

future<> storage_manager::acquire_table_storage(table_id id, std::vector<std::filesystem::path> dirs) {
    auto ts = use_table_storage(id);
    std::exception_ptr ex;
    try {
        utils::get_local_injector().enter("storage_manager_acquire_table_storage");
        // Waits for the removal of the directories of a dropped table with
        // the same ID, if one is running.
        auto units = co_await get_units(ts->lock, 1);
        ts->dropped = false;
        ts->dirs = std::move(dirs);
        if (!ts->created) {
            co_await init_table_directories(ts->dirs);
            ts->created = true;
        }
        co_return;
    } catch (...) {
        ex = std::current_exception();
    }
    co_await unuse_table_storage(id, std::move(ts));
    std::rethrow_exception(std::move(ex));
}

future<> storage_manager::release_table_storage(table_id id) noexcept {
    auto it = _table_storages.find(id);
    if (it == _table_storages.end()) {
        on_internal_error_noexcept(smlogger, format("Releasing the storage of table {}, which has no user", id));
        return make_ready_future<>();
    }
    return unuse_table_storage(id, it->second);
}

future<> storage_manager::drop_table_storage(table_id id, std::vector<std::filesystem::path> dirs) {
    auto ts = use_table_storage(id);
    try {
        // The mark is set in the order of the lock, so that a table that is
        // created later with the same ID clears it.
        auto units = co_await get_units(ts->lock, 1);
        ts->dropped = true;
        ts->dirs = std::move(dirs);
    } catch (...) {
        smlogger.warn("Failed to mark the storage of table {} as dropped: {}", id, std::current_exception());
    }
    // Removes the directories now if this is the only user.
    co_await unuse_table_storage(id, std::move(ts));
}

future<bool> storage_manager::remove_table_directory_if_unused(table_id id, std::filesystem::path dir) {
    auto ts = use_table_storage(id);
    bool unused = false;
    try {
        auto units = co_await get_units(ts->lock, 1);
        unused = ts->users == 1;
        if (unused) {
            smlogger.info("Removing the directory {} of dropped table {}", dir, id);
            co_await remove_table_directory_if_has_no_snapshots(dir);
            ts->created = false;
        }
    } catch (...) {
        smlogger.warn("Failed to remove the directory {} of dropped table {}: {}", dir, id, std::current_exception());
    }
    co_await unuse_table_storage(id, std::move(ts));
    co_return unused;
}

future<> storage_manager::await_table_storage_unused(table_id id) {
    auto it = _table_storages.find(id);
    if (it == _table_storages.end()) {
        return make_ready_future<>();
    }
    return it->second->unused.get_shared_future();
}

auto storage_manager::get_endpoint(const sstring& endpoint) -> object_storage_endpoint& {
    auto found = _object_storage_endpoints.find(endpoint);
    if (found == _object_storage_endpoints.end()) {
        smlogger.error("unable to find {} in configured object-storage endpoints", endpoint);
        throw std::invalid_argument(format("endpoint {} not found", endpoint));
    }
    return found->second;
}

shared_ptr<sstables::object_storage_client> storage_manager::get_endpoint_client(sstring endpoint) {
    auto& ep = get_endpoint(endpoint);
    if (ep.client == nullptr) {
        ep.client = make_object_storage_client(ep.cfg, _object_storage_clients_memory, [&ct = container()] (std::string ep) {
            return ct.local().get_endpoint_client(ep);
        }, _connections_per_shard);
    }
    return ep.client;
}

sstring storage_manager::get_endpoint_type(sstring endpoint) {
    return get_endpoint(endpoint).cfg.type();
}

bool storage_manager::is_known_endpoint(sstring endpoint, sstring type) const {
    auto it = _object_storage_endpoints.find(endpoint);
    return it != _object_storage_endpoints.end() && (type == "" || it->second.cfg.is_storage_of_type(type));
}

std::vector<sstring> storage_manager::endpoints(sstring type) const noexcept {
    return _object_storage_endpoints | std::views::filter([&type] (auto& ep) {
        return type == "" || ep.second.cfg.is_storage_of_type(type);
    }) | std::views::keys | std::ranges::to<std::vector>();
}

storage_manager::config_updater_sync::config_updater_sync(const db::config& cfg, storage_manager& sstm)
    : observer(cfg.object_storage_endpoints.observe([&sstm, &cfg] (const std::vector<db::object_storage_endpoint_param>&) {
        // Sync part: runs atomically in the current reactor turn 
        // Update _object_storage_endpoints so that any subsequent call to
        // get_endpoint_client() immediately sees the new configuration.
        // Each client's update_config_sync() spawns its own async background
        // work (credential refresh for S3, old client close for GCS) into a
        // client-local gated fiber.
        std::unordered_set<sstring> updates;
        for (auto& e : cfg.object_storage_endpoints()) {
            auto endpoint = e.key();
            smlogger.info("config_updater: endpoint={}, config={}", endpoint, e);
            updates.insert(endpoint);
            auto [it, added] = sstm._object_storage_endpoints.try_emplace(endpoint, e);
            if (!added) {
                if (it->second.client) {
                    it->second.client->update_config_sync(e);
                }
                it->second.cfg = e;
            }
        }
        std::erase_if(sstm._object_storage_endpoints, [&updates](const auto& e) {
            return !updates.contains(e.first);
        });
    }))
{}

storage_manager::connections_updater_sync::connections_updater_sync(const db::config& cfg, storage_manager& sstm)
    : observer(cfg.object_storage_connections_per_shard.observe([&sstm] (unsigned new_value) {
        smlogger.info("connections_updater: updating connections_per_shard to {}", new_value);
        sstm._connections_per_shard = new_value;
        for (auto& [endpoint, ep] : sstm._object_storage_endpoints) {
            if (ep.client) {
                ep.client->update_connections_per_shard(new_value);
            }
        }
    }))
{}

sstables::sstable::version_types sstables_manager::get_highest_supported_format() const noexcept {
     if (_features.mt_sstable) {
         return sstable_version_types::mt;
     } else if (_features.ms_sstable) {
         return sstable_version_types::ms;
     } else {
         return sstable_version_types::me;
     }
}

sstables::sstable::version_types sstables_manager::get_preferred_sstable_version() const {
    auto preferred_format = sstables::version_from_string(_config.format());
    auto mt_supported = bool(_features.mt_sstable);
    if (mt_supported && preferred_format == sstable_version_types::mt) {
        return sstable_version_types::mt;
    }
    // `mt` supersedes `ms` (it's just `ms`, but with a problem fixed), so
    // if `ms` is chosen, but `mt` is available, we assume that the user
    // wants `mt` and we silently pick that instead.
    if (mt_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::mt;
    }
    auto ms_supported = bool(_features.ms_sstable);
    if (ms_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::ms;
    }
    return sstable_version_types::me;
}

sstables::sstable::version_types sstables_manager::get_safe_sstable_version_for_rewrites(sstable_version_types existing_version) const {
    auto preferred_format = sstables::version_from_string(_config.format());
    auto mt_supported = bool(_features.mt_sstable) || existing_version >= sstable_version_types::mt;
    if (mt_supported && preferred_format == sstable_version_types::mt) {
        return sstable_version_types::mt;
    }
    // `mt` supersedes `ms` (it's just `ms`, but with a problem fixed), so
    // if `ms` is chosen, but `mt` is available, we assume that the user
    // wants `mt` and we silently pick that instead.
    if (mt_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::mt;
    }
    auto ms_supported = bool(_features.ms_sstable) || existing_version >= sstable_version_types::ms;
    if (ms_supported && preferred_format == sstable_version_types::ms) {
        return sstable_version_types::ms;
    }
    return sstable_version_types::me;
}

locator::host_id sstables_manager::get_local_host_id() const {
    return _resolve_host_id();
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        table_storage_ptr storage,
        generation_type generation,
        optimized_optional<sstable_id> sid,
        sstable_state state,
        sstable_version_types v,
        sstable_format_types f,
        db_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    if (!storage) {
        on_internal_error(smlogger, format("Making an sstable of table {} without a reference to its storage", schema->id()));
    }
    return make_lw_shared<sstable>(std::move(schema), std::move(storage), generation, sid, state, v, f, get_large_data_handler(), get_corrupt_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
}

sstable_writer_config sstables_manager::configure_writer(sstring origin) const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _config.column_index_size;
    cfg.promoted_index_auto_scale_threshold = (size_t)_config.column_index_auto_scale_threshold_in_kb() * 1024;
    if (!cfg.promoted_index_auto_scale_threshold) {
        cfg.promoted_index_auto_scale_threshold = std::numeric_limits<size_t>::max();
    }
    cfg.validation_level = _config.enable_sstable_key_validation
            ? mutation_fragment_stream_validation_level::clustering_key
            : mutation_fragment_stream_validation_level::token;
    cfg.summary_byte_cost = summary_byte_cost(_config.sstable_summary_ratio);
    cfg.summary_max_partitions_per_page = _config.sstable_summary_max_partitions_per_page();

    cfg.origin = std::move(origin);
    cfg.large_data_records_per_sstable = _config.large_data_records_per_sstable();

    return cfg;
}

void sstables_manager::increment_total_reclaimable_memory(sstable* sst) {
    _total_reclaimable_memory += sst->total_reclaimable_memory_size();
    _components_memory_change_event.signal();
}

future<> sstables_manager::maybe_reclaim_components() {
    while(_total_reclaimable_memory > get_components_memory_reclaim_threshold()) {
        // Memory consumption is above threshold. Reclaim from the SSTable that
        // has the most reclaimable memory to get the total consumption under limit.
        // FIXME: Take SSTable usage into account during reclaim - see https://github.com/scylladb/scylladb/issues/21897
        auto sst_with_max_memory = std::max_element(_active.begin(), _active.end(), [](const sstable& sst1, const sstable& sst2) {
            return sst1.total_reclaimable_memory_size() < sst2.total_reclaimable_memory_size();
        });

        auto memory_reclaimed = sst_with_max_memory->reclaim_memory_from_components();
        _total_memory_reclaimed += memory_reclaimed;
        _total_reclaimable_memory -= memory_reclaimed;
        _reclaimed.insert(*sst_with_max_memory);
        // TODO: As of now only bloom filter is reclaimed. Print actual component names when adding support for more components.
        smlogger.info("Reclaimed {} bytes of memory from components of {}. Total memory reclaimed so far is {} bytes",
                memory_reclaimed, sst_with_max_memory->get_filename(), _total_memory_reclaimed);
        }
        co_await coroutine::maybe_yield();
}

size_t sstables_manager::get_components_memory_reclaim_threshold() const {
    return _config.available_memory * _config.memory_reclaim_threshold();
}

size_t sstables_manager::get_memory_available_for_reclaimable_components() const {
    return get_components_memory_reclaim_threshold() - _total_reclaimable_memory;
}

future<> sstables_manager::components_reclaim_reload_fiber() {
    auto components_memory_reclaim_threshold_observer = _config.memory_reclaim_threshold.observe([&] (double) {
        // any change to the components_memory_reclaim_threshold config should trigger reload/reclaim
        _components_memory_change_event.signal();
    });

    co_await coroutine::switch_to(_maintenance_sg);

    sstlog.trace("components_reloader_fiber start");

    while (true) {
        co_await _components_memory_change_event.when();

        if (_closing) {
            co_return;
        }

        if (_total_reclaimable_memory > get_components_memory_reclaim_threshold()) {
            // reclaim memory to bring total memory usage under threshold
            co_await maybe_reclaim_components();
        } else {
            // memory available for reloading components of previously reclaimed SSTables
            co_await maybe_reload_components();
        }
    }
}

future<> sstables_manager::maybe_reload_components() {
    // Reload bloom filters from the smallest to largest so as to maximize
    // the number of bloom filters being reloaded.
    auto memory_available = get_memory_available_for_reclaimable_components();
    while (!_reclaimed.empty() && memory_available > 0) {
        auto sstable_to_reload = _reclaimed.begin();
        const size_t reclaimed_memory = sstable_to_reload->total_memory_reclaimed();
        if (reclaimed_memory > memory_available) {
            // cannot reload anymore sstables
            break;
        }

        // Increment the total memory before reloading to prevent any parallel
        // fibers from loading new bloom filters into memory.
        _total_reclaimable_memory += reclaimed_memory;
        _reclaimed.erase(sstable_to_reload);
        // Use a lw_shared_ptr to prevent the sstable from getting deleted when
        // the components are being reloaded.
        auto sstable_ptr = sstable_to_reload->shared_from_this();
        try {
            co_await sstable_ptr->reload_reclaimed_components();
        } catch (...) {
            // reload failed due to some reason
            sstlog.warn("Failed to reload reclaimed SSTable components : {:t}", std::current_exception());
            // revert back changes made before the reload
            _total_reclaimable_memory -= reclaimed_memory;
            _reclaimed.insert(*sstable_to_reload);
            break;
        }

        _total_memory_reclaimed -= reclaimed_memory;
        memory_available = get_memory_available_for_reclaimable_components();
    }
}

void sstables_manager::reclaim_memory_and_stop_tracking_sstable(sstable* sst) {
    // remove the sstable from the memory tracking metrics
    _total_reclaimable_memory -= sst->total_reclaimable_memory_size();
    _total_memory_reclaimed -= sst->total_memory_reclaimed();
    // reclaim any remaining memory from the sstable
    sst->reclaim_memory_from_components();
    // disable further reload of components
    erase_from_reclaimed(sst);
    sst->disable_component_memory_reload();
}

void sstables_manager::erase_from_reclaimed(sstable* sst) noexcept {
    if (sst->_manager_set_link.is_linked()) {
        _reclaimed.erase(_reclaimed.iterator_to(*sst));
    }
}

void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // Drop reclaimable components if they are still in memory
    // and remove SSTable from the reclaimable memory tracking
    reclaim_memory_and_stop_tracking_sstable(sst);

    // At this point, sst has a reference count of zero, since we got here from
    // lw_shared_ptr_deleter<sstables::sstable>::dispose().
    _active.erase(_active.iterator_to(*sst));
    _undergoing_close.push_back(*sst);
    // guard against sstable::close_files() calling shared_from_this() and immediately destroying
    // the result, which will dispose of the sstable recursively
    auto ptr = sst->shared_from_this();
    (void)sst->destroy().finally([ptr] {
        // destruction of ptr will call maybe_done() and release close()
    });
}

void sstables_manager::remove(sstable* sst) {
    _undergoing_close.erase(_undergoing_close.iterator_to(*sst));
    delete sst;
    _sstable_destroyed.broadcast();
    _components_memory_change_event.signal();
    maybe_done();
}

void sstables_manager::maybe_done() {
    if (_closing && _active.empty() && _undergoing_close.empty()) {
        _done.set_value();
    }
}

atomic_deletion sstables_manager::make_atomic_deletion(std::vector<shared_sstable> ssts) {
    return atomic_deletion(std::move(ssts));
}

future<utils::chunked_vector<sstable_snapshot_metadata>> sstables_manager::take_snapshot(std::vector<shared_sstable> ssts, sstring name) {
    utils::chunked_vector<sstable_snapshot_metadata> sstables_metadata;

    co_await _dir_semaphore.parallel_for_each(ssts, [&] (sstables::shared_sstable sstable) {
        auto& sst_stats = sstable->get_stats_metadata();
        sstable_snapshot_metadata md = {
            .id = sstable->sstable_identifier()->uuid(),
            .toc_name = sstable->component_basename(sstables::component_type::TOC),
            .data_size = sstable->data_size(),
            .index_size = sstable->index_size(),
            .first_token = dht::token::to_int64(sstable->get_first_decorated_key().token()),
            .last_token = dht::token::to_int64(sstable->get_last_decorated_key().token()),
            .repaired_at = sst_stats.repaired_at, 
        };
        sstables_metadata.push_back(std::move(md));
        return io_check([sstable, &name] {
            return sstable->snapshot(name);
        });
    });

    co_return sstables_metadata;
}

future<> sstables_manager::close() noexcept {
    _closing = true;
    maybe_done();
    co_await _done.get_future();
    co_await _table_storage_gate.close();
    co_await _sstable_metadata_concurrency_sem.stop();
    // stop the components reload fiber
    _components_memory_change_event.signal();
    co_await std::move(_components_reloader_status);
    co_await _signal_gate.close();
}

void sstables_manager::plug_sstables_registry(std::unique_ptr<sstables::sstables_registry> sr) noexcept {
    _sstables_registry = std::move(sr);
}

void sstables_manager::unplug_sstables_registry() noexcept {
    _sstables_registry.reset();
}

lw_shared_ptr<const data_dictionary::storage_options> sstables_manager::make_table_storage_options(const schema& s, const data_dictionary::storage_options& keyspace_so) const {
    return sstables::make_table_storage_options(*this, s, keyspace_so);
}

future<> sstables_manager::init_keyspace_storage(const data_dictionary::storage_options& so, sstring dir) {
    return sstables::init_keyspace_storage(*this, so, dir);
}

table_storage::table_storage(sstables_manager& manager, table_id id, lw_shared_ptr<const data_dictionary::storage_options> options, bool counted,
        table_storage_ptr parent)
    : _manager(manager)
    , _id(id)
    , _options(std::move(options))
    , _counted(counted)
    , _parent(std::move(parent))
{
}

table_storage::~table_storage() {
    if (_counted) {
        _manager.release_table_storage(_id);
    }
}

table_storage_ptr table_storage::with_options(lw_shared_ptr<const data_dictionary::storage_options> options) {
    return make_lw_shared<table_storage>(_manager, _id, std::move(options), false, _parent ? _parent : shared_from_this());
}

void sstables_manager::release_table_storage(table_id id) noexcept {
    // The last reference can be dropped anywhere, so the release runs in the
    // background. It removes the directory of a dropped table if this was the
    // last reference on any shard. close() waits for it.
    auto holder = _table_storage_gate.try_hold();
    if (!holder) {
        // Nothing can remove the directory anymore. It stays, as it does
        // when the node stops before the removal.
        smlogger.warn("The storage of table {} is released after the sstables manager is closed", id);
        return;
    }
    (void)_storage->container().invoke_on(0, [id] (storage_manager& sm) {
        return sm.release_table_storage(id);
    }).handle_exception([id] (std::exception_ptr ex) {
        smlogger.warn("Failed to release the storage of table {}: {}", id, ex);
    }).finally([holder = std::move(*holder)] {});
}

future<table_storage_ptr> sstables_manager::open_table_storage(const schema& s, const data_dictionary::storage_options& keyspace_so) {
    auto id = s.id();
    auto so = make_table_storage_options(s, keyspace_so);
    const auto* local = std::get_if<data_dictionary::storage_options::local>(&so->value);
    if (!local || local->dir.empty()) {
        // Object storage has no directory to create or to remove, so its
        // users need no synchronization across shards. The same holds for a
        // node without data directories.
        co_return make_lw_shared<table_storage>(*this, id, std::move(so), false);
    }
    auto dirs = get_local_directories(*local);
    if (!_storage) {
        // There is no storage manager to count the references, so nothing
        // removes the directories. Tests make such managers.
        co_await init_table_directories(dirs);
        co_return make_lw_shared<table_storage>(*this, id, std::move(so), false);
    }
    auto holder = _table_storage_gate.hold();
    co_await _storage->container().invoke_on(0, [id, dirs = std::move(dirs)] (storage_manager& sm) mutable {
        return sm.acquire_table_storage(id, std::move(dirs));
    });
    table_storage_ptr ts;
    std::exception_ptr ex;
    try {
        ts = make_lw_shared<table_storage>(*this, id, std::move(so), true);
    } catch (...) {
        ex = std::current_exception();
    }
    if (ex) {
        co_await _storage->container().invoke_on(0, [id] (storage_manager& sm) {
            return sm.release_table_storage(id);
        });
        std::rethrow_exception(std::move(ex));
    }
    co_return ts;
}

table_storage_ptr sstables_manager::attach_table_storage(const schema& s, lw_shared_ptr<const data_dictionary::storage_options> so) {
    return make_lw_shared<table_storage>(*this, s.id(), std::move(so), false);
}

table_storage_ptr sstables_manager::attach_table_storage(const schema& s, data_dictionary::storage_options so) {
    return attach_table_storage(s, make_lw_shared<const data_dictionary::storage_options>(std::move(so)));
}

future<> sstables_manager::drop_table_storage(table_id id, const data_dictionary::storage_options& table_so) {
    const auto* local = std::get_if<data_dictionary::storage_options::local>(&table_so.value);
    if (!local || local->dir.empty()) {
        // Nothing is created for a table on object storage, see
        // open_table_storage().
        co_return;
    }
    auto dirs = get_local_directories(*local);
    if (!_storage) {
        on_internal_error(smlogger, format("Dropping the storage of table {} without a storage manager", id));
    }
    co_await _storage->container().invoke_on(0, [id, dirs = std::move(dirs)] (storage_manager& sm) mutable {
        return sm.drop_table_storage(id, std::move(dirs));
    });
}

future<bool> sstables_manager::remove_table_directory_if_unused(table_id id, std::filesystem::path table_dir) {
    if (!_storage) {
        on_internal_error(smlogger, format("Removing directory {} of table {} without a storage manager", table_dir, id));
    }
    return _storage->container().invoke_on(0, [id, table_dir = std::move(table_dir)] (storage_manager& sm) mutable {
        return sm.remove_table_directory_if_unused(id, std::move(table_dir));
    });
}

future<> sstables_manager::await_table_storage_unused(table_id id) {
    if (!_storage) {
        return make_ready_future<>();
    }
    return _storage->container().invoke_on(0, [id] (storage_manager& sm) {
        return sm.await_table_storage_unused(id);
    });
}

future<> sstables_manager::await_destroyed_sstables() {
    return _sstable_destroyed.wait([this] {
        return _undergoing_close.empty();
    });
}

void sstables_manager::validate_new_keyspace_storage_options(const data_dictionary::storage_options& so) {
    std::visit(overloaded_functor {
        [] (const data_dictionary::storage_options::local&) {
        },
        [this] (const data_dictionary::storage_options::object_storage& so) {
            if (!_features.keyspace_storage_options) {
                throw exceptions::invalid_request_exception("Keyspace storage options not supported in the cluster");
            }
            // It's non-system keyspace
            // The endpoint must be configured and have the same storage type the keyspace declares
            auto requested_type = so.type | std::views::transform(&tolower) | std::ranges::to<std::string>();
            if (!is_known_endpoint(so.endpoint, requested_type)) {
                throw exceptions::configuration_exception(format("Endpoint {} not configured as {}", so.endpoint, so.type));
            }
        }
    }, so.value);
}

std::vector<std::filesystem::path> sstables_manager::get_local_directories(const data_dictionary::storage_options::local& so) const {
    return sstables::get_local_directories(_config.data_file_directories, so);
}

void sstables_manager::on_unlink(sstable* sst) {
    reclaim_memory_and_stop_tracking_sstable(sst);
    _signal_source(sst->generation(), notification_event_type::deleted);
}

sstables_registry::~sstables_registry() = default;

}   // namespace sstables
