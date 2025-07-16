#include "persistent_storage_engine.hpp"
#include "../common/logger.hpp"
#include <cstring>

namespace distributed_db
{
    PersistentStorageEngine::PersistentStorageEngine(const std::filesystem::path &data_directory)
        : _data_directory(data_directory), _last_checkpoint_sequence(0)
    {
        if (!initializeDataDirectory())
        {
            throw std::runtime_error("Failed to initialize data directory: " + _data_dirctory.string());
        }

        _snapshot_file_path = _data_directory / SNAPSHOT_FILENAME;

        const auto wal_directory = _data_directory / "wal";
        _wal = std::make_unique<WriteAheadLog>(wal_directory);

        const auto recovery_status = recover();
        if (recovery_status != Status::OK)
        {
            throw std::runtime_error("Failed to recover data from persistent storage");
        }

        LOG_INFO("Persistent storage engine initialized at: %s", _data_directory.string().c_str());
        LOG_INFO("Recovered %zu keys from storage", _data.size());
    }

    PersistentStorageEngine::~PersistentStorageEngine()
    {
        const auto flush_status = flush();
        if (flush_status != Status::OK)
        {
            LOG_ERROR("Failed to flush data during shutdown");
        }
    }
} // namespace distributed_db