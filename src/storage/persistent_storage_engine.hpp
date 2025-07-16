#ifndef __PERSISTENT_STORAGE_ENGINE_HPP__
#define __PERSISTENT_STORAGE_ENGINE_HPP__

#include "storage_engine.hpp"
#include "wal.hpp"
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

namespace distributed_db
{
    class PersistentStorageEngine : public StorageEngine
    {
    public:
        explicit PersistentStorageEngine(const std::filesystem::path &data_directory);
        ~PersistentStorageEngine() override;

    private:
        std::filesystem::path _data_directory;
        std::filesystem::path _snapshot_file_path;
        std::unique_ptr<WriteAheadLog> _wal;

        mutable std::shared_mutex _mutex;
        std::unordered_map<Key, Value> _data;
        std::uint64_t _last_checkpoint_sequence;

        static constexpr const char *SNAPSHOT_FILENAME = "snapshot.dat";
        static constexpr std::uint32_t SNAPSHOT_MAGIC_NUMBER = 0xCAFEBABE;
        static constexpr std::uint8_t SNAPSHOT_VERSION = 1;

        [[nodiscard]] bool initializeDataDirectory();
        [[nodiscard]] Status loadSnapshot();
        [[nodiscard]] Status saveSnapshot();
        [[nodiscard]] Status replayWalEntries();

        [[nodiscard]] Status serializeSnapshot(std::ofstream &file) const;
        [[nodiscard]] Status deserializeSnapshot(std::ifstream &file);

        [[nodiscard]] bool snapshotExists() const;
        [[nodiscard]] std::filesystem::path getSnapshotPath() const;

        void applyWalEntry(const WalEntry &entry);

        [[nodiscard]] Status validateSnapshot() const;
        [[nodiscard]] bool isValidSnapshotFile(const std::filesystem::path &path) const;
    };
} // namespace distributed_db

#endif // __PERSISTENT_STORAGE_ENGINE_HPP__