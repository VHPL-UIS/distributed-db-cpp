#include "wal.hpp"
#include "../common/logger.hpp"

namespace distributed_db
{
    WriteAheadLog::WriteAheadLog(const std::filesystem::path &wal_directory)
        : _wal_directory(wal_directory), _current_sequence_number(0), _entries_since_checkpoint(0)
    {
        std::error_code ec;
        std::filesystem::create_directories(_wal_directory, ec);

        if (ec)
        {
            LOG_ERROR("Failed to create WAL directory: %s", ec.message().c_str());
            throw std::runtime_error("Failed to create WAL directory: " + ec.message());
        }

        _wal_file_path = _wal_directory / "wal.log";

        if (!initializeWalFile())
        {
            throw std::runtime_error("Failed to initialize WAL file!");
        }

        LOG_INFO("WAL initialized at: %s", _wal_file_path.string().c_str());
    }

    WriteAheadLog::~WriteAheadLog()
    {
        const std::loc_guard<std::mutex> lock(_mutex);
        if (_wal_file && _wal_file->is_open())
        {
            _wal_file->flush();
            _wal_file->close();
        }
    }
}