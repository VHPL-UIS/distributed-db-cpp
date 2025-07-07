#ifndef __LOGGER_HPP__
#define __LOGGER_HPP__

#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <mutex>

namespace distributed_db
{
    enum class LogLevel
    {
        DEBUG,
        INFO,
        WARN,
        ERROR
    };

    class Logger
    {
    public:
        static Logger &getInstance()
        {
            static Logger instance;
            return instance;
        }

        template <typename... Args>
        void log(LogLevel level, const std::string &format, Args &&...args)
        {
            std::lock_guard<std::mutex> lock(_mutex);

            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);

            std::ostringstream oss;
            oss << "[" << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "] ";
            oss << "[" << levelToString(level) << "] ";
            oss << formatString(format, std::forward<Args>(args)...);

            std::cout << oss.str() << std::endl;
        }

        template <typename... Args>
        void debug(const std::string &format, Args &&...args)
        {
            log(LogLevel::DEBUG, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void info(const std::string &format, Args &&...args)
        {
            log(LogLevel::INFO, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void warn(const std::string &format, Args &&...args)
        {
            log(LogLevel::WARN, format, std::forward<Args>(args)...);
        }

        template <typename... Args>
        void error(const std::string &format, Args &&...args)
        {
            log(LogLevel::ERROR, format, std::forward<Args>(args)...);
        }

    private:
        Logger() = default;
        std::mutex _mutex;

        std::string levelToString(LogLevel level)
        {
            switch (level)
            {
            case LogLevel::DEBUG:
                return "DEBUG";
            case LogLevel::INFO:
                return "INFO";
            case LogLevel::WARN:
                return "WARN";
            case LogLevel::ERROR:
                return "ERROR";
            default:
                return "UNKNOWN";
            }
        }

        template <typename... Args>
        std::string formatString(const std::string &format, Args &&...args)
        {
            std::ostringstream oss;
            oss << format;
            ((oss << " " << args), ...);
            return oss.str();
        }
    };

#define LOG_DEBUG(...) Logger::getInstance().debug(__VA_ARGS__)
#define LOG_INFO(...) Logger::getInstance().info(__VA_ARGS__)
#define LOG_WARN(...) Logger::getInstance().warn(__VA_ARGS__)
#define LOG_ERROR(...) Logger::getInstance().error(__VA_ARGS__)

} // namespace distributed_db

#endif // __LOGGER_HPP__