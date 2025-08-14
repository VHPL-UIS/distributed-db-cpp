#include "replicator.hpp"
#include "../cluster/cluster_membership.hpp" // for endpoint utils if needed
#include <sstream>

namespace distributed_db
{
    Result<std::unique_ptr<TcpClient>> TcpKeyValueTransport::connect(
        const std::string &endpoint, std::chrono::milliseconds timeout)
    {
        auto res = MembershipUtils::parseEndpoint(endpoint);
        if (!res.ok())
            return Result<std::unique_ptr<TcpClient>>(Status::INVALID_REQUEST);
        const auto &[host, port] = res.value();

        auto client = std::make_unique<TcpClient>(timeout);
        auto st = client->connect(host, port);
        if (st != Status::OK)
            return Result<std::unique_ptr<TcpClient>>(st);
        client->setDefaultTimeout(timeout);
        return Result<std::unique_ptr<TcpClient>>(std::move(client));
    }

    Result<Value> TcpKeyValueTransport::get(const std::string &endpoint, const Key &key,
                                            std::chrono::milliseconds timeout)
    {
        auto c = connect(endpoint, timeout);
        if (!c.ok())
            return Result<Value>(c.status());
        auto res = c.value()->get(key, timeout);
        return res;
    }

    Status TcpKeyValueTransport::put(const std::string &endpoint, const Key &key, const Value &value,
                                     std::chrono::milliseconds timeout)
    {
        auto c = connect(endpoint, timeout);
        if (!c.ok())
            return c.status();
        return c.value()->put(key, value, timeout);
    }

    Status TcpKeyValueTransport::remove(const std::string &endpoint, const Key &key,
                                        std::chrono::milliseconds timeout)
    {
        auto c = connect(endpoint, timeout);
        if (!c.ok())
            return c.status();
        return c.value()->remove(key, timeout);
    }

    std::size_t Replicator::replicationFactorFor(const Key &key) const
    {
        return std::max<std::size_t>(1, _ring->getNodesForKey(key).size());
    }

    std::size_t Replicator::quorumSizeFor(const Key &key) const
    {
        auto rf = replicationFactorFor(key);
        return (rf / 2) + 1;
    }

    Status Replicator::put(const Key &key, const Value &value, std::optional<ConsistencyLevel> level)
    {
        auto owners = _router.ownerEndpoints(key);
        if (owners.empty())
            return Status::INTERNAL_ERROR;

        const auto effective = level.value_or(_cfg.write_consistency);
        const auto local_owner = _router.isLocalOwner(key);

        // Always try to write to primary first for ordering determinism
        const auto primary_ep = [&]()
        {
            auto ids = _ring->getNodesForKey(key);
            if (ids.empty())
                return std::string{};
            auto n = _registry->getNode(ids.front());
            return n ? n->getEndpoint() : std::string{};
        }();

        // Local apply if we are an owner (primary or backup)
        std::size_t acks = 0;
        std::vector<std::string> endpoints_in_order = owners;

        // ensure primary first in endpoints_in_order
        if (!primary_ep.empty() && !endpoints_in_order.empty() && endpoints_in_order.front() != primary_ep)
        {
            auto it = std::find(endpoints_in_order.begin(), endpoints_in_order.end(), primary_ep);
            if (it != endpoints_in_order.end())
                std::rotate(endpoints_in_order.begin(), it, it + 1);
        }

        // Apply locally if owner
        if (local_owner)
        {
            auto st = _storage->put(key, value);
            if (st != Status::OK)
                return st;
            ++acks;
        }

        // Fan-out to the rest (including primary if we weren't local_owner & primary is remote)
        auto st = replicatePut(key, value, endpoints_in_order, effective);
        if (st != Status::OK)
            return st;

        // For ONE we’re already good if either local applied or any remote ack happened inside replicatePut.
        return Status::OK;
    }

    Status Replicator::remove(const Key &key, std::optional<ConsistencyLevel> level)
    {
        auto owners = _router.ownerEndpoints(key);
        if (owners.empty())
            return Status::INTERNAL_ERROR;

        const auto effective = level.value_or(_cfg.write_consistency);
        const auto local_owner = _router.isLocalOwner(key);

        // Local apply if we are an owner
        if (local_owner)
        {
            auto st = _storage->remove(key);
            if (st != Status::OK && st != Status::NOT_FOUND)
                return st;
        }

        // Fan-out to the rest
        return replicateDelete(key, owners, effective);
    }

    Status Replicator::replicatePut(const Key &key, const Value &value,
                                    const std::vector<std::string> &owner_eps,
                                    ConsistencyLevel level)
    {
        const auto rf = owner_eps.size();
        const auto quorum = (level == ConsistencyLevel::ALL) ? rf : (level == ConsistencyLevel::ONE) ? 1
                                                                                                     : ((rf / 2) + 1);

        std::atomic<std::size_t> acks{0};
        std::vector<std::future<void>> futs;
        futs.reserve(owner_eps.size());

        // If local node is among owners and already applied, we still send to others,
        // but count only remote acks here (local counted implicitly by caller).
        for (const auto &ep : owner_eps)
        {
            // If this endpoint is local’s own endpoint and we already applied, skip sending to ourselves.
            auto local_ep = _local->getEndpoint();
            if (ep == local_ep)
                continue;

            futs.emplace_back(std::async(std::launch::async, [&, ep]
                                         {
                auto st = _transport->put(ep, key, value, _cfg.rpc_timeout);
                if (st == Status::OK) acks.fetch_add(1, std::memory_order_relaxed); }));
        }

        for (auto &f : futs)
            f.wait();

        // Count local ack if we’re an owner and already applied
        std::size_t local_ack = 0;
        for (const auto &ep : owner_eps)
            if (ep == _local->getEndpoint() && _router.isLocalOwner(key))
                local_ack = 1;

        const auto total = acks.load() + local_ack;
        return (total >= quorum) ? Status::OK : Status::TIMEOUT;
    }

    Status Replicator::replicateDelete(const Key &key,
                                       const std::vector<std::string> &owner_eps,
                                       ConsistencyLevel level)
    {
        const auto rf = owner_eps.size();
        const auto quorum = (level == ConsistencyLevel::ALL) ? rf : (level == ConsistencyLevel::ONE) ? 1
                                                                                                     : ((rf / 2) + 1);

        std::atomic<std::size_t> acks{0};
        std::vector<std::future<void>> futs;
        futs.reserve(owner_eps.size());

        for (const auto &ep : owner_eps)
        {
            auto st_local = Status::OK;
            if (ep == _local->getEndpoint() && _router.isLocalOwner(key))
            {
                // local already removed in caller; count as ack
                acks.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            futs.emplace_back(std::async(std::launch::async, [&, ep]
                                         {
                auto st = _transport->remove(ep, key, _cfg.rpc_timeout);
                if (st == Status::OK || st == Status::NOT_FOUND)
                    acks.fetch_add(1, std::memory_order_relaxed); }));
        }

        for (auto &f : futs)
            f.wait();

        return (acks.load() >= quorum) ? Status::OK : Status::TIMEOUT;
    }

    Result<Value> Replicator::get(const Key &key, std::optional<ConsistencyLevel> level)
    {
        auto owners = _router.ownerEndpoints(key);
        if (owners.empty())
            return Result<Value>(Status::INTERNAL_ERROR);

        const auto effective = level.value_or(_cfg.read_consistency);
        return readFromOwners(key, owners, effective);
    }

    Result<Value> Replicator::readFromOwners(const Key &key,
                                             const std::vector<std::string> &owner_eps,
                                             ConsistencyLevel level)
    {
        const auto rf = owner_eps.size();
        const auto quorum = (level == ConsistencyLevel::ALL) ? rf : (level == ConsistencyLevel::ONE) ? 1
                                                                                                     : ((rf / 2) + 1);

        // Primary first
        const auto primary_ep = [&]()
        {
            auto ids = _ring->getNodesForKey(key);
            if (ids.empty())
                return std::string{};
            auto n = _registry->getNode(ids.front());
            return n ? n->getEndpoint() : std::string{};
        }();

        std::vector<std::string> order = owner_eps;
        if (!primary_ep.empty() && !order.empty() && order.front() != primary_ep)
        {
            auto it = std::find(order.begin(), order.end(), primary_ep);
            if (it != order.end())
                std::rotate(order.begin(), it, it + 1);
        }

        // Parallel reads until quorum
        std::mutex m;
        std::condition_variable cv;
        std::size_t success = 0;
        std::vector<std::pair<std::string, Value>> successes;
        std::vector<std::future<void>> futs;

        // If local owner, read locally immediately as one success (fast-path)
        if (_router.isLocalOwner(key))
        {
            auto r = _storage->get(key);
            if (r.ok())
            {
                success++;
                successes.emplace_back(_local->getEndpoint(), r.value());
            }
        }

        for (const auto &ep : order)
        {
            if (ep == _local->getEndpoint() && _router.isLocalOwner(key))
                continue; // already counted local
            futs.emplace_back(std::async(std::launch::async, [&, ep]
                                         {
                auto r = _transport->get(ep, key, _cfg.rpc_timeout);
                if (r.ok())
                {
                    std::unique_lock<std::mutex> lk(m);
                    successes.emplace_back(ep, r.value());
                    success++;
                    cv.notify_all();
                } }));
        }

        // Wait until quorum or all done
        {
            std::unique_lock<std::mutex> lk(m);
            cv.wait_for(lk, _cfg.rpc_timeout, [&]
                        { return success >= quorum; });
        }

        for (auto &f : futs)
            f.wait();

        if (successes.empty())
            return Result<Value>(Status::NOT_FOUND);

        // Resolve conflicts (values may differ)
        std::vector<Value> vals;
        std::vector<std::string> from;
        vals.reserve(successes.size());
        from.reserve(successes.size());
        for (auto &p : successes)
        {
            from.push_back(p.first);
            vals.push_back(p.second);
        }

        const auto canonical = resolveConflict(vals, from, primary_ep);

        // Read-repair: best effort push canonical to divergent replicas
        if (_cfg.enable_read_repair)
        {
            std::vector<std::pair<std::string, Value>> divergent;
            for (std::size_t i = 0; i < vals.size(); ++i)
                if (vals[i] != canonical)
                    divergent.emplace_back(from[i], vals[i]);
            if (!divergent.empty())
                bestEffortReadRepair(key, canonical, divergent);
        }

        return Result<Value>(canonical);
    }

    Value Replicator::resolveConflict(const std::vector<Value> &values,
                                      const std::vector<std::string> &from_eps,
                                      const std::string &primary_ep) const
    {
        // Majority-wins
        std::unordered_map<std::string, std::size_t> counts;
        for (const auto &v : values)
            counts[v]++;

        const auto majority = std::max_element(
            counts.begin(), counts.end(),
            [](auto &a, auto &b)
            { return a.second < b.second; });

        // If there is a strict majority, return it
        std::size_t max_count = majority->second;
        std::size_t total = values.size();
        if (max_count > total / 2)
            return majority->first;

        // No majority: prefer primary’s value if present
        for (std::size_t i = 0; i < from_eps.size(); ++i)
            if (from_eps[i] == primary_ep)
                return values[i];

        // Fallback: first value
        return values.front();
    }

    void Replicator::bestEffortReadRepair(const Key &key, const Value &canonical,
                                          const std::vector<std::pair<std::string, Value>> &divergent)
    {
        for (const auto &[ep, _old] : divergent)
        {
            std::async(std::launch::async, [&, ep]
                       {
                auto st = _transport->put(ep, key, canonical, _cfg.rpc_timeout);
                if (st != Status::OK)
                    LOG_WARN("Read repair failed for %s on %s", key.c_str(), ep.c_str()); });
        }
    }

} // namespace distributed_db
