#ifndef __KEY_ROUTER_HPP__
#define __KEY_ROUTER_HPP__

#include "consistent_hash.hpp"
#include "node_info.hpp"
#include "../storage/storage_engine.hpp"
#include "../network/tcp_client.hpp"
#include "../common/logger.hpp"
#include <memory>
#include <vector>
#include <unordered_map>

namespace distributed_db
{
    struct RouteDecision
    {
        NodeId primary;
        std::vector<NodeId> replicas; // includes primary as replicas[0] for convenience
    };

    class KeyRouter
    {
    public:
        KeyRouter(std::shared_ptr<NodeInfo> local,
                  std::shared_ptr<NodeRegistry> registry,
                  std::shared_ptr<ConsistentHashRing> ring)
            : _local(std::move(local)),
              _registry(std::move(registry)),
              _ring(std::move(ring))
        {
        }

        // Compute owners for a key (primary + N-1 replicas)
        [[nodiscard]] RouteDecision routeKey(const Key &key) const
        {
            RouteDecision r;
            auto nodes = _ring->getNodesForKey(key);
            if (!nodes.empty())
            {
                r.primary = nodes.front();
                r.replicas = std::move(nodes);
            }
            return r;
        }

        // Should this node serve the key locally (is it an owner)?
        [[nodiscard]] bool isLocalOwner(const Key &key) const
        {
            auto owners = _ring->getNodesForKey(key);
            for (const auto &id : owners)
                if (id == _local->getId())
                    return true;
            return false;
        }

        // Returns endpoints (host:port) for owners in routing order (primary-first).
        // If a node is missing from registry (e.g., race), it is skipped.
        [[nodiscard]] std::vector<std::string> ownerEndpoints(const Key &key) const
        {
            std::vector<std::string> eps;
            auto owners = _ring->getNodesForKey(key);
            for (const auto &id : owners)
            {
                auto node = _registry->getNode(id);
                if (node)
                    eps.push_back(node->getEndpoint());
            }
            return eps;
        }

        // - If local is an owner: write locally first, then forward to other owners.
        // - Else: forward to primary owner.
        // This is a helper that returns the forwarding plan (local-first boolean and endpoints).
        struct WritePlan
        {
            bool write_local_first{false};
            std::vector<std::string> forward_to_endpoints; // exclude local endpoint
        };

        [[nodiscard]] WritePlan planWrite(const Key &key) const
        {
            WritePlan plan;
            auto owners = _ring->getNodesForKey(key);
            if (owners.empty())
                return plan;

            auto local_id = _local->getId();
            bool local_is_owner = false;
            for (auto &id : owners)
                if (id == local_id)
                {
                    local_is_owner = true;
                    break;
                }
            plan.write_local_first = local_is_owner;

            // endpoints in routing order
            for (const auto &id : owners)
            {
                if (id == local_id)
                    continue;
                auto node = _registry->getNode(id);
                if (node)
                    plan.forward_to_endpoints.push_back(node->getEndpoint());
            }
            return plan;
        }

    private:
        std::shared_ptr<NodeInfo> _local;
        std::shared_ptr<NodeRegistry> _registry;
        std::shared_ptr<ConsistentHashRing> _ring;
    };
} // namespace distributed_db

#endif // __KEY_ROUTER_HPP__
