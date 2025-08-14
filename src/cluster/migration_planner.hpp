#ifndef __MIGRATION_PLANNER_HPP__
#define __MIGRATION_PLANNER_HPP__

#include "consistent_hash.hpp"
#include "../storage/storage_engine.hpp"
#include <unordered_map>
#include <memory>

namespace distributed_db
{
    // A simple migration description:
    // keys that should move from current holder -> new primary holder
    struct MigrationPlan
    {
        // map new_owner -> list of keys it should receive
        std::unordered_map<NodeId, std::vector<Key>> to_receive;
        // total number of keys considered
        std::size_t total_keys{0};
        // number of keys moving primary ownership
        std::size_t moving_keys{0};
    };

    class MigrationPlanner
    {
    public:
        MigrationPlanner(std::shared_ptr<ConsistentHashRing> before,
                         std::shared_ptr<ConsistentHashRing> after)
            : _before(std::move(before)), _after(std::move(after)) {}

        // Compute a migration plan for a set of keys (e.g., from storage->getAllKeys()).
        // This focuses on primary movement. Replica rebalancing can be added similarly.
        [[nodiscard]] MigrationPlan computePrimaryMoves(const std::vector<Key> &keys) const
        {
            MigrationPlan plan;
            plan.total_keys = keys.size();
            for (const auto &k : keys)
            {
                auto old_owner = _before->getPrimaryNodeForKey(k);
                auto new_owner = _after->getPrimaryNodeForKey(k);
                if (old_owner != new_owner && !new_owner.empty())
                {
                    plan.to_receive[new_owner].push_back(k);
                    plan.moving_keys++;
                }
            }
            return plan;
        }

    private:
        std::shared_ptr<ConsistentHashRing> _before;
        std::shared_ptr<ConsistentHashRing> _after;
    };
} // namespace distributed_db

#endif // __MIGRATION_PLANNER_HPP__
