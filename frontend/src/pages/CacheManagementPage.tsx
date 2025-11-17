import { useQuery } from '@tanstack/react-query';
import { HardDrive, Trash2, Database, Clock } from 'lucide-react';
import { cacheApi } from '@/api';
import { Card, Badge, EmptyState, Loading, Button } from '@/components/common';
import { formatBytes, formatRelativeTime, formatNumber } from '@/utils/format';
import toast from 'react-hot-toast';

export function CacheManagementPage() {
  const { data: stats, isLoading, refetch } = useQuery({
    queryKey: ['cache-stats'],
    queryFn: () => cacheApi.getStats(),
  });

  const handleClearExpired = async () => {
    try {
      const result = await cacheApi.clearExpired();
      toast.success(`Cleared ${result.deleted_count} expired entries`);
      refetch();
    } catch (error: any) {
      toast.error(error.message || 'Failed to clear cache');
    }
  };

  const handleInvalidate = async (ruleCategory: string, database: string) => {
    try {
      const result = await cacheApi.invalidate(ruleCategory, database);
      toast.success(`Invalidated ${result.deleted_count} cache entries`);
      refetch();
    } catch (error: any) {
      toast.error(error.message || 'Failed to invalidate cache');
    }
  };

  if (isLoading) {
    return <Loading text="Loading cache statistics..." />;
  }

  if (!stats) {
    return (
      <EmptyState
        icon={HardDrive}
        title="No cache data"
        description="Cache statistics unavailable"
      />
    );
  }

  return (
    <div className="space-y-4">
      {/* Overview Card */}
      <Card title="Cache Overview" subtitle="Current cache statistics">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="p-4 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-2">
              <Database className="w-4 h-4" />
              <span className="text-xs">Total Entries</span>
            </div>
            <div className="text-2xl font-bold text-gray-100">
              {formatNumber(stats.total_entries)}
            </div>
          </div>

          <div className="p-4 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-2">
              <Database className="w-4 h-4" />
              <span className="text-xs">Active Entries</span>
            </div>
            <div className="text-2xl font-bold text-success">
              {formatNumber(stats.active_entries)}
            </div>
          </div>

          <div className="p-4 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-2">
              <Trash2 className="w-4 h-4" />
              <span className="text-xs">Expired Entries</span>
            </div>
            <div className="text-2xl font-bold text-error">
              {formatNumber(stats.expired_entries)}
            </div>
          </div>

          <div className="p-4 bg-dark-sidebar rounded-lg">
            <div className="flex items-center gap-2 text-gray-400 mb-2">
              <HardDrive className="w-4 h-4" />
              <span className="text-xs">Total Size</span>
            </div>
            <div className="text-2xl font-bold text-gray-100">
              {formatBytes(stats.total_size_mb * 1024 * 1024)}
            </div>
          </div>
        </div>

        <div className="flex gap-3 mt-6">
          <Button
            onClick={handleClearExpired}
            variant="danger"
            disabled={stats.expired_entries === 0}
          >
            <Trash2 className="w-4 h-4 mr-2" />
            Clear Expired ({stats.expired_entries})
          </Button>

          {stats.hit_rate !== undefined && (
            <div className="flex items-center gap-2 px-4 py-2 bg-dark-sidebar rounded-lg">
              <span className="text-sm text-gray-400">Hit Rate:</span>
              <Badge variant="success">
                {(stats.hit_rate * 100).toFixed(1)}%
              </Badge>
            </div>
          )}
        </div>
      </Card>

      {/* Cached Rules */}
      <Card
        title="Cached Rules"
        subtitle={`${stats.cached_rules.length} rule categories cached`}
      >
        {stats.cached_rules.length === 0 ? (
          <EmptyState
            icon={Database}
            title="No cached rules"
            description="Executed queries will be cached here for faster retrieval"
          />
        ) : (
          <div className="space-y-3">
            {stats.cached_rules.map((rule, idx) => (
              <div
                key={idx}
                className="p-4 bg-dark-sidebar rounded-lg border border-dark-border flex items-center justify-between"
              >
                <div className="flex-1">
                  <div className="flex items-center gap-3 mb-2">
                    <Database className="w-5 h-5 text-primary-400" />
                    <div>
                      <h3 className="font-medium text-gray-100">
                        {rule.rule_category}
                      </h3>
                      <p className="text-xs text-gray-400">
                        Database: {rule.database}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center gap-4 text-xs text-gray-400">
                    <div className="flex items-center gap-1">
                      <Database className="w-3 h-3" />
                      {formatNumber(rule.entry_count)} entries
                    </div>
                    <div className="flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      Last accessed {formatRelativeTime(rule.last_accessed)}
                    </div>
                  </div>
                </div>

                <Button
                  variant="danger"
                  size="sm"
                  onClick={() =>
                    handleInvalidate(rule.rule_category, rule.database)
                  }
                >
                  <Trash2 className="w-4 h-4 mr-2" />
                  Invalidate
                </Button>
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  );
}
