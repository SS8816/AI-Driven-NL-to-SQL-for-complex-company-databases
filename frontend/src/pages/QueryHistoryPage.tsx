import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { History, Bookmark, Clock, Database, ChevronRight } from 'lucide-react';
import { queriesApi } from '@/api';
import { Card, Badge, EmptyState, Loading, Button } from '@/components/common';
import { formatRelativeTime, formatNumber, formatExecutionTime } from '@/utils/format';
import toast from 'react-hot-toast';

export function QueryHistoryPage() {
  const [page, setPage] = useState(1);

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['query-history', page],
    queryFn: () => queriesApi.getHistory(page),
  });

  const handleToggleBookmark = async (queryId: number) => {
    try {
      await queriesApi.toggleBookmark(queryId);
      toast.success('Bookmark updated');
      refetch();
    } catch (error: any) {
      toast.error(error.message || 'Failed to update bookmark');
    }
  };

  if (isLoading) {
    return <Loading text="Loading query history..." />;
  }

  if (!data || data.items.length === 0) {
    return (
      <EmptyState
        icon={History}
        title="No query history"
        description="Your executed queries will appear here"
      />
    );
  }

  return (
    <div className="space-y-4">
      <Card
        title="Query History"
        subtitle={`${data.total} total queries`}
      >
        <div className="space-y-3">
          {data.items.map((query) => (
            <div
              key={query.id}
              className="p-4 bg-dark-sidebar rounded-lg border border-dark-border hover:border-primary-500/30 transition-all"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 min-w-0">
                  {/* Query Text */}
                  <div className="flex items-start gap-3 mb-3">
                    <button
                      onClick={() => handleToggleBookmark(query.id)}
                      className="mt-1"
                    >
                      <Bookmark
                        className={`w-5 h-5 ${
                          query.is_bookmarked
                            ? 'fill-warning text-warning'
                            : 'text-gray-500 hover:text-warning'
                        }`}
                      />
                    </button>

                    <div className="flex-1">
                      <p className="text-gray-100 mb-2">{query.nl_query}</p>

                      <div className="flex flex-wrap items-center gap-3 text-xs text-gray-400">
                        <div className="flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {formatRelativeTime(query.created_at)}
                        </div>

                        <div className="flex items-center gap-1">
                          <Database className="w-3 h-3" />
                          {query.rule_category}
                        </div>

                        {query.row_count !== undefined && (
                          <div className="flex items-center gap-1">
                            {formatNumber(query.row_count)} rows
                          </div>
                        )}

                        {query.execution_time_seconds !== undefined && (
                          <div className="flex items-center gap-1">
                            {formatExecutionTime(query.execution_time_seconds)}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* SQL Preview */}
                  <details className="group">
                    <summary className="cursor-pointer text-xs text-gray-500 hover:text-gray-300 flex items-center gap-1">
                      <ChevronRight className="w-3 h-3 group-open:rotate-90 transition-transform" />
                      View SQL
                    </summary>
                    <pre className="mt-2 p-3 bg-dark-bg rounded text-xs font-mono text-gray-400 overflow-x-auto">
                      {query.sql}
                    </pre>
                  </details>
                </div>

                {/* Status Badge */}
                <Badge
                  variant={
                    query.status === 'success'
                      ? 'success'
                      : query.status === 'failed'
                      ? 'error'
                      : 'default'
                  }
                >
                  {query.status}
                </Badge>
              </div>
            </div>
          ))}
        </div>

        {/* Pagination */}
        {data.total_pages > 1 && (
          <div className="flex items-center justify-between mt-6 pt-4 border-t border-dark-border">
            <p className="text-sm text-gray-400">
              Page {data.page} of {data.total_pages}
            </p>
            <div className="flex gap-2">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
              >
                Previous
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setPage((p) => Math.min(data.total_pages, p + 1))}
                disabled={page === data.total_pages}
              >
                Next
              </Button>
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}
