import { useAuthStore } from '@/stores/authStore';
import { formatDateTime } from '@/utils/format';

export function Header() {
  const { user } = useAuthStore();
  const currentDate = formatDateTime(new Date(), 'EEEE, MMMM d, yyyy');

  return (
    <header className="bg-dark-sidebar border-b border-dark-border px-6 py-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100">
            Welcome back, {user?.full_name || user?.username}!
          </h2>
          <p className="text-sm text-gray-400 mt-1">{currentDate}</p>
        </div>
        <div className="flex items-center gap-4">
          {user?.department && (
            <div className="px-3 py-1 bg-primary-500/20 border border-primary-500/30 rounded-full text-xs font-medium text-primary-400">
              {user.department}
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
