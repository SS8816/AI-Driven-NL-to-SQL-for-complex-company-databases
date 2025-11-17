import { HTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: 'default' | 'success' | 'error' | 'warning' | 'info';
}

export function Badge({
  variant = 'default',
  className,
  children,
  ...props
}: BadgeProps) {
  const variants = {
    default: 'bg-dark-border text-gray-300',
    success: 'bg-success/20 text-success border border-success/30',
    error: 'bg-error/20 text-error border border-error/30',
    warning: 'bg-warning/20 text-warning border border-warning/30',
    info: 'bg-primary-500/20 text-primary-400 border border-primary-500/30',
  };

  return (
    <span
      className={cn(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        variants[variant],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}
