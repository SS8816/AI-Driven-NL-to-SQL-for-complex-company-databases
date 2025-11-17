import { HTMLAttributes, forwardRef } from 'react';
import { cn } from '@/utils/cn';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  title?: string;
  subtitle?: string;
  headerAction?: React.ReactNode;
}

export const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ className, title, subtitle, headerAction, children, ...props }, ref) => {
    return (
      <div ref={ref} className={cn('card', className)} {...props}>
        {(title || subtitle || headerAction) && (
          <div className="flex items-start justify-between mb-4">
            <div>
              {title && (
                <h3 className="text-lg font-semibold text-gray-100">{title}</h3>
              )}
              {subtitle && (
                <p className="text-sm text-gray-400 mt-1">{subtitle}</p>
              )}
            </div>
            {headerAction && <div>{headerAction}</div>}
          </div>
        )}
        {children}
      </div>
    );
  }
);

Card.displayName = 'Card';
