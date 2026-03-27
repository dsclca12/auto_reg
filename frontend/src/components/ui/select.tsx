import * as React from 'react'
import { cn } from '@/lib/utils'

export interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {}

const Select = React.forwardRef<HTMLSelectElement, SelectProps>(
  ({ className, ...props }, ref) => {
    return (
      <select
        className={cn(
          'flex h-9 w-full rounded-md px-3 py-2 text-sm',
          'bg-[var(--bg-input)] border border-[var(--border)]',
          'text-[var(--text-primary)]',
          'transition-all duration-200',
          'focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/50 focus:border-[var(--accent)]',
          'disabled:cursor-not-allowed disabled:opacity-50',
          'appearance-none bg-[length:16px_16px] bg-[right_12px_center] bg-no-repeat',
          className
        )}
        ref={ref}
        {...props}
      />
    )
  }
)
Select.displayName = 'Select'

export { Select }