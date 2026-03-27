import * as React from 'react'
import { cn } from '@/lib/utils'

export interface LabelProps extends React.LabelHTMLAttributes<HTMLLabelElement> {
  required?: boolean
}

const Label = React.forwardRef<HTMLLabelElement, LabelProps>(
  ({ className, required, children, ...props }, ref) => {
    return (
      <label
        ref={ref}
        className={cn(
          'text-xs font-medium text-[var(--text-secondary)]',
          'transition-colors duration-200',
          required && "after:content-['*'] after:text-red-400 after:ml-0.5",
          className
        )}
        {...props}
      >
        {children}
      </label>
    )
  }
)
Label.displayName = 'Label'

export { Label }