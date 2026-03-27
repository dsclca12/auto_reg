import { useEffect, useState } from 'react'
import { cn } from '@/lib/utils'
import { CheckCircle, XCircle, AlertCircle, X } from 'lucide-react'

type ToastType = 'success' | 'error' | 'warning' | 'info'

interface Toast {
  id: string
  type: ToastType
  title: string
  message?: string
}

const icons: Record<ToastType, React.ReactNode> = {
  success: <CheckCircle className="h-5 w-5 text-emerald-400" />,
  error: <XCircle className="h-5 w-5 text-red-400" />,
  warning: <AlertCircle className="h-5 w-5 text-amber-400" />,
  info: <AlertCircle className="h-5 w-5 text-blue-400" />,
}

const bgColors: Record<ToastType, string> = {
  success: 'border-emerald-500/30 bg-emerald-500/10',
  error: 'border-red-500/30 bg-red-500/10',
  warning: 'border-amber-500/30 bg-amber-500/10',
  info: 'border-blue-500/30 bg-blue-500/10',
}

let toastId = 0
const listeners: Set<(toasts: Toast[]) => void> = new Set()
let toasts: Toast[] = []

function notify(listeners: Set<any>, toasts: Toast[]) {
  listeners.forEach(l => l([...toasts]))
}

export function toast(type: ToastType, title: string, message?: string) {
  const id = String(++toastId)
  toasts = [...toasts, { id, type, title, message }]
  notify(listeners, toasts)
  setTimeout(() => {
    toasts = toasts.filter(t => t.id !== id)
    notify(listeners, toasts)
  }, 4000)
}

export function dismissToast(id: string) {
  toasts = toasts.filter(t => t.id !== id)
  notify(listeners, toasts)
}

export function ToastContainer() {
  const [items, setItems] = useState<Toast[]>([])

  useEffect(() => {
    listeners.add(setItems)
    return () => { listeners.delete(setItems) }
  }, [])

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2">
      {items.map((t) => (
        <div
          key={t.id}
          className={cn(
            'flex items-start gap-3 p-4 rounded-lg border backdrop-blur-sm',
            'bg-[var(--bg-card)] shadow-lg',
            'animate-in slide-in-from-right fade-in duration-300',
            bgColors[t.type]
          )}
        >
          {icons[t.type]}
          <div className="flex-1">
            <p className="text-sm font-medium text-[var(--text-primary)]">{t.title}</p>
            {t.message && (
              <p className="text-xs text-[var(--text-muted)] mt-1">{t.message}</p>
            )}
          </div>
          <button
            onClick={() => dismissToast(t.id)}
            className="text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      ))}
    </div>
  )
}