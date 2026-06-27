import { cn } from '@/lib/utils'

interface KPICardProps {
  value: string
  label: string
  gradient?: string
  delta?: string
}

export function KPICard({ value, label, gradient, delta }: KPICardProps) {
  return (
    <div className="relative overflow-hidden rounded-xl border border-border/50 bg-card p-5">
      {/* Top gradient bar */}
      <div 
        className="absolute inset-x-0 top-0 h-0.5"
        style={{ 
          background: gradient || 'linear-gradient(90deg, var(--primary), var(--accent))' 
        }}
      />
      
      <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60 mb-1">
        {label}
      </div>
      <div className="text-2xl font-bold text-foreground tracking-tight">
        {value}
      </div>
      {delta && (
        <div className="mt-1 text-xs font-semibold text-primary">
          {delta}
        </div>
      )}
    </div>
  )
}

interface KPIGridProps {
  children: React.ReactNode
  className?: string
}

export function KPIGrid({ children, className }: KPIGridProps) {
  return (
    <div className={cn(
      "grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3",
      className
    )}>
      {children}
    </div>
  )
}


