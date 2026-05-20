import { cn } from '@/lib/utils'

interface SourceCardProps {
  country: string
  name: string
  type: string
  color: string
}

export function SourceCard({ country, name, type, color }: SourceCardProps) {
  return (
    <div 
      className="rounded-xl border border-border/50 bg-card p-5 text-center"
      style={{ borderTopColor: color, borderTopWidth: '2px' }}
    >
      <div 
        className="text-[10px] font-bold tracking-[0.1em] mb-2"
        style={{ color }}
      >
        {country}
      </div>
      <div className="text-sm font-semibold text-secondary-foreground mb-1">
        {name}
      </div>
      <div className="text-[10px] font-semibold uppercase tracking-[0.08em] text-muted-foreground/50">
        {type}
      </div>
    </div>
  )
}

interface StatRowProps {
  items: { value: string; label: string }[]
  className?: string
}

export function StatRow({ items, className }: StatRowProps) {
  return (
    <div className={cn(
      "grid gap-px bg-border/30 rounded-xl border border-border/50 overflow-hidden",
      className
    )}
    style={{ gridTemplateColumns: `repeat(${items.length}, 1fr)` }}
    >
      {items.map((item, i) => (
        <div key={i} className="bg-card p-4">
          <div className="text-lg font-bold text-foreground tracking-tight">
            {item.value}
          </div>
          <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/50 mt-1">
            {item.label}
          </div>
        </div>
      ))}
    </div>
  )
}
