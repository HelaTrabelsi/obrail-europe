import { cn } from '@/lib/utils'

interface SectionTitleProps {
  children: React.ReactNode
  className?: string
}

export function SectionTitle({ children, className }: SectionTitleProps) {
  return (
    <div className={cn(
      "flex items-center gap-3 text-[10px] font-bold uppercase tracking-[0.15em] text-muted-foreground/50 my-6",
      className
    )}>
      {children}
      <div className="flex-1 h-px bg-border/30" />
    </div>
  )
}
