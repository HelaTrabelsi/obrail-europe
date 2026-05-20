'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { 
  Home, Clock, BarChart3, GitBranch, 
  Leaf, CheckCircle2, Train 
} from 'lucide-react'
import { cn } from '@/lib/utils'

const navItems = [
  { href: '/', label: 'Accueil', icon: Home },
  { href: '/horaires', label: 'Horaires', icon: Clock },
  { href: '/statistiques', label: 'Statistiques', icon: BarChart3 },
  { href: '/liaisons', label: 'Liaisons', icon: GitBranch },
  { href: '/co2', label: 'CO2', icon: Leaf },
  { href: '/qualite', label: 'Qualite', icon: CheckCircle2 },
]

interface TopNavProps {
  apiStatus?: boolean
}

export function TopNav({ apiStatus = true }: TopNavProps) {
  const pathname = usePathname()

  return (
    <header className="sticky top-0 z-50 border-b border-border/50 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      {/* Main nav bar */}
      <div className="flex h-14 items-center justify-between px-6">
        {/* Brand */}
        <div className="flex items-center gap-3">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-accent">
            <Train className="h-4 w-4 text-primary-foreground" />
          </div>
          <div>
            <div className="text-sm font-semibold text-foreground tracking-tight">
              ObRail Europe
            </div>
            <div className="text-[10px] font-semibold uppercase tracking-widest text-primary">
              Observatoire Ferroviaire
            </div>
          </div>
        </div>

        {/* Status pill */}
        <div className="flex items-center gap-2 rounded-md border border-border/50 bg-card px-3 py-1.5 text-xs text-muted-foreground">
          <span 
            className={cn(
              "h-1.5 w-1.5 rounded-full",
              apiStatus 
                ? "bg-primary animate-pulse-live" 
                : "bg-destructive"
            )} 
          />
          {apiStatus ? 'En ligne' : 'Hors ligne'}
        </div>
      </div>

      {/* Navigation tabs */}
      <nav className="flex overflow-x-auto border-t border-border/30 px-6">
        {navItems.map(({ href, label, icon: Icon }) => {
          const isActive = pathname === href
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                "flex items-center gap-2 border-b-2 px-4 py-3 text-xs font-medium transition-colors whitespace-nowrap",
                isActive
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-secondary-foreground"
              )}
            >
              <Icon className="h-3.5 w-3.5" />
              {label}
            </Link>
          )
        })}
      </nav>
    </header>
  )
}
