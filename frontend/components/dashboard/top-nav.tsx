'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useEffect, useState } from 'react'
import {
  Home, Clock, BarChart3, GitBranch, Leaf, CheckCircle2,
  Train, AlertCircle, Sun, Moon, Contrast, ZoomIn, ZoomOut,
  Eye, Menu, X
} from 'lucide-react'
import { cn } from '@/lib/utils'

const navItems = [
  { href: '/',             label: 'Accueil',      icon: Home },
  { href: '/horaires',     label: 'Horaires',     icon: Clock },
  { href: '/statistiques', label: 'Statistiques', icon: BarChart3 },
  { href: '/liaisons',     label: 'Liaisons',     icon: GitBranch },
  { href: '/co2',          label: 'CO2',          icon: Leaf },
  { href: '/qualite',      label: 'Qualité',      icon: CheckCircle2 },
]

type Theme = 'dark' | 'light' | 'high-contrast'
type FontSize = 'sm' | 'base' | 'lg'

interface TopNavProps {
  apiStatus?: boolean
  dataStale?: boolean
}

export function TopNav({ apiStatus = true, dataStale = false }: TopNavProps) {
  const pathname = usePathname()
  const [theme, setTheme]         = useState<Theme>('dark')
  const [fontSize, setFontSize]   = useState<FontSize>('base')
  const [colorblind, setColorblind] = useState(false)
  const [mounted, setMounted]     = useState(false)
  const [menuOpen, setMenuOpen]   = useState(false)

  useEffect(() => {
    setMounted(true)
    const t  = localStorage.getItem('obrail-theme') as Theme | null
    const f  = localStorage.getItem('obrail-font') as FontSize | null
    const cb = localStorage.getItem('obrail-colorblind')
    if (t)  applyTheme(t, false)
    if (f)  applyFont(f, false)
    if (cb === 'true') applyColorblind(true, false)
  }, [])

  // Ferme le menu mobile quand on change de page
  useEffect(() => { setMenuOpen(false) }, [pathname])

  function applyTheme(t: Theme, save = true) {
    const html = document.documentElement
    html.classList.remove('dark', 'light', 'high-contrast')
    html.classList.add(t)
    setTheme(t)
    if (save) localStorage.setItem('obrail-theme', t)
  }

  function applyFont(s: FontSize, save = true) {
    const html = document.documentElement
    html.classList.remove('text-sm-mode', 'text-base-mode', 'text-lg-mode')
    html.classList.add(`text-${s}-mode`)
    setFontSize(s)
    if (save) localStorage.setItem('obrail-font', s)
  }

  function applyColorblind(on: boolean, save = true) {
    const root = document.documentElement
    root.classList.toggle('colorblind-mode', on)
    if (on) {
      // Bleu à la place du vert, orange à la place du rouge
      root.style.setProperty('--primary', 'oklch(0.65 0.18 250)')
      root.style.setProperty('--ring',    'oklch(0.65 0.18 250)')
      root.style.setProperty('--destructive', 'oklch(0.65 0.18 60)')
      root.style.setProperty('--chart-1', 'oklch(0.65 0.18 250)')
      root.style.setProperty('--chart-5', 'oklch(0.65 0.18 60)')
    } else {
      root.style.removeProperty('--primary')
      root.style.removeProperty('--ring')
      root.style.removeProperty('--destructive')
      root.style.removeProperty('--chart-1')
      root.style.removeProperty('--chart-5')
    }
    setColorblind(on)
    if (save) localStorage.setItem('obrail-colorblind', String(on))
  }

  function cycleTheme() {
    const next: Record<Theme, Theme> = { dark: 'light', light: 'high-contrast', 'high-contrast': 'dark' }
    applyTheme(next[theme])
  }

  function cycleFont() {
    const next: Record<FontSize, FontSize> = { sm: 'base', base: 'lg', lg: 'sm' }
    applyFont(next[fontSize])
  }

  const themeIcon = theme === 'dark' ? <Moon className="h-4 w-4" /> :
                    theme === 'light' ? <Sun className="h-4 w-4" /> :
                    <Contrast className="h-4 w-4" />
  const themeLabel = theme === 'dark' ? 'Passer en mode clair' :
                     theme === 'light' ? 'Passer en contraste élevé' :
                     'Passer en mode sombre'

  if (!mounted) return null

  return (
    <>
      <a href="#main-content" className="skip-to-content">Aller au contenu principal</a>

      <header role="banner" className="sticky top-0 z-50 border-b border-border/50 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">

        {/* Barre principale */}
        <div className="flex h-14 items-center justify-between px-4 gap-2">

          {/* Brand */}
          <div className="flex items-center gap-2 shrink-0">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-accent" aria-hidden="true">
              <Train className="h-4 w-4 text-primary-foreground" />
            </div>
            <div>
              <div className="text-sm font-semibold text-foreground tracking-tight leading-none">ObRail Europe</div>
              <div className="text-[9px] font-semibold uppercase tracking-widest text-primary hidden sm:block">Observatoire Ferroviaire</div>
            </div>
          </div>

          {/* Contrôles droite */}
          <div className="flex items-center gap-1.5">

            {dataStale && (
              <div role="alert" className="hidden sm:flex items-center gap-1.5 rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-xs text-amber-400">
                <AlertCircle className="h-3 w-3" aria-hidden="true" />
                <span>Données obsolètes</span>
              </div>
            )}

            {/* Statut API */}
            <div role="status" aria-live="polite" aria-label={apiStatus ? 'API en ligne' : 'API hors ligne'}
              className="flex items-center gap-1.5 rounded-md border border-border/50 bg-card px-2.5 py-1.5 text-xs text-muted-foreground">
              <span className={cn("h-1.5 w-1.5 rounded-full shrink-0", apiStatus ? "bg-primary animate-pulse-live" : "bg-destructive")} aria-hidden="true" />
              <span className="hidden sm:inline">{apiStatus ? 'En ligne' : 'Hors ligne'}</span>
            </div>

            {/* Taille police — desktop uniquement */}
            <button onClick={cycleFont} aria-label={`Taille police : ${fontSize}`} title="Taille de police"
              className="hidden md:flex items-center justify-center h-8 w-8 rounded-md border border-border/50 bg-card text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors focus-visible:ring-2 focus-visible:ring-ring focus:outline-none">
              {fontSize === 'lg' ? <ZoomOut className="h-4 w-4" aria-hidden="true" /> : <ZoomIn className="h-4 w-4" aria-hidden="true" />}
            </button>

            {/* Mode daltonien — desktop uniquement */}
            <button onClick={() => applyColorblind(!colorblind)}
              aria-label={colorblind ? 'Désactiver mode daltonien' : 'Activer mode daltonien'}
              aria-pressed={colorblind} title="Mode daltonien"
              className={cn(
                "hidden md:flex items-center justify-center h-8 w-8 rounded-md border bg-card transition-colors focus-visible:ring-2 focus-visible:ring-ring focus:outline-none",
                colorblind ? "border-primary text-primary bg-primary/10" : "border-border/50 text-muted-foreground hover:text-foreground hover:bg-muted/50"
              )}>
              <Eye className="h-4 w-4" aria-hidden="true" />
            </button>

            {/* Toggle thème */}
            <button onClick={cycleTheme} aria-label={themeLabel} title={themeLabel}
              className="flex items-center justify-center h-8 w-8 rounded-md border border-border/50 bg-card text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors focus-visible:ring-2 focus-visible:ring-ring focus:outline-none">
              <span aria-hidden="true">{themeIcon}</span>
            </button>

            {/* Hamburger — mobile uniquement */}
            <button
              onClick={() => setMenuOpen(!menuOpen)}
              aria-label={menuOpen ? 'Fermer le menu' : 'Ouvrir le menu'}
              aria-expanded={menuOpen}
              aria-controls="mobile-menu"
              className="flex md:hidden items-center justify-center h-8 w-8 rounded-md border border-border/50 bg-card text-muted-foreground hover:text-foreground hover:bg-muted/50 transition-colors focus-visible:ring-2 focus-visible:ring-ring focus:outline-none"
            >
              {menuOpen ? <X className="h-4 w-4" aria-hidden="true" /> : <Menu className="h-4 w-4" aria-hidden="true" />}
            </button>
          </div>
        </div>

        {/* Navigation desktop */}
        <nav role="navigation" aria-label="Navigation principale" className="hidden md:flex overflow-x-auto border-t border-border/30 px-4">
          {navItems.map(({ href, label, icon: Icon }) => {
            const isActive = pathname === href
            return (
              <Link key={href} href={href} aria-current={isActive ? 'page' : undefined}
                className={cn(
                  "flex items-center gap-2 border-b-2 px-4 py-3 text-xs font-medium transition-colors whitespace-nowrap",
                  "focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
                  isActive ? "border-primary text-primary" : "border-transparent text-muted-foreground hover:text-secondary-foreground"
                )}>
                <Icon className="h-3.5 w-3.5" aria-hidden="true" />
                {label}
              </Link>
            )
          })}
        </nav>

        {/* Menu mobile — s'ouvre sous la barre */}
        {menuOpen && (
          <nav
            id="mobile-menu"
            role="navigation"
            aria-label="Menu mobile"
            className="md:hidden border-t border-border/30 bg-background/98 backdrop-blur"
          >
            <div className="px-4 py-2 space-y-1">
              {navItems.map(({ href, label, icon: Icon }) => {
                const isActive = pathname === href
                return (
                  <Link key={href} href={href}
                    aria-current={isActive ? 'page' : undefined}
                    className={cn(
                      "flex items-center gap-3 px-3 py-3 rounded-lg text-sm font-medium transition-colors",
                      "focus:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      isActive
                        ? "bg-primary/15 text-primary border border-primary/30"
                        : "text-muted-foreground hover:text-foreground hover:bg-muted/30"
                    )}>
                    <Icon className="h-4 w-4 shrink-0" aria-hidden="true" />
                    {label}
                  </Link>
                )
              })}
            </div>

            {/* Contrôles accessibilité dans le menu mobile */}
            <div className="px-4 py-3 border-t border-border/30 flex items-center gap-2">
              <span className="text-xs text-muted-foreground">Accessibilité :</span>
              <button onClick={cycleFont} aria-label={`Taille police : ${fontSize}`}
                className="flex items-center gap-1 text-xs px-2 py-1 rounded border border-border/50 text-muted-foreground hover:text-foreground">
                {fontSize === 'lg' ? <ZoomOut className="h-3 w-3" /> : <ZoomIn className="h-3 w-3" />}
                Police {fontSize}
              </button>
              <button onClick={() => applyColorblind(!colorblind)}
                aria-pressed={colorblind}
                className={cn("flex items-center gap-1 text-xs px-2 py-1 rounded border transition-colors",
                  colorblind ? "border-primary text-primary bg-primary/10" : "border-border/50 text-muted-foreground hover:text-foreground")}>
                <Eye className="h-3 w-3" />
                Daltonien
              </button>
            </div>
          </nav>
        )}
      </header>
    </>
  )
}