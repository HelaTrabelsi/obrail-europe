'use client'

import { useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { getTrainsFromAPI, getStatsFromAPI, type Train, type Stats } from '@/lib/api-client'

export default function LiaisonsPage() {
  const [trains, setTrains] = useState<Train[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [t, s] = await Promise.all([
          getTrainsFromAPI({ limit: 50 }),
          getStatsFromAPI(),
        ])
        setTrains(t)
        setStats(s)
        setApiStatus(true)
      } catch (e) {
        setApiStatus(false)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const topGares = (() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => { counts[t.gare] = (counts[t.gare] || 0) + 1 })
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 10)
  })()

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-muted-foreground text-sm">Chargement liaisons...</div>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Réseau"
          title="Liaisons"
          titleHighlight="ferroviaires"
          subtitle="Analyse des connexions entre gares"
        />

        {stats && (
          <KPIGrid>
            <KPICard value={stats.nb_trains.toLocaleString()} label="Trains total" gradient="linear-gradient(90deg, #00c98d, #0096d6)" />
            <KPICard value={stats.nb_gares.toLocaleString()} label="Gares" gradient="linear-gradient(90deg, #6366f1, #818cf8)" />
            <KPICard value={stats.nb_operateurs.toString()} label="Opérateurs" gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" />
            <KPICard value={`${stats.distance_moyenne_km?.toFixed(0) ?? '—'} km`} label="Distance moyenne" gradient="linear-gradient(90deg, #0096d6, #38bdf8)" />
          </KPIGrid>
        )}

        <div className="mt-8 rounded-xl border border-border/50 bg-card p-6">
          <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">
            Top 10 gares les plus desservies (échantillon)
          </h3>
          <div className="space-y-3">
            {topGares.map(([gare, count], i) => (
              <div key={gare} className="flex items-center gap-4">
                <span className="text-xs text-muted-foreground w-5 shrink-0">#{i + 1}</span>
                <span className="text-sm text-foreground w-48 shrink-0 truncate">{gare}</span>
                <div className="flex-1 bg-muted/20 rounded-full h-5">
                  <div
                    className="h-5 rounded-full bg-primary/80 flex items-center justify-end pr-2"
                    style={{ width: `${(count / topGares[0][1]) * 100}%` }}
                  >
                    <span className="text-xs font-bold text-white">{count}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-4">
          {['Jour', 'Nuit', 'national', 'regional'].map(type => {
            const count = trains.filter(t =>
              t.type_service === type || t.type_ligne === type
            ).length
            return (
              <div key={type} className="rounded-xl border border-border/50 bg-card p-4 text-center">
                <p className="text-2xl font-bold text-primary">{count}</p>
                <p className="text-xs text-muted-foreground mt-1">{type}</p>
              </div>
            )
          })}
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}