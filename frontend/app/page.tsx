'use client'

import { useEffect, useState, useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { SourceCard } from '@/components/dashboard/source-card'
import { GroupedBarChart, DonutChart } from '@/components/dashboard/charts'
import {
  getTrainsFromAPI, getOperateursFromAPI, getStatsFromAPI, getHealth,
  type Train, type Operateur, type Stats
} from '@/lib/api-client'

export default function HomePage() {
  const [trains, setTrains] = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [apiStatus, setApiStatus] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const health = await getHealth()
        setApiStatus(health.status === 'ok')

        const [t, o, s] = await Promise.all([
          getTrainsFromAPI({ limit: 500 }),
          getOperateursFromAPI(),
          getStatsFromAPI(),
        ])
        setTrains(t)
        setOperators(o)
        setStats(s)
      } catch (e) {
        console.error('API indisponible:', e)
        setApiStatus(false)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const kpis = useMemo(() => {
    if (!stats) return null
    const totalDistance = trains.reduce((acc, t) => acc + t.distance_km, 0)
    const avgEmissions = 14
    const co2Evite = (totalDistance * (258 - avgEmissions)) / 1_000_000
    return {
      total: stats.nb_trains,
      nbNuit: stats.nb_nuit,
      nbJour: stats.nb_jour,
      nbOperateurs: stats.nb_operateurs,
      co2Evite: co2Evite.toFixed(0),
    }
  }, [stats, trains])

  const serviceByOperator = useMemo(() => {
    return operators.map(op => ({
      name: op.nom,
      Jour: op.nb_jour,
      Nuit: op.nb_nuit,
    }))
  }, [operators])

  const marketShare = useMemo(() => {
    return operators.map(op => ({ name: op.nom, value: op.nb_trains }))
  }, [operators])

  const sources = [
    { country: 'FR', name: 'SNCF TER', type: 'GTFS', color: '#00c98d' },
    { country: 'FR', name: 'SNCF Intercites', type: 'GTFS', color: '#00c98d' },
    { country: 'DE', name: 'Deutsche Bahn', type: 'GTFS', color: '#0096d6' },
    { country: 'DE', name: 'DB Regional', type: 'GTFS', color: '#0096d6' },
    { country: 'BE', name: 'SNCB', type: 'GTFS', color: '#f59e0b' },
  ]

  if (loading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-muted-foreground text-sm">Chargement des donnees...</div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Tableau de bord"
          title="ObRail"
          titleHighlight="Europe"
          subtitle="Donnees ferroviaires harmonisees - mobilite durable et bas-carbone"
        />

        {kpis && (
          <KPIGrid>
            <KPICard value={kpis.total.toLocaleString()} label="Trains total"
              gradient="linear-gradient(90deg, #00c98d, #0096d6)" />
            <KPICard value={kpis.nbNuit.toLocaleString()} label="Trains de nuit"
              gradient="linear-gradient(90deg, #6366f1, #818cf8)" />
            <KPICard value={kpis.nbJour.toLocaleString()} label="Trains de jour"
              gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" />
            <KPICard value={kpis.nbOperateurs.toString()} label="Operateurs"
              gradient="linear-gradient(90deg, #0096d6, #38bdf8)" />
            <KPICard value={`${kpis.co2Evite} t`} label="CO2 evite vs avion"
              gradient="linear-gradient(90deg, #00c98d, #34d399)" delta="- 95% vs avion" />
          </KPIGrid>
        )}

        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Repartition Jour / Nuit par operateur</SectionTitle>
            <GroupedBarChart
              data={serviceByOperator}
              keys={[
                { key: 'Jour', color: '#f59e0b', label: 'Jour' },
                { key: 'Nuit', color: '#6366f1', label: 'Nuit' },
              ]}
              height={280}
            />
          </div>
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Parts de marche</SectionTitle>
            <DonutChart data={marketShare} height={280} innerRadius={50} outerRadius={90} />
          </div>
        </div>

        <SectionTitle>Sources integrees</SectionTitle>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {sources.map((source) => (
            <SourceCard key={source.name} {...source} />
          ))}
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}