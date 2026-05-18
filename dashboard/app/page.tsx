'use client'

import { useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { SourceCard } from '@/components/dashboard/source-card'
import { GroupedBarChart, DonutChart } from '@/components/dashboard/charts'
import { getTrains, getOperators } from '@/lib/mock-data'

export default function HomePage() {
  const trains = getTrains()
  const operators = getOperators()

  const stats = useMemo(() => {
    const nbNuit = trains.filter(t => t.type_service === 'Nuit').length
    const nbJour = trains.filter(t => t.type_service === 'Jour').length
    const totalDistance = trains.reduce((acc, t) => acc + t.distance_km, 0)
    const avgEmissions = trains.reduce((acc, t) => acc + t.emissions_co2_gkm, 0) / trains.length
    const co2Evite = (totalDistance * (285 - avgEmissions)) / 1_000_000

    return {
      total: trains.length,
      nbNuit,
      nbJour,
      nbOperateurs: operators.length,
      co2Evite: co2Evite.toFixed(0),
    }
  }, [trains, operators])

  // Grouped bar chart data for Jour/Nuit by operator
  const serviceByOperator = useMemo(() => {
    const map = new Map<string, { Jour: number; Nuit: number }>()
    trains.forEach(t => {
      if (!map.has(t.operator)) {
        map.set(t.operator, { Jour: 0, Nuit: 0 })
      }
      const op = map.get(t.operator)!
      if (t.type_service === 'Jour') op.Jour++
      else op.Nuit++
    })
    return Array.from(map.entries()).map(([name, data]) => ({
      name,
      ...data,
    }))
  }, [trains])

  // Pie chart data for market share
  const marketShare = useMemo(() => {
    return operators.map(op => ({
      name: op.nom,
      value: op.nb_trains,
    }))
  }, [operators])

  const sources = [
    { country: 'FR', name: 'SNCF TER', type: 'GTFS', color: '#00c98d' },
    { country: 'FR', name: 'SNCF Intercites', type: 'GTFS', color: '#00c98d' },
    { country: 'DE', name: 'Deutsche Bahn', type: 'GTFS', color: '#0096d6' },
    { country: 'DE', name: 'DB Regional', type: 'GTFS', color: '#0096d6' },
    { country: 'BE', name: 'SNCB', type: 'GTFS', color: '#f59e0b' },
  ]

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Tableau de bord"
          title="ObRail"
          titleHighlight="Europe"
          subtitle="Donnees ferroviaires harmonisees - mobilite durable et bas-carbone"
        />

        <KPIGrid>
          <KPICard
            value={stats.total.toLocaleString()}
            label="Trains total"
            gradient="linear-gradient(90deg, #00c98d, #0096d6)"
          />
          <KPICard
            value={stats.nbNuit.toLocaleString()}
            label="Trains de nuit"
            gradient="linear-gradient(90deg, #6366f1, #818cf8)"
          />
          <KPICard
            value={stats.nbJour.toLocaleString()}
            label="Trains de jour"
            gradient="linear-gradient(90deg, #f59e0b, #fbbf24)"
          />
          <KPICard
            value={stats.nbOperateurs.toString()}
            label="Operateurs"
            gradient="linear-gradient(90deg, #0096d6, #38bdf8)"
          />
          <KPICard
            value={`${stats.co2Evite} t`}
            label="CO2 evite vs avion"
            gradient="linear-gradient(90deg, #00c98d, #34d399)"
            delta="- 85% vs avion"
          />
        </KPIGrid>

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
            <DonutChart
              data={marketShare}
              height={280}
              innerRadius={50}
              outerRadius={90}
            />
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
