'use client'

import { useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent, DonutChart, Histogram } from '@/components/dashboard/charts'
import { getTrains, getOperators } from '@/lib/mock-data'

export default function StatistiquesPage() {
  const trains = getTrains()
  const operators = getOperators()

  const stats = useMemo(() => {
    const nbNuit = trains.filter(t => t.type_service === 'Nuit').length
    const nbJour = trains.filter(t => t.type_service === 'Jour').length
    const distances = trains.map(t => t.distance_km)
    const avgDistance = distances.reduce((a, b) => a + b, 0) / distances.length
    const maxDistance = Math.max(...distances)

    return { total: trains.length, nbNuit, nbJour, avgDistance, maxDistance }
  }, [trains])

  // Top stations
  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => {
      counts[t.origin_station] = (counts[t.origin_station] || 0) + 1
    })
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([name, value]) => ({ name, value }))
  }, [trains])

  // Distance distribution
  const distanceDistribution = useMemo(() => {
    const buckets: Record<number, number> = {}
    const bucketSize = 50
    trains.forEach(t => {
      const bucket = Math.floor(t.distance_km / bucketSize) * bucketSize
      buckets[bucket] = (buckets[bucket] || 0) + 1
    })
    return Object.entries(buckets)
      .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
      .map(([name, value]) => ({ name: `${name}`, value }))
  }, [trains])

  // Service type distribution
  const serviceDistribution = useMemo(() => {
    return [
      { name: 'Jour', value: stats.nbJour },
      { name: 'Nuit', value: stats.nbNuit },
    ]
  }, [stats])

  // Trains by operator
  const trainsByOperator = useMemo(() => {
    return operators
      .map(op => ({ name: op.nom, value: op.nb_trains }))
      .sort((a, b) => b.value - a.value)
  }, [operators])

  // Operator comparison table
  const operatorComparison = useMemo(() => {
    return operators.map(op => {
      const opTrains = trains.filter(t => t.operator === op.nom)
      const avgDistance = opTrains.reduce((a, t) => a + t.distance_km, 0) / opTrains.length
      const avgCO2 = opTrains.reduce((a, t) => a + t.co2_emission_kg, 0) / opTrains.length
      const maxDist = Math.max(...opTrains.map(t => t.distance_km))
      return {
        operator: op.nom,
        Trains: op.nb_trains,
        Dist_moy: avgDistance.toFixed(1),
        CO2_moy: avgCO2.toFixed(2),
        Dist_max: maxDist.toFixed(0),
      }
    })
  }, [operators, trains])

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Analyse"
          title="Statistiques"
          titleHighlight="reseau"
          subtitle="Couverture ferroviaire integree par operateur"
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
            value={`${stats.avgDistance.toFixed(0)} km`}
            label="Distance moy."
            gradient="linear-gradient(90deg, #0096d6, #38bdf8)"
          />
          <KPICard
            value={`${stats.maxDistance.toFixed(0)} km`}
            label="Distance max."
            gradient="linear-gradient(90deg, #354d62, #4a6275)"
          />
        </KPIGrid>

        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Top 15 gares</SectionTitle>
            <BarChartComponent
              data={topStations}
              height={360}
              horizontal
              color="#00c98d"
            />
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Distribution des distances</SectionTitle>
            <Histogram
              data={distanceDistribution}
              height={360}
              color="#00c98d"
              xLabel="Distance (km)"
            />
          </div>
        </div>

        <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Jour vs Nuit</SectionTitle>
            <DonutChart
              data={serviceDistribution}
              colors={['#f59e0b', '#6366f1']}
              height={240}
              innerRadius={50}
              outerRadius={80}
            />
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Trains par operateur</SectionTitle>
            <BarChartComponent
              data={trainsByOperator}
              height={240}
              color="#0096d6"
            />
          </div>
        </div>

        <SectionTitle>Tableau comparatif operateurs</SectionTitle>
        <DataTable
          columns={[
            { key: 'operator', label: 'Operateur' },
            { key: 'Trains', label: 'Trains', align: 'right' },
            { key: 'Dist_moy', label: 'Dist. moy', align: 'right' },
            { key: 'CO2_moy', label: 'CO2 moy', align: 'right' },
            { key: 'Dist_max', label: 'Dist. max', align: 'right' },
          ]}
          data={operatorComparison}
        />

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}
