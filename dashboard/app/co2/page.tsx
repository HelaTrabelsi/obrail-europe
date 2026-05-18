'use client'

import { useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent, Histogram } from '@/components/dashboard/charts'
import { getTrains, getOperators } from '@/lib/mock-data'

export default function CO2Page() {
  const trains = getTrains()
  const operators = getOperators()

  const stats = useMemo(() => {
    const totalDistance = trains.reduce((acc, t) => acc + t.distance_km, 0)
    const co2Train = 14 // g/passager-km
    const co2Avion = 258 // g/passager-km

    const trainCO2 = (totalDistance * co2Train) / 1000
    const avionCO2 = (totalDistance * co2Avion) / 1000
    const evite = avionCO2 - trainCO2
    const pct = (evite / avionCO2) * 100
    const ratio = Math.round(co2Avion / co2Train)

    return { trainCO2, avionCO2, evite, pct, ratio, totalDistance }
  }, [trains])

  // Modal comparison
  const modalComparison = [
    { name: 'Train electrique', value: 6, color: '#00c98d' },
    { name: 'Train moy. UE', value: 14, color: '#0096d6' },
    { name: 'Avion long-courrier', value: 195, color: '#f87171' },
    { name: 'Avion court-courrier', value: 258, color: '#ef4444' },
  ]

  // CO2 distribution
  const co2Distribution = useMemo(() => {
    const buckets: Record<number, number> = {}
    const bucketSize = 0.5
    trains.forEach(t => {
      const bucket = Math.floor(t.co2_emission_kg / bucketSize) * bucketSize
      buckets[bucket] = (buckets[bucket] || 0) + 1
    })
    return Object.entries(buckets)
      .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0]))
      .slice(0, 30)
      .map(([name, value]) => ({ name, value }))
  }, [trains])

  // CO2 intensity by operator
  const co2ByOperator = useMemo(() => {
    return operators.map(op => {
      const opTrains = trains.filter(t => t.operator === op.nom)
      const totalCO2 = opTrains.reduce((a, t) => a + t.co2_emission_kg, 0)
      const totalDist = opTrains.reduce((a, t) => a + t.distance_km, 0)
      const gkm = totalDist > 0 ? (totalCO2 / totalDist * 1000) : 0
      return { name: op.nom, value: parseFloat(gkm.toFixed(2)) }
    }).sort((a, b) => a.value - b.value)
  }, [operators, trains])

  // Service type comparison
  const serviceComparison = useMemo(() => {
    const jour = trains.filter(t => t.type_service === 'Jour')
    const nuit = trains.filter(t => t.type_service === 'Nuit')

    return [
      {
        Type: 'Jour',
        'Nb trains': jour.length,
        'CO2 total (kg)': jour.reduce((a, t) => a + t.co2_emission_kg, 0).toFixed(0),
        'CO2 moy (kg)': (jour.reduce((a, t) => a + t.co2_emission_kg, 0) / jour.length).toFixed(3),
        'Distance moy (km)': (jour.reduce((a, t) => a + t.distance_km, 0) / jour.length).toFixed(0),
      },
      {
        Type: 'Nuit',
        'Nb trains': nuit.length,
        'CO2 total (kg)': nuit.reduce((a, t) => a + t.co2_emission_kg, 0).toFixed(0),
        'CO2 moy (kg)': (nuit.reduce((a, t) => a + t.co2_emission_kg, 0) / nuit.length).toFixed(3),
        'Distance moy (km)': (nuit.reduce((a, t) => a + t.distance_km, 0) / nuit.length).toFixed(0),
      },
    ]
  }, [trains])

  const avgCO2 = trains.reduce((a, t) => a + t.co2_emission_kg, 0) / trains.length

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Environnement"
          title="Emissions"
          titleHighlight="CO2"
          subtitle="Impact carbone compare - rail vs aviation intra-europeenne"
        />

        <KPIGrid className="grid-cols-2 lg:grid-cols-4">
          <KPICard
            value={`${stats.trainCO2.toLocaleString()} kg`}
            label="CO2 si tout en train"
            gradient="linear-gradient(90deg, #00c98d, #34d399)"
            delta="14 g/passager-km (ADEME 2023)"
          />
          <KPICard
            value={`${stats.avionCO2.toLocaleString()} kg`}
            label="CO2 si tout en avion"
            gradient="linear-gradient(90deg, #ef4444, #f87171)"
            delta="258 g/passager-km"
          />
          <KPICard
            value={`${stats.evite.toLocaleString()} kg`}
            label="CO2 evite"
            gradient="linear-gradient(90deg, #0096d6, #38bdf8)"
            delta={`- ${stats.pct.toFixed(0)}% vs avion`}
          />
          <KPICard
            value={`x ${stats.ratio}`}
            label="Avion plus emetteur"
            gradient="linear-gradient(90deg, #f59e0b, #fbbf24)"
            delta="258 / 14 g/km"
          />
        </KPIGrid>

        <SectionTitle>Comparatif modal (g CO2 / passager-km)</SectionTitle>
        <div className="rounded-xl border border-border/50 bg-card p-5">
          <BarChartComponent
            data={modalComparison}
            height={240}
            color="#00c98d"
          />
        </div>

        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Distribution des emissions par train</SectionTitle>
            <Histogram
              data={co2Distribution}
              height={240}
              color="#00c98d"
            />
            <div className="mt-4 text-xs text-muted-foreground">
              Moyenne: <span className="text-warning font-semibold">{avgCO2.toFixed(2)} kg</span>
            </div>
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Intensite carbone par operateur</SectionTitle>
            <BarChartComponent
              data={co2ByOperator}
              height={240}
              color="#00c98d"
            />
          </div>
        </div>

        <SectionTitle>Comparatif Jour vs Nuit</SectionTitle>
        <DataTable
          columns={[
            { key: 'Type', label: 'Type' },
            { key: 'Nb trains', label: 'Nb trains', align: 'right' },
            { key: 'CO2 total (kg)', label: 'CO2 total (kg)', align: 'right' },
            { key: 'CO2 moy (kg)', label: 'CO2 moy (kg)', align: 'right' },
            { key: 'Distance moy (km)', label: 'Distance moy (km)', align: 'right' },
          ]}
          data={serviceComparison}
        />

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}
