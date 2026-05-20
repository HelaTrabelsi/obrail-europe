'use client'

import { useMemo, useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent, Histogram } from '@/components/dashboard/charts'
import { getTrainsFromAPI, getOperateursFromAPI, type Train, type Operateur } from '@/lib/api-client'

export default function CO2Page() {
  const [trains, setTrains] = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [t, o] = await Promise.all([getTrainsFromAPI({ limit: 500 }), getOperateursFromAPI()])
        setTrains(t); setOperators(o); setApiStatus(true)
      } catch (e) { setApiStatus(false) } finally { setLoading(false) }
    }
    load()
  }, [])

  const stats = useMemo(() => {
    const totalDistance = trains.reduce((acc, t) => acc + t.distance_km, 0)
    const trainCO2 = (totalDistance * 14) / 1000
    const avionCO2 = (totalDistance * 258) / 1000
    const evite = avionCO2 - trainCO2
    return { trainCO2, avionCO2, evite, pct: (evite / avionCO2) * 100, ratio: Math.round(258 / 14) }
  }, [trains])

  const modalComparison = [
    { name: 'Train electrique', value: 6 },
    { name: 'Train moy. UE', value: 14 },
    { name: 'Avion long-courrier', value: 195 },
    { name: 'Avion court-courrier', value: 258 },
  ]

  const co2Distribution = useMemo(() => {
    const buckets: Record<number, number> = {}
    trains.forEach(t => {
      const co2 = t.co2_emission_kg || 0
      const b = Math.floor(co2 / 0.5) * 0.5
      buckets[b] = (buckets[b] || 0) + 1
    })
    return Object.entries(buckets).sort((a,b)=>parseFloat(a[0])-parseFloat(b[0])).slice(0,30).map(([name,value])=>({name,value}))
  }, [trains])

  const co2ByOperator = useMemo(() => operators.map(op => {
    const ot = trains.filter(t => t.operator === op.nom)
    const co2 = ot.reduce((a,t) => a+(t.co2_emission_kg||0), 0)
    const dist = ot.reduce((a,t) => a+t.distance_km, 0)
    return { name: op.nom, value: parseFloat(dist > 0 ? (co2/dist*1000).toFixed(2) : '14') }
  }).sort((a,b)=>a.value-b.value), [operators, trains])

  const serviceComparison = useMemo(() => {
    const jour = trains.filter(t => t.type_service === 'Jour')
    const nuit = trains.filter(t => t.type_service === 'Nuit')
    return [
      { Type:'Jour','Nb trains':jour.length,'CO2 total (kg)':jour.reduce((a,t)=>a+(t.co2_emission_kg||0),0).toFixed(0),'CO2 moy (kg)':(jour.reduce((a,t)=>a+(t.co2_emission_kg||0),0)/jour.length||0).toFixed(3),'Distance moy (km)':(jour.reduce((a,t)=>a+t.distance_km,0)/jour.length||0).toFixed(0) },
      { Type:'Nuit','Nb trains':nuit.length,'CO2 total (kg)':nuit.reduce((a,t)=>a+(t.co2_emission_kg||0),0).toFixed(0),'CO2 moy (kg)':(nuit.reduce((a,t)=>a+(t.co2_emission_kg||0),0)/nuit.length||0).toFixed(3),'Distance moy (km)':(nuit.reduce((a,t)=>a+t.distance_km,0)/nuit.length||0).toFixed(0) },
    ]
  }, [trains])

  const avgCO2 = trains.length > 0 ? trains.reduce((a,t)=>a+(t.co2_emission_kg||0),0)/trains.length : 0

  if (loading) return <div className="min-h-screen bg-background flex items-center justify-center"><div className="text-muted-foreground text-sm">Chargement...</div></div>

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader eyebrow="Environnement" title="Emissions" titleHighlight="CO2" subtitle="Impact carbone compare - rail vs aviation intra-europeenne" />
        <KPIGrid className="grid-cols-2 lg:grid-cols-4">
          <KPICard value={`${stats.trainCO2.toLocaleString()} kg`} label="CO2 si tout en train" gradient="linear-gradient(90deg, #00c98d, #34d399)" delta="14 g/passager-km (ADEME 2023)" />
          <KPICard value={`${stats.avionCO2.toLocaleString()} kg`} label="CO2 si tout en avion" gradient="linear-gradient(90deg, #ef4444, #f87171)" delta="258 g/passager-km" />
          <KPICard value={`${stats.evite.toLocaleString()} kg`} label="CO2 evite" gradient="linear-gradient(90deg, #0096d6, #38bdf8)" delta={`- ${stats.pct.toFixed(0)}% vs avion`} />
          <KPICard value={`x ${stats.ratio}`} label="Avion plus emetteur" gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" delta="258 / 14 g/km" />
        </KPIGrid>
        <SectionTitle>Comparatif modal (g CO2 / passager-km)</SectionTitle>
        <div className="rounded-xl border border-border/50 bg-card p-5"><BarChartComponent data={modalComparison} height={240} color="#00c98d" /></div>
        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Distribution des emissions par train</SectionTitle>
            <Histogram data={co2Distribution} height={240} color="#00c98d" />
            <div className="mt-4 text-xs text-muted-foreground">Moyenne: <span className="text-warning font-semibold">{avgCO2.toFixed(2)} kg</span></div>
          </div>
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Intensite carbone par operateur</SectionTitle>
            <BarChartComponent data={co2ByOperator} height={240} color="#00c98d" />
          </div>
        </div>
        <SectionTitle>Comparatif Jour vs Nuit</SectionTitle>
        <DataTable columns={[{key:'Type',label:'Type'},{key:'Nb trains',label:'Nb trains',align:'right'},{key:'CO2 total (kg)',label:'CO2 total (kg)',align:'right'},{key:'CO2 moy (kg)',label:'CO2 moy (kg)',align:'right'},{key:'Distance moy (km)',label:'Distance moy (km)',align:'right'}]} data={serviceComparison} />
        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL</footer>
      </main>
    </div>
  )
}