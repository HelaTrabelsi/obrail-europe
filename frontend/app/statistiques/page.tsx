'use client'

import { useMemo, useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent, DonutChart, Histogram } from '@/components/dashboard/charts'
import { getTrainsFromAPI, getOperateursFromAPI, type Train, type Operateur } from '@/lib/api-client'

export default function StatistiquesPage() {
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
    if (!trains.length) return { total: 0, nbNuit: 0, nbJour: 0, avgDistance: 0, maxDistance: 0 }
    const nbNuit = trains.filter(t => t.type_service === 'Nuit').length
    const nbJour = trains.filter(t => t.type_service === 'Jour').length
    const distances = trains.map(t => t.distance_km)
    return { total: trains.length, nbNuit, nbJour, avgDistance: distances.reduce((a,b)=>a+b,0)/distances.length, maxDistance: Math.max(...distances) }
  }, [trains])

  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => { counts[t.gare] = (counts[t.gare] || 0) + 1 })
    return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,15).map(([name,value])=>({name,value}))
  }, [trains])

  const distanceDistribution = useMemo(() => {
    const buckets: Record<number, number> = {}
    trains.forEach(t => { const b = Math.floor(t.distance_km/50)*50; buckets[b] = (buckets[b]||0)+1 })
    return Object.entries(buckets).sort((a,b)=>parseInt(a[0])-parseInt(b[0])).map(([name,value])=>({name,value}))
  }, [trains])

  const serviceDistribution = [
    { name: 'Jour', value: stats.nbJour },
    { name: 'Nuit', value: stats.nbNuit },
  ]

  const trainsByOperator = useMemo(() => operators.map(op=>({name:op.nom,value:op.nb_trains})).sort((a,b)=>b.value-a.value), [operators])

  const operatorComparison = useMemo(() => operators.map(op => {
    const ot = trains.filter(t => t.operator === op.nom)
    const avgDist = ot.length ? ot.reduce((a,t)=>a+t.distance_km,0)/ot.length : 0
    const avgCO2 = ot.length ? ot.reduce((a,t)=>a+(t.co2_emission_kg||0),0)/ot.length : 0
    const maxDist = ot.length ? Math.max(...ot.map(t=>t.distance_km)) : 0
    return { operator: op.nom, Trains: op.nb_trains, Dist_moy: avgDist.toFixed(1), CO2_moy: avgCO2.toFixed(2), Dist_max: maxDist.toFixed(0) }
  }), [operators, trains])

  if (loading) return <div className="min-h-screen bg-background flex items-center justify-center"><div className="text-muted-foreground text-sm">Chargement...</div></div>

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader eyebrow="Analyse" title="Statistiques" titleHighlight="reseau" subtitle="Couverture ferroviaire integree par operateur" />
        <KPIGrid>
          <KPICard value={stats.total.toLocaleString()} label="Trains total" gradient="linear-gradient(90deg, #00c98d, #0096d6)" />
          <KPICard value={stats.nbNuit.toLocaleString()} label="Trains de nuit" gradient="linear-gradient(90deg, #6366f1, #818cf8)" />
          <KPICard value={stats.nbJour.toLocaleString()} label="Trains de jour" gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" />
          <KPICard value={`${stats.avgDistance.toFixed(0)} km`} label="Distance moy." gradient="linear-gradient(90deg, #0096d6, #38bdf8)" />
          <KPICard value={`${stats.maxDistance.toFixed(0)} km`} label="Distance max." gradient="linear-gradient(90deg, #354d62, #4a6275)" />
        </KPIGrid>
        <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5"><SectionTitle className="mt-0 mb-4">Top 15 gares</SectionTitle><BarChartComponent data={topStations} height={360} horizontal color="#00c98d" /></div>
          <div className="rounded-xl border border-border/50 bg-card p-5"><SectionTitle className="mt-0 mb-4">Distribution des distances</SectionTitle><Histogram data={distanceDistribution} height={360} color="#00c98d" xLabel="Distance (km)" /></div>
        </div>
        <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5"><SectionTitle className="mt-0 mb-4">Jour vs Nuit</SectionTitle><DonutChart data={serviceDistribution} colors={['#f59e0b','#6366f1']} height={240} innerRadius={50} outerRadius={80} /></div>
          <div className="rounded-xl border border-border/50 bg-card p-5"><SectionTitle className="mt-0 mb-4">Trains par operateur</SectionTitle><BarChartComponent data={trainsByOperator} height={240} color="#0096d6" /></div>
        </div>
        <SectionTitle>Tableau comparatif operateurs</SectionTitle>
        <DataTable columns={[{key:'operator',label:'Operateur'},{key:'Trains',label:'Trains',align:'right'},{key:'Dist_moy',label:'Dist. moy',align:'right'},{key:'CO2_moy',label:'CO2 moy',align:'right'},{key:'Dist_max',label:'Dist. max',align:'right'}]} data={operatorComparison} />
        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL</footer>
      </main>
    </div>
  )
}