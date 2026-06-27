'use client'

import { useMemo, useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent } from '@/components/dashboard/charts'
import { getTrainsFromAPI, getStatsQualiteFromAPI, type Train, type StatsQualite } from '@/lib/api-client'
import { ShieldCheck, Calendar } from 'lucide-react'

export default function QualitePage() {
  const [trains, setTrains] = useState<Train[]>([])
  const [qualite, setQualite] = useState<StatsQualite | null>(null)
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [t, q] = await Promise.all([getTrainsFromAPI({ limit: 50 }), getStatsQualiteFromAPI()])
        setTrains(t); setQualite(q); setApiStatus(true)
      } catch (e) { setApiStatus(false) } finally { setLoading(false) }
    }
    load()
  }, [])

  const fieldCompleteness = useMemo(() => {
    if (!trains.length) return []
    const fields = ['operateur','gare','pays','type_service','type_ligne','heure_depart','heure_arrivee','distance_km','emission_co2_gkm','source_donnee']
    return fields.map(field => {
      const nonNull = trains.filter(t => t[field as keyof typeof t] !== null && t[field as keyof typeof t] !== undefined).length
      return { name: field, value: parseFloat(((nonNull/trains.length)*100).toFixed(1)) }
    }).sort((a,b)=>a.value-b.value)
  }, [trains])

  const sourceDistribution = useMemo(() => {
    if (qualite) return qualite.par_source.map(s => ({ name: s.source_donnee, value: s.nb }))
    const counts: Record<string, number> = {}
    trains.forEach(t => { counts[t.source_donnee] = (counts[t.source_donnee]||0)+1 })
    return Object.entries(counts).sort((a,b)=>b[1]-a[1]).map(([name,value])=>({name,value}))
  }, [trains, qualite])

  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => { counts[t.gare] = (counts[t.gare]||0)+1 })
    return Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,10).map(([Gare,Nb])=>({Gare,Nb}))
  }, [trains])

  const kpis = qualite || { nb_trains_total: 0, co2_manquants: 0, completude_co2_pct: 0, etl_logs: [], par_source: [] }
  const currentDate = new Date().toLocaleDateString('fr-FR', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' })

  if (loading) return <div className="min-h-screen bg-background flex items-center justify-center"><div className="text-muted-foreground text-sm">Chargement...</div></div>

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader eyebrow="Controle" title="Qualite des" titleHighlight="Donnees" subtitle="Completude, tracabilite ETL et conformite RGPD" />
        <KPIGrid className="grid-cols-2 lg:grid-cols-4">
          <KPICard value="186 902" label="Enregistrements bruts" gradient="linear-gradient(90deg, #0096d6, #38bdf8)" />
          <KPICard value={kpis.nb_trains_total.toLocaleString()} label="Apres nettoyage" gradient="linear-gradient(90deg, #00c98d, #34d399)" />
          <KPICard value="59 162" label="Doublons supprimes" gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" />
          <KPICard value={`${kpis.completude_co2_pct}%`} label="Completude CO2" gradient="linear-gradient(90deg, #ef4444, #f87171)" />
        </KPIGrid>
        <div className="mt-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2 rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Completude par champ (%)</SectionTitle>
            <BarChartComponent data={fieldCompleteness} height={400} horizontal color="#00c98d" />
          </div>
          <div className="space-y-4">
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60 mb-2 flex items-center gap-2"><Calendar className="h-3 w-3" />Mise a jour</div>
              <div className="text-lg font-bold text-foreground">{currentDate}</div>
            </div>
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-primary mb-3 flex items-center gap-2"><ShieldCheck className="h-3 w-3" />Conformite RGPD</div>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li>Aucune donnee personnelle traitee</li>
                <li>Sources open data publiques (ODbL)</li>
                <li>Table etl_logs - tracabilite complete</li>
                <li>Sources documentees et auditables</li>
              </ul>
            </div>
          </div>
        </div>
        <SectionTitle>Volume par source</SectionTitle>
        <div className="rounded-xl border border-border/50 bg-card p-5"><BarChartComponent data={sourceDistribution} height={220} color="#0096d6" /></div>
        <SectionTitle>Top 10 gares</SectionTitle>
        <DataTable columns={[{key:'Gare',label:'Gare'},{key:'Nb',label:'Nb',align:'right'}]} data={topStations} />
        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL</footer>
      </main>
    </div>
  )
}


