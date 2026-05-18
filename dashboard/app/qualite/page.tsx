'use client'

import { useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent } from '@/components/dashboard/charts'
import { getTrains, getStats } from '@/lib/mock-data'
import { ShieldCheck, Calendar } from 'lucide-react'

export default function QualitePage() {
  const trains = getTrains()
  const stats = getStats()

  // Field completeness
  const fieldCompleteness = useMemo(() => {
    const fields = [
      'operator', 'origin_station', 'destination_station', 'type_service',
      'type_ligne', 'heure_depart', 'heure_arrivee', 'distance_km',
      'emissions_co2_gkm', 'co2_emission_kg', 'source_donnee', 'pays'
    ]
    
    return fields.map(field => {
      const nonNull = trains.filter(t => t[field as keyof typeof t] !== null && t[field as keyof typeof t] !== undefined).length
      const pct = (nonNull / trains.length) * 100
      return { name: field, value: parseFloat(pct.toFixed(1)) }
    }).sort((a, b) => a.value - b.value)
  }, [trains])

  // Missing values
  const missingValues = useMemo(() => {
    const fields = [
      'operator', 'origin_station', 'destination_station', 'type_service',
      'type_ligne', 'heure_depart', 'heure_arrivee', 'distance_km',
      'emissions_co2_gkm', 'co2_emission_kg', 'source_donnee', 'pays'
    ]
    
    return fields
      .map(field => {
        const missing = trains.filter(t => t[field as keyof typeof t] === null || t[field as keyof typeof t] === undefined).length
        return { field, missing }
      })
      .filter(f => f.missing > 0)
  }, [trains])

  // Source distribution
  const sourceDistribution = useMemo(() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => {
      counts[t.source_donnee] = (counts[t.source_donnee] || 0) + 1
    })
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([name, value]) => ({ name, value }))
  }, [trains])

  // Top stations
  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    trains.forEach(t => {
      counts[t.origin_station] = (counts[t.origin_station] || 0) + 1
    })
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([Gare, Nb]) => ({ Gare, Nb }))
  }, [trains])

  const currentDate = new Date().toLocaleDateString('fr-FR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Controle"
          title="Qualite des"
          titleHighlight="Donnees"
          subtitle="Completude, tracabilite ETL et conformite RGPD"
        />

        <KPIGrid className="grid-cols-2 lg:grid-cols-4">
          <KPICard
            value={stats.avant_doublons.toLocaleString()}
            label="Enregistrements bruts"
            gradient="linear-gradient(90deg, #0096d6, #38bdf8)"
          />
          <KPICard
            value={stats.apres_doublons.toLocaleString()}
            label="Apres nettoyage"
            gradient="linear-gradient(90deg, #00c98d, #34d399)"
          />
          <KPICard
            value={stats.doublons_supprimes.toLocaleString()}
            label="Doublons supprimes"
            gradient="linear-gradient(90deg, #f59e0b, #fbbf24)"
          />
          <KPICard
            value={stats.sans_horaires_supprimes.toLocaleString()}
            label="Sans horaires exclus"
            gradient="linear-gradient(90deg, #ef4444, #f87171)"
          />
        </KPIGrid>

        <div className="mt-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2 rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Completude par champ (%)</SectionTitle>
            <BarChartComponent
              data={fieldCompleteness}
              height={400}
              horizontal
              color="#00c98d"
            />
          </div>

          <div className="space-y-4">
            {/* Update time */}
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60 mb-2 flex items-center gap-2">
                <Calendar className="h-3 w-3" />
                Mise a jour
              </div>
              <div className="text-lg font-bold text-foreground">
                {currentDate}
              </div>
            </div>

            {/* Missing values */}
            {missingValues.length === 0 ? (
              <div className="rounded-xl border-l-2 border-primary bg-primary/5 p-4 text-sm text-primary">
                Aucune valeur manquante detectee.
              </div>
            ) : (
              <div className="rounded-xl border border-border/50 bg-card p-5">
                <SectionTitle className="mt-0 mb-4">Valeurs manquantes</SectionTitle>
                <DataTable
                  columns={[
                    { key: 'field', label: 'Champ' },
                    { key: 'missing', label: 'Nb manquants', align: 'right' },
                  ]}
                  data={missingValues}
                  maxHeight="200px"
                />
              </div>
            )}

            {/* RGPD */}
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <div className="text-[10px] font-bold uppercase tracking-[0.1em] text-primary mb-3 flex items-center gap-2">
                <ShieldCheck className="h-3 w-3" />
                Conformite RGPD
              </div>
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
        <div className="rounded-xl border border-border/50 bg-card p-5">
          <BarChartComponent
            data={sourceDistribution}
            height={220}
            color="#0096d6"
          />
        </div>

        <SectionTitle>Top 10 gares</SectionTitle>
        <DataTable
          columns={[
            { key: 'Gare', label: 'Gare' },
            { key: 'Nb', label: 'Nb', align: 'right' },
          ]}
          data={topStations}
        />

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}
