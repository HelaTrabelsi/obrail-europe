'use client'

import { useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { getStatsQualiteFromAPI, type StatsQualite } from '@/lib/api-client'

export default function QualitePage() {
  const [qualite, setQualite] = useState<StatsQualite | null>(null)
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const q = await getStatsQualiteFromAPI()
        setQualite(q)
        setApiStatus(true)
      } catch (e) {
        setApiStatus(false)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-muted-foreground text-sm">Chargement qualité...</div>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Données"
          title="Qualité"
          titleHighlight="& RGPD"
          subtitle="Complétude des données · Traçabilité ETL · Conformité réglementaire"
        />

        <KPIGrid>
          <KPICard value="186 902" label="Enregistrements bruts" gradient="linear-gradient(90deg, #0096d6, #38bdf8)" />
          <KPICard value="59 162" label="Doublons supprimés" gradient="linear-gradient(90deg, #ef4444, #dc2626)" />
          <KPICard value={qualite?.nb_trains_total.toLocaleString() ?? '99 854'} label="Trains chargés" gradient="linear-gradient(90deg, #00c98d, #0096d6)" />
          <KPICard value={`${qualite?.completude_co2_pct?.toFixed(1) ?? '100'}%`} label="Complétude CO2" gradient="linear-gradient(90deg, #f59e0b, #fbbf24)" />
        </KPIGrid>

        {/* Complétude par champ */}
        <div className="mt-8 rounded-xl border border-border/50 bg-card p-6">
          <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">Complétude des champs</h3>
          <div className="space-y-3">
            {[
              { label: 'operateur_nom', pct: 100 },
              { label: 'gare_nom', pct: 100 },
              { label: 'heure_depart', pct: 100 },
              { label: 'heure_arrivee', pct: 100 },
              { label: 'distance_km', pct: 100 },
              { label: 'type_service (Jour/Nuit)', pct: 100 },
              { label: 'emission_co2_gkm', pct: qualite?.completude_co2_pct ?? 100 },
            ].map(field => (
              <div key={field.label} className="flex items-center gap-4">
                <span className="text-sm font-mono text-muted-foreground w-52 shrink-0">{field.label}</span>
                <div className="flex-1 bg-muted/20 rounded-full h-5">
                  <div
                    className="h-5 rounded-full flex items-center justify-end pr-2"
                    style={{
                      width: `${field.pct}%`,
                      backgroundColor: field.pct === 100 ? '#00c98d' : '#f59e0b'
                    }}
                  >
                    <span className="text-xs font-bold text-white">{field.pct.toFixed(0)}%</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* ETL Logs */}
        {qualite?.etl_logs && qualite.etl_logs.length > 0 && (
          <div className="mt-6 rounded-xl border border-border/50 bg-card p-6">
            <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">Derniers logs ETL</h3>
            <div className="space-y-2">
              {qualite.etl_logs.slice(0, 5).map((log, i) => (
                <div key={i} className="flex items-center gap-3 text-sm p-2 rounded bg-muted/10">
                  <span className={`w-2 h-2 rounded-full shrink-0 ${log.statut === 'success' ? 'bg-primary' : 'bg-red-500'}`} />
                  <span className="text-muted-foreground w-24 shrink-0">{log.etape}</span>
                  <span className="text-foreground flex-1">{log.source}</span>
                  <span className="font-mono text-xs">{log.nb_enregistrements?.toLocaleString()}</span>
                  <span className="text-xs text-muted-foreground">{log.run_date?.slice(0, 10)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* RGPD */}
        <div className="mt-6 rounded-xl border border-primary/30 bg-primary/5 p-6">
          <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">Conformité RGPD</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {[
              ['✅ Aucune donnée personnelle', 'Horaires, gares, opérateurs uniquement'],
              ['✅ Licences respectées', 'ODbL · Open License v2 · Creative Commons BY'],
              ['✅ API read-only', 'GET uniquement — impossible de modifier les données'],
              ['✅ Traçabilité ETL', 'Table etl_logs — audit complet de chaque chargement'],
              ['✅ Credentials sécurisés', 'Variables .env non versionnées dans .gitignore'],
              ['✅ Open Data', 'Données publiques GTFS — réutilisation libre sous licence'],
            ].map(([titre, desc]) => (
              <div key={titre} className="flex gap-3">
                <div>
                  <p className="text-sm font-bold text-foreground">{titre}</p>
                  <p className="text-xs text-muted-foreground">{desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Volume par source */}
        {qualite?.par_source && qualite.par_source.length > 0 && (
          <div className="mt-6 rounded-xl border border-border/50 bg-card p-6">
            <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">Volume par source</h3>
            <div className="space-y-3">
              {qualite.par_source.map(src => (
                <div key={src.source_donnee} className="flex items-center gap-4">
                  <span className="text-sm text-muted-foreground w-40 shrink-0">{src.source_donnee}</span>
                  <div className="flex-1 bg-muted/20 rounded-full h-5">
                    <div
                      className="h-5 rounded-full bg-primary/70 flex items-center justify-end pr-2"
                      style={{ width: `${(src.nb / (qualite.nb_trains_total || 1)) * 100}%` }}
                    >
                      <span className="text-xs font-bold text-white">{src.nb.toLocaleString()}</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}