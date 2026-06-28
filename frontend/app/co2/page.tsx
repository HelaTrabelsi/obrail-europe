'use client'

import { useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { getStatsCO2FromAPI, getStatsFromAPI, type StatsCO2, type Stats } from '@/lib/api-client'

export default function CO2Page() {
  const [co2Stats, setCo2Stats] = useState<StatsCO2[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [c, s] = await Promise.all([getStatsCO2FromAPI(), getStatsFromAPI()])
        setCo2Stats(c)
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

  const totalCO2Train = co2Stats.reduce((acc, op) => acc + op.co2_total_kg, 0)
  const totalCO2Avion = totalCO2Train * (258 / 14)
  const economie = totalCO2Avion - totalCO2Train

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-muted-foreground text-sm">Chargement CO2...</div>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Impact environnemental"
          title="Émissions"
          titleHighlight="CO2"
          subtitle="Comparatif train vs avion — Facteurs ADEME 2023"
        />

        <KPIGrid>
          <KPICard value="14 g/km" label="Train (ADEME 2023)" gradient="linear-gradient(90deg, #00c98d, #00b87a)" />
          <KPICard value="258 g/km" label="Avion court-courrier" gradient="linear-gradient(90deg, #ef4444, #dc2626)" />
          <KPICard value="18.4x" label="Avion émet plus" gradient="linear-gradient(90deg, #f59e0b, #d97706)" />
          <KPICard value="94.6%" label="CO2 économisé vs avion" gradient="linear-gradient(90deg, #00c98d, #0096d6)" />
          <KPICard
            value={`${(economie / 1000).toFixed(0)} t`}
            label="CO2 évité total"
            gradient="linear-gradient(90deg, #6366f1, #818cf8)"
          />
        </KPIGrid>

        {/* Comparatif modal */}
        <div className="mt-8 rounded-xl border border-border/50 bg-card p-6">
          <h3 className="text-sm font-bold text-primary uppercase tracking-wide mb-4">Comparatif modal — g CO2 / passager-km</h3>
          <div className="space-y-3">
            {[
              { label: 'Train électrique France', value: 6, color: '#00c98d' },
              { label: 'Train moyen UE (notre référence)', value: 14, color: '#00b87a' },
              { label: 'Avion long-courrier', value: 195, color: '#f59e0b' },
              { label: 'Avion court-courrier', value: 258, color: '#ef4444' },
            ].map(item => (
              <div key={item.label} className="flex items-center gap-4">
                <span className="text-sm text-muted-foreground w-56 shrink-0">{item.label}</span>
                <div className="flex-1 bg-muted/20 rounded-full h-6 relative">
                  <div
                    className="h-6 rounded-full flex items-center justify-end pr-2"
                    style={{ width: `${(item.value / 258) * 100}%`, backgroundColor: item.color }}
                  >
                    <span className="text-xs font-bold text-white">{item.value}g</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* CO2 par opérateur */}
        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
          {co2Stats.map(op => (
            <div key={op.operateur} className="rounded-xl border border-border/50 bg-card p-5">
              <p className="text-sm font-bold text-primary mb-3">{op.operateur}</p>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">CO2 moyen/km</span>
                  <span className="font-mono">{op.co2_moy_gkm?.toFixed(1) ?? 14} g/km</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">CO2 total</span>
                  <span className="font-mono font-bold">{(op.co2_total_kg / 1000).toFixed(0)} t</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-muted-foreground">vs avion</span>
                  <span className="font-mono text-primary">-94.6%</span>
                </div>
              </div>
            </div>
          ))}
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · ADEME 2023 · 14g CO2/km train · 258g CO2/km avion
        </footer>
      </main>
    </div>
  )
}