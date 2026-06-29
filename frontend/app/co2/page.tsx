'use client'

import { useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { getStatsCO2FromAPI, getStatsFromAPI, type StatsCO2, type Stats } from '@/lib/api-client'

export default function CO2Page() {
  const [co2Stats, setCo2Stats] = useState<StatsCO2[]>([])
  const [stats, setStats]       = useState<Stats | null>(null)
  const [loading, setLoading]   = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [c, s] = await Promise.all([getStatsCO2FromAPI(), getStatsFromAPI()])
        setCo2Stats(c); setStats(s); setApiStatus(true)
      } catch { setApiStatus(false) }
      finally { setLoading(false) }
    }
    load()
  }, [])

  const totalTrain = co2Stats.reduce((a, o) => a + o.co2_total_kg, 0)
  const totalAvion = totalTrain * (258 / 14)
  const economie   = totalAvion - totalTrain

  const modes = [
    { label: 'Train électrique France', value: 6,   note: 'TGV, Intercités électrique' },
    { label: 'Train moyen UE',          value: 14,  note: 'Référence ADEME 2023 — réseau mixte' },
    { label: 'Car / Autocar',           value: 27,  note: 'Longues distances' },
    { label: 'Voiture essence',         value: 193, note: 'Taux de remplissage 1.5 pers.' },
    { label: 'Avion long-courrier',     value: 195, note: 'Plus de 3 500 km' },
    { label: 'Avion court-courrier',    value: 258, note: 'Moins de 1 000 km — intra-européen' },
  ]

  const distMoyOp: Record<string, number> = {
    'Deutsche Bahn': 85.85, 'SNCF': 117.19, 'SNCB': 59.76
  }

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <p className="text-muted-foreground text-sm">Chargement...</p>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main id="main-content" className="mx-auto max-w-7xl px-4 md:px-6 pb-16">

        {/* En-tête */}
        <div className="mt-6 mb-8 pb-4 border-b border-border/40">
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
            ObRail Europe : Impact environnemental
          </p>
          <h1 className="text-2xl font-bold text-foreground">
            Émissions CO₂ du réseau ferroviaire européen
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            Données réelles · SNCF · Deutsche Bahn · SNCB · Facteurs ADEME 2023
          </p>
        </div>

        {/* KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-8">
          {[
            { l: 'Facteur train',    v: '14 g/km',  s: 'ADEME 2023 moyenne UE' },
            { l: 'Facteur avion',    v: '258 g/km', s: 'Court-courrier intra-EU' },
            { l: 'Ratio avion/train',v: '18,4×',    s: 'L\'avion émet davantage' },
            { l: 'Économie CO₂',     v: '94,6 %',   s: 'Vs avion même trajet' },
            { l: 'CO₂ évité',        v: `${(economie/1000).toFixed(0)} t`, s: 'Sur l\'ensemble du réseau' },
          ].map(k => (
            <div key={k.l} className="rounded-lg border border-border/50 bg-card p-4">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{k.l}</p>
              <p className="text-2xl font-bold font-mono text-foreground mt-1">{k.v}</p>
              <p className="text-[11px] text-muted-foreground mt-0.5">{k.s}</p>
            </div>
          ))}
        </div>

        {/* Comparatif modal */}
        <div className="mb-8">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
            Émissions par mode de transport : g CO₂ / passager-km (ADEME 2023)
          </h2>
          <div className="rounded-xl border border-border/50 bg-card p-6">
            <div className="space-y-4">
              {modes.map(m => (
                <div key={m.label}>
                  <div className="flex justify-between text-sm mb-1.5">
                    <span className={m.value === 14 ? 'font-semibold text-foreground' : 'text-muted-foreground'}>
                      {m.label}
                    </span>
                    <span className="font-mono text-foreground">{m.value} g/km</span>
                  </div>
                  <div className="w-full bg-muted/20 rounded-sm h-5">
                    <div
                      className="h-5 rounded-sm"
                      style={{
                        width: `${(m.value / 258) * 100}%`,
                        backgroundColor: m.value <= 14 ? '#00c98d' :
                                         m.value <= 30 ? '#6366f1' :
                                         m.value <= 200 ? '#f59e0b' : '#ef4444'
                      }}
                    />
                  </div>
                  <p className="text-[10px] text-muted-foreground mt-0.5">{m.note}</p>
                </div>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground mt-5 pt-4 border-t border-border/30">
              Source : ADEME 2023 : Base Carbone officielle. Valeurs par passager-kilomètre, taux de remplissage moyen.
            </p>
          </div>
        </div>

        {/* CO2 par opérateur */}
        <div className="mb-8">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
            Bilan CO₂ par opérateur : données extraites de la base ObRail
          </h2>
          <div className="rounded-xl border border-border/50 bg-card overflow-hidden">
            <table className="w-full text-sm" role="table">
              <thead>
                <tr className="border-b border-border/40 bg-muted/10">
                  {['Opérateur','Facteur CO₂','Trains analysés','Distance moy.','CO₂ moy./train','CO₂ total','Part réseau'].map(h => (
                    <th key={h} className="px-5 py-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {co2Stats.map((op, i) => {
                  const dist   = distMoyOp[op.operateur] || 86
                  const pct    = totalTrain > 0 ? (op.co2_total_kg / totalTrain * 100).toFixed(1) : '0'
                  const trains = i === 0 ? 58555 : i === 1 ? 19929 : 21370
                  return (
                    <tr key={op.operateur} className={`border-b border-border/20 ${i%2===0?'':'bg-muted/5'}`}>
                      <td className="px-5 py-3 font-semibold text-foreground">{op.operateur}</td>
                      <td className="px-5 py-3 font-mono">{op.co2_moy_gkm?.toFixed(0) ?? 14} g/km</td>
                      <td className="px-5 py-3 font-mono">{trains.toLocaleString()}</td>
                      <td className="px-5 py-3 font-mono">{dist.toFixed(0)} km</td>
                      <td className="px-5 py-3 font-mono text-primary font-semibold">{(dist*14/1000).toFixed(2)} kg</td>
                      <td className="px-5 py-3 font-mono font-semibold">{(op.co2_total_kg/1000).toFixed(0)} t</td>
                      <td className="px-5 py-3 font-mono">{pct} %</td>
                    </tr>
                  )
                })}
                <tr className="bg-muted/10 border-t border-border/40 font-semibold">
                  <td className="px-5 py-3">Total réseau</td>
                  <td className="px-5 py-3 font-mono">14 g/km</td>
                  <td className="px-5 py-3 font-mono">99 854</td>
                  <td className="px-5 py-3 font-mono">86 km</td>
                  <td className="px-5 py-3 font-mono text-primary">{(86*14/1000).toFixed(2)} kg</td>
                  <td className="px-5 py-3 font-mono">{(totalTrain/1000).toFixed(0)} t</td>
                  <td className="px-5 py-3 font-mono">100 %</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        {/* Bilan global */}
        <div className="mb-8">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
            Bilan carbone global : réseau ObRail 2024
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="rounded-xl border border-border/50 bg-card p-6">
              <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">Synthèse CO₂</p>
              <div className="space-y-3">
                {[
                  { l: 'Distance totale parcourue',   v: `${(8639424/1000000).toFixed(2)} millions de km` },
                  { l: 'CO₂ émis — réseau ferroviaire', v: `${(totalTrain/1000).toFixed(0)} tonnes` },
                  { l: 'CO₂ équivalent — réseau aérien', v: `${(totalAvion/1000).toFixed(0)} tonnes` },
                  { l: 'CO₂ économisé grâce au train', v: `${(economie/1000).toFixed(0)} tonnes` },
                  { l: 'Taux de réduction',            v: '94,6 %' },
                ].map(r => (
                  <div key={r.l} className="flex justify-between py-2 border-b border-border/20 last:border-0">
                    <span className="text-sm text-muted-foreground">{r.l}</span>
                    <span className="text-sm font-mono font-semibold text-foreground">{r.v}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-xl border border-border/50 bg-card p-6">
              <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">Équivalences</p>
              <div className="space-y-3">
                {[
                  { l: 'Arbres nécessaires pour absorber',  v: `${Math.round(economie/1000/0.022).toLocaleString()} arbres` },
                  { l: 'Vols Paris–New York évités',        v: `${Math.round(economie/1000/0.5).toLocaleString()} vols` },
                  { l: 'Kilomètres voiture évités',         v: `${Math.round(economie/0.193/1000000).toFixed(0)} M km` },
                  { l: 'Foyers français (énergie annuelle)', v: `${Math.round(economie/1000/5).toLocaleString()} foyers` },
                ].map(r => (
                  <div key={r.l} className="flex justify-between py-2 border-b border-border/20 last:border-0">
                    <span className="text-sm text-muted-foreground">{r.l}</span>
                    <span className="text-sm font-mono font-semibold text-foreground">{r.v}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Tableau liaisons */}
        <div className="mb-8">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
            Liaisons intra-européennes : comparatif CO₂ train vs avion
          </h2>
          <div className="rounded-xl border border-border/50 bg-card overflow-hidden">
            <table className="w-full text-sm" role="table">
              <thead>
                <tr className="border-b border-border/40 bg-muted/10">
                  {['Liaison','Distance','CO₂ Train','CO₂ Avion','Réduction','Recommandation'].map(h => (
                    <th key={h} className="px-5 py-3 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[
                  {l:'Paris → Lyon',       d:392,  r:'Alternative crédible — gain de temps porte-à-porte'},
                  {l:'Paris → Berlin',     d:878,  r:'Train de nuit recommandé (Nightjet)'},
                  {l:'Bruxelles → Paris',  d:265,  r:'Train idéal — Thalys / Eurostar'},
                  {l:'Munich → Hambourg',  d:612,  r:'Train de nuit ICE / CNL'},
                  {l:'Lyon → Grenoble',    d:45,   r:'Distance courte — modes doux prioritaires'},
                  {l:'Cologne → Paris',    d:450,  r:'Thalys — alternative directe à l\'avion'},
                  {l:'Bruxelles → Vienne', d:1143, r:'Train de nuit — ÖBB Nightjet'},
                ].map((r, i) => {
                  const co2T = +(r.d * 14  / 1000).toFixed(3)
                  const co2A = +(r.d * 258 / 1000).toFixed(1)
                  const eco  = +((co2A - co2T) / co2A * 100).toFixed(1)
                  return (
                    <tr key={i} className={`border-b border-border/20 hover:bg-muted/5 ${i%2===0?'':'bg-muted/5'}`}>
                      <td className="px-5 py-3 font-medium text-foreground">{r.l}</td>
                      <td className="px-5 py-3 font-mono">{r.d} km</td>
                      <td className="px-5 py-3 font-mono font-semibold text-primary">{co2T} kg</td>
                      <td className="px-5 py-3 font-mono text-muted-foreground">{co2A} kg</td>
                      <td className="px-5 py-3 font-mono font-semibold text-primary">−{eco} %</td>
                      <td className="px-5 py-3 text-muted-foreground text-xs">{r.r}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>

        <footer className="pt-6 border-t border-border/30 text-center text-[11px] text-muted-foreground/50">
          ObRail Europe · ADEME 2023 Base Carbone · Facteur train 14 g CO₂/km · Facteur avion 258 g CO₂/km · Green Deal UE · Licence ODbL
        </footer>
      </main>
    </div>
  )
}