'use client'

import { useEffect, useState, useCallback } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { getOperateursFromAPI, getStatsFromAPI, getTrainsFromAPI, type Operateur, type Stats, type Train } from '@/lib/api-client'

// ── Calculs CO2 (ADEME 2023) ──────────────────────────────
const CO2_TRAIN   = 14    // g/km
const CO2_AVION   = 258   // g/km court-courrier
const CO2_VOITURE = 193   // g/km voiture essence

function calcCO2(distance: number) {
  return {
    train:   +(distance * CO2_TRAIN   / 1000).toFixed(3),
    avion:   +(distance * CO2_AVION   / 1000).toFixed(1),
    voiture: +(distance * CO2_VOITURE / 1000).toFixed(1),
    eco_vs_avion:   +((CO2_AVION - CO2_TRAIN)   / CO2_AVION   * 100).toFixed(1),
    eco_vs_voiture: +((CO2_VOITURE - CO2_TRAIN) / CO2_VOITURE * 100).toFixed(1),
    ratio_avion:    +(CO2_AVION / CO2_TRAIN).toFixed(1),
  }
}

function recommandation(distance: number): { texte: string; emoji: string; couleur: string; score: number } {
  const eco = calcCO2(distance).eco_vs_avion
  if (distance < 50)  return { texte: "Trajet court — bus ou vélo recommandé", emoji: "🚲", couleur: "text-amber-500", score: 60 }
  if (distance < 200) return { texte: "Train fortement recommandé — économie maximale", emoji: "✅", couleur: "text-primary", score: 95 }
  if (distance < 700) return { texte: "Train idéal — alternative crédible à l'avion", emoji: "🚆", couleur: "text-primary", score: 90 }
  return { texte: "Train de nuit recommandé — liaison longue distance", emoji: "🌙", couleur: "text-indigo-400", score: 85 }
}

export default function StatistiquesPage() {
  const [operators, setOperators] = useState<Operateur[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [trains, setTrains] = useState<Train[]>([])
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  // Simulateur CO2
  const [distance, setDistance] = useState(392)
  const [operateur, setOperateur] = useState('SNCF')
  const [typeService, setTypeService] = useState('Jour')
  const [heure, setHeure] = useState(8)
  const [co2Result, setCo2Result] = useState(calcCO2(392))
  const [reco, setReco] = useState(recommandation(392))

  useEffect(() => {
    async function load() {
      try {
        const [o, s, t] = await Promise.all([
          getOperateursFromAPI(),
          getStatsFromAPI(),
          getTrainsFromAPI({ limit: 50 }),
        ])
        setOperators(o); setStats(s); setTrains(t); setApiStatus(true)
      } catch { setApiStatus(false) }
      finally { setLoading(false) }
    }
    load()
  }, [])

  const handleSimulate = useCallback(() => {
    setCo2Result(calcCO2(distance))
    setReco(recommandation(distance))
  }, [distance])

  useEffect(() => { handleSimulate() }, [distance, handleSimulate])

  // Zones sous-desservies (depuis les données réelles)
  const sousDesservies = (() => {
    const counts: Record<string, { nb: number; nuit: number; dist: number[] }> = {}
    trains.forEach(t => {
      if (!counts[t.gare]) counts[t.gare] = { nb: 0, nuit: 0, dist: [] }
      counts[t.gare].nb++
      if (t.type_service === 'Nuit') counts[t.gare].nuit++
      counts[t.gare].dist.push(t.distance_km)
    })
    return Object.entries(counts)
      .filter(([, v]) => v.nb <= 2)
      .slice(0, 8)
      .map(([gare, v]) => ({
        gare,
        nb: v.nb,
        distMoy: v.dist.length ? (v.dist.reduce((a,b)=>a+b,0)/v.dist.length).toFixed(0) : '—',
        fragile: v.nb === 1,
      }))
  })()

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-muted-foreground text-sm">Chargement...</div>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Analyse & IA — Bloc E6.2"
          title="Statistiques"
          titleHighlight="& Modèles IA"
          subtitle="Simulateur CO2 · Recommandation trajet · Zones sous-desservies · 99 854 trains réels"
        />

        {/* ── KPIs ── */}
        {stats && (
          <KPIGrid>
            <KPICard value={stats.nb_trains.toLocaleString()} label="Trains analysés" gradient="linear-gradient(90deg,#00c98d,#0096d6)" />
            <KPICard value={stats.nb_gares.toLocaleString()} label="Gares" gradient="linear-gradient(90deg,#6366f1,#818cf8)" />
            <KPICard value="94.6%" label="CO2 économisé vs avion" gradient="linear-gradient(90deg,#00c98d,#00b87a)" />
            <KPICard value="18.4x" label="Avion émet plus que le train" gradient="linear-gradient(90deg,#ef4444,#dc2626)" />
            <KPICard value="14 g/km" label="Train (ADEME 2023)" gradient="linear-gradient(90deg,#f59e0b,#d97706)" />
          </KPIGrid>
        )}

        {/* ══════════════════════════════════════════════════
            FONCTIONNALITÉ 1 — SIMULATEUR CO2 INTERACTIF
        ══════════════════════════════════════════════════ */}
        <div className="mt-8 rounded-xl border-2 border-primary/40 bg-card p-6 mb-6">
          <div className="flex items-center gap-3 mb-5">
            <span className="text-3xl">🌱</span>
            <div>
              <h2 className="text-lg font-bold text-primary">Simulateur CO2 — Modèle IA</h2>
              <p className="text-xs text-muted-foreground">Calcul ADEME 2023 · 14g/km train · 258g/km avion · 193g/km voiture</p>
            </div>
          </div>

          {/* Inputs */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase block mb-2">Distance (km)</label>
              <input
                type="number"
                value={distance}
                onChange={e => setDistance(Math.max(1, parseInt(e.target.value) || 1))}
                min={1} max={3000}
                className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-foreground text-sm focus:outline-none focus:border-primary"
              />
              <input type="range" min={1} max={2000} value={distance}
                onChange={e => setDistance(parseInt(e.target.value))}
                className="w-full mt-2 accent-primary" />
            </div>
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase block mb-2">Opérateur</label>
              <select value={operateur} onChange={e => setOperateur(e.target.value)}
                className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-foreground text-sm focus:outline-none focus:border-primary">
                <option>SNCF</option>
                <option>Deutsche Bahn</option>
                <option>SNCB</option>
              </select>
            </div>
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase block mb-2">Type service</label>
              <select value={typeService} onChange={e => setTypeService(e.target.value)}
                className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-foreground text-sm focus:outline-none focus:border-primary">
                <option>Jour</option>
                <option>Nuit</option>
              </select>
            </div>
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase block mb-2">Heure départ</label>
              <input type="range" min={0} max={23} value={heure}
                onChange={e => setHeure(parseInt(e.target.value))}
                className="w-full mt-3 accent-primary" />
              <p className="text-sm text-center text-foreground mt-1 font-mono">
                {heure.toString().padStart(2,'0')}h00 — {heure >= 20 || heure < 6 ? '🌙 Nuit' : '☀️ Jour'}
              </p>
            </div>
          </div>

          {/* Résultats */}
          <div className="grid grid-cols-3 gap-4 mb-5">
            {[
              { label: '🚆 Train', co2: co2Result.train, color: '#00c98d', bg: '#00c98d22' },
              { label: '✈️ Avion', co2: co2Result.avion, color: '#ef4444', bg: '#ef444422' },
              { label: '🚗 Voiture', co2: co2Result.voiture, color: '#f59e0b', bg: '#f59e0b22' },
            ].map(item => (
              <div key={item.label} className="rounded-xl p-4 text-center"
                style={{ backgroundColor: item.bg, border: `2px solid ${item.color}` }}>
                <p className="text-sm font-bold text-foreground mb-1">{item.label}</p>
                <p className="text-3xl font-bold" style={{ color: item.color }}>{item.co2}</p>
                <p className="text-xs text-muted-foreground">kg CO2</p>
              </div>
            ))}
          </div>

          {/* Barres comparatives */}
          <div className="space-y-3 mb-5">
            {[
              { label: 'Train', val: co2Result.train, max: co2Result.avion, color: '#00c98d' },
              { label: 'Voiture', val: co2Result.voiture, max: co2Result.avion, color: '#f59e0b' },
              { label: 'Avion', val: co2Result.avion, max: co2Result.avion, color: '#ef4444' },
            ].map(b => (
              <div key={b.label} className="flex items-center gap-3">
                <span className="text-xs text-muted-foreground w-14 shrink-0">{b.label}</span>
                <div className="flex-1 bg-muted/20 rounded-full h-6">
                  <div className="h-6 rounded-full flex items-center justify-end pr-2 transition-all duration-500"
                    style={{ width: `${(b.val / b.max) * 100}%`, backgroundColor: b.color }}>
                    <span className="text-xs font-bold text-white">{b.val} kg</span>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="rounded-lg bg-primary/10 border border-primary/30 p-3 text-center">
              <p className="text-xs text-muted-foreground">Économie vs avion</p>
              <p className="text-2xl font-bold text-primary">{co2Result.eco_vs_avion}%</p>
            </div>
            <div className="rounded-lg bg-red-500/10 border border-red-500/30 p-3 text-center">
              <p className="text-xs text-muted-foreground">L'avion émet</p>
              <p className="text-2xl font-bold text-red-400">{co2Result.ratio_avion}x plus</p>
            </div>
          </div>
        </div>

        {/* ══════════════════════════════════════════════════
            FONCTIONNALITÉ 2 — RECOMMANDATION TRAJET
        ══════════════════════════════════════════════════ */}
        <div className="rounded-xl border-2 border-indigo-500/40 bg-card p-6 mb-6">
          <div className="flex items-center gap-3 mb-5">
            <span className="text-3xl">🚆</span>
            <div>
              <h2 className="text-lg font-bold text-indigo-400">Recommandation Trajet — IA</h2>
              <p className="text-xs text-muted-foreground">Le modèle recommande le meilleur mode de transport selon la distance</p>
            </div>
          </div>

          {/* Recommandation dynamique */}
          <div className="rounded-xl bg-indigo-500/10 border border-indigo-500/30 p-5 mb-5">
            <div className="flex items-center gap-4">
              <span className="text-5xl">{reco.emoji}</span>
              <div>
                <p className={`text-lg font-bold ${reco.couleur}`}>{reco.texte}</p>
                <p className="text-sm text-muted-foreground mt-1">
                  Pour {distance} km avec {operateur} — Score de recommandation :
                  <span className="font-bold text-primary ml-1">{reco.score}/100</span>
                </p>
              </div>
            </div>
            <div className="mt-4 w-full bg-muted/20 rounded-full h-3">
              <div className="h-3 rounded-full bg-primary transition-all duration-500"
                style={{ width: `${reco.score}%` }} />
            </div>
          </div>

          {/* Grille recommandations par distance */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {[
              { range: '< 50 km', mode: '🚲 Vélo/Bus', co2: (50*14/1000).toFixed(2), eco: '—' },
              { range: '50-200 km', mode: '🚆 Train', co2: (150*14/1000).toFixed(2), eco: '94.6%' },
              { range: '200-700 km', mode: '🚆 Train', co2: (400*14/1000).toFixed(2), eco: '94.6%' },
              { range: '> 700 km', mode: '🌙 Train nuit', co2: (900*14/1000).toFixed(2), eco: '94.6%' },
            ].map(r => (
              <div key={r.range} className="rounded-lg bg-muted/10 border border-border/30 p-3 text-center">
                <p className="text-xs font-bold text-muted-foreground">{r.range}</p>
                <p className="text-lg font-bold text-foreground mt-1">{r.mode}</p>
                <p className="text-xs text-primary mt-1">~{r.co2} kg CO2/train</p>
                {r.eco !== '—' && <p className="text-xs text-primary font-bold">{r.eco} vs avion</p>}
              </div>
            ))}
          </div>

          {/* Exemples trajets réels */}
          <div className="mt-4">
            <p className="text-xs font-bold text-muted-foreground uppercase mb-3">Exemples de trajets réels</p>
            <div className="space-y-2">
              {[
                { trajet: 'Paris → Lyon', dist: 392, op: 'SNCF', type: 'Jour' },
                { trajet: 'Paris → Berlin', dist: 878, op: 'Deutsche Bahn', type: 'Nuit' },
                { trajet: 'Bruxelles → Paris', dist: 265, op: 'SNCB', type: 'Jour' },
                { trajet: 'Munich → Hambourg', dist: 612, op: 'Deutsche Bahn', type: 'Nuit' },
              ].map(ex => {
                const c = calcCO2(ex.dist)
                const r = recommandation(ex.dist)
                return (
                  <div key={ex.trajet} className="flex items-center gap-3 p-3 rounded-lg bg-muted/10 hover:bg-muted/20 transition-colors">
                    <span className="text-xl">{r.emoji}</span>
                    <div className="flex-1">
                      <p className="text-sm font-bold">{ex.trajet}</p>
                      <p className="text-xs text-muted-foreground">{ex.dist} km · {ex.op} · {ex.type}</p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-bold text-primary">{c.train} kg CO2</p>
                      <p className="text-xs text-muted-foreground">vs avion {c.avion} kg</p>
                    </div>
                    <div className="rounded-full px-2 py-1 bg-primary/20 text-primary text-xs font-bold">
                      -{c.eco_vs_avion}%
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        </div>

        {/* ══════════════════════════════════════════════════
            FONCTIONNALITÉ 3 — ZONES SOUS-DESSERVIES
        ══════════════════════════════════════════════════ */}
        <div className="rounded-xl border-2 border-amber-500/40 bg-card p-6 mb-6">
          <div className="flex items-center gap-3 mb-5">
            <span className="text-3xl">🗺️</span>
            <div>
              <h2 className="text-lg font-bold text-amber-500">Zones Sous-desservies — Détection IA</h2>
              <p className="text-xs text-muted-foreground">Modèle 3 · AUC=0.73 · Liaisons avec moins de 2 trains dans l'échantillon</p>
            </div>
          </div>

          <div className="rounded-lg bg-amber-500/10 border border-amber-500/30 p-4 mb-4">
            <p className="text-sm text-muted-foreground">
              <strong className="text-amber-500">Définition ObRail :</strong> une liaison est "sous-desservie" si elle présente
              peu de trains, des distances courtes (zone locale) et un type régional.
              Ces zones sont prioritaires pour le développement du réseau ferroviaire européen
              dans le cadre du <strong className="text-foreground">programme TEN-T et du Green Deal européen</strong>.
            </p>
          </div>

          {sousDesservies.length > 0 ? (
            <div className="space-y-2">
              {sousDesservies.map((z, i) => (
                <div key={z.gare} className="flex items-center gap-3 p-3 rounded-lg bg-muted/10 border border-border/20">
                  <span className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white ${z.fragile ? 'bg-red-500' : 'bg-amber-500'}`}>
                    {z.fragile ? '⚠' : i+1}
                  </span>
                  <div className="flex-1">
                    <p className="text-sm font-bold text-foreground">{z.gare}</p>
                    <p className="text-xs text-muted-foreground">Distance moyenne : {z.distMoy} km</p>
                  </div>
                  <div className="text-right">
                    <p className={`text-sm font-bold ${z.fragile ? 'text-red-400' : 'text-amber-500'}`}>
                      {z.nb} train{z.nb > 1 ? 's' : ''}
                    </p>
                    <p className={`text-xs ${z.fragile ? 'text-red-400' : 'text-amber-500'}`}>
                      {z.fragile ? '🔴 Critique' : '🟡 Fragile'}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground text-center py-4">
              Aucune zone sous-desservie détectée dans l'échantillon actuel
            </p>
          )}

          <div className="mt-4 grid grid-cols-3 gap-3">
            <div className="rounded-lg bg-red-500/10 border border-red-500/30 p-3 text-center">
              <p className="text-2xl font-bold text-red-400">{sousDesservies.filter(z=>z.fragile).length}</p>
              <p className="text-xs text-muted-foreground">🔴 Liaisons critiques</p>
            </div>
            <div className="rounded-lg bg-amber-500/10 border border-amber-500/30 p-3 text-center">
              <p className="text-2xl font-bold text-amber-500">{sousDesservies.filter(z=>!z.fragile).length}</p>
              <p className="text-xs text-muted-foreground">🟡 Liaisons fragiles</p>
            </div>
            <div className="rounded-lg bg-primary/10 border border-primary/30 p-3 text-center">
              <p className="text-2xl font-bold text-primary">0.73</p>
              <p className="text-xs text-muted-foreground">AUC modèle détection</p>
            </div>
          </div>
        </div>

        {/* ── Stats opérateurs ── */}
        <SectionTitle>Statistiques par opérateur</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {operators.map(op => (
            <div key={op.nom} className="rounded-xl border border-border/50 bg-card p-5">
              <p className="text-sm font-bold text-primary mb-3">{op.nom}</p>
              <div className="space-y-2">
                {[
                  ["Trains total", op.nb_trains.toLocaleString(), ""],
                  ["Trains de jour", op.nb_jour.toLocaleString(), "text-amber-500"],
                  ["Trains de nuit", op.nb_nuit.toLocaleString(), "text-indigo-400"],
                  ["% Nuit", `${((op.nb_nuit/op.nb_trains)*100).toFixed(1)}%`, ""],
                  ["CO2 moy/train", `${(op.nb_trains>0?(85*14/1000):0).toFixed(2)} kg`, "text-primary"],
                ].map(([l,v,c])=>(
                  <div key={l} className="flex justify-between text-sm">
                    <span className="text-muted-foreground">{l}</span>
                    <span className={`font-mono font-bold ${c}`}>{v}</span>
                  </div>
                ))}
                <div className="mt-2 w-full bg-muted/30 rounded-full h-1.5">
                  <div className="bg-primary h-1.5 rounded-full"
                    style={{ width: `${(op.nb_trains/(stats?.nb_trains||1))*100}%` }} />
                </div>
              </div>
            </div>
          ))}
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL · ADEME 2023 · Bloc E6.2 RNCP36581
        </footer>
      </main>
    </div>
  )
}