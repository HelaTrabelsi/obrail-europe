'use client'

import { useEffect, useState } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import {
  getOperateursFromAPI, getStatsFromAPI, getTrainsFromAPI, predictFromAPI,
  type Operateur, type Stats, type Train, type PredictResponse
} from '@/lib/api-client'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
const CO2_TRAIN = 14, CO2_AVION = 258, CO2_VOITURE = 193

function calcCO2(d: number) {
  return {
    train:   +(d * CO2_TRAIN   / 1000).toFixed(3),
    avion:   +(d * CO2_AVION   / 1000).toFixed(1),
    voiture: +(d * CO2_VOITURE / 1000).toFixed(1),
    eco:     +((CO2_AVION - CO2_TRAIN) / CO2_AVION * 100).toFixed(1),
    ratio:   +(CO2_AVION / CO2_TRAIN).toFixed(1),
  }
}

interface SousDesserteZone {
  gare: string
  pays: string
  operateur_principal: string
  nb_trains: number
  dist_moy_km: number
}
interface SousDesserteData {
  total_gares: number
  gares_fragiles: number
  pct_fragile: number
  zones: SousDesserteZone[]
}

// Exemples de prédictions pour les 2 enjeux
const PREDICT_EXAMPLES = [
  { label: 'Paris → Lyon',      distance_km: 392, operateur: 'SNCF',          type_service: 'Jour', type_ligne: 'national',  heure_depart: '08:30:00' },
  { label: 'Paris → Berlin',    distance_km: 878, operateur: 'Deutsche Bahn', type_service: 'Nuit', type_ligne: 'national',  heure_depart: '22:00:00' },
  { label: 'Bruxelles → Paris', distance_km: 265, operateur: 'SNCB',          type_service: 'Jour', type_ligne: 'national',  heure_depart: '07:15:00' },
]

export default function StatistiquesPage() {
  const [operators, setOperators]             = useState<Operateur[]>([])
  const [stats, setStats]                     = useState<Stats | null>(null)
  const [trains, setTrains]                   = useState<Train[]>([])
  const [sousDesserteData, setSousDesserteData] = useState<SousDesserteData | null>(null)
  const [loading, setLoading]                 = useState(true)
  const [apiStatus, setApiStatus]             = useState(false)
  const [predictResults, setPredictResults]   = useState<(PredictResponse | null)[]>([null, null, null])
  const [predictLoading, setPredictLoading]   = useState(false)

  // Simulateur
  const [distance, setDistance]   = useState(392)
  const [operateur, setOperateur] = useState('SNCF')
  const [co2, setCo2]             = useState(calcCO2(392))
  const [simResult, setSimResult] = useState<PredictResponse | null>(null)
  const [simLoading, setSimLoading] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [o, s, t, sd] = await Promise.all([
          getOperateursFromAPI(),
          getStatsFromAPI(),
          getTrainsFromAPI({ limit: 50 }),
          fetch(`${API}/stats/sous-desserte`).then(r => r.json()),
        ])
        setOperators(o); setStats(s); setTrains(t); setSousDesserteData(sd)
        setApiStatus(true)
      } catch { setApiStatus(false) }
      finally { setLoading(false) }
    }
    load()
  }, [])

  useEffect(() => {
    async function loadPredictions() {
      setPredictLoading(true)
      const results = await Promise.all(
        PREDICT_EXAMPLES.map(async ex => {
          try { return await predictFromAPI(ex) }
          catch { return null }
        })
      )
      setPredictResults(results)
      setPredictLoading(false)
    }
    loadPredictions()
  }, [])

  useEffect(() => { setCo2(calcCO2(distance)) }, [distance])

  async function handlePredict() {
    setSimLoading(true)
    try {
      const result = await predictFromAPI({
        distance_km:  distance,
        operateur,
        type_service: 'Jour',
        type_ligne:   distance > 150 ? 'national' : 'regional',
        heure_depart: '08:30:00',
      })
      setSimResult(result)
    } catch {}
    finally { setSimLoading(false) }
  }

  const heureHist = (() => {
    const h = Array(24).fill(0)
    trains.forEach(t => {
      const hr = parseInt(t.heure_depart?.split(':')[0] || '0') % 24
      if (!isNaN(hr)) h[hr]++
    })
    return h
  })()
  const maxH = Math.max(...heureHist, 1)
  const distMoyOp: Record<string, number> = {
    'Deutsche Bahn': 85.85, 'SNCB': 59.76, 'SNCF': 117.19
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

        <div className="mt-6 mb-8 pb-4 border-b border-border/40">
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
            ObRail Europe : Analyse & Statistiques : Bloc E6.2
          </p>
          <h1 className="text-2xl font-bold text-foreground">Tableau de bord analytique ferroviaire</h1>
          <p className="text-sm text-muted-foreground mt-1">
            {stats?.nb_trains.toLocaleString()} trains · SNCF · Deutsche Bahn · SNCB · Green Deal européen · Programme TEN-T
          </p>
        </div>

        {/* KPIs */}
        {stats && (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-8">
            {[
              { l: 'Trains analysés',   v: stats.nb_trains.toLocaleString(), s: 'Base GTFS 2024' },
              { l: 'Gares Europe',      v: stats.nb_gares.toLocaleString(),  s: 'FR · DE · BE' },
              { l: 'Économie CO₂',      v: '94,6 %', s: 'Vs avion ADEME 2023' },
              { l: 'Ratio avion/train', v: '18,4×',  s: 'Émissions comparées' },
              { l: 'Référence CO₂',     v: '14 g/km', s: 'ADEME 2023 UE' },
            ].map(k => (
              <div key={k.l} className="rounded-lg border border-border/50 bg-card p-4">
                <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{k.l}</p>
                <p className="text-2xl font-bold font-mono text-foreground mt-1">{k.v}</p>
                <p className="text-[11px] text-muted-foreground mt-0.5">{k.s}</p>
              </div>
            ))}
          </div>
        )}

        {/* ── SECTION 0 : 2 Modèles ML ── */}
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
          Modèles de Machine Learning — Résultats sur le jeu de test
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
          {[
            {
              n: 'Enjeu 1 — Régression CO₂',
              algo: 'Random Forest',
              metric: 'R²', val: '0,8592', bar: 86,
              color: '#00c98d',
              desc: 'Prédit les émissions CO₂ (kg) d\'un trajet ferroviaire sans data leakage',
              features: 'opérateur, service, ligne, pays, heure sin/cos, catégorie distance',
            },
            {
              n: 'Enjeu 2 — Zones sous-desservies',
              algo: 'Logistic Regression',
              metric: 'AUC', val: '0,7410', bar: 74,
              color: '#f59e0b',
              desc: 'Détecte les gares fragiles prioritaires pour le financement TEN-T',
              features: 'opérateur, pays, type ligne, heure sin/cos, CO₂/km',
            },
          ].map(m => (
            <div key={m.n} className="rounded-xl border border-border/50 bg-card p-5">
              <div className="flex items-start justify-between mb-3">
                <div>
                  <p className="text-xs font-bold text-foreground">{m.n}</p>
                  <p className="text-[11px] text-muted-foreground mt-0.5">{m.algo}</p>
                </div>
                <div className="text-right shrink-0 ml-2">
                  <p className="text-[10px] text-muted-foreground">{m.metric}</p>
                  <p className="text-xl font-bold font-mono" style={{ color: m.color }}>{m.val}</p>
                </div>
              </div>
              <div className="w-full bg-muted/20 rounded-sm h-2 mb-3">
                <div className="h-2 rounded-sm transition-all"
                  style={{ width: `${m.bar}%`, backgroundColor: m.color }} />
              </div>
              <p className="text-[11px] text-muted-foreground mb-2">{m.desc}</p>
              <p className="text-[10px] text-muted-foreground/60">Features : {m.features}</p>
            </div>
          ))}
        </div>

        {/* Note sur l'enjeu écarté */}
        <div className="rounded-lg border border-border/30 bg-muted/5 px-4 py-3 mb-8 text-[11px] text-muted-foreground">
          <span className="font-semibold text-foreground">Enjeu écarté — </span>
          La classification Jour/Nuit (substitution avion→train) a été explorée mais écartée :
          sans l'heure de départ comme feature (pour éviter le data leakage), le F1 est
          plafonné à 0,59 sur tous les algorithmes. Les données GTFS statiques ne permettent
          pas d'améliorer ce score. La sous-desserte couvre mieux la mission TEN-T d'ObRail.
        </div>

        {/* ── SECTION 0b : Prédictions live ── */}
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
          Prédictions en production — Appel API /predict en temps réel
        </h2>
        <div className="rounded-xl border border-border/50 bg-card p-5 mb-8">
          <p className="text-[11px] text-muted-foreground mb-4">
            Résultats via{' '}
            <code className="bg-muted/30 px-1 rounded text-[10px]">POST /predict</code> —
            Enjeu 1 : Random Forest (CO₂, R²=0,86) · Enjeu 2 : Logistic Regression (Sous-desserte, AUC=0,74)
          </p>
          {predictLoading ? (
            <p className="text-sm text-muted-foreground">Chargement des prédictions...</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {PREDICT_EXAMPLES.map((ex, i) => {
                const r = predictResults[i]
                return (
                  <div key={ex.label} className="rounded-lg border border-border/30 bg-muted/5 p-4">
                    <p className="text-sm font-semibold text-foreground mb-1">{ex.label}</p>
                    <p className="text-[11px] text-muted-foreground mb-3">
                      {ex.distance_km} km · {ex.operateur} · {ex.type_service}
                    </p>
                    {r ? (
                      <div className="space-y-1.5">
                        <div className="flex justify-between text-[11px]">
                          <span className="text-muted-foreground">CO₂ train prédit</span>
                          <span className="font-mono text-primary font-semibold">{r.co2_prediction_kg} kg</span>
                        </div>
                        <div className="flex justify-between text-[11px]">
                          <span className="text-muted-foreground">CO₂ avion équiv.</span>
                          <span className="font-mono text-foreground">{r.co2_avion_kg} kg</span>
                        </div>
                        <div className="flex justify-between text-[11px]">
                          <span className="text-muted-foreground">Économie</span>
                          <span className="font-mono text-primary font-semibold">{r.economie_pct} %</span>
                        </div>
                        <div className="flex justify-between text-[11px]">
                          <span className="text-muted-foreground">Ratio avion/train</span>
                          <span className="font-mono text-foreground">{r.ratio_avion_train}×</span>
                        </div>
                        <div className="flex justify-between text-[11px] pt-1 border-t border-border/20">
                          <span className="text-muted-foreground">Desserte</span>
                          <span className={`font-semibold text-[10px] px-1.5 py-0.5 rounded-full ${
                            r.sous_desserte_pred === 0
                              ? 'bg-primary/20 text-primary'
                              : 'bg-amber-500/20 text-amber-500'
                          }`}>
                            {r.sous_desserte_label}
                          </span>
                        </div>
                      </div>
                    ) : (
                      <p className="text-[11px] text-muted-foreground italic">API /predict non disponible</p>
                    )}
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {/* ── SECTION 1 : Aperçu réseau ── */}
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
          Aperçu du réseau — données réelles 2024
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-1">Départs par heure</p>
            <p className="text-[11px] text-muted-foreground mb-4">Distribution temporelle — échantillon 50 trains</p>
            <div className="flex items-end gap-px h-14" role="img" aria-label="Histogramme départs par heure">
              {heureHist.map((v, i) => (
                <div key={i} className="flex-1 rounded-sm"
                  style={{
                    height: `${(v / maxH) * 56}px`,
                    backgroundColor: (i < 6 || i >= 20) ? '#6366f1aa' : '#00c98daa',
                  }}
                  title={`${i}h : ${v} trains`}
                />
              ))}
            </div>
            <div className="flex justify-between text-[9px] text-muted-foreground mt-1">
              <span>0h</span><span>6h</span><span>12h</span><span>18h</span><span>23h</span>
            </div>
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">Répartition par opérateur</p>
            <div className="space-y-4">
              {operators.map((op, i) => {
                const pct = stats ? (op.nb_trains / stats.nb_trains * 100) : 0
                const colors = ['#0096d6', '#f59e0b', '#00c98d']
                return (
                  <div key={op.nom}>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="font-medium text-foreground">{op.nom.split(' ')[0]}</span>
                      <span className="font-mono text-muted-foreground">{op.nb_trains.toLocaleString()}</span>
                    </div>
                    <div className="w-full bg-muted/20 rounded-sm h-4">
                      <div className="h-4 rounded-sm"
                        style={{ width: `${pct}%`, backgroundColor: colors[i] }} />
                    </div>
                    <p className="text-[10px] text-muted-foreground mt-0.5">{pct.toFixed(0)} %</p>
                  </div>
                )
              })}
            </div>
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">Statistiques clés</p>
            <div className="space-y-2">
              {operators.map(op => (
                <div key={op.nom} className="py-2 border-b border-border/20 last:border-0">
                  <p className="text-xs font-semibold text-foreground mb-1">{op.nom.split(' ')[0]}</p>
                  <div className="grid grid-cols-2 gap-1 text-[11px]">
                    <span className="text-muted-foreground">Jour</span>
                    <span className="font-mono text-right">{op.nb_jour.toLocaleString()}</span>
                    <span className="text-muted-foreground">Nuit</span>
                    <span className="font-mono text-right">{op.nb_nuit.toLocaleString()}</span>
                    <span className="text-muted-foreground">% Nuit</span>
                    <span className="font-mono text-right">
                      {((op.nb_nuit / op.nb_trains) * 100).toFixed(1)} %
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* ── SECTION 2 : Simulateur CO2 ── */}
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
          Simulateur d'impact carbone — Modèle Random Forest via /predict
        </h2>
        <div className="rounded-xl border border-border/50 bg-card p-6 mb-8">
          <p className="text-sm text-muted-foreground mb-6">
            Estimez l'empreinte carbone d'un trajet et obtenez l'analyse de desserte.
            Le bouton <strong className="text-foreground">Prédire</strong> appelle le modèle
            Random Forest (R²=0,86) en production.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            <div className="md:col-span-2">
              <div className="flex justify-between mb-1">
                <label htmlFor="sim-dist"
                  className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                  Distance du trajet
                </label>
                <span className="text-sm font-mono font-semibold text-foreground">{distance} km</span>
              </div>
              <input id="sim-dist" type="range" min={1} max={2000} value={distance}
                onChange={e => { setDistance(parseInt(e.target.value)); setSimResult(null) }}
                className="w-full accent-primary" aria-label="Distance en kilomètres" />
              <div className="flex justify-between text-[10px] text-muted-foreground mt-1">
                <span>1 km</span><span>500 km</span><span>1 000 km</span><span>2 000 km</span>
              </div>
            </div>
            <div className="space-y-3">
              <div>
                <label htmlFor="sim-op"
                  className="text-xs font-semibold text-muted-foreground uppercase tracking-wider block mb-2">
                  Opérateur
                </label>
                <select id="sim-op" value={operateur}
                  onChange={e => { setOperateur(e.target.value); setSimResult(null) }}
                  className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary">
                  <option>SNCF</option>
                  <option>Deutsche Bahn</option>
                  <option>SNCB</option>
                </select>
              </div>
              <button onClick={handlePredict} disabled={simLoading}
                className="w-full flex items-center justify-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors">
                {simLoading ? 'Prédiction...' : 'Prédire via Random Forest'}
              </button>
            </div>
          </div>

          {/* Barres CO2 */}
          <div className="grid grid-cols-3 gap-3 mb-5">
            {[
              { l: 'Train (14 g/km)',     v: simResult ? simResult.co2_prediction_kg : co2.train,   c: 'text-primary',    badge: simResult ? 'ML' : null },
              { l: 'Avion (258 g/km)',    v: simResult ? simResult.co2_avion_kg      : co2.avion,   c: 'text-foreground', badge: null },
              { l: 'Voiture (193 g/km)', v: co2.voiture,                                            c: 'text-foreground', badge: null },
            ].map(item => (
              <div key={item.l} className="rounded-lg border border-border/50 bg-muted/10 p-4 text-center relative">
                {item.badge && (
                  <span className="absolute top-2 right-2 text-[9px] bg-primary/20 text-primary px-1 rounded font-bold">
                    {item.badge}
                  </span>
                )}
                <p className="text-[11px] text-muted-foreground mb-1">{item.l}</p>
                <p className={`text-2xl font-bold font-mono ${item.c}`}>{item.v}</p>
                <p className="text-[11px] text-muted-foreground mt-0.5">kg CO₂</p>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-2 gap-3 mb-5">
            <div className="rounded-lg border border-border/50 bg-muted/10 p-4 text-center">
              <p className="text-[11px] text-muted-foreground">Réduction CO₂ vs avion</p>
              <p className="text-2xl font-bold font-mono text-primary mt-1">
                {simResult ? simResult.economie_pct : co2.eco} %
              </p>
            </div>
            <div className="rounded-lg border border-border/50 bg-muted/10 p-4 text-center">
              <p className="text-[11px] text-muted-foreground">Rapport d'émissions avion/train</p>
              <p className="text-2xl font-bold font-mono text-foreground mt-1">
                {simResult ? simResult.ratio_avion_train : co2.ratio}×
              </p>
            </div>
          </div>

          {/* Résultat desserte si dispo */}
          {simResult && (
            <div className="rounded-lg border border-border/50 bg-muted/5 p-4">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-semibold text-foreground">Analyse de desserte</p>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    Modèle : {simResult.modele_desserte} — AUC=0,74
                  </p>
                </div>
                <span className={`font-bold text-sm px-3 py-1.5 rounded-full ${
                  simResult.sous_desserte_pred === 0
                    ? 'bg-primary/20 text-primary'
                    : 'bg-amber-500/20 text-amber-500'
                }`}>
                  {simResult.sous_desserte_label}
                </span>
              </div>
              <p className="text-[11px] text-muted-foreground mt-2">
                {simResult.sous_desserte_pred === 0
                  ? 'Liaison correctement desservie — pas de signalement prioritaire TEN-T'
                  : 'Liaison potentiellement fragile — candidate au financement TEN-T'}
              </p>
            </div>
          )}
        </div>

        {/* ── SECTION 3 : Zones sous-desservies ── */}
        <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground mb-4">
          Zones sous-desservies — Enjeu 2 (Logistic Regression, AUC=0,7410)
        </h2>
        <div className="rounded-xl border border-border/50 bg-card p-6 mb-8">
          <p className="text-sm text-muted-foreground mb-4">
            Une liaison est considérée fragile si elle présente ≤ 3 trains ET une distance
            moyenne comprise entre 5 et 150 km. Ces zones sont prioritaires pour les
            financements TEN-T (35,7 milliards d'euros sur 2021-2027).
          </p>
          {sousDesserteData && (
            <div className="grid grid-cols-3 gap-3 mb-5">
              {[
                { l: 'Gares analysées', v: sousDesserteData.total_gares.toLocaleString() },
                { l: 'Gares fragiles',  v: sousDesserteData.gares_fragiles.toLocaleString() },
                { l: 'Part du réseau',  v: `${sousDesserteData.pct_fragile} %` },
              ].map(k => (
                <div key={k.l} className="rounded-lg bg-muted/10 border border-border/30 p-3 text-center">
                  <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{k.l}</p>
                  <p className="text-xl font-bold font-mono text-foreground mt-1">{k.v}</p>
                </div>
              ))}
            </div>
          )}
          {sousDesserteData?.zones?.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm" role="table">
                <thead>
                  <tr className="border-b border-border/40">
                    {['Gare', 'Pays', 'Opérateur', 'Trains', 'Dist. moy.', 'Statut'].map(h => (
                      <th key={h}
                        className="pb-2 text-left text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sousDesserteData.zones.slice(0, 10).map((z, i) => (
                    <tr key={i} className="border-b border-border/20">
                      <td className="py-2 text-foreground font-medium">{z.gare}</td>
                      <td className="py-2 text-muted-foreground text-xs">{z.pays}</td>
                      <td className="py-2 text-muted-foreground text-xs">{z.operateur_principal}</td>
                      <td className="py-2 font-mono">{z.nb_trains}</td>
                      <td className="py-2 font-mono">{z.dist_moy_km} km</td>
                      <td className="py-2">
                        <span className={`px-1.5 py-0.5 rounded-full text-[10px] font-medium ${
                          z.nb_trains === 1
                            ? 'bg-red-500/15 text-red-400'
                            : 'bg-amber-500/15 text-amber-500'
                        }`}>
                          {z.nb_trains === 1 ? 'Critique' : 'Fragile'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Chargement des données...</p>
          )}
          <p className="text-[11px] text-muted-foreground mt-4 pt-3 border-t border-border/30">
            Analyse complète sur {sousDesserteData?.total_gares.toLocaleString()} gares ·
            Modèle : Logistic Regression (AUC=0,7410) · Endpoint : /stats/sous-desserte
          </p>
        </div>

        <footer className="pt-6 border-t border-border/30 text-center text-[11px] text-muted-foreground/50">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL ·
          ADEME 2023 · Green Deal UE · Programme TEN-T · Bloc E6.2 RNCP36581
        </footer>
      </main>
    </div>
  )
}