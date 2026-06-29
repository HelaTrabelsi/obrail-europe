'use client'

import React, { useState, useMemo, useEffect } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { Search, X, ChevronDown, ChevronUp, Loader2, Train as TrainIcon } from 'lucide-react'
import {
  getTrainsFromAPI, getOperateursFromAPI, getGaresFromAPI,
  type Train, type Operateur
} from '@/lib/api-client'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export default function HorairesPage() {
  const [trains, setTrains]       = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [loading, setLoading]     = useState(true)
  const [searching, setSearching] = useState(false)
  const [apiStatus, setApiStatus] = useState(false)

  const [selOp, setSelOp]   = useState('all')
  const [selSvc, setSelSvc] = useState('all')

  const [txtDep, setTxtDep]   = useState('')
  const [gasDep, setGasDep]   = useState<{id_gare:number;nom:string;pays:string}[]>([])
  const [showDep, setShowDep] = useState(false)
  const [selDep, setSelDep]   = useState('')

  const [txtArr, setTxtArr]   = useState('')
  const [gasArr, setGasArr]   = useState<{id_gare:number;nom:string;pays:string}[]>([])
  const [showArr, setShowArr] = useState(false)
  const [selArr, setSelArr]   = useState('')

  const [expandedId, setExpandedId]   = useState<number | null>(null)
  const [detailData, setDetailData]   = useState<Record<number, any>>({})
  const [loadingDetail, setLoadingDetail] = useState<number | null>(null)

  useEffect(() => {
    async function load() {
      try {
        const [t, o] = await Promise.all([
          getTrainsFromAPI({ limit: 100 }),
          getOperateursFromAPI(),
        ])
        setTrains(t); setOperators(o); setApiStatus(true)
      } catch { setApiStatus(false) }
      finally { setLoading(false) }
    }
    load()
  }, [])

  useEffect(() => {
    if (txtDep.length < 2) { setGasDep([]); return }
    const t = setTimeout(async () => {
      try { const g = await getGaresFromAPI(txtDep); setGasDep(g.slice(0, 12)); setShowDep(true) } catch {}
    }, 300)
    return () => clearTimeout(t)
  }, [txtDep])

  useEffect(() => {
    if (txtArr.length < 2) { setGasArr([]); return }
    const t = setTimeout(async () => {
      try { const g = await getGaresFromAPI(txtArr); setGasArr(g.slice(0, 12)); setShowArr(true) } catch {}
    }, 300)
    return () => clearTimeout(t)
  }, [txtArr])

  const handleSearch = async () => {
    setSearching(true)
    setExpandedId(null)
    try {
      const params: any = { limit: 200 }
      if (selOp !== 'all') params.operateur = selOp
      if (selSvc !== 'all') params.type_service = selSvc
      if (selDep) params.gare = selDep
      const t = await getTrainsFromAPI(params)
      // Filtre côté client sur gare d'arrivée
      const filtered = selArr
        ? t.filter(tr => tr.gare_arrivee?.toLowerCase().includes(selArr.toLowerCase()))
        : t
      setTrains(filtered)
    } catch {}
    finally { setSearching(false) }
  }

  const handleToggleDetail = async (train: Train) => {
    if (expandedId === train.id_train) {
      setExpandedId(null)
      return
    }
    setExpandedId(train.id_train)
    if (detailData[train.id_train]) return
    setLoadingDetail(train.id_train)
    try {
      const res = await fetch(`${API}/dessertes/${train.id_train}`)
      if (res.ok) {
        const data = await res.json()
        setDetailData(prev => ({ ...prev, [train.id_train]: data }))
      }
    } catch {}
    finally { setLoadingDetail(null) }
  }

  const stats = useMemo(() => ({
    count:    trains.length,
    avgDist:  trains.length
      ? (trains.reduce((a, t) => a + t.distance_km, 0) / trains.length).toFixed(0)
      : '0',
    nbNuit:   trains.filter(t => t.type_service === 'Nuit').length,
    totalCO2: trains.reduce((a, t) => a + t.distance_km * 14 / 1000, 0).toFixed(0),
  }), [trains])

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <p className="text-muted-foreground text-sm">Chargement...</p>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main id="main-content" className="mx-auto max-w-7xl px-4 md:px-6 pb-16">

        <div className="mt-6 mb-6 pb-4 border-b border-border/40">
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
            ObRail Europe — Horaires ferroviaires
          </p>
          <h1 className="text-2xl font-bold text-foreground">
            Recherche de <span className="text-primary">trains</span>
          </h1>
          <p className="text-sm text-muted-foreground mt-1">
            SNCF · Deutsche Bahn · SNCB · Cliquez sur une ligne pour voir le détail
          </p>
        </div>

        {/* Formulaire */}
        <div className="rounded-xl border border-border/50 bg-card p-5 mb-4">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">

            {/* Autocomplete gare départ */}
            <div className="relative">
              <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground block mb-1.5">
                Gare de départ
              </label>
              <div className="relative">
                <input value={txtDep}
                  onChange={e => { setTxtDep(e.target.value); setSelDep('') }}
                  onFocus={() => gasDep.length > 0 && setShowDep(true)}
                  onBlur={() => setTimeout(() => setShowDep(false), 150)}
                  placeholder="Ex : Paris, Berlin..."
                  className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary"
                  aria-label="Gare de départ" />
                {selDep && <button onClick={() => { setTxtDep(''); setSelDep('') }}
                  className="absolute right-2.5 top-2.5 text-muted-foreground hover:text-foreground" aria-label="Effacer">
                  <X className="h-4 w-4" /></button>}
              </div>
              {showDep && gasDep.length > 0 && (
                <div className="absolute z-50 w-full mt-1 bg-card border border-border rounded-lg shadow-lg max-h-44 overflow-y-auto">
                  {gasDep.map(g => (
                    <button key={g.id_gare} onMouseDown={() => { setTxtDep(g.nom); setSelDep(g.nom); setShowDep(false) }}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-muted/40 flex justify-between">
                      <span>{g.nom}</span><span className="text-muted-foreground text-xs">{g.pays}</span>
                    </button>
                  ))}
                </div>
              )}
              {selDep && <p className="text-xs text-primary mt-1">Départ : {selDep}</p>}
            </div>

            {/* Autocomplete gare arrivée */}
            <div className="relative">
              <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground block mb-1.5">
                Gare d'arrivée
              </label>
              <div className="relative">
                <input value={txtArr}
                  onChange={e => { setTxtArr(e.target.value); setSelArr('') }}
                  onFocus={() => gasArr.length > 0 && setShowArr(true)}
                  onBlur={() => setTimeout(() => setShowArr(false), 150)}
                  placeholder="Ex : Lyon, Hambourg..."
                  className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary"
                  aria-label="Gare d'arrivée" />
                {selArr && <button onClick={() => { setTxtArr(''); setSelArr('') }}
                  className="absolute right-2.5 top-2.5 text-muted-foreground hover:text-foreground" aria-label="Effacer">
                  <X className="h-4 w-4" /></button>}
              </div>
              {showArr && gasArr.length > 0 && (
                <div className="absolute z-50 w-full mt-1 bg-card border border-border rounded-lg shadow-lg max-h-44 overflow-y-auto">
                  {gasArr.map(g => (
                    <button key={g.id_gare} onMouseDown={() => { setTxtArr(g.nom); setSelArr(g.nom); setShowArr(false) }}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-muted/40 flex justify-between">
                      <span>{g.nom}</span><span className="text-muted-foreground text-xs">{g.pays}</span>
                    </button>
                  ))}
                </div>
              )}
              {selArr && <p className="text-xs text-primary mt-1">Arrivée : {selArr}</p>}
            </div>

            {/* Opérateur */}
            <div>
              <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground block mb-1.5">
                Opérateur
              </label>
              <select value={selOp} onChange={e => setSelOp(e.target.value)}
                className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary">
                <option value="all">Tous</option>
                {operators.map(op => <option key={op.nom} value={op.nom}>{op.nom}</option>)}
              </select>
            </div>

            {/* Service */}
            <div>
              <label className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground block mb-1.5">
                Service
              </label>
              <select value={selSvc} onChange={e => setSelSvc(e.target.value)}
                className="w-full bg-muted/20 border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary">
                <option value="all">Tous</option>
                <option value="Jour">Jour</option>
                <option value="Nuit">Nuit</option>
              </select>
            </div>
          </div>

          <div className="mt-4">
            <button onClick={handleSearch} disabled={searching}
              className="flex items-center gap-2 rounded-lg bg-primary px-6 py-2 text-sm font-semibold text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors">
              {searching
                ? <><Loader2 className="h-4 w-4 animate-spin" />Recherche...</>
                : <><Search className="h-4 w-4" />Rechercher</>
              }
            </button>
          </div>
        </div>

        {/* KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
          {[
            { l: 'Résultats',        v: stats.count.toLocaleString() },
            { l: 'Distance moyenne', v: `${stats.avgDist} km` },
            { l: 'Trains de nuit',   v: stats.nbNuit.toLocaleString() },
            { l: 'CO₂ total estimé', v: `${stats.totalCO2} kg` },
          ].map(k => (
            <div key={k.l} className="rounded-lg border border-border/50 bg-card px-4 py-3">
              <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{k.l}</p>
              <p className="text-xl font-bold font-mono text-foreground mt-0.5">{k.v}</p>
            </div>
          ))}
        </div>

        {/* Tableau */}
        {trains.length === 0 ? (
          <div className="rounded-xl border border-border/50 bg-card p-8 text-center text-muted-foreground text-sm">
            Aucun train trouvé. Modifiez vos critères de recherche.
          </div>
        ) : (
          <div className="rounded-xl border border-border/50 bg-card overflow-hidden">
            <table className="w-full text-sm" role="table">
              <thead>
                <tr className="border-b border-border/40 bg-muted/10">
                  {['Opérateur','Gare de départ','Gare d\'arrivée','H. départ','H. arrivée','Distance','CO₂ estimé','Service'].map(h => (
                    <th key={h} className="px-4 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground whitespace-nowrap">
                      {h}
                    </th>
                  ))}
                  <th className="px-3 py-3 w-8" />
                </tr>
              </thead>
              <tbody>
                {trains.slice(0, 200).map((train, i) => {
                  const isOpen    = expandedId === train.id_train
                  const detail    = detailData[train.id_train]
                  const isLoading = loadingDetail === train.id_train
                  const co2       = (train.distance_km * 14 / 1000).toFixed(3)

                  return (
                    <React.Fragment key={`train-${train.id_train}`}>
                      <tr
                        onClick={() => handleToggleDetail(train)}
                        className={`border-b border-border/20 cursor-pointer transition-colors ${
                          isOpen
                            ? 'bg-primary/5 border-primary/20'
                            : i % 2 === 0 ? 'hover:bg-muted/10' : 'bg-muted/5 hover:bg-muted/10'
                        }`}
                        role="button"
                        aria-expanded={isOpen}
                        tabIndex={0}
                        onKeyDown={e => e.key === 'Enter' && handleToggleDetail(train)}
                      >
                        <td className="px-4 py-2.5 font-medium text-foreground whitespace-nowrap">
                          {train.operateur}
                        </td>
                        <td className="px-4 py-2.5 text-foreground max-w-[160px] truncate" title={train.gare}>
                          {train.gare || '—'}
                        </td>
                        <td className="px-4 py-2.5 text-muted-foreground max-w-[160px] truncate" title={train.gare_arrivee || ''}>
                          {train.gare_arrivee || <span className="text-muted-foreground/40 text-xs italic">—</span>}
                        </td>
                        <td className="px-4 py-2.5 font-mono text-foreground whitespace-nowrap">
                          {train.heure_depart}
                        </td>
                        <td className="px-4 py-2.5 font-mono text-muted-foreground whitespace-nowrap">
                          {train.heure_arrivee}
                        </td>
                        <td className="px-4 py-2.5 font-mono text-right whitespace-nowrap">
                          {train.distance_km.toFixed(1)} km
                        </td>
                        <td className="px-4 py-2.5 font-mono text-right text-primary whitespace-nowrap">
                          {co2} kg
                        </td>
                        <td className="px-4 py-2.5">
                          <span className={`text-[10px] px-1.5 py-0.5 rounded-full font-medium ${
                            train.type_service === 'Nuit'
                              ? 'bg-indigo-500/20 text-indigo-400'
                              : 'bg-amber-500/20 text-amber-500'
                          }`}>
                            {train.type_service}
                          </span>
                        </td>
                        <td className="px-3 py-2.5">
                          {isOpen
                            ? <ChevronUp className="h-4 w-4 text-primary" aria-hidden="true" />
                            : <ChevronDown className="h-4 w-4 text-muted-foreground/40" aria-hidden="true" />
                          }
                        </td>
                      </tr>

                      {/* Panel détail */}
                      {isOpen && (
                        <tr>
                          <td colSpan={9} className="bg-primary/5 border-b border-primary/20 px-6 py-4">
                            <div className="flex items-center gap-2 mb-3">
                              <TrainIcon className="h-4 w-4 text-primary" aria-hidden="true" />
                              <p className="text-xs font-semibold text-foreground uppercase tracking-wider">
                                Détail du train #{train.id_train}
                              </p>
                            </div>
                            {isLoading ? (
                              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                                <Loader2 className="h-4 w-4 animate-spin" />
                                Chargement...
                              </div>
                            ) : (
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                                {([
                                  ['Gare de départ',      detail?.gare || train.gare],
                                  ['Gare d\'arrivée',     detail?.gare_arrivee || train.gare_arrivee || '—'],
                                  ['Heure départ',        detail?.heure_depart || train.heure_depart],
                                  ['Heure arrivée',       detail?.heure_arrivee || train.heure_arrivee],
                                  ['Distance',            `${(detail?.distance_km || train.distance_km).toFixed(1)} km`],
                                  ['CO₂ train (ADEME)',   `${((detail?.distance_km || train.distance_km) * 14 / 1000).toFixed(3)} kg`],
                                  ['CO₂ avion équivalent',`${((detail?.distance_km || train.distance_km) * 258 / 1000).toFixed(1)} kg`],
                                  ['Économie vs avion',   '94,6 %'],
                                  ['Opérateur',           detail?.operateur || train.operateur],
                                  ['Type de service',     detail?.type_service || train.type_service],
                                  ['Pays départ',         detail?.pays || train.pays],
                                  ['Source GTFS',         detail?.source_donnee || '—'],
                                ] as [string, string][]).map(([label, value]) => (
                                  <div key={label} className="rounded-lg bg-muted/20 border border-border/20 px-3 py-2">
                                    <p className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</p>
                                    <p className="text-sm font-medium text-foreground mt-0.5">{value || '—'}</p>
                                  </div>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  )
                })}
              </tbody>
            </table>

            {trains.length > 200 && (
              <div className="px-4 py-3 border-t border-border/30 text-center text-xs text-muted-foreground">
                200 premiers résultats affichés sur {trains.length}
              </div>
            )}
          </div>
        )}

        <footer className="mt-10 pt-6 border-t border-border/30 text-center text-[11px] text-muted-foreground/50">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}