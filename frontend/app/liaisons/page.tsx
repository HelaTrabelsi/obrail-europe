'use client'

import { useState, useMemo, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { KPICard, KPIGrid } from '@/components/dashboard/kpi-card'
import { SectionTitle } from '@/components/dashboard/section-title'
import { BarChartComponent } from '@/components/dashboard/charts'
import {
  getTrainsFromAPI, getOperateursFromAPI, getGaresFromAPI, getStatsFromAPI,
  type Train, type Operateur, type Stats
} from '@/lib/api-client'
import { X, Loader2 } from 'lucide-react'

// Chargement dynamique de la carte (évite SSR)
const MapComponent = dynamic(
  () => import('@/components/dashboard/MapComponent'),
  {
    ssr: false,
    loading: () => (
      <div className="h-[500px] rounded-xl bg-card border border-border/50 flex items-center justify-center text-muted-foreground text-sm">
        Chargement de la carte...
      </div>
    )
  }
)

export default function LiaisonsPage() {
  const [trains, setTrains]       = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [gares, setGares]         = useState<{ id_gare: number; nom: string; pays: string }[]>([])
  const [stats, setStats]         = useState<Stats | null>(null)
  const [loading, setLoading]     = useState(true)
  const [apiStatus, setApiStatus] = useState(false)

  // Filtres
  const [selectedOperator, setSelectedOperator] = useState('all')
  const [selectedService,  setSelectedService]  = useState('all')
  const [topN, setTopN] = useState(15)

  // Gare sélectionnée
  const [selectedStation,  setSelectedStation]  = useState<string | null>(null)
  const [stationTrains,    setStationTrains]    = useState<Train[]>([])
  const [stationLoading,   setStationLoading]   = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [t, o, g, s] = await Promise.all([
          getTrainsFromAPI({ limit: 500 }),
          getOperateursFromAPI(),
          getGaresFromAPI(),
          getStatsFromAPI(),
        ])
        setTrains(t); setOperators(o); setGares(g); setStats(s)
        setApiStatus(true)
      } catch { setApiStatus(false) }
      finally { setLoading(false) }
    }
    load()
  }, [])

  async function handleStationSelect(stationName: string) {
    setSelectedStation(stationName)
    setStationLoading(true)
    try {
      const result = await getTrainsFromAPI({ gare: stationName, limit: 100 })
      setStationTrains(result)
    } catch { setStationTrains([]) }
    finally { setStationLoading(false) }
  }

  const filteredTrains = useMemo(() => trains.filter(t => {
    if (selectedOperator !== 'all' && t.operator !== selectedOperator) return false
    if (selectedService  !== 'all' && t.type_service !== selectedService)  return false
    return true
  }), [trains, selectedOperator, selectedService])

  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    filteredTrains.forEach(t => { counts[t.gare] = (counts[t.gare] || 0) + 1 })
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, topN)
      .map(([name, value]) => ({ name, value }))
  }, [filteredTrains, topN])

  const heatmapData = useMemo(() => {
    const map = new Map<string, { Jour: number; Nuit: number }>()
    filteredTrains.forEach(t => {
      if (!map.has(t.operateur)) map.set(t.operateur, { Jour: 0, Nuit: 0 })
      const op = map.get(t.operateur)!
      if (t.type_service === 'Jour') op.Jour++; else op.Nuit++
    })
    return Array.from(map.entries()).map(([name, data]) => ({ name, ...data }))
  }, [filteredTrains])

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
          eyebrow="Réseau"
          title="Liaisons"
          titleHighlight="ferroviaires"
          subtitle="Carte interactive · Flux par opérateur · 3 pays · 3 017 gares"
        />

        {/* KPIs */}
        {stats && (
          <KPIGrid>
            <KPICard value={stats.nb_trains.toLocaleString()} label="Trains total" gradient="linear-gradient(90deg,#00c98d,#0096d6)" />
            <KPICard value={stats.nb_gares.toLocaleString()} label="Gares" gradient="linear-gradient(90deg,#6366f1,#818cf8)" />
            <KPICard value={stats.nb_operateurs.toString()} label="Opérateurs" gradient="linear-gradient(90deg,#f59e0b,#fbbf24)" />
            <KPICard value={`${stats.distance_moyenne_km?.toFixed(0) ?? '—'} km`} label="Distance moyenne" gradient="linear-gradient(90deg,#0096d6,#38bdf8)" />
            <KPICard value={stats.nb_nuit.toLocaleString()} label="Trains de nuit" gradient="linear-gradient(90deg,#6366f1,#818cf8)" />
          </KPIGrid>
        )}

        {/* Filtres */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6 mt-6">
          <div>
            <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground/60 block mb-2">
              Opérateur
            </label>
            <select
              value={selectedOperator}
              onChange={e => setSelectedOperator(e.target.value)}
              className="w-full bg-card border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary"
            >
              <option value="all">Tous</option>
              {operators.map(op => <option key={op.nom} value={op.nom}>{op.nom}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground/60 block mb-2">
              Type de service
            </label>
            <select
              value={selectedService}
              onChange={e => setSelectedService(e.target.value)}
              className="w-full bg-card border border-border/50 rounded-lg px-3 py-2 text-sm text-foreground focus:outline-none focus:border-primary"
            >
              <option value="all">Tous</option>
              <option value="Jour">Jour</option>
              <option value="Nuit">Nuit</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-bold uppercase tracking-widest text-muted-foreground/60 block mb-2">
              Top N gares : {topN}
            </label>
            <input
              type="range" min={5} max={30} step={5} value={topN}
              onChange={e => setTopN(parseInt(e.target.value))}
              className="w-full mt-2 accent-primary"
            />
          </div>
        </div>

        {/* Carte interactive */}
        <SectionTitle>Carte des gares ({gares.length} gares)</SectionTitle>
        <div className="mb-2 flex items-center gap-4 text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full bg-[#00c98d]" />France
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full bg-[#0096d6]" />Allemagne
          </span>
          <span className="flex items-center gap-1.5">
            <span className="inline-block h-2.5 w-2.5 rounded-full bg-[#f59e0b]" />Belgique
          </span>
          <span className="ml-auto">Cliquez sur une gare pour voir ses trains</span>
        </div>

        <div className="mb-6">
          <MapComponent
            stations={gares}
            selectedStation={selectedStation}
            onStationSelect={handleStationSelect}
          />
        </div>

        {/* Panel gare sélectionnée */}
        {selectedStation && (
          <div className="mb-8 rounded-xl border border-border/50 bg-card p-5">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-bold text-foreground">
                Trains au départ de <span className="text-primary">{selectedStation}</span>
              </h3>
              <button
                onClick={() => { setSelectedStation(null); setStationTrains([]) }}
                aria-label="Fermer"
                className="rounded-md p-1 text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            {stationLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Chargement des trains...
              </div>
            ) : stationTrains.length === 0 ? (
              <p className="text-sm text-muted-foreground">Aucun train trouvé pour cette gare.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-border/50 text-muted-foreground/60 text-[10px] uppercase tracking-widest">
                      <th className="pb-2 text-left">Opérateur</th>
                      <th className="pb-2 text-left">Départ</th>
                      <th className="pb-2 text-left">Arrivée</th>
                      <th className="pb-2 text-left">Type</th>
                      <th className="pb-2 text-right">Distance</th>
                      <th className="pb-2 text-right">CO2 g/km</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stationTrains.slice(0, 20).map((t, i) => (
                      <tr key={i} className="border-b border-border/30 hover:bg-muted/30 transition-colors">
                        <td className="py-2">{t.operateur}</td>
                        <td className="py-2">{t.heure_depart}</td>
                        <td className="py-2">{t.heure_arrivee}</td>
                        <td className="py-2">
                          <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${t.type_service==='Nuit'?'bg-indigo-500/20 text-indigo-400':'bg-amber-500/20 text-amber-400'}`}>
                            {t.type_service}
                          </span>
                        </td>
                        <td className="py-2 text-right">{t.distance_km} km</td>
                        <td className="py-2 text-right">{t.emission_co2_gkm ?? 14}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {stationTrains.length > 20 && (
                  <p className="mt-2 text-center text-xs text-muted-foreground">
                    + {stationTrains.length - 20} autres trains
                  </p>
                )}
              </div>
            )}
          </div>
        )}

        <footer className="mt-8 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}