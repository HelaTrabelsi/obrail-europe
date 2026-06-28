'use client'

import { useState, useMemo, useEffect } from 'react'
import dynamic from 'next/dynamic'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { SectionTitle } from '@/components/dashboard/section-title'
import { BarChartComponent, ScatterPlot } from '@/components/dashboard/charts'
import {
  getTrainsFromAPI, getOperateursFromAPI, getGaresFromAPI,
  type Train, type Operateur
} from '@/lib/api-client'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Slider } from '@/components/ui/slider'
import { X, Loader2 } from 'lucide-react'

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
  const [trains, setTrains] = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [gares, setGares] = useState<{ id_gare: number; nom: string; pays: string }[]>([])
  const [loading, setLoading] = useState(true)
  const [apiStatus, setApiStatus] = useState(false)
  const [selectedOperator, setSelectedOperator] = useState('all')
  const [selectedService, setSelectedService] = useState('all')
  const [topN, setTopN] = useState(20)
  const [selectedStation, setSelectedStation] = useState<string | null>(null)
  const [stationTrains, setStationTrains] = useState<Train[]>([])
  const [stationLoading, setStationLoading] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const [t, o, g] = await Promise.all([
          getTrainsFromAPI({ limit: 500 }),
          getOperateursFromAPI(),
          getGaresFromAPI(),
        ])
        setTrains(t)
        setOperators(o)
        setGares(g)
        setApiStatus(true)
      } catch (e) {
        setApiStatus(false)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  async function handleStationSelect(stationName: string) {
    setSelectedStation(stationName)
    setStationLoading(true)
    try {
      const result = await getTrainsFromAPI({ gare: stationName, limit: 100 })
      setStationTrains(result)
    } catch {
      setStationTrains([])
    } finally {
      setStationLoading(false)
    }
  }

  function handleClosePanel() {
    setSelectedStation(null)
    setStationTrains([])
  }

  const filteredTrains = useMemo(() => trains.filter(t => {
    if (selectedOperator !== 'all' && t.operator !== selectedOperator) return false
    if (selectedService !== 'all' && t.type_service !== selectedService) return false
    return true
  }), [trains, selectedOperator, selectedService])

  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    filteredTrains.forEach(t => { counts[t.gare] = (counts[t.gare] || 0) + 1 })
    return Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, topN).map(([name, value]) => ({ name, value }))
  }, [filteredTrains, topN])

  const scatterData = useMemo(() => filteredTrains.slice(0, 500).map(t => ({
    x: parseInt(t.heure_depart?.split(':')[0] || '0') % 24,
    y: t.distance_km,
    category: t.type_service,
  })), [filteredTrains])

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
        <PageHeader eyebrow="Flux" title="Directions &" titleHighlight="Liaisons" subtitle="Analyse des flux ferroviaires par operateur et type" />

        {/* Filtres */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">Operateur</Label>
            <Select value={selectedOperator} onValueChange={setSelectedOperator}>
              <SelectTrigger className="bg-card border-border/50"><SelectValue placeholder="Tous" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                {operators.map(op => <SelectItem key={op.nom} value={op.nom}>{op.nom}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">Type service</Label>
            <Select value={selectedService} onValueChange={setSelectedService}>
              <SelectTrigger className="bg-card border-border/50"><SelectValue placeholder="Tous" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                <SelectItem value="Jour">Jour</SelectItem>
                <SelectItem value="Nuit">Nuit</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">Top N: {topN}</Label>
            <Slider value={[topN]} onValueChange={v => setTopN(v[0])} min={5} max={30} step={5} className="mt-4" />
          </div>
        </div>

        {/* Carte */}
        <SectionTitle>Carte des gares ({gares.length} gares)</SectionTitle>
        <div className="mb-6">
          <div className="flex items-center gap-3 mb-3 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#00c98d]" />France</span>
            <span className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#0096d6]" />Allemagne</span>
            <span className="flex items-center gap-1.5"><span className="inline-block h-2.5 w-2.5 rounded-full bg-[#f59e0b]" />Belgique</span>
            <span className="ml-auto">Cliquez sur une gare pour voir ses trains</span>
          </div>
          <MapComponent
            stations={gares}
            selectedStation={selectedStation}
            onStationSelect={handleStationSelect}
          />
        </div>

        {/* Panel trains de la gare sélectionnée */}
        {selectedStation && (
          <div className="mb-8 rounded-xl border border-border/50 bg-card p-5">
            <div className="flex items-center justify-between mb-4">
              <SectionTitle className="mt-0 mb-0">
                Trains au départ de <span className="text-primary">{selectedStation}</span>
              </SectionTitle>
              <button onClick={handleClosePanel} className="rounded-md p-1 text-muted-foreground hover:text-foreground hover:bg-muted transition-colors">
                <X className="h-4 w-4" />
              </button>
            </div>
            {stationLoading ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Chargement des trains...
              </div>
            ) : stationTrains.length === 0 ? (
              <div className="text-sm text-muted-foreground">Aucun train trouvé pour cette gare.</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-border/50 text-muted-foreground/60 text-[10px] uppercase tracking-widest">
                      <th className="pb-2 text-left font-bold">Opérateur</th>
                      <th className="pb-2 text-left font-bold">Départ</th>
                      <th className="pb-2 text-left font-bold">Arrivée</th>
                      <th className="pb-2 text-left font-bold">Type</th>
                      <th className="pb-2 text-right font-bold">Distance</th>
                      <th className="pb-2 text-right font-bold">CO2 g/km</th>
                    </tr>
                  </thead>
                  <tbody>
                    {stationTrains.slice(0, 20).map((t, i) => (
                      <tr key={i} className="border-b border-border/30 hover:bg-muted/30 transition-colors">
                        <td className="py-2 text-secondary-foreground">{t.operateur}</td>
                        <td className="py-2 text-secondary-foreground">{t.heure_depart}</td>
                        <td className="py-2 text-secondary-foreground">{t.heure_arrivee}</td>
                        <td className="py-2">
                          <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${t.type_service === 'Nuit' ? 'bg-indigo-500/20 text-indigo-400' : 'bg-amber-500/20 text-amber-400'}`}>
                            {t.type_service}
                          </span>
                        </td>
                        <td className="py-2 text-right text-secondary-foreground">{t.distance_km} km</td>
                        <td className="py-2 text-right text-secondary-foreground">{t.emission_co2_gkm ?? 14}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {stationTrains.length > 20 && (
                  <div className="mt-2 text-center text-xs text-muted-foreground">
                    + {stationTrains.length - 20} autres trains
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Graphiques */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Top {topN} gares les plus desservies</SectionTitle>
            <BarChartComponent data={topStations} height={460} horizontal color="#00c98d" />
          </div>
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Distance vs heure de depart</SectionTitle>
            <ScatterPlot data={scatterData} height={460} xLabel="Heure" yLabel="Distance (km)" />
          </div>
        </div>

        <SectionTitle>Repartition operateur x type de service</SectionTitle>
        <div className="rounded-xl border border-border/50 bg-card p-5">
          <div className="grid gap-2">
            <div className="grid grid-cols-3 gap-2 text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              <div>Operateur</div>
              <div className="text-center">Jour</div>
              <div className="text-center">Nuit</div>
            </div>
            {heatmapData.map(row => (
              <div key={row.name} className="grid grid-cols-3 gap-2 items-center">
                <div className="text-sm text-secondary-foreground">{row.name}</div>
                <div className="h-8 rounded flex items-center justify-center text-xs font-medium text-foreground" style={{ backgroundColor: `rgba(0,201,141,${Math.min(row.Jour / 200, 1)})` }}>{row.Jour}</div>
                <div className="h-8 rounded flex items-center justify-center text-xs font-medium text-foreground" style={{ backgroundColor: `rgba(99,102,241,${Math.min(row.Nuit / 50, 1)})` }}>{row.Nuit}</div>
              </div>
            ))}
          </div>
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}
