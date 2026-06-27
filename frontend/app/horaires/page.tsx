'use client'

import { useState, useMemo, useEffect, useCallback } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { SectionTitle } from '@/components/dashboard/section-title'
import { StatRow } from '@/components/dashboard/source-card'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent } from '@/components/dashboard/charts'
import {
  getTrainsFromAPI, getOperateursFromAPI, getGaresFromAPI,
  type Train, type Operateur
} from '@/lib/api-client'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Download, Search, Train as TrainIcon, X } from 'lucide-react'

export default function HorairesPage() {
  const [trains, setTrains] = useState<Train[]>([])
  const [operators, setOperators] = useState<Operateur[]>([])
  const [loading, setLoading] = useState(true)
  const [searching, setSearching] = useState(false)
  const [apiStatus, setApiStatus] = useState(false)

  // Filtres simples
  const [selectedOperator, setSelectedOperator] = useState('all')
  const [selectedService, setSelectedService] = useState('all')
  const [selectedLine, setSelectedLine] = useState('all')
  const [distanceRange, setDistanceRange] = useState([0, 917])

  // Recherche gare par texte
  const [gareDepart, setGareDepart] = useState('')
  const [gareArrivee, setGareArrivee] = useState('')
  const [garesDepart, setGaresDepart] = useState<{ id_gare: number; nom: string; pays: string }[]>([])
  const [garesArrivee, setGaresArrivee] = useState<{ id_gare: number; nom: string; pays: string }[]>([])
  const [showDepartList, setShowDepartList] = useState(false)
  const [showArriveeList, setShowArriveeList] = useState(false)
  const [selectedDepart, setSelectedDepart] = useState('')
  const [selectedArrivee, setSelectedArrivee] = useState('')

  // Chargement initial
  useEffect(() => {
    async function load() {
      try {
        const [t, o] = await Promise.all([
          getTrainsFromAPI({ limit: 50 }),
          getOperateursFromAPI(),
        ])
        setTrains(t)
        setOperators(o)
        setApiStatus(true)
      } catch (e) {
        setApiStatus(false)
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  // Recherche gares départ par texte
  useEffect(() => {
    if (gareDepart.length < 2) { setGaresDepart([]); return }
    const timer = setTimeout(async () => {
      try {
        const g = await getGaresFromAPI(gareDepart)
        setGaresDepart(g.slice(0, 15))
        setShowDepartList(true)
      } catch {}
    }, 300)
    return () => clearTimeout(timer)
  }, [gareDepart])

  // Recherche gares arrivée par texte
  useEffect(() => {
    if (gareArrivee.length < 2) { setGaresArrivee([]); return }
    const timer = setTimeout(async () => {
      try {
        const g = await getGaresFromAPI(gareArrivee)
        setGaresArrivee(g.slice(0, 15))
        setShowArriveeList(true)
      } catch {}
    }, 300)
    return () => clearTimeout(timer)
  }, [gareArrivee])

  const handleSearch = async () => {
    setSearching(true)
    try {
      const params: any = { limit: 50 }
      if (selectedOperator !== 'all') params.operateur = selectedOperator
      if (selectedService !== 'all') params.type_service = selectedService
      if (selectedDepart) params.gare = selectedDepart
      if (distanceRange[0] > 0) params.dist_min = distanceRange[0]
      if (distanceRange[1] < 917) params.dist_max = distanceRange[1]
      const t = await getTrainsFromAPI(params)
      setTrains(t)
    } catch (e) {
      console.error(e)
    } finally {
      setSearching(false)
    }
  }

  const filteredTrains = useMemo(() => {
    return trains.filter(t => {
      if (selectedLine !== 'all' && t.type_ligne !== selectedLine) return false
      return true
    })
  }, [trains, selectedLine])

  const stats = useMemo(() => ({
    count: filteredTrains.length,
    totalDistance: filteredTrains.reduce((acc, t) => acc + t.distance_km, 0),
    avgDistance: filteredTrains.length > 0
      ? filteredTrains.reduce((acc, t) => acc + t.distance_km, 0) / filteredTrains.length : 0,
    totalCO2: filteredTrains.reduce((acc, t) => acc + (t.co2_emission_kg || 0), 0),
  }), [filteredTrains])

  const departuresByHour = useMemo(() => {
    const hours: Record<number, number> = {}
    filteredTrains.forEach(t => {
      const h = parseInt(t.heure_depart?.split(':')[0] || '0') % 24
      hours[h] = (hours[h] || 0) + 1
    })
    return Array.from({ length: 24 }, (_, i) => ({
      name: i.toString().padStart(2, '0') + 'h',
      value: hours[i] || 0,
    }))
  }, [filteredTrains])

  const tableColumns = [
    { key: 'operator', label: 'Opérateur' },
    { key: 'origin_station', label: 'Gare' },
    { key: 'heure_depart', label: 'H. départ' },
    { key: 'heure_arrivee', label: 'H. arrivée' },
    { key: 'distance_km', label: 'Dist. km', align: 'right' as const },
    { key: 'co2_emission_kg', label: 'CO2 kg', align: 'right' as const },
    { key: 'type_service', label: 'Type' },
  ]

  const tableData = useMemo(() => {
    return filteredTrains.slice(0, 100).map(t => ({
      ...t,
      distance_km: t.distance_km.toFixed(1),
      co2_emission_kg: (t.co2_emission_kg || 0).toFixed(3),
    }))
  }, [filteredTrains])

  const handleExport = () => {
    const csv = [
      tableColumns.map(c => c.label).join(','),
      ...tableData.map(row => tableColumns.map(c => row[c.key as keyof typeof row]).join(','))
    ].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'trains_obrail.csv'
    a.click()
  }

  if (loading) return (
    <div className="min-h-screen bg-background flex items-center justify-center">
      <div className="text-muted-foreground text-sm">Chargement des données...</div>
    </div>
  )

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={apiStatus} />
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Recherche"
          title="Horaires"
          titleHighlight="& trains"
          subtitle="Recherchez par gare de départ, opérateur, type de service ou distance"
        />

        {/* ── RECHERCHE GARE ── */}
        <div className="mb-6 rounded-xl border border-primary/30 bg-primary/5 p-5">
          <div className="flex items-center gap-2 mb-4">
            <TrainIcon className="h-4 w-4 text-primary" />
            <span className="text-sm font-bold text-primary uppercase tracking-wide">
              Recherche de trajet
            </span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">

            {/* Gare départ */}
            <div className="space-y-2 relative">
              <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
                Gare de départ
              </Label>
              <div className="relative">
                <Input
                  value={gareDepart}
                  onChange={e => { setGareDepart(e.target.value); setSelectedDepart('') }}
                  onFocus={() => garesDepart.length > 0 && setShowDepartList(true)}
                  placeholder="Tapez une ville... (ex: Paris)"
                  className="bg-card border-border/50"
                />
                {selectedDepart && (
                  <button
                    onClick={() => { setGareDepart(''); setSelectedDepart(''); setShowDepartList(false) }}
                    className="absolute right-2 top-2.5 text-muted-foreground hover:text-foreground"
                    aria-label="Effacer gare départ"
                  >
                    <X className="h-4 w-4" />
                  </button>
                )}
              </div>
              {showDepartList && garesDepart.length > 0 && (
                <div className="absolute z-50 w-full bg-card border border-border rounded-md shadow-lg max-h-48 overflow-y-auto">
                  {garesDepart.map(g => (
                    <button
                      key={g.id_gare}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-muted/50 flex justify-between"
                      onClick={() => {
                        setGareDepart(g.nom)
                        setSelectedDepart(g.nom)
                        setShowDepartList(false)
                      }}
                    >
                      <span>{g.nom}</span>
                      <span className="text-muted-foreground text-xs">{g.pays}</span>
                    </button>
                  ))}
                </div>
              )}
              {selectedDepart && (
                <p className="text-xs text-primary">✓ {selectedDepart}</p>
              )}
            </div>

            {/* Gare arrivée */}
            <div className="space-y-2 relative">
              <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
                Gare d'arrivée <span className="text-muted-foreground/40">(optionnel)</span>
              </Label>
              <div className="relative">
                <Input
                  value={gareArrivee}
                  onChange={e => { setGareArrivee(e.target.value); setSelectedArrivee('') }}
                  onFocus={() => garesArrivee.length > 0 && setShowArriveeList(true)}
                  placeholder="Tapez une ville... (ex: Lyon)"
                  className="bg-card border-border/50"
                />
                {selectedArrivee && (
                  <button
                    onClick={() => { setGareArrivee(''); setSelectedArrivee(''); setShowArriveeList(false) }}
                    className="absolute right-2 top-2.5 text-muted-foreground hover:text-foreground"
                    aria-label="Effacer gare arrivée"
                  >
                    <X className="h-4 w-4" />
                  </button>
                )}
              </div>
              {showArriveeList && garesArrivee.length > 0 && (
                <div className="absolute z-50 w-full bg-card border border-border rounded-md shadow-lg max-h-48 overflow-y-auto">
                  {garesArrivee.map(g => (
                    <button
                      key={g.id_gare}
                      className="w-full text-left px-3 py-2 text-sm hover:bg-muted/50 flex justify-between"
                      onClick={() => {
                        setGareArrivee(g.nom)
                        setSelectedArrivee(g.nom)
                        setShowArriveeList(false)
                      }}
                    >
                      <span>{g.nom}</span>
                      <span className="text-muted-foreground text-xs">{g.pays}</span>
                    </button>
                  ))}
                </div>
              )}
              {selectedArrivee && (
                <p className="text-xs text-primary">✓ {selectedArrivee}</p>
              )}
            </div>

            <div className="flex items-end">
              <Button
                onClick={handleSearch}
                disabled={searching}
                className="w-full bg-primary text-primary-foreground hover:bg-primary/90"
              >
                <Search className="h-4 w-4 mr-2" />
                {searching ? 'Recherche...' : 'Rechercher'}
              </Button>
            </div>
          </div>
        </div>

        {/* ── FILTRES AVANCÉS ── */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">Opérateur</Label>
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
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">Type ligne</Label>
            <Select value={selectedLine} onValueChange={setSelectedLine}>
              <SelectTrigger className="bg-card border-border/50"><SelectValue placeholder="Tous" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                <SelectItem value="national">National</SelectItem>
                <SelectItem value="regional">Régional</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              Distance: {distanceRange[0]}-{distanceRange[1]} km
            </Label>
            <Slider value={distanceRange} onValueChange={setDistanceRange} min={0} max={917} step={50} className="mt-4" />
          </div>
        </div>

        <div className="mb-6 flex justify-end">
          <Button onClick={handleSearch} disabled={searching} variant="outline" size="sm"
            className="bg-primary/10 border-primary/30 text-primary hover:bg-primary/20">
            <Search className="h-3.5 w-3.5 mr-2" />
            {searching ? 'Recherche...' : 'Appliquer les filtres'}
          </Button>
        </div>

        <StatRow
          items={[
            { value: stats.count.toLocaleString(), label: 'Résultats' },
            { value: `${stats.totalDistance.toLocaleString()} km`, label: 'Distance totale' },
            { value: `${stats.avgDistance.toFixed(0)} km`, label: 'Distance moyenne' },
            { value: `${stats.totalCO2.toFixed(0)} kg`, label: 'CO2 total' },
          ]}
          className="mb-6"
        />

        {filteredTrains.length === 0 ? (
          <div className="rounded-xl border-l-2 border-primary bg-primary/5 p-4 text-sm text-primary">
            Aucun train trouvé. Modifiez vos critères de recherche.
          </div>
        ) : (
          <>
            <DataTable columns={tableColumns} data={tableData} className="mb-4" />
            <Button onClick={handleExport} variant="outline" size="sm"
              className="bg-primary/10 border-primary/30 text-primary hover:bg-primary/20">
              <Download className="h-3.5 w-3.5 mr-2" />Export CSV
            </Button>
            <SectionTitle>Départs par heure</SectionTitle>
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <BarChartComponent data={departuresByHour} height={200} color="#00c98d" />
            </div>
          </>
        )}

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}


