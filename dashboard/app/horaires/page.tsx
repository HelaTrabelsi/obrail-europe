'use client'

import { useState, useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { SectionTitle } from '@/components/dashboard/section-title'
import { StatRow } from '@/components/dashboard/source-card'
import { DataTable } from '@/components/dashboard/data-table'
import { BarChartComponent } from '@/components/dashboard/charts'
import { getTrains, getOperators } from '@/lib/mock-data'
import { Label } from '@/components/ui/label'
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from '@/components/ui/select'
import { Slider } from '@/components/ui/slider'
import { Button } from '@/components/ui/button'
import { Download } from 'lucide-react'

export default function HorairesPage() {
  const trains = getTrains()
  const operators = getOperators()

  const [selectedOperator, setSelectedOperator] = useState<string>('all')
  const [selectedService, setSelectedService] = useState<string>('all')
  const [selectedLine, setSelectedLine] = useState<string>('all')
  const [distanceRange, setDistanceRange] = useState<number[]>([0, 917])

  const stations = useMemo(() => {
    const set = new Set<string>()
    trains.forEach(t => set.add(t.origin_station))
    return Array.from(set).sort()
  }, [trains])

  const filteredTrains = useMemo(() => {
    return trains.filter(t => {
      if (selectedOperator !== 'all' && t.operator !== selectedOperator) return false
      if (selectedService !== 'all' && t.type_service !== selectedService) return false
      if (selectedLine !== 'all' && t.type_ligne !== selectedLine) return false
      if (t.distance_km < distanceRange[0] || t.distance_km > distanceRange[1]) return false
      return true
    })
  }, [trains, selectedOperator, selectedService, selectedLine, distanceRange])

  const stats = useMemo(() => ({
    count: filteredTrains.length,
    totalDistance: filteredTrains.reduce((acc, t) => acc + t.distance_km, 0),
    avgDistance: filteredTrains.length > 0 
      ? filteredTrains.reduce((acc, t) => acc + t.distance_km, 0) / filteredTrains.length 
      : 0,
    totalCO2: filteredTrains.reduce((acc, t) => acc + t.co2_emission_kg, 0),
  }), [filteredTrains])

  const departuresByHour = useMemo(() => {
    const hours: Record<number, number> = {}
    filteredTrains.forEach(t => {
      const hour = parseInt(t.heure_depart.split(':')[0])
      hours[hour] = (hours[hour] || 0) + 1
    })
    return Array.from({ length: 24 }, (_, i) => ({
      name: i.toString().padStart(2, '0'),
      value: hours[i] || 0,
    }))
  }, [filteredTrains])

  const tableColumns = [
    { key: 'operator', label: 'Operateur' },
    { key: 'origin_station', label: 'Gare' },
    { key: 'heure_depart', label: 'H. depart' },
    { key: 'heure_arrivee', label: 'H. arrivee' },
    { key: 'distance_km', label: 'Dist. km', align: 'right' as const },
    { key: 'co2_emission_kg', label: 'CO2 kg', align: 'right' as const },
    { key: 'type_service', label: 'Type' },
  ]

  const tableData = useMemo(() => {
    return filteredTrains.slice(0, 100).map(t => ({
      ...t,
      distance_km: t.distance_km.toFixed(1),
      co2_emission_kg: t.co2_emission_kg.toFixed(3),
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
    a.download = 'trains.csv'
    a.click()
  }

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Recherche"
          title="Horaires"
          titleHighlight="& trains"
          subtitle="Filtrez par gare, operateur ou distance"
        />

        {/* Filters */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              Operateur
            </Label>
            <Select value={selectedOperator} onValueChange={setSelectedOperator}>
              <SelectTrigger className="bg-card border-border/50">
                <SelectValue placeholder="Tous" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                {operators.map(op => (
                  <SelectItem key={op.nom} value={op.nom}>{op.nom}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              Type service
            </Label>
            <Select value={selectedService} onValueChange={setSelectedService}>
              <SelectTrigger className="bg-card border-border/50">
                <SelectValue placeholder="Tous" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                <SelectItem value="Jour">Jour</SelectItem>
                <SelectItem value="Nuit">Nuit</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              Type ligne
            </Label>
            <Select value={selectedLine} onValueChange={setSelectedLine}>
              <SelectTrigger className="bg-card border-border/50">
                <SelectValue placeholder="Tous" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Tous</SelectItem>
                <SelectItem value="national">National</SelectItem>
                <SelectItem value="regional">Regional</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
              Distance (km): {distanceRange[0]} - {distanceRange[1]}
            </Label>
            <Slider
              value={distanceRange}
              onValueChange={setDistanceRange}
              min={0}
              max={917}
              step={50}
              className="mt-4"
            />
          </div>
        </div>

        {/* Stats row */}
        <StatRow
          items={[
            { value: stats.count.toLocaleString(), label: 'Resultats' },
            { value: `${stats.totalDistance.toLocaleString()} km`, label: 'Distance totale' },
            { value: `${stats.avgDistance.toFixed(0)} km`, label: 'Distance moyenne' },
            { value: `${stats.totalCO2.toFixed(0)} kg`, label: 'CO2 total' },
          ]}
          className="mb-6"
        />

        {filteredTrains.length === 0 ? (
          <div className="rounded-xl border-l-2 border-primary bg-primary/5 p-4 text-sm text-primary">
            Aucun train trouve.
          </div>
        ) : (
          <>
            <DataTable
              columns={tableColumns}
              data={tableData}
              className="mb-4"
            />

            <Button 
              onClick={handleExport}
              variant="outline" 
              size="sm"
              className="bg-primary/10 border-primary/30 text-primary hover:bg-primary/20"
            >
              <Download className="h-3.5 w-3.5 mr-2" />
              Export CSV
            </Button>

            <SectionTitle>Departs par heure</SectionTitle>
            <div className="rounded-xl border border-border/50 bg-card p-5">
              <BarChartComponent
                data={departuresByHour}
                height={200}
                color="#00c98d"
              />
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
