'use client'

import { useState, useMemo } from 'react'
import { TopNav } from '@/components/dashboard/top-nav'
import { PageHeader } from '@/components/dashboard/page-header'
import { SectionTitle } from '@/components/dashboard/section-title'
import { BarChartComponent, ScatterPlot, COLORS } from '@/components/dashboard/charts'
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

export default function LiaisonsPage() {
  const trains = getTrains()
  const operators = getOperators()

  const [selectedOperator, setSelectedOperator] = useState<string>('all')
  const [selectedService, setSelectedService] = useState<string>('all')
  const [topN, setTopN] = useState<number>(20)

  const filteredTrains = useMemo(() => {
    return trains.filter(t => {
      if (selectedOperator !== 'all' && t.operator !== selectedOperator) return false
      if (selectedService !== 'all' && t.type_service !== selectedService) return false
      return true
    })
  }, [trains, selectedOperator, selectedService])

  // Top stations
  const topStations = useMemo(() => {
    const counts: Record<string, number> = {}
    filteredTrains.forEach(t => {
      counts[t.origin_station] = (counts[t.origin_station] || 0) + 1
    })
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, topN)
      .map(([name, value]) => ({ name, value }))
  }, [filteredTrains, topN])

  // Scatter data: hour vs distance
  const scatterData = useMemo(() => {
    return filteredTrains
      .slice(0, 500)
      .map(t => ({
        x: parseInt(t.heure_depart.split(':')[0]),
        y: t.distance_km,
        category: t.type_service,
      }))
  }, [filteredTrains])

  // Heatmap data: operator x service type
  const heatmapData = useMemo(() => {
    const map = new Map<string, { Jour: number; Nuit: number }>()
    filteredTrains.forEach(t => {
      if (!map.has(t.operator)) {
        map.set(t.operator, { Jour: 0, Nuit: 0 })
      }
      const op = map.get(t.operator)!
      if (t.type_service === 'Jour') op.Jour++
      else op.Nuit++
    })
    return Array.from(map.entries()).map(([name, data]) => ({
      name,
      ...data,
    }))
  }, [filteredTrains])

  return (
    <div className="min-h-screen bg-background">
      <TopNav apiStatus={true} />
      
      <main className="mx-auto max-w-7xl px-6 pb-16">
        <PageHeader
          eyebrow="Flux"
          title="Directions &"
          titleHighlight="Liaisons"
          subtitle="Analyse des flux ferroviaires par operateur et type"
        />

        {/* Filters */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
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
              Top N: {topN}
            </Label>
            <Slider
              value={[topN]}
              onValueChange={(v) => setTopN(v[0])}
              min={5}
              max={30}
              step={5}
              className="mt-4"
            />
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Top {topN} gares les plus desservies</SectionTitle>
            <BarChartComponent
              data={topStations}
              height={460}
              horizontal
              color="#00c98d"
            />
          </div>

          <div className="rounded-xl border border-border/50 bg-card p-5">
            <SectionTitle className="mt-0 mb-4">Distance vs heure de depart</SectionTitle>
            <ScatterPlot
              data={scatterData}
              height={460}
              xLabel="Heure"
              yLabel="Distance (km)"
            />
          </div>
        </div>

        <SectionTitle>Repartition operateur x type de service</SectionTitle>
        <div className="rounded-xl border border-border/50 bg-card p-5">
          <div className="grid grid-cols-1 gap-2">
            {/* Simple heatmap visualization */}
            <div className="grid gap-2">
              <div className="grid grid-cols-3 gap-2 text-[10px] font-bold uppercase tracking-[0.1em] text-muted-foreground/60">
                <div>Operateur</div>
                <div className="text-center">Jour</div>
                <div className="text-center">Nuit</div>
              </div>
              {heatmapData.map((row) => (
                <div key={row.name} className="grid grid-cols-3 gap-2 items-center">
                  <div className="text-sm text-secondary-foreground">{row.name}</div>
                  <div 
                    className="h-8 rounded flex items-center justify-center text-xs font-medium text-foreground"
                    style={{ 
                      backgroundColor: `rgba(0, 201, 141, ${Math.min(row.Jour / 200, 1)})` 
                    }}
                  >
                    {row.Jour}
                  </div>
                  <div 
                    className="h-8 rounded flex items-center justify-center text-xs font-medium text-foreground"
                    style={{ 
                      backgroundColor: `rgba(99, 102, 241, ${Math.min(row.Nuit / 50, 1)})` 
                    }}
                  >
                    {row.Nuit}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <footer className="mt-16 pt-6 border-t border-border/30 text-center text-[10px] text-muted-foreground/40">
          ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL
        </footer>
      </main>
    </div>
  )
}
