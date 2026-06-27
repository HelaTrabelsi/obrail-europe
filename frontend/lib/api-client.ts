const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export interface Train {
  id_train: number
  operateur: string
  gare: string
  pays: string
  type_service: 'Jour' | 'Nuit'
  type_ligne: string
  heure_depart: string
  heure_arrivee: string
  distance_km: number
  emission_co2_gkm: number
  source_donnee: string
  operator?: string
  origin_station?: string
  destination_station?: string
  emissions_co2_gkm?: number
  co2_emission_kg?: number
}

export interface Operateur {
  id_operateur: number
  nom: string
  pays: string
  nb_trains: number
  nb_jour: number
  nb_nuit: number
}

export interface Stats {
  nb_trains: number
  nb_operateurs: number
  nb_gares: number
  nb_trajets: number
  nb_jour: number
  nb_nuit: number
  distance_moyenne_km: number
}

export interface StatsCO2 {
  operateur: string
  co2_moy_gkm: number
  co2_total_kg: number
}

export interface StatsQualite {
  nb_trains_total: number
  co2_manquants: number
  completude_co2_pct: number
  etl_logs: EtlLog[]
  par_source: { source_donnee: string; nb: number }[]
}

export interface EtlLog {
  etape: string
  source: string
  nb_enregistrements: number
  statut: string
  run_date: string
  message: string
}

export interface HealthStatus {
  status: string
  database: string
  nb_trains: number
}

async function apiFetch<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = new URL(`${API_URL}${path}`)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v))
    })
  }
  const res = await fetch(url.toString(), { cache: 'no-store' })
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`)
  return res.json()
}

function normalizeTrains(trains: Train[]): Train[] {
  return trains.map(t => ({
    ...t,
    operator: t.operateur,
    origin_station: t.gare,
    destination_station: t.gare,
    emissions_co2_gkm: t.emission_co2_gkm || 14,
    co2_emission_kg: t.distance_km * (t.emission_co2_gkm || 14) / 1000,
  }))
}

export async function getHealth(): Promise<HealthStatus> {
  return apiFetch<HealthStatus>('/health')
}

// Limite par défaut à 50 pour éviter de surcharger le navigateur
export async function getTrainsFromAPI(params?: {
  gare?: string
  gare_depart?: string
  gare_arrivee?: string
  operateur?: string
  type_service?: string
  dist_min?: number
  dist_max?: number
  limit?: number
}): Promise<Train[]> {
  const { gare_depart, gare_arrivee, ...rest } = params || {}
  // Si gare_depart ou gare_arrivee → utilise le champ gare de l'API
  const gare = gare_depart || gare_arrivee || rest.gare
  const trains = await apiFetch<Train[]>('/dessertes/search', {
    limit: 50,
    ...rest,
    ...(gare ? { gare } : {}),
  })
  return normalizeTrains(trains)
}

// Récupère la liste des gares disponibles
export async function getGaresFromAPI(nom?: string): Promise<{ id_gare: number; nom: string; pays: string }[]> {
  return apiFetch('/gares', nom ? { nom } : {})
}

export async function getOperateursFromAPI(): Promise<Operateur[]> {
  return apiFetch<Operateur[]>('/operateurs')
}

export async function getStatsFromAPI(): Promise<Stats> {
  return apiFetch<Stats>('/stats')
}

export async function getStatsCO2FromAPI(): Promise<StatsCO2[]> {
  return apiFetch<StatsCO2[]>('/stats/co2')
}

export async function getStatsQualiteFromAPI(): Promise<StatsQualite> {
  return apiFetch<StatsQualite>('/stats/qualite')
}

export async function getStatsCouvertureFromAPI() {
  return apiFetch('/stats/couverture')
}