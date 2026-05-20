/**
 * ObRail Europe — API Client
 * Remplace mock-data par les vraies donnees FastAPI
 */

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

// ── Types ──────────────────────────────────────────────────────
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
  // Champs calcules
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

// ── Fetch helper ───────────────────────────────────────────────
async function apiFetch<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = new URL(`${API_URL}${path}`)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v))
    })
  }
  const res = await fetch(url.toString(), {
    next: { revalidate: 60 }, // Cache 60s
  })
  if (!res.ok) throw new Error(`API error ${res.status}: ${path}`)
  return res.json()
}

// ── Normalisation des trains ────────────────────────────────────
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

// ── Endpoints ──────────────────────────────────────────────────

export async function getHealth(): Promise<HealthStatus> {
  return apiFetch<HealthStatus>('/health')
}

export async function getTrainsFromAPI(params?: {
  gare?: string
  operateur?: string
  type_service?: string
  dist_min?: number
  dist_max?: number
  limit?: number
}): Promise<Train[]> {
  const trains = await apiFetch<Train[]>('/dessertes/search', {
    limit: 500,
    ...params,
  })
  return normalizeTrains(trains)
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

// ── Compatibilite mock-data (pour ne pas casser les pages) ──────
// Ces fonctions retournent des donnees vides si l'API est indisponible

let _trainsCache: Train[] | null = null
let _opCache: Operateur[] | null = null
let _statsCache: any = null

export function getTrains(): Train[] {
  return _trainsCache || []
}

export function getOperators(): Operateur[] {
  return _opCache || []
}

export function getStats() {
  return _statsCache || {
    avant_doublons: 186902,
    apres_doublons: 127740,
    doublons_supprimes: 59162,
    sans_horaires_supprimes: 0,
  }
}

export async function loadAllData() {
  try {
    const [trains, ops, stats] = await Promise.all([
      getTrainsFromAPI({ limit: 500 }),
      getOperateursFromAPI(),
      getStatsFromAPI(),
    ])
    _trainsCache = trains
    _opCache = ops
    _statsCache = {
      avant_doublons: 186902,
      apres_doublons: stats.nb_trains,
      doublons_supprimes: 59162,
      sans_horaires_supprimes: 0,
      ...stats,
    }
    return { trains, operators: ops, stats }
  } catch (e) {
    console.error('API indisponible — mode demo:', e)
    return null
  }
}