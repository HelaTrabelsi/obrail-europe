// Mock data types for ObRail Europe dashboard
export interface Train {
  id: number
  operator: string
  origin_station: string
  destination_station: string
  type_service: 'Jour' | 'Nuit'
  type_ligne: 'national' | 'regional'
  heure_depart: string
  heure_arrivee: string
  distance_km: number
  emissions_co2_gkm: number
  co2_emission_kg: number
  source_donnee: string
  pays: string
}

export interface Stats {
  avant_doublons: number
  apres_doublons: number
  doublons_supprimes: number
  sans_horaires_supprimes: number
  total_trajets: number
  repartition_operateurs: Record<string, number>
  repartition_type_service: Record<string, number>
  date_transformation: string
}

export interface Operator {
  id: number
  nom: string
  pays: string
  nb_trains: number
  nb_jour: number
  nb_nuit: number
}

// Generate realistic mock data
function generateMockTrains(): Train[] {
  const operators = [
    { name: 'SNCF TER', pays: 'FR' },
    { name: 'SNCF Intercites', pays: 'FR' },
    { name: 'Deutsche Bahn', pays: 'DE' },
    { name: 'DB Regional', pays: 'DE' },
    { name: 'SNCB', pays: 'BE' },
  ]

  const stationsFR = [
    'Paris Gare de Lyon', 'Lyon Part-Dieu', 'Marseille Saint-Charles', 
    'Bordeaux Saint-Jean', 'Lille Europe', 'Nantes', 'Strasbourg',
    'Toulouse Matabiau', 'Nice Ville', 'Montpellier Saint-Roch',
    'Rennes', 'Le Mans', 'Tours', 'Dijon Ville', 'Grenoble'
  ]

  const stationsDE = [
    'Berlin Hauptbahnhof', 'München Hauptbahnhof', 'Hamburg Hauptbahnhof',
    'Frankfurt (Main) Hbf', 'Köln Hauptbahnhof', 'Stuttgart Hauptbahnhof',
    'Düsseldorf Hauptbahnhof', 'Leipzig Hauptbahnhof', 'Dresden Hauptbahnhof',
    'Hannover Hauptbahnhof', 'Nürnberg Hauptbahnhof', 'Bremen Hauptbahnhof'
  ]

  const stationsBE = [
    'Bruxelles-Midi', 'Anvers-Central', 'Gand-Saint-Pierre',
    'Liège-Guillemins', 'Bruges', 'Namur', 'Louvain',
    'Charleroi-Sud', 'Mons', 'Ostende'
  ]

  const trains: Train[] = []
  let id = 1

  for (let i = 0; i < 500; i++) {
    const operator = operators[Math.floor(Math.random() * operators.length)]
    let stations: string[]
    
    if (operator.pays === 'FR') stations = stationsFR
    else if (operator.pays === 'DE') stations = stationsDE
    else stations = stationsBE

    const originIdx = Math.floor(Math.random() * stations.length)
    let destIdx = Math.floor(Math.random() * stations.length)
    while (destIdx === originIdx) {
      destIdx = Math.floor(Math.random() * stations.length)
    }

    const hour = Math.floor(Math.random() * 24)
    const type_service = hour >= 22 || hour < 5 ? 'Nuit' : 'Jour'
    const distance_km = Math.floor(50 + Math.random() * 800)
    const emissions_co2_gkm = 3.2 + Math.random() * 1.2

    trains.push({
      id: id++,
      operator: operator.name,
      origin_station: stations[originIdx],
      destination_station: stations[destIdx],
      type_service,
      type_ligne: operator.name.includes('Regional') || operator.name.includes('TER') ? 'regional' : 'national',
      heure_depart: `${hour.toString().padStart(2, '0')}:${Math.floor(Math.random() * 60).toString().padStart(2, '0')}`,
      heure_arrivee: `${((hour + 1 + Math.floor(Math.random() * 4)) % 24).toString().padStart(2, '0')}:${Math.floor(Math.random() * 60).toString().padStart(2, '0')}`,
      distance_km,
      emissions_co2_gkm,
      co2_emission_kg: (emissions_co2_gkm * distance_km) / 1000,
      source_donnee: `gtfs_${operator.name.toLowerCase().replace(/\s+/g, '_')}`,
      pays: operator.pays,
    })
  }

  return trains
}

function generateMockStats(): Stats {
  return {
    avant_doublons: 125000,
    apres_doublons: 99854,
    doublons_supprimes: 25146,
    sans_horaires_supprimes: 3420,
    total_trajets: 99854,
    repartition_operateurs: {
      'SNCF TER': 35420,
      'Deutsche Bahn': 28560,
      'SNCF Intercites': 15230,
      'DB Regional': 12340,
      'SNCB': 8304,
    },
    repartition_type_service: {
      'Jour': 89650,
      'Nuit': 10204,
    },
    date_transformation: new Date().toISOString(),
  }
}

// Cached data
let cachedTrains: Train[] | null = null
let cachedStats: Stats | null = null

export function getTrains(): Train[] {
  if (!cachedTrains) {
    cachedTrains = generateMockTrains()
  }
  return cachedTrains
}

export function getStats(): Stats {
  if (!cachedStats) {
    cachedStats = generateMockStats()
  }
  return cachedStats
}

export function getOperators(): Operator[] {
  const trains = getTrains()
  const operatorMap = new Map<string, Operator>()

  trains.forEach((train, index) => {
    if (!operatorMap.has(train.operator)) {
      operatorMap.set(train.operator, {
        id: operatorMap.size + 1,
        nom: train.operator,
        pays: train.pays,
        nb_trains: 0,
        nb_jour: 0,
        nb_nuit: 0,
      })
    }
    const op = operatorMap.get(train.operator)!
    op.nb_trains++
    if (train.type_service === 'Jour') op.nb_jour++
    else op.nb_nuit++
  })

  return Array.from(operatorMap.values())
}
