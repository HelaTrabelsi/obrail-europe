'use client'

import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

const STATION_COORDS: Record<string, [number, number]> = {
  // France
  'Paris': [48.8566, 2.3522],
  'Paris Gare de Lyon': [48.8448, 2.3734],
  'Paris Gare du Nord': [48.8809, 2.3553],
  'Paris Montparnasse': [48.8412, 2.3196],
  'Lyon': [45.7640, 4.8357],
  'Lyon Part-Dieu': [45.7604, 4.8596],
  'Lyon Perrache': [45.7490, 4.8266],
  'Marseille': [43.2965, 5.3698],
  'Marseille Saint-Charles': [43.3028, 5.3806],
  'Bordeaux': [44.8378, -0.5792],
  'Bordeaux Saint-Jean': [44.8256, -0.5562],
  'Toulouse': [43.6047, 1.4442],
  'Toulouse Matabiau': [43.6112, 1.4536],
  'Strasbourg': [48.5734, 7.7521],
  'Nantes': [47.2184, -1.5536],
  'Rennes': [48.1173, -1.6778],
  'Lille': [50.6292, 3.0573],
  'Lille Flandres': [50.6366, 3.0699],
  'Lille Europe': [50.6390, 3.0752],
  'Nice': [43.7102, 7.2620],
  'Nice Ville': [43.7047, 7.2618],
  'Montpellier': [43.6108, 3.8767],
  'Grenoble': [45.1885, 5.7245],
  'Tours': [47.3941, 0.6848],
  'Dijon': [47.3220, 5.0415],
  'Clermont-Ferrand': [45.7772, 3.0870],
  'Rouen': [49.4432, 1.0993],
  'Le Mans': [47.9956, 0.2026],
  'Metz': [49.1193, 6.1757],
  'Nancy': [48.6921, 6.1844],
  'Reims': [49.2583, 4.0317],
  'Mulhouse': [47.7408, 7.3386],
  'Perpignan': [42.6983, 2.8954],
  'Toulon': [43.1242, 5.9304],
  'Poitiers': [46.5802, 0.3404],
  'Caen': [49.1829, -0.3707],
  'Angers': [47.4784, -0.5632],
  'Brest': [48.3905, -4.4860],
  'Lorient': [47.7489, -3.3721],
  'Quimper': [47.9969, -4.0978],
  'Amiens': [49.8941, 2.3024],
  'Limoges': [45.8336, 1.2611],
  'Besançon': [47.2378, 6.0241],
  'Orléans': [47.9029, 1.9040],
  'Valenciennes': [50.3576, 3.5237],
  // Germany
  'Berlin': [52.5200, 13.4050],
  'Berlin Hbf': [52.5250, 13.3694],
  'München': [48.1351, 11.5820],
  'München Hbf': [48.1402, 11.5600],
  'Munich': [48.1351, 11.5820],
  'Hamburg': [53.5753, 10.0153],
  'Hamburg Hbf': [53.5531, 10.0064],
  'Frankfurt': [50.1109, 8.6821],
  'Frankfurt Hbf': [50.1065, 8.6632],
  'Frankfurt(Main)Hbf': [50.1065, 8.6632],
  'Frankfurt am Main': [50.1109, 8.6821],
  'Frankfurt(Main)': [50.1109, 8.6821],
  'Köln': [50.9333, 6.9500],
  'Köln Hbf': [50.9431, 6.9590],
  'Koeln': [50.9333, 6.9500],
  'Cologne': [50.9333, 6.9500],
  'Stuttgart': [48.7758, 9.1829],
  'Stuttgart Hbf': [48.7843, 9.1827],
  'Düsseldorf': [51.2217, 6.7762],
  'Düsseldorf Hbf': [51.2196, 6.7944],
  'Dusseldorf': [51.2217, 6.7762],
  'Leipzig': [51.3397, 12.3731],
  'Leipzig Hbf': [51.3454, 12.3814],
  'Dresden': [51.0504, 13.7373],
  'Dresden Hbf': [51.0404, 13.7329],
  'Hannover': [52.3759, 9.7320],
  'Hannover Hbf': [52.3773, 9.7414],
  'Nürnberg': [49.4521, 11.0767],
  'Nürnberg Hbf': [49.4462, 11.0825],
  'Nuremberg': [49.4521, 11.0767],
  'Bremen': [53.0793, 8.8017],
  'Bremen Hbf': [53.0831, 8.8136],
  'Dortmund': [51.5136, 7.4653],
  'Dortmund Hbf': [51.5178, 7.4593],
  'Mannheim': [49.4875, 8.4660],
  'Mannheim Hbf': [49.4793, 8.4697],
  'Freiburg': [47.9990, 7.8421],
  'Freiburg(Breisgau) Hbf': [47.9970, 7.8407],
  'Augsburg': [48.3654, 10.8855],
  'Augsburg Hbf': [48.3653, 10.8856],
  'Erfurt': [50.9726, 11.0360],
  'Erfurt Hbf': [50.9726, 11.0360],
  'Rostock': [54.0887, 12.1404],
  'Karlsruhe': [49.0069, 8.4037],
  'Karlsruhe Hbf': [49.0089, 8.4017],
  'Mainz': [49.9988, 8.2746],
  'Mainz Hbf': [49.9998, 8.2735],
  'Bonn': [50.7323, 7.0954],
  'Bonn Hbf': [50.7317, 7.0966],
  'Aachen': [50.7753, 6.0839],
  'Aachen Hbf': [50.7679, 6.0912],
  'Münster': [51.9607, 7.6261],
  'Münster(Westf)Hbf': [51.9562, 7.6352],
  'Bochum': [51.4818, 7.2162],
  'Bochum Hbf': [51.4782, 7.2211],
  'Essen': [51.4508, 7.0131],
  'Essen Hbf': [51.4508, 7.0131],
  'Wiesbaden': [50.0782, 8.2398],
  'Wiesbaden Hbf': [50.0714, 8.2429],
  'Heidelberg': [49.4040, 8.6753],
  'Saarbrücken': [49.2415, 6.9908],
  'Kassel': [51.3165, 9.4660],
  'Kiel': [54.3155, 10.1327],
  'Lübeck': [53.8660, 10.6866],
  'Magdeburg': [52.1326, 11.6262],
  'Bielefeld': [52.0302, 8.5325],
  // Belgium
  'Bruxelles': [50.8503, 4.3517],
  'Bruxelles-Midi': [50.8354, 4.3360],
  'Bruxelles-Central': [50.8456, 4.3567],
  'Bruxelles-Nord': [50.8597, 4.3609],
  'Brussels': [50.8503, 4.3517],
  'Brussels-South': [50.8354, 4.3360],
  'Liège': [50.6326, 5.5797],
  'Liège-Guillemins': [50.6244, 5.5669],
  'Liege': [50.6326, 5.5797],
  'Gand': [51.0543, 3.7174],
  'Gent-Sint-Pieters': [51.0361, 3.7103],
  'Gent': [51.0543, 3.7174],
  'Ghent': [51.0543, 3.7174],
  'Bruges': [51.2093, 3.2247],
  'Brugge': [51.2093, 3.2247],
  'Anvers': [51.2213, 4.4051],
  'Antwerpen-Centraal': [51.2172, 4.4213],
  'Antwerpen': [51.2213, 4.4051],
  'Antwerp': [51.2213, 4.4051],
  'Namur': [50.4669, 4.8674],
  'Charleroi': [50.4108, 4.4446],
  'Charleroi-Sud': [50.4108, 4.4446],
  'Mons': [50.4543, 3.9563],
  'Leuven': [50.8823, 4.7138],
  'Hasselt': [50.9302, 5.3380],
  'Kortrijk': [50.8240, 3.2622],
  'Ostende': [51.2289, 2.9158],
}

const COUNTRY_COLORS: Record<string, string> = {
  'FR': '#00c98d',
  'France': '#00c98d',
  'DE': '#0096d6',
  'Allemagne': '#0096d6',
  'Germany': '#0096d6',
  'BE': '#f59e0b',
  'Belgique': '#f59e0b',
  'Belgium': '#f59e0b',
}

interface Station {
  nom: string
  pays: string
}

interface MapComponentProps {
  stations: Station[]
  selectedStation: string | null
  onStationSelect: (stationName: string) => void
}

function findCoords(name: string): [number, number] | undefined {
  if (STATION_COORDS[name]) return STATION_COORDS[name]
  const lower = name.toLowerCase()
  for (const [key, coords] of Object.entries(STATION_COORDS)) {
    if (lower.includes(key.toLowerCase()) || key.toLowerCase().includes(lower)) {
      return coords
    }
  }
  return undefined
}

export default function MapComponent({ stations, selectedStation, onStationSelect }: MapComponentProps) {
  const mapped = stations
    .map(s => ({ ...s, coords: findCoords(s.nom) }))
    .filter((s): s is Station & { coords: [number, number] } => s.coords !== undefined)

  return (
    <MapContainer
      center={[48.5, 4.5]}
      zoom={5}
      style={{ height: '500px', width: '100%', borderRadius: '12px' }}
    >
      <TileLayer
        attribution='&copy; <a href="https://carto.com/">CartoDB</a>'
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        subdomains="abcd"
      />
      {mapped.map(station => (
        <CircleMarker
          key={station.nom}
          center={station.coords}
          radius={selectedStation === station.nom ? 10 : 7}
          pathOptions={{
            fillColor: COUNTRY_COLORS[station.pays] ?? '#6366f1',
            color: selectedStation === station.nom ? '#ffffff' : 'transparent',
            fillOpacity: 0.9,
            weight: 2,
          }}
          eventHandlers={{ click: () => onStationSelect(station.nom) }}
        >
          <Popup>
            <div style={{ minWidth: 130 }}>
              <div style={{ fontWeight: 600, marginBottom: 4 }}>{station.nom}</div>
              <div style={{ fontSize: 11, color: '#aaa' }}>{station.pays}</div>
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  )
}
