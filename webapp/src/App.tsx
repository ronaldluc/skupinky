import { useEffect, useRef, useState, useCallback } from 'react'
import maplibregl from 'maplibre-gl'
import type { Map as MapLibreMap, MapLayerMouseEvent, GeoJSONSource } from 'maplibre-gl'

type DSPoint = {
  id: string
  index: number
  name: string
  is_sds: boolean
  provider: string
  provider_full: string
  kraj: string
  okres: string
  obec: string
  address: string
  capacity: number | null
  status: 'green' | 'orange' | 'red' | 'unknown'
  free_min: number | null
  free_max: number | null
  weekend_active: boolean
  free_by_day: Record<string, number | null>
  website_url: string | null
  search_query: string
  lon: number
  lat: number
}

type KrajStat = {
  kraj: string
  ds_count_total: number
  capacity_total: number
  free_min_total: number
  green: number
  orange: number
  red: number
  unknown: number
}

const DAY_ORDER = ['Pondělí', 'Úterý', 'Středa', 'Čtvrtek', 'Pátek', 'Sobota', 'Neděle'] as const
const DAY_SHORT: Record<string, string> = {
  'Pondělí': 'Po', 'Úterý': 'Út', 'Středa': 'St',
  'Čtvrtek': 'Čt', 'Pátek': 'Pá', 'Sobota': 'So', 'Neděle': 'Ne',
}

function fmtNum(v: number | null | undefined): string {
  if (v === null || v === undefined) return '–'
  const n = Number(v)
  return Number.isFinite(n) ? String(Math.round(n * 10) / 10) : '–'
}

function statusLabel(status: string): string {
  switch (status) {
    case 'green': return 'Volné každý pracovní den'
    case 'orange': return 'Volné jen některé dny'
    case 'red': return 'Pravděpodobně plně obsazeno'
    default: return 'Nedostatek dat'
  }
}

function toFeatureCollection(points: DSPoint[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: points.map((p) => ({
      type: 'Feature' as const,
      geometry: { type: 'Point' as const, coordinates: [p.lon, p.lat] },
      properties: {
        id: p.id,
        index: p.index,
        name: p.name,
        provider: p.provider,
        is_sds: p.is_sds,
        address: p.address,
        obec: p.obec,
        okres: p.okres,
        kraj: p.kraj,
        capacity: p.capacity,
        status: p.status,
        free_min: p.free_min,
        free_max: p.free_max,
        weekend_active: p.weekend_active,
        website_url: p.website_url,
        search_query: p.search_query,
        free_po: p.free_by_day['Pondělí'] ?? null,
        free_ut: p.free_by_day['Úterý'] ?? null,
        free_st: p.free_by_day['Středa'] ?? null,
        free_ct: p.free_by_day['Čtvrtek'] ?? null,
        free_pa: p.free_by_day['Pátek'] ?? null,
        free_so: p.free_by_day['Sobota'] ?? null,
        free_ne: p.free_by_day['Neděle'] ?? null,
      },
    })),
  }
}

const DAY_PROP: Record<string, string> = {
  'Pondělí': 'free_po', 'Úterý': 'free_ut', 'Středa': 'free_st',
  'Čtvrtek': 'free_ct', 'Pátek': 'free_pa', 'Sobota': 'free_so', 'Neděle': 'free_ne',
}

function dayVal(props: Record<string, unknown>, day: string): string {
  const v = props[DAY_PROP[day]]
  if (v === null || v === undefined || v === '') return '–'
  return fmtNum(Number(v))
}

function popupHtml(p: Record<string, unknown>, withWebBtn: boolean): string {
  const dayHeader = DAY_ORDER.map((d) => `<span class="pp-dh">${DAY_SHORT[d]}</span>`).join('')
  const dayValues = DAY_ORDER.map((d) => {
    const v = dayVal(p, d)
    return `<span class="pp-dc">${v}</span>`
  }).join('')

  const url = (p.website_url as string) || `https://www.google.com/search?q=${encodeURIComponent(p.search_query as string)}`
  const webBtn = withWebBtn
    ? `<a class="pp-web" href="${url}" target="_blank" rel="noopener noreferrer">Web &#x2197;</a>`
    : ''

  return `<article class="pp">
    <h3 class="pp-title">${p.name}</h3>
    <p class="pp-addr">${p.address || ''}</p>
    <div class="pp-metrics">
      <p>Volná místa: <strong>${fmtNum(p.free_min as number)} / ${fmtNum(p.capacity as number)}</strong></p>
      <p>${statusLabel(p.status as string)}</p>
    </div>
    <div class="pp-table">
      <div class="pp-head">${dayHeader}</div>
      <div class="pp-vals">${dayValues}</div>
    </div>
    ${webBtn}
  </article>`
}

export default function App() {
  const mapRef = useRef<MapLibreMap | null>(null)
  const mapNodeRef = useRef<HTMLDivElement | null>(null)
  const allPointsRef = useRef<DSPoint[]>([])
  const [freeSum, setFreeSum] = useState(0)
  const [capSum, setCapSum] = useState(0)
  const [onlySds, setOnlySds] = useState(false)
  const [showGreen, setShowGreen] = useState(false)
  const [showOrange, setShowOrange] = useState(false)
  const [panelOpen, setPanelOpen] = useState(true)
  const readyRef = useRef(false)

  const filtersRef = useRef({ onlySds, showGreen, showOrange })
  filtersRef.current = { onlySds, showGreen, showOrange }

  const updateViewport = useCallback(() => {
    const map = mapRef.current
    if (!map || !readyRef.current) return

    const { onlySds: sds, showGreen: grn, showOrange: ora } = filtersRef.current
    const bounds = map.getBounds()
    const w = bounds.getWest(), e = bounds.getEast(), s = bounds.getSouth(), n = bounds.getNorth()

    let pts = allPointsRef.current
    if (sds) pts = pts.filter((p) => p.is_sds)
    if (grn || ora) {
      pts = pts.filter((p) => (grn && p.status === 'green') || (ora && p.status === 'orange'))
    }

    const inView = pts.filter((p) => p.lon >= w && p.lon <= e && p.lat >= s && p.lat <= n)

    const src = map.getSource('ds') as GeoJSONSource | undefined
    if (src) src.setData(toFeatureCollection(inView))

    let fr = 0, cp = 0
    for (const p of inView) {
      if (p.free_min != null) fr += p.free_min
      if (p.capacity != null) cp += p.capacity
    }
    setFreeSum(Math.round(fr))
    setCapSum(Math.round(cp))
  }, [])

  useEffect(() => {
    updateViewport()
  }, [onlySds, showGreen, showOrange, updateViewport])

  useEffect(() => {
    let destroyed = false

    void Promise.all([
      fetch('/data/ds_points.json').then((r) => r.json()),
      fetch('/data/kraj_stats.json').then((r) => r.json()),
      fetch('/data/kraje.geojson').then((r) => r.json()),
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ]).then(([pts, kstats, krajeGeo]: [DSPoint[], KrajStat[], any]) => {
      if (destroyed) return
      allPointsRef.current = pts

      const kstatsMap = new Map<string, KrajStat>()
      for (const s of kstats) kstatsMap.set(s.kraj, s)

      if (!mapNodeRef.current) return
      const map = new maplibregl.Map({
        container: mapNodeRef.current,
        style: 'https://tiles.openfreemap.org/styles/liberty',
        center: [15.3, 49.85],
        zoom: 7,
        minZoom: 5.5,
        maxZoom: 18,
      })
      mapRef.current = map

      map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right')

      map.on('load', () => {
        if (destroyed) return
        readyRef.current = true

        const enriched = {
          ...krajeGeo,
          features: (krajeGeo.features as Array<Record<string, unknown>>).map((f: Record<string, unknown>) => {
            const s = kstatsMap.get(f.name as string)
            return {
              ...f,
              properties: {
                ...(f.properties as Record<string, unknown> || {}),
                kraj_name: f.name,
                ds_count_total: s?.ds_count_total ?? 0,
                capacity_total: s?.capacity_total ?? 0,
              },
            }
          }),
        }

        map.addSource('kraje', { type: 'geojson', data: enriched })
        map.addLayer({
          id: 'kraje-line',
          type: 'line',
          source: 'kraje',
          paint: { 'line-color': '#243447', 'line-width': ['interpolate', ['linear'], ['zoom'], 6, 1.0, 10, 1.6], 'line-opacity': 0.55 },
        })

        map.addSource('ds', { type: 'geojson', data: toFeatureCollection([]) })

        map.addLayer({
          id: 'ds-layer',
          type: 'circle',
          source: 'ds',
          filter: ['==', ['get', 'is_sds'], false],
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 4, 12, 7],
            'circle-color': ['match', ['get', 'status'], 'green', '#22c55e', 'orange', '#f59e0b', 'red', '#ef4444', '#94a3b8'],
            'circle-stroke-color': '#0f172a',
            'circle-stroke-width': 1.2,
            'circle-opacity': 0.92,
          },
        })

        map.addLayer({
          id: 'sds-layer',
          type: 'circle',
          source: 'ds',
          filter: ['==', ['get', 'is_sds'], true],
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 10, 12, 16],
            'circle-color': ['match', ['get', 'status'], 'green', '#16a34a', 'orange', '#d97706', 'red', '#dc2626', '#64748b'],
            'circle-stroke-color': '#f8fafc',
            'circle-stroke-width': 2.4,
            'circle-opacity': 0.95,
          },
        })

        map.addLayer({
          id: 'hover-hit',
          type: 'circle',
          source: 'ds',
          paint: {
            'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, ['case', ['==', ['get', 'is_sds'], true], 16, 10], 12, ['case', ['==', ['get', 'is_sds'], true], 22, 14]],
            'circle-color': '#000',
            'circle-opacity': 0,
            'circle-stroke-width': 0,
          },
        })

        const hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 14 })
        const clickPopup = new maplibregl.Popup({ closeButton: true, closeOnClick: true, offset: 14 })
        let clickPopupOpen = false

        clickPopup.on('close', () => { clickPopupOpen = false })

        const onHover = (e: MapLayerMouseEvent) => {
          if (clickPopupOpen) return
          const f = e.features?.[0]
          if (!f) return
          const p = f.properties as Record<string, unknown>
          map.getCanvas().style.cursor = 'pointer'
          hoverPopup.setLngLat(e.lngLat).setHTML(popupHtml(p, false)).addTo(map)
        }

        map.on('mouseenter', 'hover-hit', onHover)
        map.on('mousemove', 'hover-hit', onHover)
        map.on('mouseleave', 'hover-hit', () => { map.getCanvas().style.cursor = ''; if (!clickPopupOpen) hoverPopup.remove() })

        map.on('click', 'hover-hit', (e: MapLayerMouseEvent) => {
          const f = e.features?.[0]
          if (!f) return
          const p = f.properties as Record<string, unknown>
          hoverPopup.remove()
          clickPopupOpen = true
          clickPopup.setLngLat(e.lngLat).setHTML(popupHtml(p, true)).addTo(map)
        })

        map.on('click', (e: maplibregl.MapMouseEvent) => {
          if (!clickPopupOpen) return
          const features = map.queryRenderedFeatures(e.point, { layers: ['hover-hit'] })
          if (!features.length) { clickPopup.remove(); clickPopupOpen = false }
        })

        map.on('moveend', updateViewport)
        updateViewport()

        navigator.geolocation?.getCurrentPosition(
          (pos) => { map.flyTo({ center: [pos.coords.longitude, pos.coords.latitude], zoom: 12 }) },
          () => {},
          { enableHighAccuracy: false, timeout: 6000 },
        )
      })
    })

    return () => { destroyed = true; mapRef.current?.remove(); mapRef.current = null; readyRef.current = false }
  }, [updateViewport])

  return (
    <>
      <div className="map-container" ref={mapNodeRef} />

      {/* Mobile toggle */}
      <button
        className="panel-toggle"
        onClick={() => setPanelOpen((v) => !v)}
        aria-label={panelOpen ? 'Skrýt panel' : 'Zobrazit panel'}
      >
        {panelOpen ? '✕' : '☰'}
      </button>

      <aside className={`panel ${panelOpen ? '' : 'panel-hidden'}`}>
        <h1>Dostupnost dětských skupin v&nbsp;ČR</h1>

        <div className="stat-line">
          Na obrazovce <strong>{freeSum}/{capSum}</strong> volných míst
        </div>

        <div className="filters">
          <label className={`chip ${onlySds ? 'chip-on' : ''}`}>
            <input type="checkbox" checked={onlySds} onChange={(e) => setOnlySds(e.target.checked)} />
            Pouze sousedské dětské skupiny
          </label>
          <label className={`chip chip-green ${showGreen ? 'chip-green-on' : ''}`}>
            <input type="checkbox" checked={showGreen} onChange={(e) => setShowGreen(e.target.checked)} />
            Volné každý den
          </label>
          <label className={`chip chip-green ${showOrange ? 'chip-green-on' : ''}`}>
            <input type="checkbox" checked={showOrange} onChange={(e) => setShowOrange(e.target.checked)} />
            Volné některé dny
          </label>
        </div>

        <div className="legend">
          <div className="legend-row"><span className="dot dot-green" />Volné každý pracovní den</div>
          <div className="legend-row"><span className="dot dot-orange" />Volné jen některé dny</div>
          <div className="legend-row"><span className="dot dot-red" />Pravděpodobně plně obsazeno</div>
          <div className="legend-row"><span className="dot dot-grey" />Nedostatek dat</div>
        </div>

        <p className="disclaimer">
          Data z ledna 2026 z otevřených dat MPSV.<br />
          Odhad volných míst je pouze orientační.
        </p>
      </aside>
    </>
  )
}
