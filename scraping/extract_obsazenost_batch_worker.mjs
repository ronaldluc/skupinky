import fs from 'node:fs/promises'
import path from 'node:path'
import crypto from 'node:crypto'
import { chromium } from 'playwright'

const ROOT = path.resolve('.')
const URL = 'https://data.mpsv.cz/portal/sestavy/statistiky/detske-skupiny/evidence-poskytovatelu'
const BATCH_FILE = process.env.BATCH_FILE
const BATCH_ID = process.env.BATCH_ID || 'unknown'
const RUN_DIR = process.env.RUN_DIR
const STEP_SLEEP_MS = Number(process.env.STEP_SLEEP_MS || '100')
const SKIP_EXISTING_DIR = process.env.SKIP_EXISTING_DIR ? path.resolve(process.env.SKIP_EXISTING_DIR) : null

if (!BATCH_FILE) {
  throw new Error('BATCH_FILE env var is required')
}
if (!RUN_DIR) {
  throw new Error('RUN_DIR env var is required')
}

const OUT_DIR = path.resolve(RUN_DIR)
const ALL_DIR = path.join(OUT_DIR, 'all_ds')
const STATE_DIR = path.join(OUT_DIR, 'state')
const META_FILE = path.join(STATE_DIR, `batch_${BATCH_ID}_meta.json`)
const DONE_FILE = path.join(STATE_DIR, `batch_${BATCH_ID}_done.json`)
const ERR_FILE = path.join(STATE_DIR, `batch_${BATCH_ID}_errors.json`)
const PROGRESS_FILE = path.join(STATE_DIR, `batch_${BATCH_ID}_progress.json`)

const DAYS = new Set(['Pondělí', 'Úterý', 'Středa', 'Čtvrtek', 'Pátek', 'Sobota', 'Neděle'])
const HEADERS = [
  'den',
  'datum',
  'před 6:00',
  '6:00-7:00',
  '7:00-8:00',
  '8:00-9:00',
  '9:00-10:00',
  '10:00-11:00',
  '11:00-12:00',
  '12:00-13:00',
  '13:00-14:00',
  '14:00-15:00',
  '15:00-16:00',
  '16:00-17:00',
  '17:00-18:00',
  'po 18:00',
  'orientační počet volných míst',
]

function slugify(value) {
  return String(value || 'unknown')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .toLowerCase()
    .slice(0, 120)
}

function normalizeText(s) {
  return String(s || '').replace(/\s+/g, ' ').trim()
}

function toIsoDate(dmy) {
  const m = /^(\d{2})\.(\d{2})\.(\d{2})$/.exec(dmy)
  if (!m) return dmy
  return `20${m[3]}-${m[2]}-${m[1]}`
}

function toCsv(headers, rows) {
  const esc = (v) => {
    const s = String(v ?? '')
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s
  }
  return [headers.map(esc).join(','), ...rows.map((r) => r.map(esc).join(','))].join('\n')
}

async function disableCookieOverlay(page) {
  await page.evaluate(() => {
    const portal = document.getElementById('portal')
    if (portal) {
      portal.style.pointerEvents = 'none'
      portal.innerHTML = ''
    }
  })
}

async function stepPause(page) {
  if (STEP_SLEEP_MS > 0) {
    await page.waitForTimeout(STEP_SLEEP_MS)
  }
}

async function clearAndFill(locator, value, page) {
  const clickClearX = async () => {
    const clicked = await locator.evaluate((el) => {
      const root = el.parentElement || el
      const nodes = Array.from(root.querySelectorAll('button,[role="button"],span,div,i'))
      for (const n of nodes) {
        const txt = `${n.textContent || ''} ${n.getAttribute('aria-label') || ''} ${n.getAttribute('title') || ''}`.toLowerCase().trim()
        if (txt === 'x' || txt.includes('×') || txt.includes('clear') || txt.includes('vymaz') || txt.includes('odstran')) {
          n.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }))
          return true
        }
      }
      return false
    })
    if (clicked) {
      await stepPause(page)
      return true
    }
    return false
  }

  await locator.click({ force: true })
  await stepPause(page)
  await clickClearX()
  await clickClearX()
  await page.keyboard.press('Control+A')
  await stepPause(page)
  await page.keyboard.press('Backspace')
  await stepPause(page)
  if (value) {
    await page.keyboard.type(value, { delay: 5 })
    await stepPause(page)
  }
  await page.keyboard.press('Enter')
  await stepPause(page)
}

async function pickFirstResultRow(frame, page) {
  const rowCell = frame.locator('[role="row"]').nth(1).locator('[role="gridcell"]').first()
  await rowCell.click({ force: true, timeout: 8000 })
  await stepPause(page)
}

async function openObsazenost(frame, page) {
  await frame.getByText('OBSAZENOST 1 DS').first().click({ timeout: 10000, force: true })
  await stepPause(page)
  await page.waitForTimeout(1400)
}

async function backToList(frame, page) {
  const back = frame.getByText('ZPĚT NA VÝBĚR DS').first()
  if (await back.count()) {
    await back.click({ force: true })
    await stepPause(page)
    await page.waitForTimeout(900)
  }
}

async function clearVisualSelection(frame, page) {
  const resetTexts = ['RESET FILTRŮ', 'Reset filtrů', 'Reset filters']
  for (const t of resetTexts) {
    const btn = frame.getByText(t, { exact: true }).first()
    if (await btn.count()) {
      await btn.click({ force: true }).catch(() => {})
      await stepPause(page)
      return
    }
  }

  const clearLabels = ['Vymazat výběr', 'Clear selections', 'Clear selection']
  for (const label of clearLabels) {
    const btn = frame.getByTitle(label).first()
    if (await btn.count()) {
      await btn.click({ force: true }).catch(() => {})
      await stepPause(page)
      return
    }
  }
  for (const label of clearLabels) {
    const btn = frame.getByLabel(label).first()
    if (await btn.count()) {
      await btn.click({ force: true }).catch(() => {})
      await stepPause(page)
      return
    }
  }
}

async function extractVisibleRows(frame) {
  const text = await frame.locator('body').innerText()
  const lines = text
    .split(/\r?\n/)
    .map((x) => x.trim())
    .filter(Boolean)

  const start = lines.indexOf('OBSAZENOST DĚTSKÉ SKUPINY')
  if (start < 0) return []

  let end = lines.length
  for (const marker of ['EVIDENCE POSKYTOVATELŮ', 'řádek se vybral', 'SOUHRNNÁ DATA']) {
    const idx = lines.indexOf(marker, start + 1)
    if (idx > 0) end = Math.min(end, idx)
  }

  const blacklist = new Set([
    'Posunout nahoru',
    'Posunout dolů',
    'Posunout doleva',
    'Posunout doprava',
    'Výběr řádku',
    'Vybrat řádek',
    'den',
    'datum',
    'před 6:00',
    '6:00-7:00',
    '7:00-8:00',
    '8:00-9:00',
    '9:00-10:00',
    '10:00-11:00',
    '11:00-12:00',
    '12:00-13:00',
    '13:00-14:00',
    '14:00-15:00',
    '15:00-16:00',
    '16:00-17:00',
    '17:00-18:00',
    'po 18:00',
    'orientační počet volných míst',
    'OBSAZENOST DĚTSKÉ SKUPINY',
  ])

  const work = lines.slice(start, end).filter((x) => !blacklist.has(x))
  const out = []
  for (let i = 0; i < work.length; i += 1) {
    const day = work[i]
    const date = work[i + 1]
    if (!DAYS.has(day)) continue
    if (!/^\d{2}\.\d{2}\.\d{2}$/.test(date || '')) continue
    const values = work.slice(i + 2, i + 17)
    if (values.length < 15) continue
    out.push([day, date, ...values])
    i += 16
  }
  return out
}

async function extractOne(frame, page, item) {
  const providerSearch = frame.locator('div[placeholder="Vyhledat podle názvu poskytovatele dětské skupiny"]').first()
  const dsSearch = frame.locator('div[placeholder="Vyhledat podle názvu dětské skupiny"]').first()
  const strictPick = process.env.STRICT_PICK === '1'

  await clearVisualSelection(frame, page)
  await clearAndFill(providerSearch, normalizeText(item.nazev_poskytovatele), page)
  await stepPause(page)
  await clearAndFill(dsSearch, normalizeText(item.nazev_ds), page)
  await stepPause(page)

  if (strictPick) {
    const picked = await frame.evaluate((args) => {
      const fold = (x) => String(x || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\s+/g, ' ').trim().toLowerCase()
      const tds = fold(args.ds)
      const tpr = fold(args.provider)
      const rows = Array.from(document.querySelectorAll('[role="row"]')).slice(1)
      for (const tr of rows) {
        const cells = Array.from(tr.querySelectorAll('[role="gridcell"]'))
        if (cells.length < 4) continue
        const provider = fold(cells[1]?.textContent || '')
        const ds = fold(cells[3]?.textContent || '')
        const dsMatch = ds === tds || ds.includes(tds) || tds.includes(ds)
        const providerMatch = !tpr || provider.includes(tpr) || tpr.includes(provider)
        if (dsMatch && providerMatch) {
          const clickCell = cells[0] || cells[3]
          clickCell.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }))
          return true
        }
      }
      return false
    }, { ds: item.nazev_ds, provider: item.nazev_poskytovatele })
    await stepPause(page)
    if (!picked) {
      throw new Error(`DS row not found after filtering: ${item.nazev_ds}`)
    }
  } else {
    await pickFirstResultRow(frame, page)
  }

  await stepPause(page)
  await openObsazenost(frame, page)
  await stepPause(page)

  const byDate = new Map()
  let stagnant = 0
  for (let i = 0; i < 80; i += 1) {
    const rows = await extractVisibleRows(frame)
    let changed = false
    for (const row of rows) {
      const date = toIsoDate(row[1])
      if (!byDate.has(date)) {
        byDate.set(date, [row[0], date, ...row.slice(2).map((x) => Number(String(x).replace(',', '.')))])
        changed = true
      }
    }
    if (changed) stagnant = 0
    else stagnant += 1
    if (stagnant >= 6) break

    await page.keyboard.press('PageDown')
    await stepPause(page)
  }

  await backToList(frame, page)
  return [...byDate.values()].sort((a, b) => a[1].localeCompare(b[1]))
}

async function saveProgress(total, done, errors) {
  await fs.writeFile(PROGRESS_FILE, JSON.stringify({ total, done, errors }, null, 2), 'utf8')
}

async function initSession() {
  const browser = await chromium.launch({ headless: process.env.HEADFUL === '1' ? false : true })
  const page = await browser.newPage({ locale: 'cs-CZ' })
  page.setDefaultTimeout(35000)

  let loaded = false
  let loadErr = null
  for (let attempt = 1; attempt <= 5; attempt += 1) {
    try {
      await page.goto(URL, { waitUntil: 'domcontentloaded', timeout: 60000 })
      loaded = true
      break
    } catch (e) {
      loadErr = e
      await page.waitForTimeout(1500 * attempt)
    }
  }
  if (!loaded) throw loadErr || new Error('page.goto failed')

  const cookieBtn = page.getByRole('button', { name: 'Souhlasím se všemi' })
  if (await cookieBtn.count()) {
    await cookieBtn.first().click({ timeout: 2000 }).catch(() => {})
    await stepPause(page)
  }

  await disableCookieOverlay(page)
  await page.getByRole('tab', { name: 'Power BI' }).click()
  await stepPause(page)
  await page.waitForTimeout(8000)
  await disableCookieOverlay(page)

  const frame = page.frame({ url: /app\.powerbi\.com\/reportEmbed/ })
  if (!frame) throw new Error('Power BI iframe nenalezen.')
  return { browser, page, frame }
}

async function main() {
  await fs.mkdir(ALL_DIR, { recursive: true })
  await fs.mkdir(STATE_DIR, { recursive: true })

  const seed = JSON.parse(await fs.readFile(path.resolve(BATCH_FILE), 'utf8'))

  let { browser, page, frame } = await initSession()

  const forceRewrite = process.env.FORCE_REWRITE === '1'
  const enforceUniquePayload = process.env.ENFORCE_UNIQUE_PAYLOAD === '1'
  const payloadHashes = new Set()
  const restartRetries = new Map()

  const done = []
  const errors = []

  for (let i = 0; i < seed.length; i += 1) {
    const item = seed[i]
    const filename = `${String(item.index).padStart(5, '0')}_${slugify(item.nazev_ds)}__${slugify(item.nazev_poskytovatele)}.csv`
    const fullpath = path.join(ALL_DIR, filename)

    if (!forceRewrite) {
      try {
        await fs.access(fullpath)
        done.push({ index: item.index, file: path.relative(ROOT, fullpath), skipped: true, rows: null })
        await saveProgress(seed.length, done.length, errors.length)
        continue
      } catch (_e) {
        // file does not exist, continue with extraction
      }
      if (SKIP_EXISTING_DIR) {
        const canonicalPath = path.join(SKIP_EXISTING_DIR, filename)
        try {
          await fs.access(canonicalPath)
          done.push({ index: item.index, file: path.relative(ROOT, canonicalPath), skipped: true, rows: null })
          await saveProgress(seed.length, done.length, errors.length)
          continue
        } catch (_e) {
          // file does not exist, continue with extraction
        }
      }
    }

    try {
      const rows = await extractOne(frame, page, item)
      if (!rows.length) {
        throw new Error(`Empty obsazenost export for DS: ${item.nazev_ds}`)
      }
      if (enforceUniquePayload) {
        const payload = rows.map((r) => r.join(',')).join('\n')
        const hash = crypto.createHash('md5').update(payload).digest('hex')
        if (payloadHashes.has(hash)) {
          throw new Error(`Duplicate payload export for DS: ${item.nazev_ds}`)
        }
        payloadHashes.add(hash)
      }

      await fs.writeFile(fullpath, toCsv(HEADERS, rows), 'utf8')
      done.push({ index: item.index, file: path.relative(ROOT, fullpath), skipped: false, rows: rows.length })
    } catch (e) {
      const msg = String(e?.message || e)
      if (msg.includes('Target page, context or browser has been closed')) {
        const n = (restartRetries.get(item.index) || 0) + 1
        restartRetries.set(item.index, n)
        if (n <= 3) {
          try {
            await browser.close()
          } catch (_ignore) {
            // ignore close errors
          }
          ;({ browser, page, frame } = await initSession())
          i -= 1
          await saveProgress(seed.length, done.length, errors.length)
          continue
        }
      }

      errors.push({
        index: item.index,
        ds: item.nazev_ds,
        provider: item.nazev_poskytovatele,
        error: msg,
      })
      await backToList(frame, page).catch(() => {})
    }

    await saveProgress(seed.length, done.length, errors.length)
  }

  const meta = {
    batch_id: BATCH_ID,
    extracted_at: new Date().toISOString(),
    source_url: URL,
    total_ds: seed.length,
    completed: done.length,
    errors: errors.length,
    output_dir: path.relative(ROOT, ALL_DIR),
  }

  await fs.writeFile(META_FILE, JSON.stringify(meta, null, 2), 'utf8')
  await fs.writeFile(DONE_FILE, JSON.stringify(done, null, 2), 'utf8')
  await fs.writeFile(ERR_FILE, JSON.stringify(errors, null, 2), 'utf8')

  await browser.close()
  console.log(JSON.stringify(meta))
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
