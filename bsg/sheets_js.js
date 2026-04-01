import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { JWT } from "google-auth-library";
import { GoogleSpreadsheet } from "google-spreadsheet";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const SCOPES = [
  "https://spreadsheets.google.com/feeds",
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive.file",
  "https://www.googleapis.com/auth/drive",
];

export const COL_NOM = 3;
export const COL_SOLDE = 4;
export const COL_CUMUL = 5;
export const COL_ID_DISCORD = 6;
export const COL_PARTICIPATIONS = 7;
export const COL_REGEAR = 8;

function getCacheTtlMs() {
  const rawMs = process.env.SHEET_CACHE_TTL_MS;
  if (rawMs != null && String(rawMs).trim() !== "") {
    const n = Number(rawMs);
    if (Number.isFinite(n) && n >= 0) return Math.trunc(n);
  }

  const rawSec = process.env.SHEET_CACHE_TTL_SECONDS;
  if (rawSec != null && String(rawSec).trim() !== "") {
    const n = Number(rawSec);
    if (Number.isFinite(n) && n >= 0) return Math.trunc(n * 1000);
  }

  // Par défaut: 60s pour éviter d'exploser le quota lors des sync/rafales.
  return 60 * 1000;
}

function getDocCacheTtlMs() {
  const rawMs = process.env.SHEET_DOC_CACHE_TTL_MS;
  if (rawMs != null && String(rawMs).trim() !== "") {
    const n = Number(rawMs);
    if (Number.isFinite(n) && n >= 0) return Math.trunc(n);
  }
  return 15 * 60 * 1000;
}

function getActivitiesCacheTtlMs() {
  const rawMs = process.env.SHEET_ACTIVITIES_CACHE_TTL_MS;
  if (rawMs != null && String(rawMs).trim() !== "") {
    const n = Number(rawMs);
    if (Number.isFinite(n) && n >= 0) return Math.trunc(n);
  }
  return 60 * 1000;
}

function getActivitiesHeaderRow() {
  const raw = process.env.SHEET_ACTIVITIES_HEADER_ROW;
  const n = raw == null ? 3 : Number(raw);
  return Number.isFinite(n) && n >= 1 ? Math.trunc(n) : 3;
}

function getActivitiesScanRows() {
  const raw = process.env.SHEET_ACTIVITIES_SCAN_ROWS;
  const n = raw == null ? 400 : Number(raw);
  return Number.isFinite(n) && n >= 1 ? Math.trunc(n) : 400;
}

function getActivitiesFullScanMaxRows() {
  const raw = process.env.SHEET_ACTIVITIES_FULL_SCAN_MAX_ROWS;
  const n = raw == null ? 2000 : Number(raw);
  return Number.isFinite(n) && n >= 1 ? Math.trunc(n) : 2000;
}

const CACHE_TTL_MS = getCacheTtlMs();
const DOC_CACHE_TTL_MS = getDocCacheTtlMs();
const ACTIVITIES_CACHE_TTL_MS = getActivitiesCacheTtlMs();

const cellCacheExpiry = new Map();
const cellCacheLoaded = new Map();
const cellCacheInFlight = new Map();

let docCache = null;
let docCacheSheetId = null;
let docCacheLoadedAt = 0;
let docCacheInFlight = null;

const activitiesCache = new Map();
const activitiesInFlight = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getHeader(obj, name) {
  const h = obj?.response?.headers;
  if (!h) return null;
  if (typeof h.get === "function") return h.get(name);
  return h[name] ?? h[name.toLowerCase()] ?? null;
}

function getRetryAfterMs(err) {
  const v = getHeader(err, "retry-after");
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;
  const n = Number(s);
  if (Number.isFinite(n) && n >= 0) return Math.trunc(n * 1000);
  const d = Date.parse(s);
  if (!Number.isNaN(d)) {
    const ms = d - Date.now();
    return ms > 0 ? ms : 0;
  }
  return null;
}

function isRateLimitError(err) {
  const code = err?.code ?? err?.response?.status ?? err?.response?.statusCode;
  if (code === 429) return true;
  const msg = String(err?.message ?? "").toLowerCase();
  return msg.includes("quota exceeded") || msg.includes("rate limit") || msg.includes("[429]");
}

async function withSheetsBackoff(fn, { retries = 6, baseDelayMs = 500, maxDelayMs = 15_000 } = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (!isRateLimitError(err) || attempt >= retries) throw err;
      attempt++;

      const retryAfterMs = getRetryAfterMs(err);
      const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
      const jitter = Math.floor(Math.random() * 250);
      const delay = Math.min(maxDelayMs, retryAfterMs ?? exp + jitter);
      await sleep(delay);
    }
  }
}

function getCacheKey(sheet) {
  return String(sheet.sheetId);
}

function colToA1(colIndex1Based) {
  let n = Number(colIndex1Based);
  if (!Number.isFinite(n) || n < 1) return "A";
  n = Math.trunc(n);
  let s = "";
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function normHeader(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function baseKey(key) {
  const raw = String(key ?? "");
  const idx = raw.indexOf("__");
  return idx === -1 ? raw : raw.slice(0, idx);
}

function isCacheValid(key) {
  return Date.now() < (cellCacheExpiry.get(key) ?? 0);
}

function touchCache(sheet) {
  const key = getCacheKey(sheet);
  cellCacheExpiry.set(key, Date.now() + CACHE_TTL_MS);
  cellCacheLoaded.set(key, true);
}

function invalidateCache(sheet) {
  const key = getCacheKey(sheet);
  cellCacheExpiry.delete(key);
  cellCacheLoaded.delete(key);
  cellCacheInFlight.delete(key);
}

export function invalidateSheetCache(sheet) {
  invalidateCache(sheet);
}

async function loadFullJoueursSheetCached(sheet) {
  const key = getCacheKey(sheet);
  if (isCacheValid(key)) return;

  const inFlight = cellCacheInFlight.get(key);
  if (inFlight) {
    await inFlight;
    return;
  }

  const p = (async () => {
    await withSheetsBackoff(() => sheet.loadCells("A:H"));
    cellCacheExpiry.set(key, Date.now() + CACHE_TTL_MS);
    cellCacheLoaded.set(key, true);
  })();

  cellCacheInFlight.set(key, p);
  try {
    await p;
  } finally {
    cellCacheInFlight.delete(key);
  }
}

function getHeaderRows() {
  const raw = process.env.SHEET_HEADER_ROWS;
  const n = raw == null ? 3 : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : 3;
}

function parseMoney(value) {
  if (value == null) return null;
  const s = String(value)
    .replace(/€/g, "")
    .replace(/\s+/g, "")
    .replace(/,/g, ".")
    .trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseIntSafe(value) {
  const n = Number(String(value ?? "").trim());
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function findCredentialsFile() {
  const envPath = process.env.GOOGLE_SERVICE_ACCOUNT_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (envPath) return envPath;

  const candidates = [
    path.join(__dirname, "credentials.json"),
    path.join(__dirname, "service_account.json"),
  ];
  for (const c of candidates) {
    if (fs.existsSync(c)) return c;
  }
  return null;
}

export function getJwtClient() {
  const credPath = findCredentialsFile();
  if (!credPath) {
    throw new Error(
      "Credentials introuvables. Mets `credentials.json` à la racine du projet ou définis GOOGLE_SERVICE_ACCOUNT_FILE."
    );
  }

  const json = JSON.parse(fs.readFileSync(credPath, "utf8"));
  if (!json.client_email || !json.private_key) {
    throw new Error(`Fichier credentials invalide: ${credPath} (client_email/private_key manquants)`);
  }

  return new JWT({
    email: json.client_email,
    key: json.private_key,
    scopes: SCOPES,
  });
}

export async function getDoc() {
  const sheetId = process.env.SHEET_ID;
  if (!sheetId) throw new Error("SHEET_ID manquant (variable d'environnement)");

  if (
    docCache &&
    docCacheSheetId === sheetId &&
    Date.now() - docCacheLoadedAt < DOC_CACHE_TTL_MS
  ) {
    return docCache;
  }

  if (docCacheInFlight && docCacheSheetId === sheetId) {
    return await docCacheInFlight;
  }

  docCacheSheetId = sheetId;
  docCacheInFlight = (async () => {
    const jwt = getJwtClient();
    const doc = new GoogleSpreadsheet(sheetId, jwt);
    await withSheetsBackoff(() => doc.loadInfo());
    docCache = doc;
    docCacheLoadedAt = Date.now();
    return doc;
  })();

  try {
    return await docCacheInFlight;
  } finally {
    docCacheInFlight = null;
  }
}

export async function getSheetByName(name) {
  const doc = await getDoc();
  const sheet = doc.sheetsByTitle[name];
  if (!sheet) throw new Error(`Onglet introuvable: ${name}`);
  return sheet;
}

async function loadFullJoueursSheet(joueursSheet) {
  await loadFullJoueursSheetCached(joueursSheet);
}

export async function countRegisteredUsers(joueursSheet) {
  await loadFullJoueursSheet(joueursSheet);
  const ids = new Set();
  for (let row = 1; row <= joueursSheet.rowCount; row++) {
    const cell = joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1);
    const v = String(cell.value ?? "").trim();
    if (v) ids.add(v);
  }
  return ids.size;
}

export async function findUserRowIndexByDiscordId(joueursSheet, discordId) {
  await loadFullJoueursSheet(joueursSheet);
  const target = String(discordId);
  for (let row = 1; row <= joueursSheet.rowCount; row++) {
    const cell = joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1);
    const v = String(cell.value ?? "").trim();
    if (v && v === target) return row;
  }
  return null;
}

async function findFirstEmptyRowInColumnF(joueursSheet) {
  await loadFullJoueursSheet(joueursSheet);
  const startRow = getHeaderRows() + 1;
  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const cell = joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1);
    const v = String(cell.value ?? "").trim();
    if (!v) return row;
  }
  return joueursSheet.rowCount + 1;
}

function normName(s) {
  return String(s ?? "").trim().toLowerCase().replace(/\s+/g, " ");
}

function namesMatch(sheetName, discordName) {
  const a = normName(sheetName);
  const b = normName(discordName);
  return a === b || a.includes(b) || b.includes(a);
}

async function findRowByNameWithoutId(joueursSheet, userName) {
  await loadFullJoueursSheet(joueursSheet);
  const startRow = getHeaderRows() + 1;
  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const idCell = joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1);
    const idVal = String(idCell.value ?? "").trim();
    if (idVal) continue;
    const nameCell = joueursSheet.getCell(row - 1, COL_NOM - 1);
    const nameVal = String(nameCell.value ?? "").trim();
    if (nameVal && namesMatch(nameVal, userName)) return row;
  }
  return null;
}

export async function addUser(joueursSheet, discordId, userName) {
  const existing = await findUserRowIndexByDiscordId(joueursSheet, discordId);
  if (existing) return false;

  const rowByName = await findRowByNameWithoutId(joueursSheet, userName);
  if (rowByName) {
    const key = getCacheKey(joueursSheet);
    const fullLoaded = isCacheValid(key) && cellCacheLoaded.get(key);
    if (!fullLoaded) {
      invalidateCache(joueursSheet);
      await withSheetsBackoff(() => joueursSheet.loadCells(`F${rowByName}:F${rowByName}`));
    }

    joueursSheet.getCell(rowByName - 1, COL_ID_DISCORD - 1).value = String(discordId);
    await withSheetsBackoff(() => joueursSheet.saveUpdatedCells());
    touchCache(joueursSheet);
    return true;
  }

  const row = await findFirstEmptyRowInColumnF(joueursSheet);
  const prevRowCount = joueursSheet.rowCount;
  const resized = row > prevRowCount;
  if (resized) {
    await withSheetsBackoff(() => joueursSheet.resize({ rowCount: row }));
  }

  const key = getCacheKey(joueursSheet);
  const fullLoaded = isCacheValid(key) && cellCacheLoaded.get(key);
  if (!fullLoaded || resized) {
    invalidateCache(joueursSheet);
    await withSheetsBackoff(() => joueursSheet.loadCells(`A${row}:H${row}`));
  }

  joueursSheet.getCell(row - 1, COL_NOM - 1).value = userName;
  joueursSheet.getCell(row - 1, COL_SOLDE - 1).value = "0 €";
  joueursSheet.getCell(row - 1, COL_CUMUL - 1).value = "0 €";
  joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1).value = String(discordId);
  joueursSheet.getCell(row - 1, COL_PARTICIPATIONS - 1).value = 0;
  joueursSheet.getCell(row - 1, COL_REGEAR - 1).value = 0;

  await withSheetsBackoff(() => joueursSheet.saveUpdatedCells());
  touchCache(joueursSheet);
  return true;
}

export async function updateUserName(joueursSheet, discordId, newName) {
  const row = await findUserRowIndexByDiscordId(joueursSheet, discordId);
  if (!row) return false;

  await loadFullJoueursSheet(joueursSheet);
  const currentName = String(joueursSheet.getCell(row - 1, COL_NOM - 1).value ?? "").trim();
  if (currentName === String(newName).trim()) return false;

  const key = getCacheKey(joueursSheet);
  const fullLoaded = isCacheValid(key) && cellCacheLoaded.get(key);
  if (!fullLoaded) {
    invalidateCache(joueursSheet);
    await withSheetsBackoff(() => joueursSheet.loadCells(`C${row}:C${row}`));
  }

  joueursSheet.getCell(row - 1, COL_NOM - 1).value = newName;
  await withSheetsBackoff(() => joueursSheet.saveUpdatedCells());
  touchCache(joueursSheet);
  return true;
}

function normNameSimple(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function namesRoughlyMatchSimple(a, b) {
  const x = normNameSimple(a);
  const y = normNameSimple(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}

export async function bulkUpsertUsers(
  joueursSheet,
  users,
  { updateNames = true, fillDefaultsForNewRows = true } = {}
) {
  const list = Array.isArray(users) ? users : [];
  const startRow = getHeaderRows() + 1;

  // Toujours charger une vue cohérente une seule fois.
  await loadFullJoueursSheet(joueursSheet);

  const idToRow = new Map();
  const nameNoIdRows = []; // { row, name }
  const freeRows = []; // rows with empty ID (can be reused)

  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const idCell = joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1);
    const id = String(idCell.value ?? "").trim();
    const nameCell = joueursSheet.getCell(row - 1, COL_NOM - 1);
    const name = String(nameCell.value ?? "").trim();

    if (id) {
      if (!idToRow.has(id)) idToRow.set(id, row);
      continue;
    }

    freeRows.push(row);
    if (name) nameNoIdRows.push({ row, name });
  }

  const initialRowCount = joueursSheet.rowCount;
  let nextAppendRow = initialRowCount + 1;
  let requiredRowCount = initialRowCount;

  const newRows = []; // { row, discordId, userName }

  let processed = 0;
  let filledIds = 0;
  let renamed = 0;
  let created = 0;

  const allocateRow = () => {
    if (freeRows.length) return freeRows.shift();
    const row = nextAppendRow++;
    newRows.push({ row, discordId: null, userName: null });
    requiredRowCount = Math.max(requiredRowCount, row);
    return row;
  };

  for (const u of list) {
    const discordId = String(u?.discordId ?? "").trim();
    const userName = String(u?.userName ?? "").trim();
    if (!discordId) continue;
    processed++;

    const existingRow = idToRow.get(discordId) ?? null;
    if (existingRow) {
      if (updateNames && userName) {
        const cell = joueursSheet.getCell(existingRow - 1, COL_NOM - 1);
        const cur = String(cell.value ?? "").trim();
        if (cur !== userName) {
          cell.value = userName;
          renamed++;
        }
      }
      continue;
    }

    // Essaye de matcher une ligne existante avec nom mais sans ID.
    let matched = null;
    if (userName) {
      for (let i = 0; i < nameNoIdRows.length; i++) {
        const it = nameNoIdRows[i];
        if (!it) continue;
        if (namesRoughlyMatchSimple(it.name, userName)) {
          matched = it;
          nameNoIdRows.splice(i, 1);
          break;
        }
      }
    }

    const row = matched?.row ?? allocateRow();
    idToRow.set(discordId, row);

    if (row <= initialRowCount) {
      // Ligne existante: on peut écrire tout de suite.
      joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1).value = discordId;
      filledIds++;

      const nameCell = joueursSheet.getCell(row - 1, COL_NOM - 1);
      const curName = String(nameCell.value ?? "").trim();
      if (userName && !curName) nameCell.value = userName;
      continue;
    }

    // Nouvelle ligne: on applique après resize/loadCells.
    const slot = newRows.find((x) => x.row === row);
    if (slot) {
      slot.discordId = discordId;
      slot.userName = userName;
    } else {
      newRows.push({ row, discordId, userName });
    }
  }

  if (requiredRowCount > initialRowCount) {
    const oldCount = initialRowCount;
    await withSheetsBackoff(() => joueursSheet.resize({ rowCount: requiredRowCount }));

    // Après resize, charger les cellules des nouvelles lignes.
    const lastColA1 = colToA1(8);
    await withSheetsBackoff(() =>
      joueursSheet.loadCells(`A${oldCount + 1}:${lastColA1}${requiredRowCount}`)
    );

    for (const nr of newRows) {
      if (!nr?.discordId) continue;
      const row = nr.row;
      joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1).value = String(nr.discordId);
      filledIds++;

      if (nr.userName) {
        joueursSheet.getCell(row - 1, COL_NOM - 1).value = String(nr.userName);
      }

      if (fillDefaultsForNewRows) {
        joueursSheet.getCell(row - 1, COL_SOLDE - 1).value = "0 €";
        joueursSheet.getCell(row - 1, COL_CUMUL - 1).value = "0 €";
        joueursSheet.getCell(row - 1, COL_PARTICIPATIONS - 1).value = 0;
        joueursSheet.getCell(row - 1, COL_REGEAR - 1).value = 0;
      }
      created++;
    }
  }

  await withSheetsBackoff(() => joueursSheet.saveUpdatedCells());
  touchCache(joueursSheet);

  return { processed, filledIds, renamed, created };
}

function objectToLowerMap(obj) {
  const m = new Map();
  for (const [k, v] of Object.entries(obj ?? {})) m.set(String(k).toLowerCase(), v);
  return m;
}

async function loadActivitiesObjectsCached(activitesSheet, { mode = "tail" } = {}) {
  const key = getCacheKey(activitesSheet);
  const cacheKey = `${key}:activities:${String(mode)}`;
  const cached = activitiesCache.get(cacheKey);
  if (cached && Date.now() < cached.expiresAt) return cached.items;

  const inFlight = activitiesInFlight.get(cacheKey);
  if (inFlight) return await inFlight;

  const p = (async () => {
    // On évite loadHeaderRow/getRows car ça exige des headers uniques.
    const headerRow = getActivitiesHeaderRow();
    const firstDataRow = headerRow + 1;

    const maxCols = Math.max(1, Math.trunc(activitesSheet.columnCount ?? 1));
    const sheetRows = Math.max(firstDataRow, Math.trunc(activitesSheet.rowCount ?? firstDataRow));
    const maxRows = sheetRows;
    const lastColA1 = colToA1(maxCols);

    // Charge uniquement le header + une fenêtre de lignes (tail/head) par défaut.
    const scanRows = getActivitiesScanRows();
    let startDataRow = firstDataRow;
    let endDataRow = maxRows;
    const m = String(mode ?? "tail").toLowerCase();
    if (m === "tail") {
      startDataRow = Math.max(firstDataRow, maxRows - scanRows + 1);
      endDataRow = maxRows;
    } else if (m === "head") {
      startDataRow = firstDataRow;
      endDataRow = Math.min(maxRows, firstDataRow + scanRows - 1);
    } else {
      // full
      startDataRow = firstDataRow;
      endDataRow = maxRows;
    }

    await withSheetsBackoff(() => activitesSheet.loadCells(`A${headerRow}:${lastColA1}${headerRow}`));
    if (startDataRow <= endDataRow) {
      await withSheetsBackoff(() => activitesSheet.loadCells(`A${startDataRow}:${lastColA1}${endDataRow}`));
    }

    const headerCounts = new Map();
    const headerKeys = [];
    for (let c = 1; c <= maxCols; c++) {
      const cell = activitesSheet.getCell(headerRow - 1, c - 1);
      const raw = String(cell?.value ?? cell?.formattedValue ?? "").trim();
      if (!raw) {
        headerKeys.push(null);
        continue;
      }
      const base = raw;
      const n = (headerCounts.get(base) ?? 0) + 1;
      headerCounts.set(base, n);
      const keyName = n === 1 ? base : `${base}__${n}`;
      headerKeys.push(keyName);
    }

    const items = [];
    for (let r = startDataRow; r <= endDataRow; r++) {
      const obj = {};
      let hasAny = false;
      for (let c = 1; c <= maxCols; c++) {
        const k = headerKeys[c - 1];
        if (!k) continue;
        const cell = activitesSheet.getCell(r - 1, c - 1);
        const v = cell?.formattedValue ?? cell?.value;
        const s = v == null ? "" : String(v).trim();
        if (s) hasAny = true;
        obj[k] = v;
      }
      if (!hasAny) continue;
      items.push(obj);
    }

    activitiesCache.set(cacheKey, { expiresAt: Date.now() + ACTIVITIES_CACHE_TTL_MS, items });
    return items;
  })();

  activitiesInFlight.set(cacheKey, p);
  try {
    return await p;
  } finally {
    activitiesInFlight.delete(cacheKey);
  }
}

function readCellText(cell) {
  if (cell.formattedValue != null) return String(cell.formattedValue).trim();
  return String(cell.value ?? "").trim();
}

export async function getBalance(joueursSheet, discordId) {
  const row = await findUserRowIndexByDiscordId(joueursSheet, discordId);
  if (!row) return null;
  await loadFullJoueursSheet(joueursSheet);
  const name = String(joueursSheet.getCell(row - 1, COL_NOM - 1).value ?? "").trim();
  const balance = readCellText(joueursSheet.getCell(row - 1, COL_SOLDE - 1));
  const cumulative = readCellText(joueursSheet.getCell(row - 1, COL_CUMUL - 1));
  return { name, balance, cumulative };
}

export async function listRegisteredDiscordUsers(joueursSheet) {
  await loadFullJoueursSheet(joueursSheet);
  const startRow = getHeaderRows() + 1;
  const out = [];
  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const id = String(joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1).value ?? "").trim();
    if (!id) continue;
    const name = String(joueursSheet.getCell(row - 1, COL_NOM - 1).value ?? "").trim();
    out.push({ discordId: id, sheetName: name });
  }
  return out;
}

export async function getTopPlayers(joueursSheet, sortColIndex, topN = 10) {
  const startRow = getHeaderRows() + 1;
  await loadFullJoueursSheet(joueursSheet);

  const rows = [];
  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const name = String(joueursSheet.getCell(row - 1, COL_NOM - 1).value ?? "").trim();
    const raw = joueursSheet.getCell(row - 1, sortColIndex - 1).value;
    if (!name) continue;

    let value = null;
    if (sortColIndex === COL_SOLDE || sortColIndex === COL_CUMUL) value = parseMoney(raw);
    else value = parseIntSafe(raw);

    if (value == null) continue;
    rows.push({ name, value });
  }

  rows.sort((a, b) => b.value - a.value);
  return rows.slice(0, topN);
}

export async function getColumnSum(joueursSheet, colIndex) {
  const startRow = getHeaderRows() + 1;
  await loadFullJoueursSheet(joueursSheet);
  let sum = 0;
  for (let row = startRow; row <= joueursSheet.rowCount; row++) {
    const raw = joueursSheet.getCell(row - 1, colIndex - 1).value;
    const n = parseIntSafe(raw);
    if (n != null) sum += n;
  }
  return sum;
}

export async function listActivities(activitesSheet, limit = 10) {
  // Par défaut on scanne la fin (dernières activités). Si la sheet a été agrandie
  // et contient beaucoup de lignes vides en bas, on fallback sur un scan du haut.
  let rows = await loadActivitiesObjectsCached(activitesSheet, { mode: "tail" });
  if (!rows.length) rows = await loadActivitiesObjectsCached(activitesSheet, { mode: "head" });
  const items = [];
  for (const obj of rows) {
    const m = objectToLowerMap(obj);

    // Support headers dupliqués: on compare la base du header (avant __2, __3...)
    const findByBase = (wanted) => {
      const w = normHeader(wanted);
      for (const [k, v] of m.entries()) {
        if (normHeader(baseKey(k)) === w) return v;
      }
      return null;
    };

    const id = String(findByBase("id") ?? "").trim();
    if (!id) continue;
    const title = String(
      findByBase("titre") ?? findByBase("title") ?? findByBase("name") ?? ""
    ).trim();
    const date = String(findByBase("date") ?? "").trim();
    items.push({ id, title, date });
  }
  items.reverse();
  return items.slice(0, limit);
}

export async function getActivityById(activitesSheet, id) {
  const tryFind = async (mode) => {
    const rows = await loadActivitiesObjectsCached(activitesSheet, { mode });
    const target = String(id);
    for (const obj of rows) {
      const m = objectToLowerMap(obj);
      let rid = "";
      for (const [k, v] of m.entries()) {
        if (normHeader(baseKey(k)) === "id") {
          rid = String(v ?? "").trim();
          break;
        }
      }
      if (rid === target) return obj;
    }
    return null;
  };

  const hit = await tryFind("tail");
  if (hit) return hit;

  const hit2 = await tryFind("head");
  if (hit2) return hit2;

  // Full scan (optionnel) si la sheet n'est pas gigantesque.
  const maxRows = getActivitiesFullScanMaxRows();
  const rowCount = Math.trunc(activitesSheet.rowCount ?? 0);
  if (rowCount > 0 && rowCount <= maxRows) {
    return await tryFind("full");
  }

  return null;
}
