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

const CACHE_TTL_MS = 2 * 1000;
const cellCacheExpiry = new Map();
const cellCacheLoaded = new Map();

function getCacheKey(sheet) {
  return String(sheet.sheetId);
}

function isCacheValid(key) {
  return Date.now() < (cellCacheExpiry.get(key) ?? 0);
}

function invalidateCache(sheet) {
  const key = getCacheKey(sheet);
  cellCacheExpiry.delete(key);
  cellCacheLoaded.delete(key);
}

export function invalidateSheetCache(sheet) {
  invalidateCache(sheet);
}

async function loadFullJoueursSheetCached(sheet) {
  const key = getCacheKey(sheet);
  if (isCacheValid(key)) return;
  await sheet.loadCells("A:H");
  cellCacheExpiry.set(key, Date.now() + CACHE_TTL_MS);
  cellCacheLoaded.set(key, true);
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

  const jwt = getJwtClient();
  const doc = new GoogleSpreadsheet(sheetId, jwt);
  await doc.loadInfo();
  return doc;
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
    invalidateCache(joueursSheet);
    await joueursSheet.loadCells(`F${rowByName}:F${rowByName}`);
    joueursSheet.getCell(rowByName - 1, COL_ID_DISCORD - 1).value = String(discordId);
    await joueursSheet.saveUpdatedCells();
    return true;
  }

  const row = await findFirstEmptyRowInColumnF(joueursSheet);
  if (row > joueursSheet.rowCount) {
    await joueursSheet.resize({ rowCount: row });
  }

  invalidateCache(joueursSheet);
  await joueursSheet.loadCells(`A${row}:H${row}`);

  joueursSheet.getCell(row - 1, COL_NOM - 1).value = userName;
  joueursSheet.getCell(row - 1, COL_SOLDE - 1).value = "0 €";
  joueursSheet.getCell(row - 1, COL_CUMUL - 1).value = "0 €";
  joueursSheet.getCell(row - 1, COL_ID_DISCORD - 1).value = String(discordId);
  joueursSheet.getCell(row - 1, COL_PARTICIPATIONS - 1).value = 0;
  joueursSheet.getCell(row - 1, COL_REGEAR - 1).value = 0;

  await joueursSheet.saveUpdatedCells();
  return true;
}

export async function updateUserName(joueursSheet, discordId, newName) {
  const row = await findUserRowIndexByDiscordId(joueursSheet, discordId);
  if (!row) return false;

  await loadFullJoueursSheet(joueursSheet);
  const currentName = String(joueursSheet.getCell(row - 1, COL_NOM - 1).value ?? "").trim();
  if (currentName === String(newName).trim()) return false;

  invalidateCache(joueursSheet);
  await joueursSheet.loadCells(`C${row}:C${row}`);
  joueursSheet.getCell(row - 1, COL_NOM - 1).value = newName;
  await joueursSheet.saveUpdatedCells();
  return true;
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
  await activitesSheet.loadHeaderRow(3);
  const rows = await activitesSheet.getRows();
  const items = [];
  for (const r of rows) {
    const id = String(r.get("ID") ?? "").trim();
    if (!id) continue;
    const title = String(r.get("TITRE") ?? r.get("Titre") ?? r.get("NAME") ?? "").trim();
    const date = String(r.get("DATE") ?? r.get("Date") ?? "").trim();
    items.push({ id, title, date });
  }
  items.reverse();
  return items.slice(0, limit);
}

export async function getActivityById(activitesSheet, id) {
  await activitesSheet.loadHeaderRow(3);
  const rows = await activitesSheet.getRows();
  const target = String(id);
  for (const r of rows) {
    const rid = String(r.get("ID") ?? "").trim();
    if (rid === target) {
      const obj = {};
      for (const k of Object.keys(r.toObject())) obj[k] = r.get(k);
      return obj;
    }
  }
  return null;
}
