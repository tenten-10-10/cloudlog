// Google Apps Script Web App: JSON POST -> Spreadsheet appendRow
// Deploy as Web App (use /exec)
//
// Requirements:
// - Spreadsheet fixed by ID (no getActiveSpreadsheet())
// - Sheet auto-create + header row
// - doPost JSON parse + secret verify + appendRow
// - Same-day (JST) dedupe by asin + "|" + price
// - doGet keeps compatibility (health check + query append)
// - doOptions for preflight (best-effort CORS)

const SPREADSHEET_ID = "166Bq0EO_qu4L1cpZCUmsZeaDi_kleBU9oVSR2igF7O4";
const SHEET_NAME = "log";
const SECRET = "potluck_secret_123";

const HEADER = ["ts", "title", "price", "asin", "queue_url", "brand", "priority"];

function doGet(e) {
  try {
    const p = (e && e.parameter) || {};
    const hasAppend = HEADER.slice(1).some((k) => {
      const v = p[k];
      return v != null && String(v).trim() !== "";
    });

    if (!hasAppend) {
      return json_({ ok: true, alive: true, sheet: SHEET_NAME, id: SPREADSHEET_ID });
    }

    if (!isAuthorized_(p.secret)) {
      return json_({ ok: false, error: "unauthorized" }, 401);
    }

    const res = appendWithDedup_(p);
    return json_(res);
  } catch (err) {
    Logger.log("doGet error: %s", err && err.stack ? err.stack : String(err));
    return json_({ ok: false, error: "internal_error" }, 500);
  }
}

function doPost(e) {
  try {
    let payload = {};
    try {
      payload = parseJsonBody_(e);
    } catch (err) {
      Logger.log("bad_json: %s", err && err.stack ? err.stack : String(err));
      return json_({ ok: false, error: "bad_json" }, 400);
    }

    if (!isAuthorized_(payload.secret)) {
      return json_({ ok: false, error: "unauthorized" }, 401);
    }

    const res = appendWithDedup_(payload);
    return json_(res);
  } catch (err) {
    Logger.log("doPost error: %s", err && err.stack ? err.stack : String(err));
    return json_({ ok: false, error: "internal_error" }, 500);
  }
}

function doOptions(e) {
  // Preflight endpoint (best-effort; Apps Script may ignore headers).
  return json_({});
}

function json_(obj, code) {
  const out = ContentService.createTextOutput(JSON.stringify(obj || {})).setMimeType(ContentService.MimeType.JSON);
  applyCors_(out);
  // Apps Script Web Apps can't reliably set HTTP status codes, so we return code in JSON only if needed.
  // Keep response always JSON as requested.
  return out;
}

function applyCors_(out) {
  // Some environments expose setHeader; others don't. Best-effort.
  try {
    out.setHeader("Access-Control-Allow-Origin", "*");
    out.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    out.setHeader("Access-Control-Allow-Headers", "Content-Type");
  } catch (err) {
    // ignore
  }
}

function isAuthorized_(provided) {
  return String(provided || "") === String(SECRET || "");
}

function parseJsonBody_(e) {
  const raw = e && e.postData && e.postData.contents ? String(e.postData.contents) : "";
  if (!raw) return {};
  return JSON.parse(raw);
}

function getSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_NAME);
  ensureHeader_(sh);
  return sh;
}

function ensureHeader_(sh) {
  const lastRow = sh.getLastRow();
  if (lastRow < 1) {
    sh.getRange(1, 1, 1, HEADER.length).setValues([HEADER]);
    return;
  }
  const first = sh.getRange(1, 1, 1, HEADER.length).getValues()[0];
  const same = HEADER.every((h, i) => String(first[i] || "").trim() === h);
  if (same) return;
  sh.insertRowBefore(1);
  sh.getRange(1, 1, 1, HEADER.length).setValues([HEADER]);
}

function appendWithDedup_(p) {
  const lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    const sh = getSheet_();

    const now = new Date();
    const ts = Utilities.formatDate(now, "Asia/Tokyo", "yyyy/MM/dd HH:mm:ss");
    const day = Utilities.formatDate(now, "Asia/Tokyo", "yyyy/MM/dd");

    const title = String((p && p.title) || "");
    const price = String((p && p.price) || "").trim();
    const asin = String((p && p.asin) || "").trim();
    const queueUrl = String((p && p.queue_url) || "");
    const brand = String((p && p.brand) || "");
    const priority = String((p && p.priority) || "");

    if (isDuplicateToday_(sh, day, asin, price)) {
      return { ok: true, appended: false, skipped: "duplicate" };
    }

    sh.appendRow([ts, title, price, asin, queueUrl, brand, priority]);
    return { ok: true, appended: true };
  } finally {
    lock.releaseLock();
  }
}

function isDuplicateToday_(sh, day, asin, price) {
  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return false; // only header

  // Scan from the bottom; stop once we leave today's rows (assuming append is chronological).
  const values = sh.getRange(2, 1, lastRow - 1, 4).getValues();
  const asinS = String(asin || "").trim();
  const priceS = String(price || "").trim();

  for (let i = values.length - 1; i >= 0; i--) {
    const row = values[i];
    const tsCell = row[0];
    let rowDay = "";
    try {
      if (tsCell instanceof Date) {
        rowDay = Utilities.formatDate(tsCell, "Asia/Tokyo", "yyyy/MM/dd");
      } else {
        const s = String(tsCell || "");
        const m = s.match(/(\d{4})[\\/-](\d{2})[\\/-](\d{2})/);
        rowDay = m ? `${m[1]}/${m[2]}/${m[3]}` : s.slice(0, 10);
      }
    } catch (err) {
      const s = String(tsCell || "");
      const m = s.match(/(\d{4})[\\/-](\d{2})[\\/-](\d{2})/);
      rowDay = m ? `${m[1]}/${m[2]}/${m[3]}` : s.slice(0, 10);
    }
    if (rowDay && rowDay !== day) break;
    const rowPrice = String(row[2] || "").trim();
    const rowAsin = String(row[3] || "").trim();
    if (rowAsin === asinS && rowPrice === priceS) return true;
  }
  return false;
}
