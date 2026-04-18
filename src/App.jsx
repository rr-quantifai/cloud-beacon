import React, { useState, useMemo, useRef, useCallback, useEffect } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer } from "recharts";
import _ from "lodash";
import Papa from "papaparse";

/* ═══════════════════════════════════════════════════════════════════
   PERSISTENT STORAGE — IndexedDB
   ═══════════════════════════════════════════════════════════════════ */
let idb = null;
const getIDB = () => { if (idb) return idb; idb = new Promise((res, rej) => { const req = indexedDB.open("cloud-beacon", 1); req.onupgradeneeded = (e) => { e.target.result.createObjectStore("kv"); }; req.onsuccess = () => res(req.result); req.onerror = () => { idb = null; rej(req.error); }; }); return idb; };
const psGet = async (k) => { try { const db = await getIDB(); return new Promise(r => { const req = db.transaction("kv","readonly").objectStore("kv").get(k); req.onsuccess = () => r(req.result ?? null); req.onerror = () => r(null); }); } catch { return null; } };
const psSet = async (k, v) => { try { const db = await getIDB(); return new Promise(r => { const tx = db.transaction("kv","readwrite"); tx.objectStore("kv").put(v, k); tx.oncomplete = () => r(); tx.onerror = () => r(); }); } catch {} };
const psDel = async (k) => { try { const db = await getIDB(); return new Promise(r => { const tx = db.transaction("kv","readwrite"); tx.objectStore("kv").delete(k); tx.oncomplete = () => r(); tx.onerror = () => r(); }); } catch {} };

/* ═══════════════════════════════════════════════════════════════════
   UTILITIES
   ═══════════════════════════════════════════════════════════════════ */
const parseCsv = (file) => new Promise((res, rej) => {
  Papa.parse(file, {
    header: true, skipEmptyLines: true, dynamicTyping: false,
    delimitersToGuess: [",", ";", "\t", "|"],
    transformHeader: (h) => h.trim().replace(/^\uFEFF/, ""),
    complete: (r) => res(r.data.map(row => { const o = {}; for (const k in row) o[k.trim()] = (row[k] || "").toString().trim(); return o; })),
    error: () => rej(new Error("Parse failed")),
  });
});
const parseD = (s) => { if (!s) return null; const d = new Date(s + "T00:00:00Z"); return isNaN(d) ? null : d; };
const calcEMA = (vals, p) => { const k = 2 / (p + 1); const r = []; let e = null; for (const v of vals) { e = e === null ? v : v * k + e * (1 - k); r.push(Math.round(e * 100) / 100); } return r; };
const isYes = (v) => (v || "").trim().toLowerCase() === "yes";

/* Month-key helpers */
const mkKey = (y, m) => String(y) + "-" + String(m).padStart(2, "0");
const offsetMonth = (ym, n) => { const [y, m] = ym.split("-").map(Number); const t = y * 12 + (m - 1) + n; return mkKey(Math.floor(t / 12), (t % 12) + 1); };
const getEventMonth = (ds) => { const d = parseD(ds); return d ? mkKey(d.getUTCFullYear(), d.getUTCMonth() + 1) : null; };
const getFwdMonths = (eYM, n) => Array.from({ length: n }, (_, i) => offsetMonth(eYM, i + 1));
const getYoYMonths = (fwd) => fwd.map(ym => offsetMonth(ym, -12));
const getIMMMonths = (eYM, n) => Array.from({ length: n }, (_, i) => offsetMonth(eYM, -(n - i)));

/* ═══════════════════════════════════════════════════════════════════
   CONSTANTS
   ═══════════════════════════════════════════════════════════════════ */
const MO = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
const PROD_COLORS = { AI: "#8b5cf6", BizApps: "#059669", Cloud: "#3b82f6", "Modern Work": "#ec4899", Security: "#f59e0b" };
const PROD_BADGE = { AI: "bg-purple-100 text-purple-700", BizApps: "bg-emerald-100 text-emerald-700", Cloud: "bg-blue-100 text-blue-700", "Modern Work": "bg-pink-100 text-pink-700", Security: "bg-amber-100 text-amber-700" };
const EVENT_HEADERS = ["Event Date","Event Name","Event Type","Event Venue","Product","Country","Provider","Partner ID","Partner Name","Attendee Name","Attendance"];
const SALES_HEADERS = ["Sale Date","Sale Value","Product","Partner ID","Customer Name"];
const detectType = (rows) => { if (!rows || !rows.length) return null; const h = Object.keys(rows[0]).map(k => k.trim().toLowerCase()); if (h.length === EVENT_HEADERS.length && EVENT_HEADERS.every(e => h.includes(e.toLowerCase()))) return "event"; if (h.length === SALES_HEADERS.length && SALES_HEADERS.every(e => h.includes(e.toLowerCase()))) return "sales"; return null; };
const VALID_PRODUCTS = new Set(["AI", "BizApps", "Cloud", "Modern Work", "Security"]);
const VALID_EVENT_TYPES = new Set(["Online", "Offline"]);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const SALE_VALUE_RE = /^\d+(\.\d+)?$/;
const validateRows = (rows, type) => { for (const r of rows) { if (type === "event") { if (!DATE_RE.test(r["Event Date"])) return false; if (!VALID_PRODUCTS.has(r["Product"])) return false; if (!VALID_EVENT_TYPES.has(r["Event Type"])) return false; const att = (r["Attendance"] || "").toLowerCase(); if (att !== "yes" && att !== "no") return false; } else { if (!DATE_RE.test(r["Sale Date"])) return false; if (!SALE_VALUE_RE.test(r["Sale Value"])) return false; if (!VALID_PRODUCTS.has(r["Product"])) return false; } } return true; };
const makeDateStr = (y, m, d) => y + "-" + String(m).padStart(2, "0") + "-" + String(d).padStart(2, "0");
const TEST_EVT = ["events_2024_05.csv","events_2024_12.csv","events_2025_01.csv","events_2025_02.csv","events_2025_03.csv","events_2025_04.csv"];
const TEST_SAL = ["sales_2023_07.csv","sales_2023_08.csv","sales_2023_09.csv","sales_2023_10.csv","sales_2023_11.csv","sales_2023_12.csv","sales_2024_01.csv","sales_2024_02.csv","sales_2024_03.csv","sales_2024_04.csv","sales_2024_05.csv","sales_2024_06.csv","sales_2024_07.csv","sales_2024_08.csv","sales_2024_09.csv","sales_2024_10.csv","sales_2024_11.csv","sales_2024_12.csv","sales_2025_01.csv","sales_2025_02.csv","sales_2025_03.csv","sales_2025_04.csv","sales_2025_05.csv","sales_2025_06.csv","sales_2025_07.csv"];
const MIN_DESKTOP = 1024;
const loadTestData = async () => {
  const load = async (name, dateCol) => { const r = await fetch("/test-data/" + name); const text = await r.text(); const rows = await parseCsv(text); const d = parseD(rows[0]?.[dateCol]); if (!d) return []; const mo = "" + (d.getUTCMonth() + 1), yr = "" + d.getUTCFullYear(); return rows.map(row => ({ ...row, _uMonth: mo, _uYear: yr })); };
  const [evts, sals] = await Promise.all([Promise.all(TEST_EVT.map(f => load(f, "Event Date"))), Promise.all(TEST_SAL.map(f => load(f, "Sale Date")))]);
  return { events: evts.flat(), sales: sals.flat() };
};

/* ═══════════════════════════════════════════════════════════════════
   DATA HELPERS
   ═══════════════════════════════════════════════════════════════════ */
const groupEvents = (evts) => {
  const g = _.groupBy(evts, r => JSON.stringify([r["Event Name"], r["Event Date"], r["Product"]]));
  return Object.entries(g).map(([key, rows]) => {
    const [name, date, prod] = JSON.parse(key);
    const pidAtt = {};
    const partnerDetails = {};
    for (const r of rows) {
      const pid = (r["Partner ID"] || "").trim(); if (!pid) continue;
      if (!partnerDetails[pid]) partnerDetails[pid] = { regs: 0, att: 0 };
      partnerDetails[pid].regs++;
      const yes = isYes(r["Attendance"]);
      if (yes) { partnerDetails[pid].att++; pidAtt[pid] = true; }
      else if (!(pid in pidAtt)) pidAtt[pid] = false;
    }
    const attendingPIDs = Object.keys(pidAtt).filter(p => pidAtt[p]);
    const nonAttendingPIDs = Object.keys(pidAtt).filter(p => !pidAtt[p]);
    const yesRows = rows.filter(r => isYes(r["Attendance"]));
    return { key, eventName: name, eventDate: date, product: prod, eventType: rows[0]?.["Event Type"], venue: rows[0]?.["Event Venue"], country: rows[0]?.["Country"], provider: rows[0]?.["Provider"], totalRegistrations: Object.keys(pidAtt).length, totalPartners: attendingPIDs.length, totalAttendees: yesRows.length, baselineUniverse: nonAttendingPIDs.length, partners: attendingPIDs, nonAttendingPartners: nonAttendingPIDs, partnerDetails, rows };
  });
};

/* ═══════════════════════════════════════════════════════════════════
   CALCULATION ENGINE
   ═══════════════════════════════════════════════════════════════════ */
const makeNull = () => ({ ratioYoY: null, ratioIMM: null, histYoY: 0, fwdYoY: 0, histIMM: 0, fwdIMM: 0, impactPartners: [], eventStatus: null, fwdRange: null, yoyRange: null, immRange: null, fwdAvail: false, yoyAvail: false, immAvail: false });

const idxGet = (idx, mode, vm, pid, prod, ym) => { const k = mode === "total" ? pid + "|||" + ym : pid + "|||" + prod + "|||" + ym; return (vm === "customers" ? (mode === "total" ? idx.custPM.get(k) : idx.custPPM.get(k)) : (mode === "total" ? idx.byPM.get(k) : idx.byPPM.get(k))) || 0; };

const calcEventImpact = (group, idx, mode, fwdN, vm) => {
  const eYM = getEventMonth(group.eventDate);
  if (!eYM) return makeNull();
  const fwd = getFwdMonths(eYM, fwdN), yoy = getYoYMonths(fwd), imm = getIMMMonths(eYM, fwdN);
  const fwdAvail = fwd.every(ym => idx.months.has(ym));
  if (!fwdAvail) return { ...makeNull(), fwdRange: fwd, yoyRange: yoy, immRange: imm };
  const yoyAvail = yoy.every(ym => idx.months.has(ym));
  const immAvail = imm.every(ym => idx.months.has(ym));
  const sum = (pid, ms) => { let s = 0; for (const ym of ms) s += idxGet(idx, mode, vm, pid, group.product, ym); return s; };
  const partners = []; let hY = 0, fY = 0, hI = 0, fI = 0;
  for (const pid of group.partners) {
    const first = idx.partnerFirst.get(pid);
    const fSum = first ? sum(pid, fwd) : 0;
    let rY = null, pHY = 0, pFY = 0;
    if (yoyAvail && first) { const h = sum(pid, yoy); pHY = h; pFY = fSum; hY += h; fY += fSum; if (h > 0) { rY = fSum / h; } else if (fSum > 0) { rY = Infinity; } }
    let rI = null, pHI = 0, pFI = 0;
    if (immAvail && first) { const h = sum(pid, imm); pHI = h; pFI = fSum; hI += h; fI += fSum; if (h > 0) { rI = fSum / h; } else if (fSum > 0) { rI = Infinity; } }
    partners.push({ pid, ratioYoY: rY, ratioIMM: rI, found: !!first, histYoY: pHY, fwdYoY: pFY, histIMM: pHI, fwdIMM: pFI });
  }
  return { ratioYoY: hY > 0 ? fY / hY : (fY > 0 ? Infinity : null), ratioIMM: hI > 0 ? fI / hI : (fI > 0 ? Infinity : null), histYoY: hY, fwdYoY: fY, histIMM: hI, fwdIMM: fI, impactPartners: partners, eventStatus: "ok", fwdRange: fwd, yoyRange: yoy, immRange: imm, fwdAvail: true, yoyAvail, immAvail };
};

const calcBaselines = (group, idx, mode, fwdN, vm) => {
  const eYM = getEventMonth(group.eventDate);
  if (!eYM) return { yoy: null, imm: null };
  const fwd = getFwdMonths(eYM, fwdN);
  if (!fwd.every(ym => idx.months.has(ym))) return { yoy: null, imm: null };
  const yoy = getYoYMonths(fwd), imm = getIMMMonths(eYM, fwdN);
  const yoyAvail = yoy.every(ym => idx.months.has(ym)), immAvail = imm.every(ym => idx.months.has(ym));
  const sum = (pid, ms) => { let s = 0; for (const ym of ms) s += idxGet(idx, mode, vm, pid, group.product, ym); return s; };
  let bF = 0, bY = 0, bI = 0;
  for (const pid of group.nonAttendingPartners) { bF += sum(pid, fwd); if (yoyAvail) bY += sum(pid, yoy); if (immAvail) bI += sum(pid, imm); }
  return {
  yoy: yoyAvail ? (bY > 0 ? bF / bY : (bF > 0 ? Infinity : null)) : null,
  imm: immAvail ? (bI > 0 ? bF / bI : (bF > 0 ? Infinity : null)) : null
};
};

/* ═══════════════════════════════════════════════════════════════════
   DISPLAY HELPERS
   ═══════════════════════════════════════════════════════════════════ */
const descTie = (valFn, dateFn) => (a, b) => { const d = valFn(b) - valFn(a); return d !== 0 ? d : (new Date(dateFn(b)) - new Date(dateFn(a))); };
const DASH = { text: "—", color: "text-gray-400" };
const NO_CSV = { text: "No CSV", color: "text-gray-400" };
const n = (v) => v == null ? "—" : v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const rc = (r) => r == null ? "text-gray-400" : r >= 1.5 ? "text-emerald-600" : r >= 1 ? "text-emerald-500" : r >= 0.8 ? "text-amber-500" : "text-red-500";
const sv = (r, csvOk) => !csvOk ? -1e16 : (r != null && isFinite(r)) ? r : r === Infinity ? -1e11 : -1e15;
const fmtDate = (s, short = true) => { const d = parseD(s); if (!d) return ""; return d.getUTCDate() + "-" + MO[d.getUTCMonth()] + "-" + (short ? String(d.getUTCFullYear()).slice(2) : d.getUTCFullYear()); };
const fmtYM = (ym) => { const [y, m] = ym.split("-").map(Number); return MO[m - 1] + "-" + String(y).slice(2); };
const monthRange = (ms, arrow = "→") => !ms || !ms.length ? "—" : ms.length === 1 ? fmtYM(ms[0]) : fmtYM(ms[0]) + " " + arrow + " " + fmtYM(ms[ms.length - 1]);
const HL = (text, q) => { if (!q) return text; const p = [], lo = text.toLowerCase(), ql = q.toLowerCase(); let last = 0, i; while ((i = lo.indexOf(ql, last)) !== -1) { if (i > last) p.push(text.slice(last, i)); p.push(<span key={i} className="bg-blue-600 text-white rounded-sm px-0.5">{text.slice(i, i + ql.length)}</span>); last = i + ql.length; } if (last < text.length) p.push(text.slice(last)); return p.length ? <>{p}</> : text; };
const formatImpact = (ratio, csvOk) => {
  if (!csvOk) return NO_CSV;
  if (ratio != null && isFinite(ratio)) return { text: n(ratio) + "x", color: rc(ratio) };
  if (ratio === Infinity) return { text: "∞", color: "text-emerald-600" };
  return DASH;
};

/* ═══════════════════════════════════════════════════════════════════
   ATOM COMPONENTS
   ═══════════════════════════════════════════════════════════════════ */
const Dots = () => (<span className="inline-flex items-center gap-1">{[0,1,2].map(i=><span key={i} className="w-1.5 h-1.5 rounded-full bg-gray-300" style={{animation:"dotPulse 1.2s infinite",animationDelay:i*0.2+"s"}}/>)}<style>{`@keyframes dotPulse{0%,80%,100%{opacity:.3;transform:scale(.8)}40%{opacity:1;transform:scale(1)}}`}</style></span>);
const Badge = ({ text, className }) => <span className={"inline-block px-2 py-0.5 text-xs font-medium rounded-full whitespace-nowrap " + className}>{text}</span>;
const TabSwitch = ({ items, active, onChange, disabledKeys }) => (<div className="flex gap-1 bg-gray-100 rounded-lg p-1">{items.map(([k, l]) => { const dis = disabledKeys?.includes(k); return <button key={k} onClick={() => !dis && onChange(k)} className={"px-4 py-2 text-sm font-medium rounded-md transition whitespace-nowrap " + (active === k ? "bg-white text-gray-900 shadow-sm" : dis ? "text-gray-300 cursor-not-allowed" : "text-gray-500 hover:text-gray-700")}>{l}</button>; })}</div>);

/* ═══════════════════════════════════════════════════════════════════
   FILTER COMPONENTS
   ═══════════════════════════════════════════════════════════════════ */
const MultiSel = ({ label, values, onChange, options, placeholder, disabled, dropUp }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  useEffect(() => { if (!open) return; const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }; document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h); }, [open]);
  const toggle = (v) => onChange(values.includes(v) ? values.filter(x => x !== v) : [...values, v]);
  const getLabel = (v) => { const o = options.find(x => (typeof x === "object" ? x.value : "" + x) === v); return typeof o === "object" ? o.label : o != null ? "" + o : v; };
  const dis = disabled || options.length === 0;
  return (
    <div ref={ref} className="relative">
      {label && <label className="block text-xs font-medium text-gray-500 mb-1">{label}</label>}
      <div onClick={() => !dis && setOpen(!open)} className={"w-full px-2.5 py-1.5 text-sm border rounded-lg bg-white h-[36px] flex items-center " + (dis ? "opacity-50 cursor-not-allowed border-gray-200" : values.length > 0 ? "cursor-pointer border-blue-400 hover:border-blue-500" : "cursor-pointer border-gray-200 hover:border-blue-300")}>
        <span className={(values.length > 0 ? "text-blue-600" : "text-gray-400") + " truncate"}>{values.length === 0 ? (placeholder || "All") : values.length === 1 ? getLabel(values[0]) : "Multiple selections"}</span>
      </div>
      {open && !dis && (<div className={"absolute z-50 w-full bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto divide-y divide-gray-100 " + (dropUp ? "bottom-full mb-1" : "mt-1")}>
        {options.map(o => { const v = typeof o === "object" ? o.value : "" + o, l = typeof o === "object" ? o.label : "" + o, chk = values.includes(v);
          return (<div key={v} onClick={() => toggle(v)} className="text-xs cursor-pointer flex items-center gap-2 hover:bg-gray-50" style={{ padding: "12px" }}>
            <div className={"w-3.5 h-3.5 rounded border flex items-center justify-center flex-shrink-0 " + (chk ? "border-blue-600" : "border-gray-300")}>{chk && <svg width="8" height="8" viewBox="0 0 24 24" fill="none" stroke="#2563eb" strokeWidth="4"><path d="M20 6L9 17l-5-5"/></svg>}</div>
            <span className="truncate">{l}</span></div>); })}
      </div>)}
    </div>
  );
};

const DateFilter = ({ tree, selected, onChange }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  useEffect(() => { if (!open) return; const h = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }; document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h); }, [open]);
  const selSet = useMemo(() => new Set(selected), [selected]);
  const yearDates = (y) => y.months.flatMap(m => m.days.map(d => makeDateStr(y.year, m.month, d)));
  const monthDates = (y, m) => m.days.map(d => makeDateStr(y.year, m.month, d));
  const allOf = (ds) => ds.length > 0 && ds.every(d => selSet.has(d));
  const someOf = (ds) => ds.some(d => selSet.has(d));
  const toggleYear = (y) => { const yD = yearDates(y), next = new Set(selSet); if (allOf(yD)) yD.forEach(d => next.delete(d)); else yD.forEach(d => next.add(d)); onChange([...next]); };
  const toggleMonth = (y, m) => { const mD = monthDates(y, m), next = new Set(selSet); if (allOf(mD)) mD.forEach(d => next.delete(d)); else mD.forEach(d => next.add(d)); onChange([...next]); };
  const toggleDate = (ds) => { const next = new Set(selSet); if (next.has(ds)) next.delete(ds); else next.add(ds); onChange([...next]); };
  const Chk = ({ checked, partial, onClick }) => (<div onClick={onClick} className={"w-4 h-4 rounded border flex items-center justify-center flex-shrink-0 cursor-pointer transition " + (checked ? "border-blue-600" : partial ? "border-blue-400" : "border-gray-300 hover:border-gray-400")}>{checked && <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#2563eb" strokeWidth="4"><path d="M20 6L9 17l-5-5"/></svg>}{partial && !checked && <div className="w-2 h-0.5 bg-blue-400 rounded-sm"/>}</div>);
  const displayText = selected.length === 0 ? "All" : selected.length === 1 ? fmtDate(selected[0], false) : "Multiple selections";
  return (
    <div ref={ref} className="relative">
      <label className="block text-xs font-medium text-gray-500 mb-1">Date</label>
      <div onClick={() => setOpen(!open)} className={"w-full px-2.5 py-1.5 text-sm border rounded-lg bg-white h-[36px] flex items-center cursor-pointer " + (selected.length > 0 ? "border-blue-400 hover:border-blue-500" : "border-gray-200 hover:border-blue-300")}>
        <span className={(selected.length > 0 ? "text-blue-600" : "text-gray-400") + " truncate"}>{displayText}</span>
      </div>
      {open && (
        <div className="absolute z-50 mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-72 overflow-y-auto" style={{ minWidth: "100%" }}>
          {tree.map(y => { const yD = yearDates(y), yChk = allOf(yD), ySome = someOf(yD), yExp = ySome || yChk;
            return (<div key={y.year} className="border-b border-gray-100 last:border-b-0">
              <div className="flex items-center gap-2 hover:bg-gray-50" style={{ padding: "12px" }}><Chk checked={yChk} partial={ySome} onClick={() => toggleYear(y)} /><span className="text-xs font-bold text-gray-800 cursor-pointer select-none" onClick={() => toggleYear(y)}>{y.year}</span></div>
              {yExp && y.months.map(m => { const mD = monthDates(y, m), mChk = allOf(mD), mSome = someOf(mD), mExp = mSome || mChk;
                return (<div key={m.month}>
                  <div className="flex items-center gap-2 hover:bg-gray-50 border-t border-dashed border-gray-200" style={{ padding: "12px" }}><Chk checked={mChk} partial={mSome} onClick={() => toggleMonth(y, m)} /><span className="text-xs font-semibold text-gray-700 cursor-pointer select-none" onClick={() => toggleMonth(y, m)}>{MO[m.month - 1] + "-" + y.year.slice(2)}</span></div>
                  {mExp && m.days.map(d => { const ds = makeDateStr(y.year, m.month, d), chk = selSet.has(ds);
                    return (<div key={d} className="flex items-center gap-2 hover:bg-gray-50 cursor-pointer border-t border-dotted border-gray-200" style={{ padding: "12px" }} onClick={() => toggleDate(ds)}><Chk checked={chk} partial={false} onClick={(e) => { e.stopPropagation(); toggleDate(ds); }} /><span className="text-xs text-gray-600">{d + "-" + MO[m.month - 1] + "-" + y.year.slice(2)}</span></div>); })}
                </div>); })}
            </div>); })}
        </div>)}
    </div>
  );
};

/* ═══════════════════════════════════════════════════════════════════
   SECTION COMPONENTS
   ═══════════════════════════════════════════════════════════════════ */
const ChartTip = ({ active, payload, label }) => { if (!active || !payload || !payload.length) return null; const evts = payload[0]?.payload?._events; return (<div className="bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-xs"><p className="font-semibold text-gray-700 mb-1.5">{fmtYM(label)}</p>{evts && evts.map((ev, i) => (<div key={"ev"+i} className="flex items-center gap-2"><div className="w-2 h-2 rounded-full" style={{ backgroundColor: PROD_COLORS[ev.product] || "#6b7280" }} /><span className="text-gray-500">{ev.name}:</span><span className="font-medium text-gray-800 ml-auto">{ev.product}</span></div>))}{payload.filter(p => p.value != null && p.value !== 0).map((p, i) => (<div key={i} className="flex items-center gap-2"><div className="w-2 h-2 rounded-full" style={{ backgroundColor: p.color }} /><span className="text-gray-500">{p.name}:</span><span className="font-medium text-gray-800 ml-auto">{Number(p.value).toLocaleString(undefined,{maximumFractionDigits:0})}</span></div>))}</div>); };

const UploadPanel = ({ uploadState, handleUpload, fileRef }) => {
  const st = uploadState?.status;
  const bc = st==="error"?"border-red-200":st==="partial"?"border-amber-200":st==="success"?"border-emerald-200":"border-blue-300";
  const bg = st==="error"?"bg-red-50 hover:bg-red-100":st==="partial"?"bg-amber-50 hover:bg-amber-100":st==="success"?"bg-emerald-50 hover:bg-emerald-100":"bg-blue-50 hover:bg-blue-100";
  const bs = st==="uploading"||!st?"border-dashed":"";
  return (
    <div className="bg-white rounded-xl border border-gray-200 pt-4 px-4 pb-0 flex flex-col" style={{height:"262px"}}>
      <div className="flex items-center gap-2 mb-3"><div className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center flex-shrink-0"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" strokeWidth="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12"/></svg></div><div><div className="flex items-center"><h2 className="text-sm font-semibold text-gray-900">Data Upload</h2><span className="mx-2 text-gray-300">·</span><a href="https://docs.google.com/spreadsheets/d/1KO4vfas0vqBYjlnG2iJ3EPyvOmzEn5RSBsiLSyOAUeg/edit?gid=0#gid=0" target="_blank" rel="noopener noreferrer" className="text-xs text-indigo-500 hover:text-indigo-700 font-medium">Event Format</a><span className="mx-2 text-gray-300">·</span><a href="https://docs.google.com/spreadsheets/d/1KO4vfas0vqBYjlnG2iJ3EPyvOmzEn5RSBsiLSyOAUeg/edit?gid=1832273612#gid=1832273612" target="_blank" rel="noopener noreferrer" className="text-xs text-violet-500 hover:text-violet-700 font-medium">Sales Format</a></div><p className="text-xs text-gray-400">Upload CSVs — file type is auto-detected</p></div></div>
      <label className={"flex flex-col items-center justify-center w-full flex-1 border-2 rounded-xl cursor-pointer transition mb-4 "+bc+" "+bg+" "+bs}>
        {st==="uploading"?<><p className="text-xs font-medium text-gray-600 mb-3">Processing{uploadState.total>1?" ("+uploadState.current+"/"+uploadState.total+")":""}...</p><div className="w-48 h-1.5 bg-blue-100 rounded-full overflow-hidden"><div className="h-full bg-blue-500 rounded-full transition-all duration-500 ease-out" style={{width:uploadState.progress+"%"}}/></div></>
        :st==="error"?<div className="flex items-center gap-2"><div className="w-5 h-5 rounded-full bg-red-100 flex items-center justify-center"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="3"><path d="M18 6L6 18M6 6l12 12"/></svg></div><span className="text-xs font-semibold text-red-700">{uploadState.message}</span></div>
        :st==="partial"?<div className="flex items-center gap-2"><div className="w-5 h-5 rounded-full bg-amber-100 flex items-center justify-center"><span style={{color:"#d97706",fontSize:"14px",fontWeight:"bold",lineHeight:"1"}}>!</span></div><span className="text-xs font-semibold text-amber-700">{uploadState.message}</span></div>
        :st==="success"?<div className="flex items-center gap-2"><div className="w-5 h-5 rounded-full bg-emerald-100 flex items-center justify-center"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#10b981" strokeWidth="3"><path d="M20 6L9 17l-5-5"/></svg></div><span className="text-xs font-semibold text-emerald-700">{uploadState.message}</span></div>
        :<><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" strokeWidth="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12"/></svg><span className="text-xs text-gray-500 mt-2">Click to upload CSVs</span></>}
        {st!=="uploading"&&<input ref={fileRef} type="file" accept=".csv" multiple className="hidden" onChange={e=>{handleUpload(e.target.files);if(fileRef.current)fileRef.current.value="";}}/>}
      </label>
    </div>
  );
};

const DataCoverage = ({ coverageData, flashKeys }) => {
  const evtYears = Object.keys(coverageData.evt).sort(), salYears = Object.keys(coverageData.sal).sort();
  return (
    <div className="bg-white rounded-xl border border-gray-200 pt-4 px-4 pb-0 flex flex-col" style={{height:"262px"}}>
      <div className="flex items-center gap-2 pb-3 border-b border-gray-100 flex-shrink-0"><div className="w-8 h-8 rounded-lg bg-gray-50 flex items-center justify-center flex-shrink-0"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#6b7280" strokeWidth="2"><rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></svg></div><div><h2 className="text-sm font-semibold text-gray-900">Data Coverage</h2><p className="text-xs text-gray-400">CSVs uploaded</p></div></div>
      <div className={"flex-1 overflow-y-auto flex flex-col " + (evtYears.length===0&&salYears.length===0?"":"pt-3 pb-4")}>
        {evtYears.length===0&&salYears.length===0?<div className="flex flex-col items-center justify-center text-center flex-1"><p className="text-xs text-gray-400">No data uploaded yet</p></div>
        :<div className="flex flex-col">
          {evtYears.length>0&&<div className={"flex flex-col gap-3 "+(salYears.length>0?"border-b border-gray-100 pb-3 mb-3":"")}><p className="text-xs font-semibold text-indigo-600">Event Data</p>{evtYears.map(y=><div key={y} className="flex items-center flex-wrap leading-none"><span className="text-xs font-semibold text-gray-700">{y}</span>{coverageData.evt[y].map(m=><span key={m} className="flex items-center"><span className="mx-1.5 text-gray-300">·</span><span className={"text-xs transition-colors duration-500 "+(flashKeys.includes("evt-"+y+"-"+m)?"text-emerald-600":"text-gray-600")}>{MO[m-1]}</span></span>)}</div>)}</div>}
          {salYears.length>0&&<div className="flex flex-col gap-3"><p className="text-xs font-semibold text-violet-600">Sales Data</p>{salYears.map(y=><div key={y} className="flex items-center flex-wrap leading-none"><span className="text-xs font-semibold text-gray-700">{y}</span>{coverageData.sal[y].map(m=><span key={m} className="flex items-center"><span className="mx-1.5 text-gray-300">·</span><span className={"text-xs transition-colors duration-500 "+(flashKeys.includes("sal-"+y+"-"+m)?"text-emerald-600":"text-gray-600")}>{MO[m-1]}</span></span>)}</div>)}</div>}
        </div>}
      </div>
    </div>
  );
};

const FilterBar = ({ fo, fDates, setFDates, fProd, setFProd, fType, setFType, fVenue, setFVenue, fCountry, setFCountry, fProvider, setFProvider, fName, onNameChange }) => (
  <div className="bg-white rounded-xl border border-gray-200 p-4 mb-6">
    <div className="flex items-center mb-3"><h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Filters</h3><span className="mx-2 text-gray-300">·</span><button onClick={()=>{setFDates([]);setFProd([]);setFType([]);setFVenue([]);setFCountry([]);setFProvider([]);onNameChange("");}} className="text-xs text-blue-600 hover:text-blue-700 font-medium">Reset</button></div>
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-3">
      <div><label className="block text-xs font-medium text-gray-500 mb-1">Name</label><input type="text" value={fName} onChange={e=>onNameChange(e.target.value)} placeholder="Type to find..." className={"w-full px-2.5 py-1.5 text-sm border rounded-lg bg-white h-[36px] placeholder-gray-400 focus:outline-none truncate " + (fName.trim() ? "border-blue-400 text-blue-600 focus:border-blue-500" : "border-gray-200 text-gray-800 focus:border-blue-300")}/></div>
      <DateFilter tree={fo.dateTree} selected={fDates} onChange={setFDates} />
      <MultiSel label="Product" values={fProd} onChange={setFProd} options={fo.prods}/>
      <MultiSel label="Type" values={fType} onChange={setFType} options={fo.types}/>
      <MultiSel label="Venue" values={fVenue} onChange={setFVenue} options={fo.venues}/>
      <MultiSel label="Country" values={fCountry} onChange={setFCountry} options={fo.countries}/>
      <MultiSel label="Provider" values={fProvider} onChange={setFProvider} options={fo.providers}/>
    </div>
  </div>
);

const SummaryCards = ({ summaryData, topPartnersData, globalNameMap }) => {
  const [lb1Page, setLb1Page] = useState(0);
  const [lb1View, setLb1View] = useState("yoy");
  const [lb2Page, setLb2Page] = useState(0);
  const [lb2View, setLb2View] = useState("yoy");
  const lb1Titles = ["Event Leaderboard", "Partner Leaderboard"];
  const lb2Titles = ["Product Comparison", "Type Comparison", "Venue Comparison", "Country Comparison", "Provider Comparison"];
  const lb2Data = [summaryData.product, summaryData.eventType, summaryData.venue, summaryData.country, summaryData.provider];
  const isImp = (v) => v === "yoy" || v === "imm";
  const ViewSw = ({ view, setView }) => (<>{["yoy","imm","attendance"].map(v => (<span key={v} className="flex items-center gap-1.5"><span className="text-xs text-gray-300">·</span><button onClick={()=>setView(v)} className={"text-xs uppercase transition "+(view===v?"font-semibold text-gray-700":"text-gray-400 hover:text-gray-600")}>{v==="yoy"?"Impact YOY":v==="imm"?"Impact IMM":"Attendance"}</button></span>))}</>);
  const DotNav = ({ count, active, onChange }) => (<div className="flex items-center gap-1.5">{Array.from({length:count},(_,i)=>(<button key={i} onClick={()=>onChange(i)} className={"w-1.5 h-1.5 rounded-full transition "+(active===i?"bg-gray-700":"bg-gray-300 hover:bg-gray-400")}/>))}</div>);
  const lb1List = lb1Page===0 ? (lb1View==="yoy"?summaryData.topEventsYoY:lb1View==="imm"?summaryData.topEventsIMM:summaryData.topEventsByAtt) : (lb1View==="yoy"?topPartnersData.impactYoY:lb1View==="imm"?topPartnersData.impactIMM:topPartnersData.attendance);
  const lb2Raw = lb2Data[lb2Page] || [];
  const lb2List = !lb2Raw.length ? [] : lb2View==="yoy" ? [...lb2Raw].filter(x=>x.avgRatioYoY!=null&&isFinite(x.avgRatioYoY)).sort(descTie(x=>x.avgRatioYoY,x=>x.latestDate)) : lb2View==="imm" ? [...lb2Raw].filter(x=>x.avgRatioIMM!=null&&isFinite(x.avgRatioIMM)).sort(descTie(x=>x.avgRatioIMM,x=>x.latestDate)) : [...lb2Raw].sort(descTie(x=>x.totalPartners,x=>x.latestDate));
  return (
    <div className="mb-6 grid grid-cols-1 md:grid-cols-2 gap-4">
      <div className="bg-white rounded-xl border border-gray-200 p-4 h-[128px] flex flex-col">
        <div className="flex items-center justify-between mb-2 flex-shrink-0">
          <div className="flex items-center gap-1.5"><p className="text-xs font-semibold text-gray-500 uppercase tracking-wider">{lb1Titles[lb1Page]}</p><ViewSw view={lb1View} setView={setLb1View}/></div>
          <DotNav count={2} active={lb1Page} onChange={setLb1Page}/>
        </div>
        <div className="flex-1 overflow-y-auto">
          {!lb1List.length?<p className="text-lg font-bold text-gray-400">—</p>:(<div className="flex flex-col divide-y divide-gray-100">{lb1Page===0?lb1List.map(e=>{const val=isImp(lb1View)?formatImpact(lb1View==="yoy"?e.ratioYoY:e.ratioIMM,true):null;return <div key={e.key} className="flex items-center justify-between pt-1.5 last:pb-0 pb-1.5"><span className="text-xs text-gray-700 flex items-center gap-1 min-w-0"><span className="font-medium shrink-0">{e.eventName}</span><span className="text-gray-300">·</span><span className="text-gray-500 truncate">{fmtDate(e.eventDate)}</span><span className="text-gray-300">·</span><span className="text-gray-500 truncate">{e.product}</span></span><span className={"text-xs font-medium shrink-0 ml-2 "+(isImp(lb1View)?val.color:"text-blue-600")}>{isImp(lb1View)?val.text:e.totalPartners}</span></div>;}):lb1List.map(p=>{const name=globalNameMap[p.pid];const val=isImp(lb1View)?formatImpact(p.ratio,true):null;return <div key={p.pid} className="flex items-center justify-between pt-1.5 last:pb-0 pb-1.5"><span className="text-xs text-gray-700 flex items-center gap-1 min-w-0"><span className="font-medium shrink-0">{p.pid}</span>{name&&<><span className="text-gray-300">·</span><span className="text-gray-500 truncate">{name}</span></>}</span><span className={"text-xs font-medium shrink-0 ml-2 "+(isImp(lb1View)?val.color:"text-blue-600")}>{isImp(lb1View)?val.text:p.events}</span></div>;})}</div>)}
        </div>
      </div>
      <div className="bg-white rounded-xl border border-gray-200 p-4 h-[128px] flex flex-col">
        <div className="flex items-center justify-between mb-2 flex-shrink-0">
          <div className="flex items-center gap-1.5"><p className="text-xs font-semibold text-gray-500 uppercase tracking-wider">{lb2Titles[lb2Page]}</p><ViewSw view={lb2View} setView={setLb2View}/></div>
          <DotNav count={5} active={lb2Page} onChange={setLb2Page}/>
        </div>
        <div className="flex-1 overflow-y-auto">
          {!lb2List.length?<p className="text-lg font-bold text-gray-400">—</p>:(<div className="flex flex-col divide-y divide-gray-100">{lb2List.map(d=>{const val=isImp(lb2View)?formatImpact(lb2View==="yoy"?d.avgRatioYoY:d.avgRatioIMM,true):null;return <div key={d.key} className="flex items-center justify-between pt-1.5 last:pb-0 pb-1.5"><span className="text-xs text-gray-700 font-medium">{d.key}</span><span className={"text-xs font-medium shrink-0 ml-2 "+(isImp(lb2View)?val.color:"text-blue-600")}>{isImp(lb2View)?val.text:d.totalPartners}</span></div>;})}</div>)}
        </div>
      </div>
    </div>
  );
};

const PAGE_SIZE = 25;

const EventTable = ({ sortedGrouped, tableOvr, fName, sortCol, sortDir, toggleSort, openModal }) => {
  const [page, setPage] = useState(0);
  const totalPages = Math.max(1, Math.ceil(sortedGrouped.length / PAGE_SIZE));
  const safePage = Math.min(page, totalPages - 1);
  const pageData = sortedGrouped.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);
  useEffect(() => setPage(0), [sortedGrouped]);
  const SH = (col, label) => <th className="px-4 py-3 text-xs font-medium uppercase tracking-wider cursor-pointer select-none hover:text-gray-700 transition whitespace-nowrap" onClick={()=>toggleSort(col)} style={{color:sortCol===col?"#1d4ed8":"#6b7280"}}>{label}</th>;
  const TH = (label) => <th className="px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">{label}</th>;
  return (
  <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
    <div className="p-4 border-b border-gray-100"><div className="flex items-center justify-between">
      <div className="flex items-center gap-3 flex-wrap">
        <span className="text-sm font-semibold text-gray-700">{sortedGrouped.length} Event{sortedGrouped.length!==1?"s":""}</span>
        {tableOvr&&<><span className="text-gray-300">·</span><span className="text-xs text-gray-400">Table Overall</span><span className="text-gray-300">·</span><span className="text-xs font-bold text-gray-500 uppercase">Impact YOY:</span><span className={"text-sm font-bold ml-1 "+tableOvr.yoy.color}>{tableOvr.yoy.text}</span><span className="text-gray-300">·</span><span className="text-xs font-bold text-gray-500 uppercase">Impact IMM:</span><span className={"text-sm font-bold ml-1 "+tableOvr.imm.color}>{tableOvr.imm.text}</span></>}
      </div>
      <div className="flex items-center flex-shrink-0">
        <button onClick={() => setPage(0)} disabled={safePage === 0} className={"px-2 py-1 text-xs font-medium rounded-md transition " + (safePage === 0 ? "text-gray-300 cursor-not-allowed" : "text-gray-500 hover:bg-gray-100")}>First</button>
        <span className="text-gray-300 mx-1">·</span>
        <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={safePage === 0} className={"px-2 py-1 text-xs font-medium rounded-md transition " + (safePage === 0 ? "text-gray-300 cursor-not-allowed" : "text-gray-500 hover:bg-gray-100")}>Prev</button>
        <span className="text-gray-300 mx-1">·</span>
        <span className="px-2 py-1 text-xs font-medium text-gray-500">{safePage + 1} / {totalPages}</span>
        <span className="text-gray-300 mx-1">·</span>
        <button onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={safePage >= totalPages - 1} className={"px-2 py-1 text-xs font-medium rounded-md transition " + (safePage >= totalPages - 1 ? "text-gray-300 cursor-not-allowed" : "text-gray-500 hover:bg-gray-100")}>Next</button>
        <span className="text-gray-300 mx-1">·</span>
        <button onClick={() => setPage(totalPages - 1)} disabled={safePage >= totalPages - 1} className={"pl-2 py-1 text-xs font-medium rounded-md transition " + (safePage >= totalPages - 1 ? "text-gray-300 cursor-not-allowed" : "text-gray-500 hover:bg-gray-100")}>Last</button>
      </div>
    </div></div>
    <div className="overflow-x-auto"><table className="w-full text-sm" style={{minWidth:"2000px"}}><thead><tr className="bg-gray-50 text-left">
      {TH("Name")}{SH("date","Date")}{["Product","Type","Venue","Country","Provider"].map(h=><th key={h} className="px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider">{h}</th>)}
      {SH("registrations","Registrations")}{SH("attendance","Attendance")}{SH("attendees","Attendees")}
      {TH("Historical YOY")}{TH("Historical IMM")}{TH("Forward Period")}
      {SH("ratioYoY","Impact YOY")}{SH("ratioIMM","Impact IMM")}
      {SH("blUniverse","Baseline Universe")}{SH("blYoY","Baseline YOY")}{SH("blIMM","Baseline IMM")}
      <th className="px-4 py-3"></th>
    </tr></thead><tbody className="divide-y divide-gray-100">{pageData.map(g => {
      const yoyOk = g.fwdAvail && g.yoyAvail, immOk = g.fwdAvail && g.immAvail;
      const iY = formatImpact(g.ratioYoY, yoyOk), iI = formatImpact(g.ratioIMM, immOk);
      const bY = formatImpact(g.baselineYoY, yoyOk), bI = formatImpact(g.baselineIMM, immOk);
      return(<tr key={g.key} className="hover:bg-gray-50 transition">
        <td className="px-4 py-3 font-medium text-gray-900 whitespace-nowrap">{HL(g.eventName, fName.trim())}</td>
        <td className="px-4 py-3 text-gray-600 whitespace-nowrap">{fmtDate(g.eventDate)}</td>
        <td className="px-4 py-3"><Badge text={g.product} className={PROD_BADGE[g.product] || "bg-gray-100 text-gray-700"}/></td>
        <td className="px-4 py-3"><Badge text={g.eventType} className={g.eventType==="Online"?"bg-cyan-100 text-cyan-700":"bg-orange-100 text-orange-700"}/></td>
        <td className="px-4 py-3 text-gray-600 whitespace-nowrap">{g.venue}</td>
        <td className="px-4 py-3 text-gray-600 whitespace-nowrap">{g.country}</td>
        <td className="px-4 py-3 text-gray-600 whitespace-nowrap">{g.provider}</td>
        <td className="px-4 py-3 font-semibold text-gray-900">{g.totalRegistrations}</td>
        <td className="px-4 py-3 font-semibold text-gray-900">{g.totalPartners}</td>
        <td className="px-4 py-3 font-semibold text-gray-900">{g.totalAttendees}</td>
        <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(g.yoyRange)}</td>
        <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(g.immRange, "←")}</td>
        <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(g.fwdRange)}</td>
        <td className={"px-4 py-3 font-bold whitespace-nowrap "+iY.color}>{iY.text}</td>
        <td className={"px-4 py-3 font-bold whitespace-nowrap "+iI.color}>{iI.text}</td>
        <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{g.baselineUniverse}</td>
        <td className={"px-4 py-3 font-bold whitespace-nowrap "+bY.color}>{bY.text}</td>
        <td className={"px-4 py-3 font-bold whitespace-nowrap "+bI.color}>{bI.text}</td>
        <td className="px-4 py-3"><button onClick={()=>openModal(g)} className="px-3 py-1.5 text-xs font-semibold text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition whitespace-nowrap">Partner Analysis</button></td>
      </tr>);})}</tbody></table></div>
  </div>
  );
};

const PartnerLookup = ({ salesIndex, valueMode }) => {
  const [pid, setPid] = useState("");
  const [selProds, setSelProds] = useState([]);
  const [result, setResult] = useState(null);
  const prods = useMemo(() => { const s = new Set(); for (const k of salesIndex.byPPM.keys()) { const p = k.split("|||"); if (p.length === 3) s.add(p[1]); } return [...s].sort(); }, [salesIndex]);
  const sortedMonths = useMemo(() => [...salesIndex.months].sort(), [salesIndex]);
  const last3 = useMemo(() => { const ms = sortedMonths.slice(-3); return ms.map(ym => ({ ym, val: 0, label: ms.length ? n(0) : "No CSV", color: "text-gray-400" })); }, [sortedMonths]);
  const doFetch = () => {
    const id = pid.trim(); if (!id) return;
    if (!sortedMonths.length) { setResult({ pid: id, months: last3, total: 0 }); return; }
    const ms = sortedMonths.slice(-3);
    const useAll = !selProds.length;
    const vMap = valueMode === "customers" ? salesIndex.custPM : salesIndex.byPM;
    const vpMap = valueMode === "customers" ? salesIndex.custPPM : salesIndex.byPPM;
    const months = ms.map(ym => {
      const v = useAll ? (vMap.get(id + "|||" + ym) || 0) : selProds.reduce((s, p) => s + (vpMap.get(id + "|||" + p + "|||" + ym) || 0), 0);
      return { ym, val: v };
    });
    setResult({ pid: id, months, total: _.sumBy(months, "val") });
  };
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4 mt-6">
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-3 flex-shrink-0">
          <input type="text" value={pid} onChange={e => setPid(e.target.value)} onKeyDown={e => e.key === "Enter" && doFetch()} placeholder="Partner ID" className="px-2.5 py-1.5 text-sm border border-gray-200 rounded-lg bg-white h-[36px] placeholder-gray-400 focus:outline-none focus:border-blue-300 w-40" />
          <div className="w-40"><MultiSel values={selProds} onChange={setSelProds} options={prods} dropUp /></div>
          <button onClick={doFetch} disabled={!pid.trim()} className={"px-3 py-1.5 text-xs font-semibold rounded-lg transition whitespace-nowrap h-[36px] " + (!pid.trim() ? "text-gray-400 bg-gray-100 cursor-not-allowed" : "text-white bg-blue-600 hover:bg-blue-700")}>Partner Sales</button>
        </div>
        <div className="flex items-center gap-3 flex-wrap min-w-0 ml-auto">
          {(()=>{const ms = result ? result.months : last3;
            return <>{ms.map((m, i) => (<span key={m.ym} className="flex items-center">{i > 0 && <span className="text-gray-300 mr-3">·</span>}<span className="text-xs text-gray-500">{fmtYM(m.ym)}:</span><span className={"text-sm font-semibold ml-1.5 " + (m.color || (m.val > 0 ? "text-gray-900" : "text-gray-400"))}>{m.label || n(m.val)}</span></span>))}<span className="text-gray-300">·</span><span className="text-xs font-bold text-gray-500 uppercase">TOTAL:</span><span className={"text-sm font-bold ml-1.5 " + (result ? (result.total > 0 ? "text-blue-600" : "text-gray-400") : "text-gray-400")}>{result ? n(result.total) : n(0)}</span></>;
          })()}
        </div>
      </div>
    </div>
  );
};

const PartnerModal = ({ modal, salesIndex, sales, events, analysisMode, valueMode, onClose }) => {
  const [selIdx, setSelIdx] = useState(null);
  const [mSortCol, setMSortCol] = useState(null);
  const [mSortDir, setMSortDir] = useState("desc");
  const [chartVis, setChartVis] = useState({ dots: true, others: true });
  const toggleMSort = (col) => { if (mSortCol===col) { if (mSortDir==="desc") setMSortDir("asc"); else { setMSortCol(null); setMSortDir("desc"); } } else { setMSortCol(col); setMSortDir("desc"); } };
  const pNameMap = useMemo(() => Object.fromEntries(modal.rows.map(r => [r["Partner ID"], r["Partner Name"] || ""])), [modal]);
  const chartLabel = analysisMode === "total" ? "Total Sales" : (modal.product || "") + " Sales";
  const yoyOk = modal.fwdAvail && modal.yoyAvail, immOk = modal.fwdAvail && modal.immAvail;

  const chartData = useMemo(() => {
    if (selIdx === null) return null;
    const p = modal.impactPartners[selIdx]; if (!p || !p.found) return null;
    const eYM = getEventMonth(modal.eventDate); if (!eYM) return null;
    const months = Array.from({ length: 25 }, (_, i) => offsetMonth(eYM, i - 12));
    const pEvtByMonth = {};
    for (const ev of events) { if (ev["Partner ID"] !== p.pid) continue; const eym = getEventMonth(ev["Event Date"]); if (!eym) continue; if (!pEvtByMonth[eym]) pEvtByMonth[eym] = []; pEvtByMonth[eym].push(ev); }
    const data = months.map(ym => {
      const val = idxGet(salesIndex, analysisMode, valueMode, p.pid, modal.product, ym);
      const pE = pEvtByMonth[ym] || [];
      const evts = _.uniqBy(pE.map(ev => ({ name: ev["Event Name"], product: ev["Product"], isCurrent: ev["Event Date"] === modal.eventDate && ev["Event Name"] === modal.eventName })), ev => ev.name + "|||" + ev.product);
      return { month: ym, sales: val || null, _sales: val, _events: evts.length ? evts : null };
    });
    const ema = calcEMA(data.map(d => d._sales), 3); data.forEach((d, i) => { d.ema3 = ema[i]; });
    data.forEach(d => { if (d._sales === 0) d.sales = null; });
    const markers = data.flatMap(d => (d._events || []).map(e => ({ ...e, month: d.month })));
    return { data, markers, partnerID: p.pid };
  }, [modal, selIdx, salesIndex, events, analysisMode, valueMode]);

  const mIY = formatImpact(modal.ratioYoY, yoyOk), mII = formatImpact(modal.ratioIMM, immOk);
  const mBY = formatImpact(modal.baselineYoY, yoyOk), mBI = formatImpact(modal.baselineIMM, immOk);
  const pd = modal.partnerDetails || {};

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col" onClick={e=>e.stopPropagation()}>
        <div className="px-6 py-6 border-b border-gray-200 flex items-center justify-between flex-shrink-0">
          <div className="min-w-0 mr-4">
            <h2 className="text-base font-bold text-gray-900">{modal.eventName}</h2>
            <p className="text-xs text-gray-400 truncate">{fmtDate(modal.eventDate)} · {modal.product} · {modal.totalPartners} partner{modal.totalPartners !== 1 ? "s" : ""}</p>
          </div>
          <button onClick={onClose} className="w-8 h-8 rounded-lg hover:bg-gray-100 flex items-center justify-center transition flex-shrink-0"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#6b7280" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12"/></svg></button>
        </div>
        <div className="overflow-auto flex-1">
          <div className="px-6 py-6 border-b border-gray-100 flex flex-col" style={{height:"403px"}}>
            {selIdx===null||!chartData?(<div className="flex flex-col items-center justify-center text-center flex-1"><div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2"><path d="M3 3v18h18"/><path d="M18 17V9M13 17V5M8 17v-3"/></svg></div><p className="text-sm text-gray-400">Select a partner below to view their sales timeline</p></div>):(
            <div className="rounded-xl border border-gray-200 overflow-hidden">
              <div className="p-4 border-b border-gray-100"><div className="flex items-center justify-between">
                <div><p className="text-sm font-semibold text-gray-800">{chartData.partnerID}{pNameMap[chartData.partnerID] ? " · " + pNameMap[chartData.partnerID] : ""}</p><p className="text-xs text-gray-400">{chartLabel} · Monthly</p></div>
                <div className="flex items-center gap-1.5">{[["dots","Monthly Sales"],["others","Other Events"]].map(([k,l])=>(<button key={k} onClick={()=>setChartVis(p=>({...p,[k]:!p[k]}))} className={"px-2.5 py-1 text-xs font-medium rounded-full border transition "+(chartVis[k]?"bg-white text-blue-600 border-blue-400 hover:border-blue-500":"bg-white text-gray-400 border-gray-300 hover:border-blue-300")}>{l}</button>))}</div>
              </div></div>
              <div className="px-4 pt-4" style={{marginBottom:"-12px"}}><ResponsiveContainer width="100%" height={280}><LineChart data={chartData.data} margin={{top:0,right:0,left:0,bottom:0}}><CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6"/><XAxis dataKey="month" tick={false} axisLine={false} padding={{left:0,right:0}}/><YAxis tick={false} axisLine={false} width={0}/><Tooltip content={<ChartTip/>}/>
                {chartVis.dots&&<Line type="monotone" dataKey="sales" stroke="#374151" strokeWidth={0} dot={{r:2.5,fill:"#374151",strokeWidth:0}} activeDot={{r:4,fill:"#374151"}} name={chartLabel}/>}
                <Line type="monotone" dataKey="ema3" stroke="#3b82f6" strokeWidth={1.5} dot={false} strokeDasharray="4 2" name="3 EMA"/>
                {chartData.markers.map((m,i)=>{ if (!m.isCurrent && !chartVis.others) return null; return <ReferenceLine key={i} x={m.month} stroke={PROD_COLORS[m.product]||"#6b7280"} strokeWidth={m.isCurrent?2.5:1} strokeDasharray={m.isCurrent?"0":"4 3"} opacity={m.isCurrent?1:0.6}/>; })}
              </LineChart></ResponsiveContainer></div>
            </div>)}
          </div>
          <div className="px-6 pt-4 pb-6">
            <div className="flex items-center mb-4"><div className="flex items-center gap-3 flex-wrap">
              <span className="text-xs text-gray-400">Table Overall</span>
              <span className="text-gray-300">·</span><span className="text-xs font-bold text-gray-500 uppercase">Impact YOY:</span><span className={"text-sm font-bold "+mIY.color}>{mIY.text}</span>
              <span className="text-gray-300">·</span><span className="text-xs font-bold text-gray-500 uppercase">Impact IMM:</span><span className={"text-sm font-bold "+mII.color}>{mII.text}</span>
            </div></div>
            {!modal.impactPartners.length?(<div className="py-6 text-center text-sm text-gray-400">{!sales.length?"Upload Sales Data":"—"}</div>):(
            <div className="rounded-xl border border-gray-200 overflow-hidden"><div className="overflow-x-auto"><table className="w-full text-sm"><thead><tr className="bg-gray-50">
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">ID</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Name</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Registrations</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Attendees</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Historical YOY</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Historical IMM</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Forward Period</th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider whitespace-nowrap cursor-pointer select-none hover:text-gray-700 transition" onClick={()=>toggleMSort("impactYoY")} style={{color:mSortCol==="impactYoY"?"#1d4ed8":"#6b7280"}}>Impact YOY</th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider whitespace-nowrap cursor-pointer select-none hover:text-gray-700 transition" onClick={()=>toggleMSort("impactIMM")} style={{color:mSortCol==="impactIMM"?"#1d4ed8":"#6b7280"}}>Impact IMM</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Baseline Universe</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Baseline YOY</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">Baseline IMM</th>
              <th className="px-4 py-3"></th>
            </tr></thead><tbody className="divide-y divide-gray-100">{(()=>{
              const indexed = modal.impactPartners.map((p,i)=>({...p,origIdx:i}));
              const sorted = mSortCol?[...indexed].sort((a,b)=>{const csvOk=mSortCol==="impactYoY"?yoyOk:immOk;const av=sv(mSortCol==="impactYoY"?a.ratioYoY:a.ratioIMM,csvOk),bv=sv(mSortCol==="impactYoY"?b.ratioYoY:b.ratioIMM,csvOk);const d=mSortDir==="desc"?bv-av:av-bv;return d!==0?d:a.pid.localeCompare(b.pid);}):indexed;
              return sorted.map(p=>{const isSel=selIdx===p.origIdx;const pIY=formatImpact(p.ratioYoY,yoyOk),pII=formatImpact(p.ratioIMM,immOk);const det=pd[p.pid]||{regs:0,att:0};
                return(<tr key={p.origIdx} className="transition hover:bg-gray-50">
                <td className="px-4 py-3 font-medium text-gray-900 whitespace-nowrap">{p.pid}</td>
                <td className="px-4 py-3 text-gray-600 whitespace-nowrap">{pNameMap[p.pid] || ""}</td>
                <td className="px-4 py-3 font-semibold text-gray-900 whitespace-nowrap">{det.regs}</td>
                <td className="px-4 py-3 font-semibold text-gray-900 whitespace-nowrap">{det.att}</td>
                <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(modal.yoyRange)}</td>
                <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(modal.immRange, "←")}</td>
                <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{monthRange(modal.fwdRange)}</td>
                <td className={"px-4 py-3 font-bold whitespace-nowrap "+pIY.color}>{pIY.text}</td>
                <td className={"px-4 py-3 font-bold whitespace-nowrap "+pII.color}>{pII.text}</td>
                <td className="px-4 py-3 text-gray-400 whitespace-nowrap">{modal.baselineUniverse}</td>
                <td className={"px-4 py-3 font-bold whitespace-nowrap "+mBY.color}>{mBY.text}</td>
                <td className={"px-4 py-3 font-bold whitespace-nowrap "+mBI.color}>{mBI.text}</td>
                <td className="px-4 py-3"><button onClick={()=>setSelIdx(isSel?null:p.origIdx)} disabled={!p.found} className={"px-3 py-1.5 text-xs font-semibold rounded-lg transition border "+(!p.found?"text-gray-300 bg-white border-gray-200 cursor-not-allowed":isSel?"text-blue-600 bg-white border-blue-400 hover:border-blue-500":"text-gray-400 bg-white border-gray-300 hover:border-blue-300")}>Chart</button></td>
              </tr>);});
            })()}</tbody></table></div></div>)}
          </div>
        </div>
      </div>
    </div>
  );
};

/* ═══════════════════════════════════════════════════════════════════
   ERROR BOUNDARY
   ═══════════════════════════════════════════════════════════════════ */
class ErrorBoundary extends React.Component {
  constructor(props) { super(props); this.state = { hasError: false }; }
  static getDerivedStateFromError() { return { hasError: true }; }
  handleReset = async () => {
    try { const db = await getIDB(); const tx = db.transaction("kv","readwrite"); tx.objectStore("kv").clear(); await new Promise(r => { tx.oncomplete = r; tx.onerror = r; }); } catch {}
    this.setState({ hasError: false }); window.location.reload();
  };
  render() {
    if (this.state.hasError) return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center" style={{fontFamily:"'Inter',system-ui,sans-serif"}}><div className="text-center max-w-md">
        <div className="w-12 h-12 rounded-full bg-red-100 flex items-center justify-center mx-auto mb-4"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2"><path d="M12 9v4M12 17h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/></svg></div>
        <h2 className="text-lg font-bold text-gray-900 mb-2">Something went wrong</h2>
        <p className="text-sm text-gray-500 mb-4">An unexpected error occurred. You can try resetting the application data.</p>
        <button onClick={this.handleReset} className="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700 transition">Reset & Reload</button>
      </div></div>
    );
    return this.props.children;
  }
}

/* ═══════════════════════════════════════════════════════════════════
   MAIN APP
   ═══════════════════════════════════════════════════════════════════ */
function App() {
  const [events, setEvents] = useState([]);
  const [sales, setSales] = useState([]);
  const [dataLoaded, setDataLoaded] = useState(false);
  const [tab, setTab] = useState("analytics");
  const [uploadState, setUploadState] = useState(null);
  const [flashKeys, setFlashKeys] = useState([]);
  const fileRef = useRef(null);
  const [analysisMode, setAnalysisMode] = useState("product");
  const [valueMode, setValueMode] = useState("value");
  const [fwdMonths, setFwdMonths] = useState(1);
  const [fDates, setFDates] = useState([]); const [fProd,setFProd]=useState([]); const [fType,setFType]=useState([]);
  const [fVenue,setFVenue]=useState([]); const [fCountry,setFCountry]=useState([]); const [fProvider,setFProvider]=useState([]);
  const [fName, setFName] = useState("");
  const [debouncedName, setDebouncedName] = useState("");
  const nameTimer = useRef(null);
  const handleNameChange = useCallback((val) => { setFName(val); if (nameTimer.current) clearTimeout(nameTimer.current); nameTimer.current = setTimeout(() => setDebouncedName(val), 300); }, []);
  const [sortCol,setSortCol]=useState(null); const [sortDir,setSortDir]=useState("desc");
  const [modal, setModal] = useState(null);
  const [mode, setModeRaw] = useState("test");
  const setMode = (m) => { setModeRaw(m); setTab("analytics"); };
  const [isDesktop, setIsDesktop] = useState(() => window.innerWidth >= MIN_DESKTOP);

  useEffect(() => { const h = () => setIsDesktop(window.innerWidth >= MIN_DESKTOP); window.addEventListener("resize", h); return () => window.removeEventListener("resize", h); }, []);

  useEffect(() => { setDataLoaded(false); (async () => { try {
    if (mode === "test") { const td = await loadTestData(); setEvents(td.events); setSales(td.sales); }
    else { const eidx = await psGet("evt_idx") || []; const eC = await Promise.all(eidx.map(k => psGet("evt:" + k))); const e = eC.flat().filter(Boolean); const sidx = await psGet("sal_idx") || []; const sC = await Promise.all(sidx.map(k => psGet("sal:" + k))); const s = sC.flat().filter(Boolean); setEvents(e); setSales(s); }
  } catch { setEvents([]); setSales([]); } setDataLoaded(true); })(); }, [mode]);

  useEffect(() => { if (!dataLoaded || !events.length || mode !== "live") return; const t = setTimeout(async () => {
    const bm = _.groupBy(events, r => r._uYear + ":" + r._uMonth); const ks = Object.keys(bm);
    const ok = await psGet("evt_idx") || []; for (const k of ok) { if (!ks.includes(k)) await psDel("evt:" + k); }
    for (const k of ks) await psSet("evt:" + k, bm[k]); await psSet("evt_idx", ks);
  }, 300); return () => clearTimeout(t); }, [events, dataLoaded, mode]);

  useEffect(() => { if (!dataLoaded || !sales.length || mode !== "live") return; const t = setTimeout(async () => {
    const bm = _.groupBy(sales, r => r._uYear + ":" + r._uMonth); const ks = Object.keys(bm);
    const ok = await psGet("sal_idx") || []; for (const k of ok) { if (!ks.includes(k)) await psDel("sal:" + k); }
    for (const k of ks) await psSet("sal:" + k, bm[k]); await psSet("sal_idx", ks);
  }, 300); return () => clearTimeout(t); }, [sales, dataLoaded, mode]);

  useEffect(() => { if (uploadState?.status==="success"||uploadState?.status==="partial"||uploadState?.status==="error") { const handler = () => { setUploadState(null); setFlashKeys([]); }; const t = setTimeout(() => window.addEventListener("click", handler, { once: true }), 300); return () => { clearTimeout(t); window.removeEventListener("click", handler); }; } }, [uploadState]);

  const salesIndex = useMemo(() => {
    const byPM = new Map(), byPPM = new Map(), custPM = new Map(), custPPM = new Map();
    const months = new Set(), partnerFirst = new Map();
    const csPM = new Map(), csPPM = new Map();
    for (const s of sales) {
      const d = parseD(s["Sale Date"]); if (!d) continue;
      const ym = mkKey(d.getUTCFullYear(), d.getUTCMonth() + 1), v = parseFloat(s["Sale Value"]) || 0;
      const pid = (s["Partner ID"] || "").trim(), prod = (s["Product"] || "").trim(), cust = (s["Customer Name"] || "").trim();
      months.add(ym);
      if (pid) {
        const pmk = pid + "|||" + ym; byPM.set(pmk, (byPM.get(pmk) || 0) + v);
        const ppk = pid + "|||" + prod + "|||" + ym; byPPM.set(ppk, (byPPM.get(ppk) || 0) + v);
        if (cust) {
          if (!csPM.has(pmk)) csPM.set(pmk, new Set()); csPM.get(pmk).add(cust);
          if (!csPPM.has(ppk)) csPPM.set(ppk, new Set()); csPPM.get(ppk).add(cust);
        }
        if (!partnerFirst.has(pid) || ym < partnerFirst.get(pid)) partnerFirst.set(pid, ym);
      }
    }
    for (const [k, s] of csPM) custPM.set(k, s.size);
    for (const [k, s] of csPPM) custPPM.set(k, s.size);
    return { byPM, byPPM, custPM, custPPM, months, partnerFirst };
  }, [sales]);

  const { coverageData, reportCounts } = useMemo(() => {
    const build = (data, col) => { const by = {}; (data||[]).forEach(r => { const d=parseD(r[col]); if(!d) return; const y=""+d.getUTCFullYear(), m=d.getUTCMonth()+1; if(!by[y]) by[y]=[]; if(!by[y].includes(m)) by[y].push(m); }); Object.keys(by).forEach(y => by[y].sort((a,b)=>a-b)); return by; };
    const evt = build(events,"Event Date"), sal = build(sales,"Sale Date");
    return { coverageData: { evt, sal }, reportCounts: { evt: Object.values(evt).reduce((s,ms)=>s+ms.length,0), sal: Object.values(sal).reduce((s,ms)=>s+ms.length,0) } };
  }, [events, sales]);

  const handleUpload = useCallback(async (files) => {
    if (!files||!files.length) return; const list = Array.from(files), total = list.length;
    const evtB = [], salB = []; let fail = 0; const fk = [];
    for (let i = 0; i < list.length; i++) { setUploadState({status:"uploading",progress:Math.round((i/total)*100),current:i+1,total}); try { await new Promise(r=>setTimeout(r,50)); const rows = await parseCsv(list[i]); const type = detectType(rows); if (!type) { fail++; continue; } if (!validateRows(rows, type)) { fail++; continue; } const dateCol = type==="event"?"Event Date":"Sale Date"; const fd = parseD(rows[0]?.[dateCol]); if (!fd) { fail++; continue; } const mo = ""+(fd.getUTCMonth()+1), yr = ""+fd.getUTCFullYear(); if (rows.some(r => { const d=parseD(r[dateCol]); return !d||""+(d.getUTCMonth()+1)!==mo||""+d.getUTCFullYear()!==yr; })) { fail++; continue; } const tagged = rows.map(r => ({...r, _uMonth:mo, _uYear:yr})); if (type==="event") { evtB.push({rows:tagged,mo,yr}); fk.push("evt-"+yr+"-"+mo); } else { salB.push({rows:tagged,mo,yr}); fk.push("sal-"+yr+"-"+mo); } } catch { fail++; } }
    setUploadState({status:"uploading",progress:100}); await new Promise(r=>setTimeout(r,200));
    let eS=0,sS=0,eU=0,sU=0; const seenE = new Set(), seenS = new Set();
    for (const b of evtB) { const k=b.yr+"-"+b.mo; if(events.some(r=>r._uMonth===b.mo&&r._uYear===b.yr)||seenE.has(k)) eU++; else eS++; seenE.add(k); }
    for (const b of salB) { const k=b.yr+"-"+b.mo; if(sales.some(r=>r._uMonth===b.mo&&r._uYear===b.yr)||seenS.has(k)) sU++; else sS++; seenS.add(k); }
    if (evtB.length) setEvents(prev => { let r=[...prev]; for (const b of evtB) { r=r.filter(x=>!(x._uMonth===b.mo&&x._uYear===b.yr)); r.push(...b.rows); } return r; });
    if (salB.length) setSales(prev => { let r=[...prev]; for (const b of salB) { r=r.filter(x=>!(x._uMonth===b.mo&&x._uYear===b.yr)); r.push(...b.rows); } return r; });
    setFlashKeys(fk); const ok = eS+sS+eU+sU;
    if (!ok) { setUploadState({status:"error",message:"Data upload failed"}); return; }
    const parts = []; if(eS>0&&eU>0) parts.push("Event data added and updated"); else if(eS>0) parts.push("Event data added"); else if(eU>0) parts.push("Event data updated"); if(sS>0&&sU>0) parts.push("Sales data added and updated"); else if(sS>0) parts.push("Sales data added"); else if(sU>0) parts.push("Sales data updated"); if(fail>0) { parts.push(fail+" file"+(fail>1?"s":"")+" failed"); setUploadState({status:"partial",message:parts.join(" · ")}); } else { setUploadState({status:"success",message:parts.join(" · ")}); }
  }, [events, sales]);

  const allEventsGrouped = useMemo(() => groupEvents(events), [events]);

  const impactCache = useMemo(() => {
    const cache = new Map();
    for (const g of allEventsGrouped) {
      const imp = calcEventImpact(g, salesIndex, analysisMode, fwdMonths, valueMode);
      const bl = imp.eventStatus === "ok" ? calcBaselines(g, salesIndex, analysisMode, fwdMonths, valueMode) : { yoy: null, imm: null };
      cache.set(g.key, { ...g, ...imp, baselineYoY: bl.yoy, baselineIMM: bl.imm });
    }
    return cache;
  }, [allEventsGrouped, salesIndex, analysisMode, fwdMonths, valueMode]);

  const allGrouped = useMemo(() => {
    let groups = [...impactCache.values()];
    if (fDates.length) { const s = new Set(fDates); groups = groups.filter(g => s.has(g.eventDate)); }
    if (fProd.length) groups = groups.filter(g => fProd.includes(g.product));
    if (fType.length) groups = groups.filter(g => fType.includes(g.eventType));
    if (fVenue.length) groups = groups.filter(g => fVenue.includes(g.venue));
    if (fCountry.length) groups = groups.filter(g => fCountry.includes(g.country));
    if (fProvider.length) groups = groups.filter(g => fProvider.includes(g.provider));
    return groups;
  }, [impactCache, fDates, fProd, fType, fVenue, fCountry, fProvider]);

  const fo = useMemo(() => {
    const allGroups = allEventsGrouped;
    const dateSet = fDates.length ? new Set(fDates) : null, prodSet = fProd.length ? new Set(fProd) : null;
    const typeSet = fType.length ? new Set(fType) : null, venueSet = fVenue.length ? new Set(fVenue) : null;
    const countrySet = fCountry.length ? new Set(fCountry) : null, providerSet = fProvider.length ? new Set(fProvider) : null;
    const dateMap = {}, prods = new Set(), types = new Set(), venues = new Set(), countries = new Set(), providers = new Set();
    for (const g of allGroups) {
      const pDate = !dateSet || dateSet.has(g.eventDate), pProd = !prodSet || prodSet.has(g.product);
      const pType = !typeSet || typeSet.has(g.eventType), pVenue = !venueSet || venueSet.has(g.venue);
      const pCountry = !countrySet || countrySet.has(g.country), pProvider = !providerSet || providerSet.has(g.provider);
      if (pProd && pType && pVenue && pCountry && pProvider) { const d = parseD(g.eventDate); if (d) { const y = "" + d.getUTCFullYear(), m = d.getUTCMonth() + 1, day = d.getUTCDate(); if (!dateMap[y]) dateMap[y] = {}; if (!dateMap[y][m]) dateMap[y][m] = new Set(); dateMap[y][m].add(day); } }
      if (pDate && pType && pVenue && pCountry && pProvider && g.product) prods.add(g.product);
      if (pDate && pProd && pVenue && pCountry && pProvider && g.eventType) types.add(g.eventType);
      if (pDate && pProd && pType && pCountry && pProvider && g.venue) venues.add(g.venue);
      if (pDate && pProd && pType && pVenue && pProvider && g.country) countries.add(g.country);
      if (pDate && pProd && pType && pVenue && pCountry && g.provider) providers.add(g.provider);
    }
    const dateTree = Object.keys(dateMap).sort().map(y => ({ year: y, months: Object.keys(dateMap[y]).sort((a,b) => +a - +b).map(m => ({ month: +m, days: [...dateMap[y][m]].sort((a,b) => a-b) })) }));
    return { dateTree, prods: [...prods].sort(), types: [...types].sort(), venues: [...venues].sort(), countries: [...countries].sort(), providers: [...providers].sort() };
  }, [allEventsGrouped, fDates, fProd, fType, fVenue, fCountry, fProvider]);

  const toggleSort = (col) => { if (sortCol===col) { if (sortDir==="desc") setSortDir("asc"); else { setSortCol(null); setSortDir("desc"); } } else { setSortCol(col); setSortDir("desc"); } };

  const sortedGrouped = useMemo(() => {
    const nameQ = debouncedName.trim().toLowerCase();
    const nameFiltered = nameQ ? allGrouped.filter(g => g.eventName.toLowerCase().includes(nameQ)) : allGrouped;
    if (!sortCol) return nameFiltered;
    return [...nameFiltered].sort((a,b) => { let av, bv;
      if (sortCol==="registrations") { av=a.totalRegistrations; bv=b.totalRegistrations; }
      else if (sortCol==="attendance") { av=a.totalPartners; bv=b.totalPartners; }
      else if (sortCol==="attendees") { av=a.totalAttendees; bv=b.totalAttendees; }
      else if (sortCol==="ratioYoY") { av=sv(a.ratioYoY,a.fwdAvail&&a.yoyAvail); bv=sv(b.ratioYoY,b.fwdAvail&&b.yoyAvail); }
      else if (sortCol==="ratioIMM") { av=sv(a.ratioIMM,a.fwdAvail&&a.immAvail); bv=sv(b.ratioIMM,b.fwdAvail&&b.immAvail); }
      else if (sortCol==="blUniverse") { av=a.baselineUniverse; bv=b.baselineUniverse; }
      else if (sortCol==="blYoY") { av=sv(a.baselineYoY,a.fwdAvail&&a.yoyAvail); bv=sv(b.baselineYoY,b.fwdAvail&&b.yoyAvail); }
      else if (sortCol==="blIMM") { av=sv(a.baselineIMM,a.fwdAvail&&a.immAvail); bv=sv(b.baselineIMM,b.fwdAvail&&b.immAvail); }
      else if (sortCol==="date") { av=new Date(a.eventDate).getTime()||0; bv=new Date(b.eventDate).getTime()||0; }
      const d=sortDir==="desc"?bv-av:av-bv; return d!==0?d:new Date(b.eventDate)-new Date(a.eventDate); });
  }, [allGrouped, sortCol, sortDir, debouncedName]);

  const tableOvr = useMemo(() => { if (!sales.length || !allGrouped.length) return null; const ok = allGrouped.filter(g => g.eventStatus === "ok"); if (!ok.length) return null; const hY = _.sumBy(ok, "histYoY"), fY = _.sumBy(ok, "fwdYoY"), hI = _.sumBy(ok, "histIMM"), fI = _.sumBy(ok, "fwdIMM"); return { yoy: formatImpact(hY > 0 ? fY / hY : (fY > 0 ? Infinity : null), true), imm: formatImpact(hI > 0 ? fI / hI : (fI > 0 ? Infinity : null), true) }; }, [allGrouped, sales]);

  const summaryData = useMemo(() => {
    if (!allGrouped.length || !sales.length) return null;
    const bd = k => { const g = _.groupBy(allGrouped, k); return Object.entries(g).map(([key, evts]) => { const vY = evts.filter(e => e.ratioYoY != null && isFinite(e.ratioYoY)); const vI = evts.filter(e => e.ratioIMM != null && isFinite(e.ratioIMM)); return { key, avgRatioYoY: vY.length ? _.meanBy(vY, "ratioYoY") : null, avgRatioIMM: vI.length ? _.meanBy(vI, "ratioIMM") : null, totalPartners: _.sumBy(evts, "totalPartners"), latestDate: _.maxBy(evts, "eventDate")?.eventDate || "" }; }); };
    const veYoY = allGrouped.filter(e => e.ratioYoY != null && isFinite(e.ratioYoY)), veIMM = allGrouped.filter(e => e.ratioIMM != null && isFinite(e.ratioIMM));
    return { topEventsYoY: veYoY.length ? [...veYoY].sort(descTie(e => e.ratioYoY, e => e.eventDate)).slice(0,10) : [], topEventsIMM: veIMM.length ? [...veIMM].sort(descTie(e => e.ratioIMM, e => e.eventDate)).slice(0,10) : [], topEventsByAtt: [...allGrouped].sort(descTie(e => e.totalPartners, e => e.eventDate)).slice(0,10), product: bd("product"), eventType: bd("eventType"), venue: bd("venue"), country: bd("country"), provider: bd("provider") };
  }, [allGrouped, sales]);

  const topPartnersData = useMemo(() => {
    if (!allGrouped.length) return { impactYoY: [], impactIMM: [], attendance: [] };
    const byP = {};
    for (const g of allGrouped) { if (!g.impactPartners) continue; for (const p of g.impactPartners) { if (!p.pid) continue; if (!byP[p.pid]) byP[p.pid] = { pid: p.pid, histYoY: 0, fwdYoY: 0, histIMM: 0, fwdIMM: 0, events: 0, latestDate: "" }; const b = byP[p.pid]; b.events++; b.histYoY += p.histYoY; b.fwdYoY += p.fwdYoY; b.histIMM += p.histIMM; b.fwdIMM += p.fwdIMM; if (g.eventDate > b.latestDate) b.latestDate = g.eventDate; } }
    const all = Object.values(byP);
    const impactYoY = all.map(p => ({ pid: p.pid, ratio: p.histYoY>0 ? p.fwdYoY/p.histYoY : null, latestDate: p.latestDate })).filter(p => p.ratio!=null && isFinite(p.ratio)).sort(descTie(p => p.ratio, p => p.latestDate)).slice(0,10);
    const impactIMM = all.map(p => ({ pid: p.pid, ratio: p.histIMM>0 ? p.fwdIMM/p.histIMM : null, latestDate: p.latestDate })).filter(p => p.ratio!=null && isFinite(p.ratio)).sort(descTie(p => p.ratio, p => p.latestDate)).slice(0,10);
    const attendance = [...all].sort(descTie(p => p.events, p => p.latestDate)).slice(0,10).map(p => ({ pid: p.pid, events: p.events }));
    return { impactYoY, impactIMM, attendance };
  }, [allGrouped]);

  const globalNameMap = useMemo(() => { const m = {}, d = {}; for (const r of events) { const pid = r["Partner ID"], dt = r["Event Date"]; if (pid && (!d[pid] || dt > d[pid])) { m[pid] = r["Partner Name"] || ""; d[pid] = dt; } } return m; }, [events]);

  const clearAll = useCallback(async () => {
    setEvents([]); setSales([]); setModal(null); setUploadState(null); setFlashKeys([]);
    setFDates([]); setFProd([]); setFType([]); setFVenue([]); setFCountry([]); setFProvider([]); setFName(""); setDebouncedName("");
    setSortCol(null); setSortDir("desc");
    const ek = await psGet("evt_idx") || []; for (const k of ek) await psDel("evt:" + k); await psDel("evt_idx");
    const sk = await psGet("sal_idx") || []; for (const k of sk) await psDel("sal:" + k); await psDel("sal_idx");
  }, []);

  const openModal = (g) => setModal(g);
  const closeModal = () => setModal(null);
  const hasEvt = events.length > 0, hasSales = sales.length > 0;

  if (!isDesktop) return (<div className="min-h-screen bg-gray-50 flex items-center justify-center" style={{fontFamily:"'Inter',system-ui,sans-serif"}}><p className="text-sm text-gray-500">Accessible only on a desktop</p></div>);
  if (!dataLoaded) return (<div className="min-h-screen bg-gray-50 flex items-center justify-center" style={{fontFamily:"'Inter',system-ui,sans-serif"}}><Dots /></div>);

  return (
    <div className="min-h-screen bg-gray-50" style={{fontFamily:"'Inter',system-ui,sans-serif"}}>
      <div className="bg-white border-b border-gray-200"><div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3"><div className="w-9 h-9 rounded-lg bg-gradient-to-br from-blue-600 to-indigo-700 flex items-center justify-center flex-shrink-0"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg></div><div><h1 className="text-lg font-bold text-gray-900 leading-tight tracking-tight">Cloud Beacon</h1><p className="text-xs text-gray-400">Event impact analyzer</p></div></div>
        <div className="flex items-center gap-3"><span className="flex items-center text-xs whitespace-nowrap"><span className="text-gray-500">Event CSVs:</span><span className="font-semibold text-gray-900 ml-1.5">{reportCounts.evt}</span><span className="text-gray-300 mx-3">·</span><span className="text-gray-500">Sales CSVs:</span><span className="font-semibold text-gray-900 ml-1.5">{reportCounts.sal}</span></span><div className="w-px h-5 bg-gray-200"/><button onClick={clearAll} disabled={mode==="test"} className={"px-3 py-1.5 text-xs font-medium rounded-lg transition whitespace-nowrap h-[32px] "+(mode==="test"?"text-gray-400 bg-gray-100 cursor-not-allowed":"text-red-600 bg-red-50 hover:bg-red-100")}>Clear All Data</button><div className="flex bg-gray-100 rounded-lg p-0.5 h-[32px]"><button onClick={()=>setMode("test")} className={"px-3 flex items-center text-xs font-medium rounded-md transition whitespace-nowrap "+(mode==="test"?"bg-white text-red-600 shadow-sm":"text-gray-500 hover:text-gray-700")}>Test</button><button onClick={()=>setMode("live")} className={"px-3 flex items-center text-xs font-medium rounded-md transition whitespace-nowrap "+(mode==="live"?"bg-white text-emerald-600 shadow-sm":"text-gray-500 hover:text-gray-700")}>Live</button></div></div>
      </div></div>

      <div className="max-w-7xl mx-auto px-6 py-6">
        <div className="flex items-center justify-between mb-6">
          <TabSwitch items={[["analytics","Analytics"],["upload","Data Upload"]]} active={tab} onChange={setTab} disabledKeys={mode==="test"?["upload"]:undefined} />
          {tab==="analytics"&&hasEvt&&hasSales&&<div className="flex items-center gap-3">
            <TabSwitch items={[["1","1M"],["2","2M"],["3","3M"]]} active={""+fwdMonths} onChange={v=>setFwdMonths(+v)} />
            <TabSwitch items={[["product","Product Sales"],["total","Total Sales"]]} active={analysisMode} onChange={setAnalysisMode} />
            <TabSwitch items={[["value","Value"],["customers","Customers"]]} active={valueMode} onChange={setValueMode} />
          </div>}
        </div>

        {tab==="upload"&&(<div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"24px"}}>
          <UploadPanel uploadState={uploadState} handleUpload={handleUpload} fileRef={fileRef} />
          <DataCoverage coverageData={coverageData} flashKeys={flashKeys} />
        </div>)}

        {tab==="analytics"&&(<div>
          {hasEvt&&hasSales&&summaryData&&<SummaryCards summaryData={summaryData} topPartnersData={topPartnersData} globalNameMap={globalNameMap}/>}
          {hasEvt&&<FilterBar fo={fo} fDates={fDates} setFDates={setFDates} fProd={fProd} setFProd={setFProd} fType={fType} setFType={setFType} fVenue={fVenue} setFVenue={setFVenue} fCountry={fCountry} setFCountry={setFCountry} fProvider={fProvider} setFProvider={setFProvider} fName={fName} onNameChange={handleNameChange}/>}
          {allGrouped.length===0?(<div className="bg-white rounded-xl border border-gray-200 p-8 text-center flex flex-col items-center justify-center" style={{height:"262px"}}><div className="w-12 h-12 rounded-full bg-gray-100 flex items-center justify-center mb-3"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12"/></svg></div><p className="text-sm text-gray-500">{hasEvt?"No events match filters.":"Upload event and sales CSVs to get started"}</p></div>)
          :<EventTable sortedGrouped={sortedGrouped} tableOvr={tableOvr} fName={fName}sortCol={sortCol} sortDir={sortDir} toggleSort={toggleSort} openModal={openModal}/>}
          {hasEvt&&<PartnerLookup salesIndex={salesIndex} valueMode={valueMode} />}
        </div>)}
      </div>

      {modal&&<PartnerModal modal={modal} salesIndex={salesIndex} sales={sales} events={events} analysisMode={analysisMode} valueMode={valueMode} onClose={closeModal}/>}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════════
   EXPORT WITH ERROR BOUNDARY
   ═══════════════════════════════════════════════════════════════════ */
export default function CloudBeacon() { return <ErrorBoundary><App /></ErrorBoundary>; }