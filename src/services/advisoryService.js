// Official disaster advisory aggregation.
//
// PAGASA / NDRRMC / DENR do not offer a stable free JSON API and their sites block
// server-side scraping, so this service combines:
//   - Manual advisories posted by admins/officials (pasted from official bulletins)
//     for PAGASA / NDRRMC / DENR / LGU
//   - One reliable free automated feed that covers the Philippines without a key:
//       * USGS earthquake catalog near the municipality  -> tagged PHIVOLCS
//     (ReliefWeb's API was retired (v1 = 410) and v2 now requires a pre-approved
//      appname, so it is no longer used.)
// Everything is cached in the official_advisories table (deduped by external_id)
// so the public feed works even when an upstream source is down.
const axios = require('axios');
const cron = require('node-cron');
const pool = require('../../config/db');

const HIN_LAT = Number(process.env.WEATHER_LAT || 10.394278);
const HIN_LNG = Number(process.env.WEATHER_LNG || 125.198427);
const PLACE = process.env.WEATHER_LOCATION || 'Hinunangan, Southern Leyte';
const TIMEOUT = 9000;

function magLevel(mag) {
  if (mag >= 6) return 'critical';
  if (mag >= 5) return 'warning';
  return 'info';
}

// Insert-or-ignore an advisory (external_id is UNIQUE, so re-runs never duplicate).
async function upsertExternal(a) {
  const [r] = await pool.query(
    `INSERT IGNORE INTO official_advisories
       (agency, advisory_type, title, affected_areas, alert_level, details, issued_at, source_url, external_id, auto)
     VALUES (?,?,?,?,?,?,?,?,?,1)`,
    [a.agency, a.advisory_type, a.title, a.affected_areas || null, a.alert_level || null,
     a.details || null, a.issued_at || null, a.source_url || null, a.external_id]);
  return r.affectedRows > 0;
}

async function fetchEarthquakes() {
  const url = 'https://earthquake.usgs.gov/fdsnws/event/1/query';
  const resp = await axios.get(url, {
    params: {
      format: 'geojson', latitude: HIN_LAT, longitude: HIN_LNG,
      maxradiuskm: 300, minmagnitude: 4.0, orderby: 'time', limit: 10
    },
    timeout: TIMEOUT
  });
  const feats = (resp.data && resp.data.features) || [];
  let added = 0;
  for (const f of feats) {
    const p = f.properties || {};
    const mag = p.mag != null ? Number(p.mag) : null;
    const when = p.time ? new Date(p.time) : new Date();
    // Convert to a MySQL DATETIME string in Philippine Time (UTC+8).
    const issued = new Date(when.getTime() + 8 * 3600 * 1000).toISOString().slice(0, 19).replace('T', ' ');
    const ok = await upsertExternal({
      agency: 'PHIVOLCS',
      advisory_type: 'earthquake',
      title: `M${mag != null ? mag.toFixed(1) : '?'} earthquake - ${p.place || 'near the municipality'}`,
      affected_areas: `${p.place || ''} (300 km watch radius of ${PLACE})`,
      alert_level: mag != null ? magLevel(mag) : 'info',
      details: `Magnitude ${mag != null ? mag.toFixed(1) : 'unknown'} earthquake recorded at ${p.place || 'unknown location'}. ` +
        (p.tsunami ? 'A tsunami advisory was issued for this event. ' : '') +
        'Monitor PHIVOLCS for official depth, intensity and aftershock information. Source: USGS Earthquake Hazards Program.',
      issued_at: issued,
      source_url: `https://earthquake.usgs.gov/earthquakes/eventpage/${f.id}`,
      external_id: `usgs-${f.id}`
    });
    if (ok) added++;
  }
  return added;
}

// Pulls all automated feeds. Each source is isolated so one failure never blocks
// the others. Returns how many NEW advisories were stored.
async function refresh() {
  let added = 0;
  const results = await Promise.allSettled([fetchEarthquakes()]);
  results.forEach((r, i) => {
    if (r.status === 'fulfilled') added += r.value;
    else console.error(`[advisories] feed ${i} failed:`, r.reason && r.reason.message);
  });
  // Keep the table from growing unbounded: retain the newest 300 auto advisories.
  try {
    await pool.query(
      `DELETE FROM official_advisories WHERE auto = 1 AND id NOT IN (
         SELECT id FROM (SELECT id FROM official_advisories WHERE auto = 1 ORDER BY id DESC LIMIT 300) x)`);
  } catch (err) {
    console.error('[advisories] prune failed:', err.message);
  }
  return added;
}

function startMonitor() {
  // Refresh shortly after boot, then every 30 minutes.
  setTimeout(() => refresh().catch(err => console.error('[advisories] initial refresh failed:', err.message)), 8000);
  cron.schedule('*/30 * * * *', () => {
    refresh().catch(err => console.error('[advisories] refresh failed:', err.message));
  });
  console.log('[advisories] Official advisory monitor started (every 30 minutes).');
}

module.exports = { refresh, startMonitor, fetchEarthquakes };
