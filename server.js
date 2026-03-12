// ============================================================
// MYNTRA RECON — Node.js / Express API
// Receives parsed sheet rows from the frontend and upserts
// them into MySQL.
//
// Install deps:  npm install express mysql2 cors
// Run:           node server.js
// ============================================================

const express = require('express');
const mysql   = require('mysql2/promise');
const cors    = require('cors');

const app  = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));   // sheets can be large

// ── DB CONFIG ────────────────────────────────────────────────
// Railway injects MYSQLHOST, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE,
// MYSQLPORT automatically when you add a MySQL plugin.
// The DATABASE_URL fallback lets Railway also connect via single URL.
const DB_CONFIG = process.env.DATABASE_URL ? {
  uri:                process.env.DATABASE_URL,
  waitForConnections: true,
  connectionLimit:    10,
} : {
  host:               process.env.MYSQLHOST     || 'localhost',
  port:               parseInt(process.env.MYSQLPORT || '3306'),
  user:               process.env.MYSQLUSER     || 'root',
  password:           process.env.MYSQLPASSWORD || 'your_password',
  database:           process.env.MYSQLDATABASE || 'myntra_recon',
  waitForConnections: true,
  connectionLimit:    10,
};

let pool;
(async () => {
  pool = await mysql.createPool(DB_CONFIG);
  console.log('✅ MySQL pool created');
})();

// ── UNIQUE KEY PER TABLE ─────────────────────────────────────
// Single string  = one column.
// Array of strings = composite key (ALL columns must match).
const UNIQUE_KEYS = {
  uni: 'invoice_code',
  mor: 'order_release_id',
  mrr: 'order_release_id',
  sr:  ['bill_no', 'bill_date'],   // composite
  srr: ['bill_no_key', 'sr_date'], // composite
  pay: 'order_release_id',
};

// ── COLUMN DEFINITIONS PER TABLE ─────────────────────────────
// Maps the normalized field names (sent by the frontend) to DB
// column names.  Add / remove columns here if the schema changes.
const TABLE_COLS = {
  uni: ['invoice_code','display_order_code','total_price','order_date',
        'sales_order_status','facility','channel_name'],

  mor: ['seller_order_id','order_release_id','status'],

  mrr: ['order_release_id','order_id','seller_order_id','status',
        'invoice_code','sr_number','sr_value','return_date',
        'return_delivered_date','sr_date'],

  sr:  ['po_number','bill_no','bill_date','bill_value'],

  srr: ['po_number','bill_no_key','invoice_code','sr_number',
        'sr_value','sr_date'],

  pay: ['seller_order_id','order_release_id','final_payment',
        'order_type','customer_paid_amt','commission','igst_tcs',
        'cgst_tcs','sgst_tcs','tds','logistics_commission',
        'settled','marketing_charges'],
};

// ── UPSERT HELPER ────────────────────────────────────────────
// Supports both single-column and composite unique keys.
// For each row:
//   1. Check if the unique key (single or composite) exists in DB
//   2. If yes  → DELETE the old row, INSERT the new one
//   3. If no   → plain INSERT
//
// Runs inside a single transaction per batch.
async function upsertRows(table, rows) {
  const cols  = TABLE_COLS[table];
  const ukRaw = UNIQUE_KEYS[table];
  // Normalise to array so the rest of the code is uniform
  const ukCols = Array.isArray(ukRaw) ? ukRaw : [ukRaw];
  const conn   = await pool.getConnection();

  let inserted = 0, updated = 0, skipped = 0;

  try {
    await conn.beginTransaction();

    for (const row of rows) {
      // Collect unique key values — skip row if ANY key part is missing
      const ukVals = ukCols.map(c => row[c]);
      if (ukVals.some(v => v === null || v === undefined || v === '')) {
        skipped++; continue;
      }

      // Build values object — only the columns defined for this table
      const vals = {};
      for (const col of cols) {
        const v = row[col];
        vals[col] = (v === '' || v === undefined || v === null) ? null : v;
      }

      // Build WHERE clause for composite key  e.g. `bill_no` = ? AND `bill_date` = ?
      const whereClause = ukCols.map(c => `\`${c}\` = ?`).join(' AND ');

      // Check existence
      const [existing] = await conn.query(
        `SELECT id FROM \`${table}\` WHERE ${whereClause} LIMIT 1`,
        ukVals
      );

      if (existing.length > 0) {
        // DELETE old + INSERT new
        await conn.query(
          `DELETE FROM \`${table}\` WHERE ${whereClause}`,
          ukVals
        );
        updated++;
      } else {
        inserted++;
      }

      // INSERT (runs for both new and replaced rows)
      await conn.query(
        `INSERT INTO \`${table}\` (${cols.map(c => `\`${c}\``).join(',')})
         VALUES (${cols.map(() => '?').join(',')})`,
        cols.map(c => vals[c] ?? null)
      );
    }

    await conn.commit();
    return { inserted, updated, skipped };

  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

// ── ROUTES ───────────────────────────────────────────────────
// POST /sync/:sheet
// Body: { rows: [ { col: val, ... }, ... ] }
// The frontend sends already-normalized rows using the same
// field names as TABLE_COLS above.

const VALID_TABLES = ['uni','mor','mrr','sr','srr','pay'];

app.post('/sync/:sheet', async (req, res) => {
  const { sheet } = req.params;

  if (!VALID_TABLES.includes(sheet)) {
    return res.status(400).json({ error: `Unknown sheet: ${sheet}` });
  }

  const { rows } = req.body;
  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ error: 'No rows provided' });
  }

  try {
    const result = await upsertRows(sheet, rows);
    console.log(`[${sheet.toUpperCase()}] inserted=${result.inserted} updated=${result.updated} skipped=${result.skipped}`);
    res.json({ success: true, ...result });
  } catch (err) {
    console.error(`[${sheet.toUpperCase()}] DB error:`, err.message);
    res.status(500).json({ error: err.message });
  }
});

// Health check
app.get('/health', (_, res) => res.json({ status: 'ok' }));

// ── SERVE FRONTEND ────────────────────────────────────────────
// Serves myntra-recon-v6.html at the root URL so clients just
// open your Railway URL and the app loads immediately.
const path = require('path');
app.get('/', (_, res) => {
  res.sendFile(path.join(__dirname, 'myntra-recon-v6.html'));
});

// ── START ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`🚀 API running on http://localhost:${PORT}`));
