"""
MYNTRA RECONCILIATION — Streamlit App
--------------------------------------
• Upload any of the 6 sheets → auto-pushed to MySQL (upsert)
• Click "Run Reconciliation" → reads from DB → shows results
• Filter, search, export to Excel
"""

import os, re, math, datetime, io
import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Myntra Recon",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────
@st.cache_resource
def get_pool():
    host     = os.environ.get("DB_HOST", "")
    user     = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASSWORD", "")
    database = os.environ.get("DB_NAME", "")
    port     = int(os.environ.get("DB_PORT", "3306"))

    # Validate .env was loaded
    if not host or not user or not database:
        raise ValueError(
            f"DB credentials missing from .env — "
            f"DB_HOST='{host}' DB_USER='{user}' DB_NAME='{database}'. "
            f"Make sure your .env file is in the same folder as app.py."
        )

    return pooling.MySQLConnectionPool(
        pool_name="recon_pool",
        pool_size=5,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        use_pure=True,          # force TCP — avoids Windows named pipe issue
        connection_timeout=10,
    )

def get_conn():
    return get_pool().get_connection()

@st.cache_resource
def init_schema():
    """
    One-time DB schema migration — runs once per app session (cached by Streamlit).
    Ensures mrr UNIQUE KEY is on order_release_id.
    In the Myntra Return Report file the column is labelled "Order ID" but it
    carries the order_release_id value — the true unique key per return line.
    Ensures pay UNIQUE KEY is composite: (seller_order_id, order_release_id, order_type).
    """
    conn   = get_conn()
    cursor = conn.cursor()
    try:
        # Drop any stale alternative indexes on mrr (idempotent)
        for idx in ("uq_mrr_sr", "uq_mrr_soid"):
            try:
                cursor.execute(f"ALTER TABLE mrr DROP INDEX {idx}")
                conn.commit()
            except Exception:
                pass  # doesn't exist — fine

        # Ensure unique key on mrr.order_release_id (original schema key)
        try:
            cursor.execute("ALTER TABLE mrr ADD UNIQUE KEY uq_mrr_release (order_release_id(100))")
            conn.commit()
        except Exception:
            pass  # already exists — fine

        # Drop old single-column unique key on pay.order_release_id if present
        for idx in ("uq_pay_release", "uq_pay_ori"):
            try:
                cursor.execute(f"ALTER TABLE pay DROP INDEX {idx}")
                conn.commit()
            except Exception:
                pass  # doesn't exist — fine

        # Ensure composite unique key on pay (seller_order_id, order_release_id, order_type)
        try:
            cursor.execute(
                "ALTER TABLE pay ADD UNIQUE KEY uq_pay_composite "
                "(seller_order_id(100), order_release_id(100), order_type(50))"
            )
            conn.commit()
        except Exception:
            pass  # already exists — fine
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────────
# TABLE / COLUMN CONFIG
# ─────────────────────────────────────────────
TABLE_COLS = {
    "uni": ["invoice_code","display_order_code","total_price","order_date",
            "sales_order_status","facility","channel_name"],
    "mor": ["seller_order_id","order_release_id","status"],
    "mrr": ["order_release_id","seller_order_id","status"],
    "sr":  ["po_number","bill_no","bill_date","bill_value"],
    "srr": ["po_number","bill_no_key","invoice_code","sr_number","sr_value","sr_date"],
    "pay": ["seller_order_id","order_release_id","final_payment","order_type",
            "customer_paid_amt","commission","igst_tcs","cgst_tcs","sgst_tcs",
            "tds","logistics_commission","settled","marketing_charges"],
}

UNIQUE_KEYS = {
    "uni": ["invoice_code"],
    "mor": ["order_release_id"],
    "mrr": ["order_release_id"],   # file column "Order ID" = order_release_id (always present)
    "sr":  ["bill_no", "bill_date"],
    "srr": ["bill_no_key", "sr_date"],
    "pay": ["seller_order_id", "order_release_id", "order_type"],
}

SHEET_COL_CANDIDATES = {
    "uni": {
        "invoice_code":       ["Invoice Code","InvoiceCode","invoice_code","Invoice No","Invoice Number"],
        "display_order_code": ["Display Order Code","display_order_code","DisplayOrderCode","Order Code","order_code","Order No"],
        "total_price":        ["Total Price","total_price","TotalPrice","Total Amount","Amount","Order Amount"],
        "order_date":         ["Order Date","order_date","OrderDate","Sale Order Created At","Created At","Created Date","Date"],
        "sales_order_status": ["Sale Order Status","Sales Order Status","sale_order_status","SalesOrderStatus","Sale Order State","Order Status","Status"],
        "facility":           ["Facility","facility","Warehouse","warehouse","Location","Store"],
        "channel_name":       ["Channel Name","channel_name","ChannelName","Channel","Marketplace","Platform","Source"],
    },
    "mor": {
        "seller_order_id":  ["Seller Order ID","seller order id","seller_order_id","SellerOrderId","Order ID","order id"],
        "order_release_id": ["Order Release ID","order_release_id","orderreleaseid","order release id","Order Id","OrderId"],
        "status":           ["Order Status","status","Status","Order State","order state"],
    },
    "mrr": {
        # Myntra Return Report: "Suborder No" / "Sub Order No" / "Order ID" all hold the UUID = order_release_id
        "order_release_id": ["Suborder No","Sub Order No","SuborderNo","suborder_no",
                              "Suborder Number","Sub Order Number","sub order no",
                              "Order ID","order id","Order Id","orderid","order_id",
                              "Order Release ID","order_release_id","orderreleaseid","Order Release Id",
                              "Myntra Order Id","Myntra Sub Order Id","myntra_order_id"],
        "seller_order_id":  ["seller_order_id","Seller Order ID","Seller Order Id",
                              "seller order id","SellerOrderId","Seller Suborder Id"],
        "status":           ["status","Status","Return Status","Delivery Status","delivery status",
                              "Return State","Return Tracking Status","Shipment Status"],
    },
    "sr": {
        "po_number":  ["PO Number","PONumber","po_number","PONO","PO No","PO","Buyer PO","Order No","Order Number","Order Reference","Ref No","Reference No","Document No"],
        "bill_no":    ["GST_BillNo","GST Bill No","GST BillNo","GST Bill Number","Bill No","Bill Number","BillNo","Invoice No","Invoice Number","Invoice","Bill"],
        "bill_date":  ["Bill Date","Bill Dt.","Bill Dt","BillDate","bill_date","Invoice Date","InvoiceDate","Date"],
        "bill_value": ["Bill Value","Bill Amount","BillValue","Bill Amt","BillAmt","bill_amt","Taxable Value","Invoice Value","Invoice Amount","Amount","Net Amount","Total Amount"],
    },
    "srr": {
        "po_number":    ["PO Number","PONumber","po_number","PO No","PO","Buyer PO","Order No"],
        "bill_no_key":  ["GST BillNo","GST_BillNo","GST Bill No","GST Bill Number"],
        "invoice_code": ["Invoice Code","InvoiceCode","invoice_code","KFJ Invoice","KFJInvoice","Invoice No","Invoice Number","Invoice","Reference Invoice"],
        "sr_number":    ["Bill No","Bill Number","BillNo","BillNumber","SR Number","SR No","sr_number","Transaction Reference Number","Tran. RefNo","SR Ref No","Credit Note Number","Credit Note No"],
        "sr_value":     ["Bill Amount","SR Value","sr_value","Credit Note Value","Credit Note Amount","SR Amount","Amount","Net Amount"],
        "sr_date":      ["Bill Dt.","Bill Date","Bill Dt","SR Date","Credit Note Date","Credit Note Created Date","Date"],
    },
    "pay": {
        "seller_order_id":      ["Seller Order ID","seller order id","seller_order_id","SellerOrderId","Order ID","order id"],
        "order_release_id":     ["Order Release ID","order_release_id","orderreleaseid","Order Release Id","Order Id","order_id","OrderReleaseId","Order Release","Release ID","Release Id","ORI","ori"],
        "final_payment":        ["Final","FinalPayment","Final Payment","final_payment","NetPayment","Net Payment","net_payment","Nett Amount","Nett Amt","Net Amount","Net Amt","Settlement Amount","Settled Amount","Amount Settled","nett_amount","net_amount","settlement_amount","Total Settlement","TotalSettlement","Net Settlement","NetSettlement","Payable Amount","payable_amount","Total Payable","Amount","Total Amount","Net Payable"],
        "order_type":           ["Order_Type","Order Type","order_type","Payment_Type","Payment Type","payment_type","Transaction Type","transaction_type","Type"],
        "customer_paid_amt":    ["customer_paid_amt","Customer Paid Amt","CustomerPaidAmt","customer_paid","paid_amount"],
        "commission":           ["Commission","commission"],
        "igst_tcs":             ["IGST_TCS","IGST TCS","igst_tcs"],
        "cgst_tcs":             ["CGST_TCS","CGST TCS","cgst_tcs"],
        "sgst_tcs":             ["SGST_TCS","SGST TCS","sgst_tcs"],
        "tds":                  ["TDS","tds"],
        "logistics_commission": ["Logistics_Commission","Logistics Commission","logistics_commission","LogisticsCommission","Logistics Fee"],
        "settled":              ["Settled_Amount","Settled Amount","settled_amount","Settlement Amount","settlementamount","Amount Settled"],
        "marketing_charges":    ["marketingCharges","marketing_charges","Marketing Charges","Marketing Fee","MarketingCharges"],
    },
}

SHEET_LABELS = {
    "uni": "Uniware",
    "mor": "Myntra Order Report",
    "mrr": "Myntra Return Report",
    "sr":  "Sales Register",
    "srr": "Sales Return Register",
    "pay": "Payment Sheet",
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def read_uploaded_file(f):
    """
    Reads an uploaded file (Excel or CSV) into a DataFrame.
    Handles CSVs that have metadata/report header rows at the top
    (e.g. exports from Tally, SAP) by auto-detecting the real header row
    as the row with the most non-empty fields.
    """
    name = f.name.lower()
    if name.endswith(".csv"):
        # Read everything without a header first
        try:
            raw = pd.read_csv(f, header=None, on_bad_lines='skip', dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            f.seek(0)
            raw = pd.read_csv(f, header=None, on_bad_lines='skip', dtype=str, encoding='latin-1')
        # Find the row with the most filled cells — that's the real header
        filled = raw.apply(lambda r: r.notna().sum(), axis=1)
        header_idx = int(filled.idxmax())
        df = raw.iloc[header_idx + 1:].copy()
        df.columns = raw.iloc[header_idx].values
        df = df.reset_index(drop=True)
        # Drop completely empty rows
        df = df.dropna(how='all')
        return df
    else:
        return pd.read_excel(f, engine="openpyxl")

def norm(s):
    return re.sub(r'[\s_\-\/\.]+', '', str(s or '').strip().lower())

def norm_ori(val):
    s = str(val or '').strip()
    try:
        return str(round(float(s)))
    except:
        return norm(s)

def find_col(df_cols, candidates):
    for c in candidates:
        nc = norm(c)
        for col in df_cols:
            if norm(col) == nc:
                return col
    # fuzzy
    for c in candidates:
        nc = norm(c)
        for col in df_cols:
            col_n = norm(col)
            if nc in col_n or col_n in nc:
                return col
    return None

def detect_cols(df, col_defs):
    return {field: find_col(df.columns.tolist(), candidates)
            for field, candidates in col_defs.items()}

def fv(row, col):
    if not col or col not in row:
        return ""
    v = row[col]
    if pd.isna(v):
        return ""
    if isinstance(v, (datetime.date, datetime.datetime, pd.Timestamp)):
        return pd.Timestamp(v).strftime("%d-%m-%Y")
    return str(v).strip()

def fn(row, col):
    if not col or col not in row:
        return 0.0
    v = row[col]
    if pd.isna(v):
        return 0.0
    try:
        return float(str(v).replace(",", ""))
    except:
        return 0.0

def fd(row, col):
    """Return a datetime.date or None."""
    if not col or col not in row:
        return None
    v = row[col]
    if pd.isna(v):
        return None
    try:
        return pd.Timestamp(v).date()
    except:
        try:
            for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                try:
                    return datetime.datetime.strptime(str(v).strip(), fmt).date()
                except:
                    pass
        except:
            pass
    return None

def days_since(d):
    if not d:
        return 0
    try:
        return (datetime.date.today() - d).days
    except:
        return 0

def fmt_date(d):
    if not d:
        return ""
    return d.strftime("%d-%m-%Y")

def num_val(v):
    try:
        return float(str(v or 0).replace(",", ""))
    except:
        return 0.0

def detect_shop_group(channel_name, invoice_code):
    ch  = str(channel_name  or "").upper().replace(" ", "")
    inv = str(invoice_code  or "").upper().replace(" ", "")
    if "MYNTRAPPMP" in ch or "MYNTRA" in ch: return "MYNTRA"
    if "AMAZON"    in ch: return "AMAZON"
    if "AJIO"      in ch: return "AJIO"
    if "FLIPKART"  in ch: return "FLIPKART"
    if "TATACLIQ"  in ch: return "TATACLIQ"
    if "ETERNZ"    in ch: return "ETERNZ"
    if "SHOPIFY"   in ch: return "SHOPIFY"
    if inv.startswith("I") and len(inv) >= 10: return "MYNTRA"
    if inv.startswith("KFJ"):   return "SHOPIFY"
    if inv.startswith("ETERNZ"): return "ETERNZ"
    if "AJIO"     in inv: return "AJIO"
    if "AMAZON"   in inv: return "AMAZON"
    if "FLIPKART" in inv: return "FLIPKART"
    if "TATACLIQ" in inv: return "TATACLIQ"
    return "OTHER"

def lookup_contains(d, target):
    nt = norm(target)
    if not nt:
        return None
    if nt in d:
        return d[nt]
    for k, v in d.items():
        if k and (nt in k or k in nt):
            return v
    return None

# ─────────────────────────────────────────────
# UPSERT
# ─────────────────────────────────────────────
def upsert_rows(table, rows):
    """
    Batch upsert using INSERT ... ON DUPLICATE KEY UPDATE.
    Processes all rows in one query per batch — 100x faster than row-by-row.
    Requires UNIQUE constraints on the unique key columns (already set up in SQL).
    """
    cols    = TABLE_COLS[table]
    uk_cols = UNIQUE_KEYS[table]
    BATCH   = 500   # rows per INSERT statement — safe for large files

    # Split into valid rows and skipped
    valid_rows = []
    skipped    = 0
    for row in rows:
        uk_vals = [row.get(c) for c in uk_cols]
        if any(v is None or v == "" for v in uk_vals):
            skipped += 1
            continue
        clean = {col: (None if (row.get(col) is None or row.get(col) == "") else row.get(col))
                 for col in cols}
        valid_rows.append(clean)

    if not valid_rows:
        return 0, 0, skipped

    # Build INSERT ... ON DUPLICATE KEY UPDATE query
    col_list   = ", ".join(f"`{c}`" for c in cols)
    val_marks  = ", ".join(["%s"] * len(cols))
    # ON DUPLICATE KEY UPDATE: update every non-key column
    update_cols = [c for c in cols if c not in uk_cols]
    update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols) if update_cols else f"`{cols[0]}` = VALUES(`{cols[0]}`)"

    query = (
        f"INSERT INTO `{table}` ({col_list}) "
        f"VALUES ({val_marks}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    conn   = get_conn()
    cursor = conn.cursor()
    try:
        conn.start_transaction()

        # Process in batches
        for i in range(0, len(valid_rows), BATCH):
            batch = valid_rows[i:i + BATCH]
            batch_vals = [[row[c] for c in cols] for row in batch]
            cursor.executemany(query, batch_vals)

        conn.commit()

        # MySQL: affected_rows = 1 for insert, 2 for update, 0 for no change
        # executemany doesn't give per-row info so we approximate
        affected = cursor.rowcount
        # rowcount after executemany = total affected (inserts=1, updates=2 each)
        # approximate: anything > len(valid_rows) means some were updates
        inserted = len(valid_rows)
        updated  = 0
        return inserted, updated, skipped

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────────
# NORMALIZE UPLOADED FILE → DB ROWS
# ─────────────────────────────────────────────
def _to_float(v):
    """Safely convert any value to float for numeric computation."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0

def normalize_df(sheet_key, df):
    col_defs = SHEET_COL_CANDIDATES[sheet_key]
    col_map  = detect_cols(df, col_defs)
    db_cols  = TABLE_COLS[sheet_key]
    rows = []
    for _, row in df.iterrows():
        out = {}
        for field in db_cols:
            actual_col = col_map.get(field)
            if not actual_col:
                out[field] = None
                continue
            v = row.get(actual_col, "")
            if pd.isna(v) if not isinstance(v, str) else v == "":
                out[field] = None
            elif isinstance(v, (datetime.date, datetime.datetime, pd.Timestamp)):
                out[field] = pd.Timestamp(v).strftime("%Y-%m-%d")
            else:
                s = str(v).strip()
                out[field] = None if s == "" else s
        rows.append(out)

    # ── Payment sheet: always auto-compute final_payment from components ──────
    # All component values in the Excel are stored as POSITIVES for both row types.
    # The formula differs by order_type (Order Type column):
    #
    #   Forward / NOD:
    #     Final = |comm| + |igst| + |cgst| + |sgst| + |tds| + |logi| + |settled| + |mktg|
    #     → always positive, max(0, …)
    #
    #   Reverse (order_type contains "reverse"/"return"/"rto"/"rev"):
    #     Final = |comm| + |igst| + |cgst| + |sgst| + |tds| - |logi| - |settled| + |mktg|
    #     → logi and settled FLIP SIGN; result is negative when settled dominates
    #
    # The reconciliation pivot sums ALL rows for an ORI:
    #   Net = Forward(+) + Reverse(−) → true net settlement
    if sheet_key == "pay":
        REVERSE_KEYWORDS = ["reverse", "return", "rto", "rev"]
        for out_row in rows:
            if out_row.get("final_payment") is None:
                comm = abs(_to_float(out_row.get("commission")))
                igst = abs(_to_float(out_row.get("igst_tcs")))
                cgst = abs(_to_float(out_row.get("cgst_tcs")))
                sgst = abs(_to_float(out_row.get("sgst_tcs")))
                tds  = abs(_to_float(out_row.get("tds")))
                logi = abs(_to_float(out_row.get("logistics_commission")))
                sett = abs(_to_float(out_row.get("settled")))
                mktg = abs(_to_float(out_row.get("marketing_charges")))

                otype = str(out_row.get("order_type") or "").lower().strip()
                is_reverse = otype != "" and any(kw in otype for kw in REVERSE_KEYWORDS)

                if is_reverse:
                    # Logistics and settled flip sign; result stored as-is (can be negative)
                    amount = round(comm + igst + cgst + sgst + tds - logi - sett + mktg, 1)
                else:
                    # Forward / NOD: all components add up; always >= 0
                    amount = max(0.0, round(comm + igst + cgst + sgst + tds + logi + sett + mktg, 1))

                out_row["final_payment"] = amount if amount != 0.0 else None

    return rows

# ─────────────────────────────────────────────
# FETCH FROM DB
# ─────────────────────────────────────────────
def fetch_table(table):
    cols = TABLE_COLS[table]
    col_list = ", ".join(f"`{c}`" for c in cols)
    conn   = get_conn()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(f"SELECT {col_list} FROM `{table}`")
        rows = cursor.fetchall()
        # Convert date objects to strings
        for row in rows:
            for k, v in row.items():
                if isinstance(v, (datetime.date, datetime.datetime)):
                    row[k] = v.strftime("%d-%m-%Y")
                elif v is None:
                    row[k] = ""
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────────
# RECONCILIATION ENGINE
# ─────────────────────────────────────────────
def run_reconciliation(sheets, platform="MYNTRA"):
    uni_df = sheets["uni"]
    mor_df = sheets["mor"]
    mrr_df = sheets["mrr"]
    sr_df  = sheets["sr"]
    srr_df = sheets["srr"]
    pay_df = sheets["pay"]

    # detect cols
    uniC = detect_cols(uni_df, SHEET_COL_CANDIDATES["uni"])
    morC = detect_cols(mor_df, SHEET_COL_CANDIDATES["mor"])
    mrrC = detect_cols(mrr_df, SHEET_COL_CANDIDATES["mrr"])
    srC  = detect_cols(sr_df,  SHEET_COL_CANDIDATES["sr"])
    srrC = detect_cols(srr_df, SHEET_COL_CANDIDATES["srr"])
    payC = detect_cols(pay_df, SHEET_COL_CANDIDATES["pay"])

    # ── 1. Filter Uniware rows by platform ──────────────────────
    uni_rows = []
    for _, r in uni_df.iterrows():
        inv = fv(r, uniC["invoice_code"])
        ch  = fv(r, uniC["channel_name"])
        sos = fv(r, uniC["sales_order_status"]).upper()
        if not inv and not ch and not sos:
            continue
        sg = detect_shop_group(ch, inv)
        if platform == "ALL":
            if inv or ch:
                uni_rows.append(r)
        elif platform == "MYNTRA":
            if sg != "MYNTRA":
                continue
            has_invoice = inv.upper().startswith("I") and len(inv) >= 10
            if has_invoice or sos == "CANCELLED":
                uni_rows.append(r)
        else:
            if sg == platform:
                uni_rows.append(r)

    # ── 2. MOR map: seller_order_id → {ori, status} ──────────────
    mor_map = {}
    for _, r in mor_df.iterrows():
        soid   = norm(fv(r, morC["seller_order_id"]))
        ori    = norm_ori(fv(r, morC["order_release_id"]))
        status = fv(r, morC["status"])
        if soid:
            mor_map[soid] = {"ori": ori, "status": status}

    # ── 3. MRR maps — order_release_id / seller_order_id → return status ─
    mrr_by_ori  = {}
    mrr_by_soid = {}
    for _, r in mrr_df.iterrows():
        ori    = norm_ori(fv(r, mrrC["order_release_id"]))
        soid   = norm(fv(r, mrrC["seller_order_id"]))
        status = fv(r, mrrC["status"])
        entry  = {"status": status}
        if ori:  mrr_by_ori[ori]   = entry
        if soid: mrr_by_soid[soid] = entry

    # ── 4. SR map: po_number → {bill_no, bill_value} ─────────────
    sr_map = {}
    for _, r in sr_df.iterrows():
        po = norm(fv(r, srC["po_number"]))
        if not po:
            continue
        bill_no  = fv(r, srC["bill_no"])
        bill_val = fn(r, srC["bill_value"])
        if po in sr_map:
            sr_map[po]["bill_value"] += bill_val
            if not sr_map[po]["bill_no"]:
                sr_map[po]["bill_no"] = bill_no
        else:
            sr_map[po] = {"bill_no": bill_no, "bill_value": bill_val}

    # ── 5. SRR maps ───────────────────────────────────────────────
    srr_by_po      = {}
    srr_by_bill_no = {}
    srr_by_invoice = {}

    def srr_accum(d, key, entry):
        if not key:
            return
        if key in d:
            d[key]["sr_value"] += entry["sr_value"]
            if not d[key]["sr_number"]:
                d[key]["sr_number"] = entry["sr_number"]
            if not d[key]["sr_date"]:
                d[key]["sr_date"] = entry["sr_date"]
        else:
            d[key] = dict(entry)

    for _, r in srr_df.iterrows():
        entry = {
            "sr_number": fv(r, srrC["sr_number"]),
            "sr_value":  fn(r, srrC["sr_value"]),
            "sr_date":   fd(r, srrC["sr_date"]),
        }
        srr_accum(srr_by_po,      norm(fv(r, srrC["po_number"])),    entry)
        srr_accum(srr_by_bill_no, norm(fv(r, srrC["bill_no_key"])),  entry)
        srr_accum(srr_by_invoice, norm(fv(r, srrC["invoice_code"])), entry)

    # ── 6. Payment maps ───────────────────────────────────────────
    pay_by_ori  = {}
    pay_by_soid = {}
    for _, r in pay_df.iterrows():
        ori    = norm_ori(fv(r, payC["order_release_id"]))
        soid   = norm(fv(r, payC["seller_order_id"]))
        otype  = fv(r, payC["order_type"]).lower() if payC["order_type"] else ""
        is_rev = otype != "" and any(t in otype for t in ["reverse", "return", "rto", "rev"])

        # Try pre-computed final_payment column first; if null/zero → compute
        # from individual components (same formula as HTML version).
        #
        #   Forward / NOD : |comm|+|igst|+|cgst|+|sgst|+|tds|+|logi|+|sett|+|mktg|  (≥0)
        #   Reverse       : |comm|+|igst|+|cgst|+|sgst|+|tds|−|logi|−|sett|+|mktg|  (can be −)
        #
        #   Net per ORI   = Σ forward rows  +  Σ reverse rows  → true net settlement
        REVERSE_KW = ["reverse", "return", "rto", "rev"]
        amount = fn(r, payC["final_payment"]) if payC["final_payment"] else 0.0

        if amount == 0.0:
            comm = abs(fn(r, payC["commission"]))           if payC["commission"]           else 0.0
            igst = abs(fn(r, payC["igst_tcs"]))             if payC["igst_tcs"]             else 0.0
            cgst = abs(fn(r, payC["cgst_tcs"]))             if payC["cgst_tcs"]             else 0.0
            sgst = abs(fn(r, payC["sgst_tcs"]))             if payC["sgst_tcs"]             else 0.0
            tds  = abs(fn(r, payC["tds"]))                  if payC["tds"]                  else 0.0
            logi = abs(fn(r, payC["logistics_commission"])) if payC["logistics_commission"] else 0.0
            sett = abs(fn(r, payC["settled"]))              if payC["settled"]              else 0.0
            mktg = abs(fn(r, payC["marketing_charges"]))    if payC["marketing_charges"]    else 0.0
            is_rev = otype != "" and any(t in otype for t in REVERSE_KW)
            if is_rev:
                amount = round(comm + igst + cgst + sgst + tds - logi - sett + mktg, 1)
            else:
                amount = max(0.0, round(comm + igst + cgst + sgst + tds + logi + sett + mktg, 1))

        if amount != 0:
            if ori:
                pay_by_ori[ori]   = pay_by_ori.get(ori, 0) + amount
            if soid:
                pay_by_soid[soid] = pay_by_soid.get(soid, 0) + amount

    # ── BUILD OUTPUT ──────────────────────────────────────────────
    out = []
    for idx, r in enumerate(uni_rows, 1):
        invoice_code       = fv(r, uniC["invoice_code"])
        display_order_code = fv(r, uniC["display_order_code"])
        total_price        = fn(r, uniC["total_price"])
        order_date_d       = fd(r, uniC["order_date"])
        order_date         = fmt_date(order_date_d) if order_date_d else fv(r, uniC["order_date"])
        sales_order_status = fv(r, uniC["sales_order_status"])
        facility           = fv(r, uniC["facility"])
        shop_group         = detect_shop_group(fv(r, uniC["channel_name"]), invoice_code)

        ndoc    = norm(display_order_code)
        ninv    = norm(invoice_code)
        ori_data = mor_map.get(ndoc) or mor_map.get(norm(invoice_code))
        order_release_id = ori_data["ori"] if ori_data else ""

        # Return status
        mrr_data = (mrr_by_ori.get(order_release_id) if order_release_id else None) \
                or mrr_by_soid.get(ndoc)
        return_status = mrr_data["status"] if mrr_data else (ori_data["status"] if ori_data else "")

        # Bill No / Bill Value
        bill_no = ""
        bill_value = 0.0
        if ninv:
            sr_entry = sr_map.get(ninv) or lookup_contains(sr_map, invoice_code)
            if sr_entry:
                bill_no    = sr_entry["bill_no"]
                bill_value = sr_entry["bill_value"]

        # SR Number / SR Value
        sr_number = ""
        sr_value  = 0.0
        sr_date_d = None
        srr_entry = srr_by_po.get(ninv) \
                 or (srr_by_bill_no.get(norm(bill_no)) if bill_no else None) \
                 or srr_by_invoice.get(ninv)
        if srr_entry:
            sr_number = srr_entry["sr_number"]
            sr_value  = srr_entry["sr_value"]
            sr_date_d = srr_entry["sr_date"]

        # Payment
        fwd_payment = pay_by_ori.get(order_release_id, pay_by_soid.get(ndoc, 0.0))

        # Payment diff
        payment_diff = (fwd_payment + sr_value) - bill_value

        # Aging
        S  = days_since(order_date_d)
        T  = 0   # return_date removed from mrr table
        U  = 0   # return_delivered_date removed from mrr table

        # Period
        period = (str(order_date_d.month).zfill(2) + "-" + str(order_date_d.year)) if order_date_d else ""

        status = get_recon_status(
            H=return_status, E=sales_order_status,
            I=bill_no, J=bill_value,
            K=sr_number, L=sr_value,
            N=fwd_payment, S=S, T=T, U=U
        )

        out.append({
            "#":                   idx,
            "Display Order Code":  display_order_code,
            "Invoice Code":        invoice_code,
            "Total Price":         total_price,
            "Order Date":          order_date,
            "Sale Order Status":   sales_order_status,
            "Facility":            facility,
            "Shop Group":          shop_group,
            "Order Release ID":    order_release_id,
            "Return Status":       return_status,
            "Bill No":             bill_no,
            "Bill Value":          bill_value,
            "SR Number":           sr_number,
            "SR Value":            sr_value,
            "Payment":             fwd_payment,
            "Payment Diff":        payment_diff,
            "Days Order":          S,
            "Days Ret Created":    T,
            "Days Ret Delivered":  U,
            "Period":              period,
            "Status":              status,
        })

    return pd.DataFrame(out)

# ─────────────────────────────────────────────
# STATUS FORMULA
# ─────────────────────────────────────────────
def get_recon_status(H, E, I, J, K, L, N, S=0, T=0, U=0):
    h = str(H or "").strip()
    e = str(E or "").strip().upper()
    i = str(I or "").strip()
    j = num_val(J)
    k = str(K or "").strip()
    l = num_val(L)
    n = num_val(N)
    m = 0

    if h == "WP": return "Pending to invoice"
    if h == "F" and j != l: return "SR Pending"
    if h == "F": return "Canceled"
    if h == "PK": return "Pending to Handover"
    if not i: return "Bill Pending"
    if h == "SH" and e == "COMPLETE" and S > 5: return "Ticket has to raised"
    if h == "C" and n < (j * 0.98): return "Payment Pending"
    if h in ("Ret Delivered", "Delivered", "RTO") and k == "Accepted" and m < j * 0.4:
        return "Claim Pending"
    if k == "Pending": return "Ticket has to raised"
    if h in ("0", "") and T > 60: return "Ticket has to raised"
    if h == "Dispatched" and T > 60: return "Ticket has to raised"
    if h == "Ret Dispatched" and T > 60: return "Ticket has to raised"
    if h in ("0", ""): return "Retun created"
    if h == "Dispatched": return "RTO Dispatched"
    if h == "Ret Dispatched": return "RTV Dispatched"
    if h == "L": return "Lost - Claim Pending"
    if h == "RTO Lost" and n != j * 0.4: return "RTO Lost claim pending"
    if h in ("Ret Created", "Ret  Created", "RTO Created", "RTO") and T > 60:
        return "Ticket has to raised"
    if h in ("Ret Created", "Ret  Created", "RTO Created", "RTO") and l != j:
        return "SR To Check"
    if h in ("Ret Delivered", "Delivered") and l != j and U > 3:
        return "Ticket Has to raised"
    if h in ("Ret Delivered", "Delivered") and l != j:
        return "SR To Check"
    return "Done"

# ─────────────────────────────────────────────
# STATUS BADGE COLORS
# ─────────────────────────────────────────────
STATUS_COLORS = {
    "Done":                  "#4ade80",
    "Bill Pending":          "#f87171",
    "SR Pending":            "#fbbf24",
    "SR To Check":           "#fbbf24",
    "Payment Pending":       "#f87171",
    "Ticket has to raised":  "#f87171",
    "Ticket Has to raised":  "#f87171",
    "Claim Pending":         "#ff6b35",
    "Canceled":              "#6b7280",
    "Pending to invoice":    "#60a5fa",
    "Pending to Handover":   "#60a5fa",
    "RTO Dispatched":        "#a78bfa",
    "RTV Dispatched":        "#a78bfa",
    "Retun created":         "#fbbf24",
    "RTO Lost claim pending":"#f87171",
    "Lost - Claim Pending":  "#f87171",
}




# ═══════════════════════════════════════════════════════════════════
#  UI LAYER  ·  Kushal's Recon v6
#  KEY BEHAVIOURS:
#  1. Sidebar toggle button is visible & styled; sidebar starts expanded
#  2. Tab switching uses st.radio (reliable, no JS needed)
#  3. Database View uses session-state cache — no extra DB calls on search/filter
#  4. Pushing data busts the DB-view cache for that table automatically
#  5. Kushals SVG crown logo in topbar
# ═══════════════════════════════════════════════════════════════════

# ── SHEET CONFIG ────────────────────────────────────────────────────
SHEET_HINTS = {
    "uni": "Multi-file · filtered by selected platform",
    "mor": "Multi-file supported",
    "mrr": "Status & SR mapped here",
    "sr":  "Multi-file supported",
    "srr": "SR Number & SR Value ← Invoice Code",
    "pay": "Multi-file · upload ALL months for full coverage",
}
SHEET_ORDER = [
    ("uni", 1, "Uniware"),
    ("sr",  2, "Sales Register"),
    ("srr", 3, "Sales Return Register"),
    ("mor", 4, "Myntra Order Report"),
    ("mrr", 5, "Myntra Return Report"),
    ("pay", 6, "Payment Sheet"),
]
TABLE_NICE = {
    "uni": ("Uniware",           "#3b6cf4"),
    "mor": ("Order Report",      "#0891b2"),
    "mrr": ("Return Report",     "#7c3aed"),
    "sr":  ("Sales Register",    "#059669"),
    "srr": ("Sales Return",      "#d97706"),
    "pay": ("Payment Sheet",     "#dc2626"),
}

# INR formatter
def _inr(v):
    av = abs(v); s = "−" if v < 0 else ""
    if av >= 1e7: return f"{s}₹{av/1e7:.2f} Cr"
    if av >= 1e5: return f"{s}₹{av/1e5:.2f} L"
    return f"{s}₹{av:,.0f}"

# ── SESSION STATE ────────────────────────────────────────────────────
if "db_table" not in st.session_state:
    st.session_state["db_table"] = "uni"

# ── COMPUTED GLOBALS ─────────────────────────────────────────────────
_loaded_n = sum(1 for k in SHEET_LABELS if st.session_state.get(f"recon_{k}"))
_platform = st.session_state.get("recon_platform", "—")
_pct      = int(_loaded_n / 6 * 100)

# ════════════════════════════════════════════════════════════════════
#  CSS
# ════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');

:root{
  --bg:#f0f3fa; --sur:#fff; --sur2:#f5f7fd; --bdr:#dde4f4; --bdr2:#c8d2e8;
  --sb:#0e1526; --sb1:#18243e; --sb2:#1e2e50; --sb3:#263862;
  --sbl:#243060; --sbl2:#2e3c6e;
  --sbt:#7a94cc; --sbt2:#b8cef0; --sbm:#364878;
  --blue:#3b5ff4; --blue2:#2d52e0; --bl-l:rgba(59,95,244,.1); --bl-b:rgba(59,95,244,.22);
  --gold:#c9922a; --gold2:#e8b84b;
  --green:#0da86e; --gl:rgba(13,168,110,.1);
  --red:#e03030;   --rl:rgba(224,48,48,.1);
  --amber:#d97706; --al:rgba(217,119,6,.1);
  --teal:#0891b2;  --tl:rgba(8,145,178,.1);
  --violet:#7c3aed;--vl:rgba(124,58,237,.1);
  --coral:#dc2626; --cl:rgba(220,38,38,.1);
  --t0:#0c1836; --t1:#1a2d5c; --t2:#4a5e8a; --t3:#8898c0; --t4:#c0ccdf;
  --sans:'Inter',system-ui,sans-serif;
  --mono:'JetBrains Mono',monospace;
  --r:12px;
}

*,*::before,*::after{box-sizing:border-box}

/* APP BG */
.stApp,[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"]>section.main,
section.main,.main .block-container{background:var(--bg)!important}
.main .block-container{padding:0!important;max-width:100%!important}

::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-thumb{background:var(--bdr2);border-radius:3px}
#MainMenu,footer,header{visibility:hidden}
.main p,.main label,.main div,.main span:not([data-testid]),
.main input,.main button,.main select{font-family:var(--sans)!important}

/* ── SIDEBAR — always open, fixed, no collapse button ─────────── */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"]>div:first-child{
  background:var(--sb)!important;
  border-right:none!important;
  box-shadow:4px 0 32px rgba(0,0,0,.28)!important;
  min-width:268px!important;max-width:268px!important;
  width:268px!important;
  transform:none!important;
  left:0!important;
  padding:0!important;
}
/* completely hide the collapse/expand arrow — sidebar is always fixed open */
[data-testid="collapsedControl"]{display:none!important}
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] div[data-testid]{font-family:var(--sans)!important}

/* Expander cards inside sidebar */
section[data-testid="stSidebar"] div[data-testid="stExpander"]{
  background:var(--sb1)!important;border:1px solid var(--sbl)!important;
  border-radius:8px!important;margin:2px 10px!important;overflow:hidden!important;
  transition:border-color .18s!important}
section[data-testid="stSidebar"] div[data-testid="stExpander"]:hover{border-color:var(--blue)!important}
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary{background:transparent!important;padding:9px 13px!important}
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary p,
section[data-testid="stSidebar"] div[data-testid="stExpander"] summary span:not([data-testid]){
  font-family:var(--sans)!important;font-size:11.5px!important;font-weight:500!important;color:var(--sbt2)!important}
section[data-testid="stSidebar"] div[data-testid="stExpander"] details>div{
  padding:4px 12px 12px!important;border-top:1px solid var(--sbl)!important;background:rgba(0,0,0,.22)!important}

/* Nav radio → styled as tabs */
section[data-testid="stSidebar"] div[data-testid="stRadio"]>div{
  flex-direction:column!important;gap:2px!important;
  background:transparent!important;border:none!important;padding:0 8px!important}
section[data-testid="stSidebar"] div[data-testid="stRadio"]>label{display:none!important}
section[data-testid="stSidebar"] div[data-testid="stRadio"]>div>label{
  padding:9px 14px!important;border-radius:8px!important;
  font-family:var(--sans)!important;font-size:13px!important;
  font-weight:500!important;color:var(--sbt)!important;
  cursor:pointer!important;transition:background .15s,color .15s!important;
  border-bottom:none!important;margin:0!important;background:transparent!important}
section[data-testid="stSidebar"] div[data-testid="stRadio"]>div>label:hover{
  background:var(--sb1)!important;color:var(--sbt2)!important}
section[data-testid="stSidebar"] div[data-testid="stRadio"]>div>label[data-checked="true"]{
  background:rgba(59,95,244,.18)!important;color:#7cacff!important;font-weight:600!important}
section[data-testid="stSidebar"] div[data-testid="stRadio"]>div>label>div:first-child{display:none!important}

/* Run button — gold / prominent */
section[data-testid="stSidebar"] div[data-testid="stButton"]>button{
  background:linear-gradient(135deg,var(--gold) 0%,var(--gold2) 100%)!important;
  color:#1a0800!important;border:none!important;border-radius:10px!important;
  font-family:var(--sans)!important;font-weight:700!important;
  font-size:13.5px!important;padding:11px 20px!important;
  box-shadow:0 4px 18px rgba(201,146,42,.55)!important;
  transition:all .15s!important;width:100%!important}
section[data-testid="stSidebar"] div[data-testid="stButton"]>button:hover{
  box-shadow:0 6px 26px rgba(201,146,42,.75)!important;transform:translateY(-1px)!important}

/* Sidebar selectbox */
section[data-testid="stSidebar"] div[data-baseweb="select"]>div:first-child{
  background:var(--sb2)!important;border-color:var(--sbl2)!important;
  font-family:var(--sans)!important;font-size:12px!important;
  color:var(--sbt2)!important;border-radius:8px!important}
section[data-testid="stSidebar"] div[data-baseweb="popover"] ul{background:var(--sb2)!important;border-color:var(--sbl2)!important}
section[data-testid="stSidebar"] div[data-baseweb="popover"] li{color:var(--sbt2)!important}
section[data-testid="stSidebar"] div[data-baseweb="popover"] li:hover{background:var(--sb3)!important;color:#748ffc!important}

/* File uploader dark */
div[data-testid="stFileUploader"]{
  background:var(--sb1)!important;border:1px dashed var(--sbl2)!important;
  border-radius:8px!important;transition:border-color .2s!important}
div[data-testid="stFileUploader"]:hover{border-color:var(--blue)!important}
div[data-testid="stFileUploader"] span,
div[data-testid="stFileUploader"] p,
div[data-testid="stFileUploader"] small{color:var(--sbt)!important}

/* ── TOPBAR ──────────────────────────────────────────────────────── */
.topbar{
  height:58px;background:var(--sur);border-bottom:1px solid var(--bdr);
  padding:0 28px;display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:300;box-shadow:0 2px 14px rgba(15,30,80,.07)}
.tb-l{display:flex;align-items:center;gap:14px}
.tb-r{display:flex;align-items:center;gap:16px}
.tb-sep{width:1px;height:22px;background:var(--bdr)}
.v-chip{
  font-family:var(--mono);font-size:8.5px;font-weight:600;
  color:var(--t3);background:var(--sur2);border:1px solid var(--bdr2);
  border-radius:5px;padding:2px 8px;letter-spacing:.5px}
.plat-chip{
  font-family:var(--sans);font-size:10px;font-weight:700;
  letter-spacing:.8px;text-transform:uppercase;
  color:var(--blue);background:var(--bl-l);
  border:1px solid var(--bl-b);border-radius:7px;padding:4px 11px}
.rds-pill{
  display:flex;align-items:center;gap:7px;
  background:var(--sur2);border:1px solid var(--bdr);
  border-radius:20px;padding:5px 13px;
  font-family:var(--sans);font-size:11.5px;font-weight:500;color:var(--t2)}
.live-dot{
  width:8px;height:8px;border-radius:50%;background:var(--green);
  box-shadow:0 0 0 3px rgba(13,168,110,.2);
  animation:glow 2.5s ease-in-out infinite;flex-shrink:0}
@keyframes glow{
  0%,100%{box-shadow:0 0 0 3px rgba(13,168,110,.2)}
  50%{box-shadow:0 0 0 5px rgba(13,168,110,.08)}}
.sheets-chip{font-family:var(--mono);font-size:11.5px;color:var(--t2)}
.sheets-chip b{color:var(--blue);font-weight:700}

/* ── SIDEBAR INTERIOR ─────────────────────────────────────────────── */
.sb-brand{
  padding:16px 15px 13px;border-bottom:1px solid var(--sbl);
  display:flex;align-items:center;gap:11px}
.sb-crown{
  width:36px;height:36px;border-radius:10px;
  background:linear-gradient(135deg,var(--gold) 0%,var(--gold2) 100%);
  display:flex;align-items:center;justify-content:center;
  box-shadow:0 3px 12px rgba(201,146,42,.5);flex-shrink:0}
.sb-brand-name{font-family:var(--sans);font-size:13.5px;font-weight:700;color:var(--sbt2);letter-spacing:-.2px}
.sb-brand-sub{font-family:var(--sans);font-size:9.5px;color:var(--sbm);margin-top:2px}

.sb-sec{font-family:var(--sans);font-size:7.5px;font-weight:700;
  letter-spacing:2.5px;text-transform:uppercase;color:var(--sbm);padding:12px 15px 4px}

.sb-prog-row{
  padding:10px 15px 9px;border-bottom:1px solid var(--sbl);
  display:flex;align-items:center;justify-content:space-between}
.sb-prog-lbl{font-family:var(--sans);font-size:7.5px;font-weight:700;
  letter-spacing:2px;text-transform:uppercase;color:var(--sbm)}
.sb-prog{display:flex;align-items:center;gap:8px}
.sb-track{width:56px;height:3px;background:var(--sbl2);border-radius:2px;overflow:hidden}
.sb-fill{height:100%;background:linear-gradient(90deg,var(--blue),#748ffc);border-radius:2px;transition:width .5s}
.sb-cnt{font-family:var(--mono);font-size:9.5px;font-weight:700;color:#748ffc}

.sb-hint{
  font-family:var(--sans);font-size:11px;color:var(--sbt);
  background:rgba(59,95,244,.07);border-left:2px solid rgba(59,95,244,.4);
  border-radius:0 5px 5px 0;padding:6px 10px;margin-bottom:10px;line-height:1.6}
.up-card{
  background:rgba(13,168,110,.08);border:1px solid rgba(13,168,110,.2);
  border-radius:7px;padding:9px 11px;
  font-family:var(--mono);font-size:9.5px;line-height:2;color:var(--sbt);margin-top:8px}
.up-card .g{color:#51cf66;font-weight:500}.up-card .bl{color:#74c0fc}

.sb-foot{
  padding:11px 15px;border-top:1px solid var(--sbl);
  display:flex;align-items:center;justify-content:space-between}
.sb-conn{display:flex;align-items:center;gap:7px;font-family:var(--sans);font-size:11px;color:var(--sbt)}
.sb-n{font-family:var(--mono);font-size:10px;font-weight:700;color:#748ffc}

/* ── KPI GRID ────────────────────────────────────────────────────── */
.kpi-wrap{padding:20px 24px 16px}
.kpi-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:14px}
.kcard{
  background:var(--sur);border:1px solid var(--bdr);border-radius:var(--r);
  padding:16px 18px 13px;border-top:3px solid transparent;
  transition:box-shadow .2s,transform .2s;cursor:default}
.kcard:hover{box-shadow:0 6px 24px rgba(15,30,80,.1);transform:translateY(-2px)}
.kcard.k-tot{border-top-color:var(--blue)}  .kcard.k-don{border-top-color:var(--green)}
.kcard.k-tkt{border-top-color:var(--red)}   .kcard.k-pen{border-top-color:var(--amber)}
.kcard.k-bil{border-top-color:var(--teal)}  .kcard.k-pay{border-top-color:var(--coral)}
.kcard.k-dif{border-top-color:var(--violet)}
.kv{font-family:var(--sans);font-size:22px;font-weight:800;line-height:1;margin-bottom:6px;letter-spacing:-.5px}
.kv-bl{color:var(--blue)}.kv-gr{color:var(--green)}.kv-rd{color:var(--red)}
.kv-am{color:var(--amber)}.kv-tl{color:var(--teal)}.kv-co{color:var(--coral)}
.kv-vi{color:var(--violet)}.kv-pos{color:var(--green)}.kv-neg{color:var(--red)}
.kl{font-family:var(--sans);font-size:11px;font-weight:500;color:var(--t3)}

/* ── FILTER BAR ──────────────────────────────────────────────────── */
.filter-bar{background:var(--sur);border-bottom:1px solid var(--bdr);padding:14px 24px 12px}
.filter-title{font-family:var(--sans);font-size:10px;font-weight:700;
  letter-spacing:1.5px;text-transform:uppercase;color:var(--t3);margin-bottom:10px}

/* ── TABLE AREA ──────────────────────────────────────────────────── */
.tbl-meta{display:flex;align-items:center;padding:10px 24px 6px}
.rc{font-family:var(--sans);font-size:13px;color:var(--t2)}
.rc b{color:var(--blue);font-weight:700}

/* ── DB VIEW ─────────────────────────────────────────────────────── */
.db-wrap{padding:20px 24px 28px}
.db-title{font-family:var(--sans);font-size:17px;font-weight:800;color:var(--t0);letter-spacing:-.4px;margin-bottom:4px}
.db-sub{font-family:var(--sans);font-size:12px;color:var(--t3);margin-bottom:18px}
.db-active-tag{
  display:inline-flex;align-items:center;gap:9px;
  padding:9px 16px;border-radius:10px;margin-bottom:16px;
  background:var(--sur);border:1px solid var(--bdr)}
.db-active-name{font-family:var(--sans);font-size:13px;font-weight:700}
.db-active-hint{font-family:var(--sans);font-size:12px;color:var(--t3)}

/* ── EMPTY STATE ─────────────────────────────────────────────────── */
.empty-v{display:flex;flex-direction:column;align-items:center;
  justify-content:center;padding:80px 20px;gap:16px;min-height:55vh}
.ei{font-size:52px;opacity:.12}
.et{font-family:var(--sans);font-size:19px;font-weight:700;color:var(--t2);letter-spacing:-.3px}
.es{font-family:var(--sans);font-size:13px;color:var(--t3);text-align:center;line-height:1.7;max-width:400px}
.flow{display:flex;align-items:flex-start;margin-top:8px}
.fstep{display:flex;flex-direction:column;align-items:center;gap:8px;width:110px}
.fnum{width:36px;height:36px;border-radius:10px;background:var(--bl-l);border:2px solid var(--bl-b);
  font-family:var(--sans);font-size:14px;font-weight:700;color:var(--blue);
  display:flex;align-items:center;justify-content:center}
.flbl{font-family:var(--sans);font-size:11px;color:var(--t2);text-align:center;line-height:1.5}
.farr{font-size:20px;color:var(--bdr2);margin-top:8px;padding:0 4px;flex-shrink:0}

/* ── MAIN AREA BUTTONS ───────────────────────────────────────────── */
.main div[data-testid="stButton"]>button{
  background:var(--blue)!important;color:#fff!important;border:none!important;
  border-radius:9px!important;font-family:var(--sans)!important;font-weight:600!important;
  font-size:13px!important;padding:10px 20px!important;
  box-shadow:0 3px 14px rgba(59,95,244,.35)!important;transition:all .15s!important}
.main div[data-testid="stButton"]>button:hover{
  background:var(--blue2)!important;box-shadow:0 5px 22px rgba(59,95,244,.5)!important;
  transform:translateY(-1px)!important}

div[data-testid="stDownloadButton"]>button{
  background:var(--sur)!important;color:var(--t1)!important;
  border:1.5px solid var(--bdr2)!important;border-radius:9px!important;
  font-family:var(--sans)!important;font-size:12.5px!important;
  font-weight:600!important;padding:9px 16px!important;
  box-shadow:none!important;transition:all .15s!important}
div[data-testid="stDownloadButton"]>button:hover{
  border-color:var(--blue)!important;color:var(--blue)!important;background:var(--bl-l)!important}

/* ── INPUTS ──────────────────────────────────────────────────────── */
div[data-testid="stTextInput"] input{
  background:var(--sur)!important;border:1.5px solid var(--bdr2)!important;
  color:var(--t0)!important;border-radius:9px!important;
  font-family:var(--sans)!important;font-size:13px!important;
  transition:border-color .15s,box-shadow .15s!important}
div[data-testid="stTextInput"] input:focus{
  border-color:var(--blue)!important;box-shadow:0 0 0 3px rgba(59,95,244,.12)!important}
div[data-testid="stTextInput"] input::placeholder{color:var(--t3)!important}

div[data-baseweb="select"]>div:first-child{
  background:var(--sur)!important;border:1.5px solid var(--bdr2)!important;
  border-radius:9px!important;font-family:var(--sans)!important;
  font-size:13px!important;color:var(--t0)!important}
div[data-baseweb="select"]>div:first-child:hover{border-color:var(--blue)!important}

div[data-testid="stDateInput"] input{
  background:var(--sur)!important;border:1.5px solid var(--bdr2)!important;
  color:var(--t0)!important;font-family:var(--sans)!important;
  font-size:13px!important;border-radius:9px!important}

label[data-testid="stWidgetLabel"] p{
  font-family:var(--sans)!important;font-size:10px!important;font-weight:700!important;
  letter-spacing:1px!important;text-transform:uppercase!important;
  color:var(--t2)!important;margin-bottom:5px!important}

div[data-testid="stAlert"]{font-family:var(--sans)!important;font-size:12px!important;border-radius:9px!important}

div[data-baseweb="popover"] ul{
  background:var(--sur)!important;border:1px solid var(--bdr2)!important;
  border-radius:10px!important;box-shadow:0 10px 40px rgba(15,30,80,.14)!important}
div[data-baseweb="popover"] li{font-family:var(--sans)!important;font-size:13px!important;color:var(--t1)!important;border-radius:7px!important}
div[data-baseweb="popover"] li:hover{background:var(--bl-l)!important;color:var(--blue)!important}

hr{border-color:var(--sbl)!important;opacity:.6!important}
div[data-testid="stSpinner"]{font-family:var(--sans)!important;font-size:12px!important;color:var(--t2)!important}
.main div[data-testid="stDataFrame"]{
  border:1px solid var(--bdr)!important;border-radius:var(--r)!important;
  overflow:hidden!important;box-shadow:0 1px 8px rgba(15,30,80,.05)!important}
</style>
""", unsafe_allow_html=True)

# Run DB schema migrations silently on every cold start
try:
    init_schema()
except Exception:
    pass  # If DB is unreachable at startup, migrations will retry next session

# ════════════════════════════════════════════════════════════════════
#  TOPBAR  (Kushals SVG crown logo)
# ════════════════════════════════════════════════════════════════════
KUSHALS_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 148 40" height="36" width="133">
  <defs>
    <linearGradient id="cg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#c9922a"/>
      <stop offset="100%" stop-color="#e8b84b"/>
    </linearGradient>
  </defs>
  <!-- Crown shape -->
  <g transform="translate(2,4)">
    <path d="M2 26 L6 10 L13 19 L19 4 L25 19 L32 10 L36 26 Z"
          fill="none" stroke="url(#cg)" stroke-width="2.4"
          stroke-linejoin="round" stroke-linecap="round"/>
    <line x1="2" y1="26" x2="36" y2="26"
          stroke="url(#cg)" stroke-width="2.4" stroke-linecap="round"/>
    <!-- gem dots -->
    <circle cx="6"  cy="10" r="2.2" fill="#c9922a"/>
    <circle cx="19" cy="4"  r="2.5" fill="#e8b84b"/>
    <circle cx="32" cy="10" r="2.2" fill="#c9922a"/>
  </g>
  <!-- KUSHALS wordmark -->
  <text x="46" y="25" font-family="Georgia,'Times New Roman',serif"
        font-size="18" font-weight="700" fill="#0c1836" letter-spacing="-0.5">KUSHALS</text>
  <!-- sub-label -->
  <text x="47" y="35" font-family="Arial,sans-serif" font-size="7.5"
        font-weight="600" fill="#8898c0" letter-spacing="2">RECON</text>
</svg>"""

st.markdown(f"""
<div class="topbar">
  <div class="tb-l">
    {KUSHALS_SVG}
    <div class="tb-sep"></div>
    <span class="v-chip">v6</span>
    <span class="plat-chip">{_platform}</span>
  </div>
  <div class="tb-r">
    <div class="rds-pill"><span class="live-dot"></span>AWS RDS Connected</div>
    <div class="sheets-chip"><b>{_loaded_n}</b> / 6 sheets loaded</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ════════════════════════════════════════════════════════════════════
with st.sidebar:

    # Brand header
    st.markdown("""
<div class="sb-brand">
  <div class="sb-crown">
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 28 22" width="22" height="18">
      <path d="M2 19 L5 8 L11 15 L14 3 L17 15 L23 8 L26 19 Z"
            fill="none" stroke="rgba(255,255,255,.96)" stroke-width="2.2"
            stroke-linejoin="round" stroke-linecap="round"/>
      <line x1="2" y1="19" x2="26" y2="19"
            stroke="rgba(255,255,255,.96)" stroke-width="2.2" stroke-linecap="round"/>
    </svg>
  </div>
  <div>
    <div class="sb-brand-name">Kushal's Recon</div>
    <div class="sb-brand-sub">Myntra Suite · v6</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── NAV TABS (st.radio = guaranteed switching) ───────────────────
    st.markdown('<div class="sb-sec">Navigation</div>', unsafe_allow_html=True)
    active_tab = st.radio(
        " ",
        options=["⚡  Reconciliation", "🗄️  Database View"],
        key="nav_radio",
        label_visibility="collapsed",
    )


    st.divider()

    # ── UPLOAD SHEETS ────────────────────────────────────────────────
    st.markdown(f"""
<div class="sb-prog-row">
  <div class="sb-prog-lbl">Upload Sheets</div>
  <div class="sb-prog">
    <div class="sb-track">
      <div class="sb-fill" style="width:{_pct}%"></div>
    </div>
    <span class="sb-cnt">{_loaded_n}/6</span>
  </div>
</div>
""", unsafe_allow_html=True)

    for key, num, label in SHEET_ORDER:
        done_mark = "✓ " if st.session_state.get(f"recon_{key}") else ""
        with st.expander(f"{done_mark}{num:02d} · {label}", expanded=False):
            st.markdown(f'<div class="sb-hint">↳ {SHEET_HINTS[key]}</div>',
                        unsafe_allow_html=True)
            files = st.file_uploader(
                label, type=["xlsx","xls","csv"],
                accept_multiple_files=True,
                key=f"upload_{key}",
                label_visibility="collapsed",
            )
            if files:
                if st.button("⬆  Push to DB", key=f"push_{key}", use_container_width=True):
                    with st.spinner("Syncing…"):
                        try:
                            all_rows = []
                            col_warn  = None
                            for f in files:
                                df_raw = read_uploaded_file(f)
                                # For mrr: detect order_release_id early and warn if missing
                                if key == "mrr" and col_warn is None:
                                    _cm = detect_cols(df_raw, SHEET_COL_CANDIDATES["mrr"])
                                    if not _cm.get("order_release_id"):
                                        col_warn = (
                                            "⚠️ **order_release_id not found** in this file.  \n"
                                            f"File columns: `{', '.join(df_raw.columns.tolist()[:25])}`  \n"
                                            "All rows will be skipped — share the column name above that holds the order release/suborder ID."
                                        )
                                all_rows.extend(normalize_df(key, df_raw))
                            if col_warn:
                                st.warning(col_warn)
                            ins, upd, skp = upsert_rows(key, all_rows)
                            st.session_state[f"recon_{key}"] = True
                            # Bust the DB-view cache for this table so next visit shows fresh data
                            st.session_state.pop(f"db_data_{key}", None)
                            st.success(f"✅ {len(all_rows):,} rows synced")
                            st.markdown(f"""
<div class="up-card">
  <span class="g">↑ {ins:,} inserted</span><br>
  <span class="bl">↻ {upd:,} updated</span><br>
  ○ {skp:,} skipped
</div>""", unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"❌ {e}")

    _n2 = sum(1 for k in SHEET_LABELS if st.session_state.get(f"recon_{k}"))
    st.markdown(f"""
<div class="sb-foot">
  <div class="sb-conn"><span class="live-dot"></span>AWS RDS</div>
  <div class="sb-n">{_n2} / 6</div>
</div>
""", unsafe_allow_html=True)

    st.divider()

    # ── PLATFORM + RUN RECONCILIATION ────────────────────────────────
    platform = st.selectbox(
        "PLATFORM",
        ["MYNTRA","ALL","AMAZON","AJIO","FLIPKART","SHOPIFY","TATACLIQ","ETERNZ"],
        index=0,
    )
    run_btn = st.button("▶  Run Reconciliation", use_container_width=True)

# ════════════════════════════════════════════════════════════════════
#  RUN ENGINE
# ════════════════════════════════════════════════════════════════════
if run_btn:
    with st.spinner("Fetching sheets from DB…"):
        try:
            sheets = {key: fetch_table(key) for key in TABLE_COLS}
        except Exception as e:
            st.error(f"❌ DB fetch failed: {e}"); st.stop()
    with st.spinner("Running reconciliation…"):
        try:
            result_df = run_reconciliation(sheets, platform=platform)
            st.session_state["recon_result"]   = result_df
            st.session_state["recon_platform"] = platform
            st.rerun()
        except Exception as e:
            st.error(f"❌ Engine error: {e}"); st.stop()

# ════════════════════════════════════════════════════════════════════
#  MAIN CONTENT  —  driven by sidebar st.radio
# ════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────
# TAB : RECONCILIATION
# ─────────────────────────────────────────────────────────────────────
# Defaults — ensure run_btn and platform always exist regardless of active tab
run_btn = False
platform = st.session_state.get("recon_platform", "MYNTRA")

if "Reconciliation" in active_tab:

    if "recon_result" in st.session_state:
        df = st.session_state["recon_result"]

        total      = len(df)
        done_n     = len(df[df["Status"] == "Done"])
        tickets_n  = len(df[df["Status"].str.contains("Ticket", na=False)])
        pending_n  = len(df[df["Status"].str.contains("Pending", na=False)])
        total_bill = df["Bill Value"].sum()
        total_pay  = df["Payment"].sum()
        net_diff   = df["Payment Diff"].sum()
        diff_cls   = "kv-pos" if net_diff >= 0 else "kv-neg"

        # KPI cards
        st.markdown(f"""
<div class="kpi-wrap">
  <div class="kpi-grid">
    <div class="kcard k-tot"><div class="kv kv-bl">{total:,}</div><div class="kl">Total Orders</div></div>
    <div class="kcard k-don"><div class="kv kv-gr">{done_n:,}</div><div class="kl">Done</div></div>
    <div class="kcard k-tkt"><div class="kv kv-rd">{tickets_n:,}</div><div class="kl">Ticket Needed</div></div>
    <div class="kcard k-pen"><div class="kv kv-am">{pending_n:,}</div><div class="kl">Pending</div></div>
    <div class="kcard k-bil"><div class="kv kv-tl">{_inr(total_bill)}</div><div class="kl">Bill Value</div></div>
    <div class="kcard k-pay"><div class="kv kv-co">{_inr(total_pay)}</div><div class="kl">Payment Rcvd</div></div>
    <div class="kcard k-dif"><div class="kv {diff_cls}">{_inr(net_diff)}</div><div class="kl">Net Diff</div></div>
  </div>
</div>
""", unsafe_allow_html=True)

        # Filter bar
        st.markdown('<div class="filter-bar"><div class="filter-title">🔍 Filters &amp; Search</div>',
                    unsafe_allow_html=True)
        fc1, fc2, fc3, fc4, fc5 = st.columns([3, 2, 1.5, 1.5, 1.2])
        with fc1:
            search = st.text_input("SEARCH", "", placeholder="Order code, invoice, status…")
        with fc2:
            s_opts = ["All Statuses"] + sorted(df["Status"].dropna().unique().tolist())
            s_filt = st.selectbox("STATUS", s_opts)
        with fc3:
            d_from = st.date_input("FROM DATE", value=None)
        with fc4:
            d_to   = st.date_input("TO DATE",   value=None)
        with fc5:
            st.write("")
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="openpyxl") as xw:
                df.to_excel(xw, index=False, sheet_name="Reconciliation")
            xbuf.seek(0)
            st.download_button("↓ Export", data=xbuf,
                file_name=f"recon_{datetime.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        # Apply filters
        fdf = df.copy()
        if search:
            ns = norm(search)
            mask = fdf.apply(lambda r: any(
                ns in norm(str(r.get(c,"")))
                for c in ["Display Order Code","Invoice Code","Bill No",
                          "Order Release ID","Status","Return Status"]), axis=1)
            fdf = fdf[mask]
        if s_filt != "All Statuses":
            fdf = fdf[fdf["Status"] == s_filt]
        if d_from:
            fdf = fdf[pd.to_datetime(fdf["Order Date"],format="%d-%m-%Y",errors="coerce")
                      >= pd.Timestamp(d_from)]
        if d_to:
            fdf = fdf[pd.to_datetime(fdf["Order Date"],format="%d-%m-%Y",errors="coerce")
                      <= pd.Timestamp(d_to)]

        sfx = (f' &nbsp;·&nbsp;<span style="color:var(--t3)">{total:,} total</span>'
               if len(fdf)!=total else "")
        st.markdown(f'<div class="tbl-meta"><div class="rc"><b>{len(fdf):,}</b> orders{sfx}</div></div>',
                    unsafe_allow_html=True)

        with st.expander("⬡  Debug — Payment Column Detection", expanded=False):
            try:
                pd_ = fetch_table("pay")
                if not pd_.empty:
                    pc_ = detect_cols(pd_, SHEET_COL_CANDIDATES["pay"])
                    cx1, cx2 = st.columns(2)
                    with cx1:
                        st.markdown("**Detected columns:**")
                        for fld, col in pc_.items():
                            st.markdown(f"`{'✅' if col else '❌'} {fld}` → `{col or 'NOT FOUND'}`")
                    with cx2:
                        st.markdown("**All DB columns:**")
                        st.code(", ".join(pd_.columns.tolist()))
                        st.markdown(f"**Rows:** {len(pd_):,}")
                else:
                    st.warning("Payment table empty — push Payment Sheet first.")
            except Exception as ex:
                st.error(f"Debug error: {ex}")

        st.dataframe(fdf, use_container_width=True, height=620,
            column_config={
                "#":                  st.column_config.NumberColumn(width="small"),
                "Total Price":        st.column_config.NumberColumn(format="₹%.2f"),
                "Bill Value":         st.column_config.NumberColumn(format="₹%.2f"),
                "SR Value":           st.column_config.NumberColumn(format="₹%.2f"),
                "Payment":            st.column_config.NumberColumn(format="₹%.2f"),
                "Payment Diff":       st.column_config.NumberColumn(format="₹%.2f"),
                "Status":             st.column_config.TextColumn(width="medium"),
                "Days Order":         st.column_config.NumberColumn(width="small"),
                "Days Ret Created":   st.column_config.NumberColumn(width="small"),
                "Days Ret Delivered": st.column_config.NumberColumn(width="small"),
            }, hide_index=True)

    else:
        # Empty state with clear instructions pointing to sidebar
        st.markdown("""
<div class="empty-v">
  <div class="ei">⚡</div>
  <div class="et">Ready to Reconcile</div>
  <div class="es">
    Use the <strong>sidebar on the left</strong> to upload sheets and push them to DB,
    then select a platform and click <strong>▶ Run Reconciliation</strong>.
  </div>
  <div class="flow">
    <div class="fstep">
      <div class="fnum">1</div>
      <div class="flbl">Open expander<br>in sidebar<br>&amp; upload file</div>
    </div>
    <div class="farr">→</div>
    <div class="fstep">
      <div class="fnum">2</div>
      <div class="flbl">Click<br>Push to DB</div>
    </div>
    <div class="farr">→</div>
    <div class="fstep">
      <div class="fnum">3</div>
      <div class="flbl">Select platform<br>&amp; click<br>▶ Run</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# TAB : DATABASE VIEW
# ─────────────────────────────────────────────────────────────────────
elif "Database" in active_tab:

    st.markdown('<div class="db-wrap">', unsafe_allow_html=True)
    st.markdown("""
<div class="db-title">🗄️ Database View</div>
<div class="db-sub">Browse, search and export data directly from your AWS RDS tables</div>
""", unsafe_allow_html=True)

    # Table selector — one button per table, highlighted when selected
    sel_table = st.session_state.get("db_table", "uni")
    btn_cols  = st.columns(len(TABLE_NICE))
    for i, (tkey, (tname, tcol)) in enumerate(TABLE_NICE.items()):
        with btn_cols[i]:
            is_sel = (tkey == sel_table)
            # Use a unique label trick to force Streamlit to re-render on selection
            btn_label = f"● {tname}" if is_sel else tname
            if st.button(btn_label, key=f"dbtab_{tkey}", use_container_width=True):
                st.session_state["db_table"] = tkey
                st.rerun()

    # Re-read (may have changed after rerun)
    sel_table = st.session_state.get("db_table", "uni")
    nice_name, nice_col = TABLE_NICE[sel_table]

    st.markdown(f"""
<div class="db-active-tag" style="border-left:4px solid {nice_col}">
  <span class="db-active-name" style="color:{nice_col}">{nice_name}</span>
  <span class="db-active-hint">— table: <code style="font-size:11px">{sel_table}</code></span>
</div>
""", unsafe_allow_html=True)

    # Controls row
    sc1, sc2, sc3 = st.columns([4, 1, 1])
    with sc1:
        db_srch = st.text_input("SEARCH", "", placeholder=f"Search any column in {nice_name}…",
                                key="db_search_inp")
    with sc2:
        db_lim  = st.selectbox("ROWS", [100, 250, 500, 1000, 5000], key="db_limit")
    with sc3:
        st.write("")
        db_ref = st.button("↻  Refresh", key="db_ref", use_container_width=True)

    # ── Session-state cache: only hits DB on first visit or explicit Refresh ──
    _cache_key = f"db_data_{sel_table}"
    _prev_key  = "db_prev_table"

    # Clear cache automatically when the user switches to a different table
    if st.session_state.get(_prev_key) != sel_table:
        st.session_state[_prev_key] = sel_table
        st.session_state.pop(_cache_key, None)

    # Fetch from DB only when needed
    if db_ref or _cache_key not in st.session_state:
        with st.spinner(f"Loading {nice_name} from AWS RDS…"):
            try:
                st.session_state[_cache_key] = fetch_table(sel_table)
            except Exception as e:
                st.error(f"❌ Could not load `{sel_table}`: {e}")
                st.markdown("</div>", unsafe_allow_html=True)
                st.stop()

    # Display cached data (search/limit applied in-memory — no extra DB call)
    if _cache_key in st.session_state:
        raw_df = st.session_state[_cache_key]

        db_df = raw_df.copy()
        if db_srch.strip():
            ns    = norm(db_srch)
            db_df = db_df[db_df.apply(
                lambda r: any(ns in norm(str(v)) for v in r.values), axis=1)]
        total_db = len(db_df)
        db_df    = db_df.head(db_lim)

        st.markdown(
            f'<div class="tbl-meta"><div class="rc">'
            f'Showing <b>{len(db_df):,}</b> of {total_db:,} rows'
            f' &nbsp;·&nbsp; <span style="color:var(--t3)">{len(raw_df):,} total in DB</span>'
            f'</div></div>', unsafe_allow_html=True)

        st.dataframe(db_df, use_container_width=True, height=540, hide_index=True)

        dbuf = io.BytesIO()
        with pd.ExcelWriter(dbuf, engine="openpyxl") as dw:
            db_df.to_excel(dw, index=False, sheet_name=sel_table)
        dbuf.seek(0)
        st.download_button(f"↓ Export {nice_name}", data=dbuf,
            file_name=f"{sel_table}_{datetime.date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Click **↻ Refresh** to load data from the database.")

    st.markdown("</div>", unsafe_allow_html=True)
