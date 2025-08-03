from flask import Flask, render_template, request, jsonify
import pandas as pd
import sqlite3

app = Flask(__name__)

def initialize_db():
    df_raw = pd.read_excel("b36.xls", header=None, dtype=str)

    # Find header row
    header_row = None
    for i in range(len(df_raw)):
        row = df_raw.iloc[i].astype(str).str.upper()
        if row.str.contains("MÃ HÀNG").any() and row.str.contains("VỊ TRÍ").any():
            header_row = i
            break
    if header_row is None:
        raise Exception("❌ Cannot find header row with 'MÃ HÀNG' and 'VỊ TRÍ'")

    df_raw.columns = df_raw.iloc[header_row]
    df = df_raw.iloc[header_row + 1:].reset_index(drop=True)
    df.columns = [str(col).strip().upper() for col in df.columns]

    standard_columns = [
        "NO", "LOC", "MAHANG", "TENHANG", "SOLUONG",
        "SOTHUNG", "PALLET", "PO", "NCC", "DELIVERYDATE", "RECEIPTDATE"
    ]
    rename_map = {}
    for i in range(min(len(df.columns), len(standard_columns))):
        rename_map[df.columns[i]] = standard_columns[i]
    df.rename(columns=rename_map, inplace=True)

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[~df["LOC"].astype(str).str.upper().str.contains("PICKTO|PACK", na=False)]
    df = df[df["MAHANG"].notna() & (df["MAHANG"].astype(str).str.strip() != "")]

    # Merge PACK + QPACK
    try:
        df_pack = pd.read_excel("PACK PPL MPE IMPORT.xlsx", dtype=str)
        df_pack.columns = [c.strip().upper() for c in df_pack.columns]
        df_pack = df_pack[["PACKKEY", "CASECNT", "PALLET"]].dropna(subset=["PACKKEY"])
        df_pack.rename(columns={
            "PACKKEY": "MAHANG",
            "CASECNT": "PACK",
            "PALLET": "QPACK"
        }, inplace=True)
        df = df.merge(df_pack, on="MAHANG", how="left")
    except Exception as e:
        print("⚠️ Merge error:", e)

    conn = sqlite3.connect("data.db")
    df.to_sql("data", conn, if_exists="replace", index=False)
    conn.close()
    print("✅ Saved to data.db")

@app.route("/")
def index():
    return render_template("data.html")

@app.route("/search")
def search():
    sku = request.args.get("sku", "")
    po = request.args.get("po", "")
    pallet = request.args.get("pallet", "")

    try:
        block_df = pd.read_excel("block.xlsx")
        blocked_items = block_df.iloc[:, 0].dropna().astype(str).tolist()
    except:
        blocked_items = []

    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()

    block_condition = f"AND MAHANG NOT IN ({','.join(['?'] * len(blocked_items))})" if blocked_items else ""

    sql = f"""
        SELECT * FROM data
        WHERE 1=1
            AND MAHANG LIKE ?
            AND PO LIKE ?
            AND PALLET LIKE ?
            {block_condition}
        ORDER BY DELIVERYDATE ASC, LOC ASC, MAHANG ASC
        LIMIT 1000
    """

    params = [f"%{sku}%", f"%{po}%", f"%{pallet}%"] + blocked_items
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    conn.close()

    formatted_rows = []
    for row in rows:
        row = list(row)
        try:
            row[9] = pd.to_datetime(row[9], errors="coerce").strftime("%d/%m/%Y")
        except:
            pass

        try:
            sol = int(str(row[4]).replace(",", "").strip())
            qpk = int(str(row[-1]).replace(",", "").strip())
            row_class = "highlight-pallet" if sol == qpk else ""
        except:
            row_class = ""

        formatted_rows.append({"row": row, "class": row_class})

    return jsonify({
        "headers": headers,
        "rows": formatted_rows
    })

if __name__ == "__main__":
    initialize_db()
    app.run(debug=True)
