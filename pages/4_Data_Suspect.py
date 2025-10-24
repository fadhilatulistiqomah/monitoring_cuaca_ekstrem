import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, timedelta

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Data Suspect", layout="wide")

# ==========================================================
# 🧭 KONTEN SIDEBAR
# ==========================================================
#st.sidebar.image("Logo_BMKG.png", caption="Dashboard Monitoring Cuaca Ekstrem")
st.sidebar.markdown("""
    <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)

# ==========================================================
# 📅 WIDGET TANGGAL (Digunakan oleh semua tab)
# ==========================================================
st.title("Pengecekan Data Suspect")

pilih_tanggal = st.date_input(
    "📅 Pilih tanggal acuan untuk pengecekan data:",
    #value=date(2025, 1, 2),
    value=date.today(),
    min_value=date(2025, 1, 1),
    max_value=date(2025, 12, 31),
    help="Untuk Heavy Rain, tanggal ini akan menampilkan data dari jam 01:00 hari sebelumnya hingga 00:00 tanggal yang dipilih."
)

# ==========================================================
# 📑 PEMBUATAN TABS
# ==========================================================
tab1, tab2 = st.tabs(["Heavy Rain Suspect", "Gale Suspect"])


# --- KODE UNTUK TAB 1: HEAVY RAIN SUSPECT ---
with tab1:
    #st.header("Perbandingan Curah Hujan 24 Jam vs Akumulasi Per Jam")

    # --- Koneksi ke database SQLite ---
    db_path_salah = "data_salah2.db"
    table_name_salah = "data_salah"
    conn_salah = sqlite3.connect(db_path_salah)

    # # --- Logika Tanggal untuk Heavy Rain ---
    # # Jika Anda pilih 2 Jan, variabel ini akan berisi 1 Jan.
    # tanggal_untuk_query = pilih_tanggal - timedelta(days=1)

    # # --- Query untuk Heavy Rain ---
    # query_hr = f"""
    # SELECT station_wmo_id, NAME, jam, sandi_gts,
    #        Curah_Hujan, Curah_Hujan_Jam, tanggal
    # FROM {table_name_salah}
    # WHERE (tanggal = ? AND jam != '00:00')
    #    OR (tanggal = ? AND jam = '00:00')
    # ORDER BY station_wmo_id, jam
    # """
    # # Note: query diubah sedikit agar lebih jelas. `date(?, '+1 day')` diganti dengan `pilih_tanggal`
    # df_hr = pd.read_sql_query(query_hr, conn_salah, params=(tanggal_untuk_query.strftime("%Y-%m-%d"), pilih_tanggal.strftime("%Y-%m-%d")))
    # --- Logika Tanggal untuk Heavy Rain ---
    # Jika Anda pilih 2 Okt, variabel ini akan berisi 1 Okt.
    tanggal_untuk_query = pilih_tanggal - timedelta(days=1)

    # --- Query untuk Heavy Rain ---
    # Karena script processing Anda sudah MENYATUKAN semua data (termasuk 00Z) 
    # di bawah satu tanggal observasi, kita HANYA perlu query ke satu tanggal itu.
    query_hr = f"""
    SELECT station_wmo_id, NAME, jam, sandi_gts,
        Curah_Hujan, Curah_Hujan_Jam, tanggal
    FROM {table_name_salah}
    WHERE tanggal = ?
    ORDER BY station_wmo_id, jam
    """

    # Kita sekarang hanya butuh SATU parameter
    params_hr = (tanggal_untuk_query.strftime("%Y-%m-%d"),)
    df_hr = pd.read_sql_query(query_hr, conn_salah, params=params_hr)
    conn_salah.close()

    # --- Pemrosesan Data Heavy Rain ---
    mask = ~(
        (df_hr["jam"] == "00:00") &
        (df_hr.groupby("station_wmo_id")["jam"].transform("count") == 1)
    )
    df_hr = df_hr[mask].copy()

    df_hr["sort_order"] = df_hr["jam"].apply(lambda x: 1 if x == "00:00" else 0)
    df_hr = df_hr.sort_values(by=["station_wmo_id", "sort_order", "jam"]).drop(columns="sort_order")

    if df_hr.empty:
        st.info(f"Tidak ada data curah hujan suspect untuk periode yang berakhir pada {pilih_tanggal.strftime('%d %B %Y')} pukul 00:00 UTC.")
    else:
        # --- Tampilkan data per stasiun ---
        for station_id, group in df_hr.groupby(["station_wmo_id", "NAME"]):
            st.subheader(f"{station_id[0]} - {station_id[1]}")

            df_table = group[["jam", "sandi_gts", "Curah_Hujan_Jam"]].reset_index(drop=True)
            df_table = df_table.fillna("-")
            df_table.columns = ["Jam", "Sandi Synop", "Curah Hujan"]
            st.write(df_table.to_html(index=False, escape=False), unsafe_allow_html=True)

            curah_hujan_00 = group.loc[group["jam"] == "00:00", "Curah_Hujan"].sum()
            curah_hujan_jam = group["Curah_Hujan_Jam"].sum()
            selisih = curah_hujan_00 - curah_hujan_jam

            df_summary = pd.DataFrame({
                "Curah Hujan 24 Jam (RRR)": [f"{curah_hujan_00} mm" if curah_hujan_00 != 0 else "-"],
                "Total Akumulasi Per Jam (6RRRtR)": [f"{curah_hujan_jam} mm" if curah_hujan_jam != 0 else "-"],
                "Selisih": [f"{selisih:.1f} mm" if selisih != 0 else "-"]
            })

            st.write(df_summary.to_html(index=False, escape=False, classes="summary-table"), unsafe_allow_html=True)
            st.markdown("---")


# --- KODE UNTUK TAB 2: GALE SUSPECT ---
with tab2:
    #st.header("Laporan Angin Kencang (Gale) dengan Data 'nddff' Kosong")

    # --- Koneksi ke database ---
    db_path_gale = "data_lengkap2.db"
    table_name_gale = "data_lengkap"
    conn_gale = sqlite3.connect(db_path_gale)

    # --- Query untuk mencari data Gale dengan nddff kosong pada tanggal yang dipilih ---
    # Logika tanggalnya lebih sederhana, hanya mencari di tanggal yang dipilih
    query_gale = f"""
    SELECT station_wmo_id, NAME, jam, sandi_gts
    FROM {table_name_gale}
    WHERE tanggal = ? AND (nddff IS NULL OR nddff = '')
    ORDER BY station_wmo_id, jam
    """
    params_gale = (pilih_tanggal.strftime("%Y-%m-%d"),)
    df_gale = pd.read_sql_query(query_gale, conn_gale, params=params_gale)
    conn_gale.close()

    # --- Tampilkan hasil ---
    if df_gale.empty:
        st.info(f"✅ Tidak ada data Gale suspect (nddff kosong) pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
    else:
        st.warning(f" Ditemukan {len(df_gale)} laporan Gale suspect (nddff kosong) pada tanggal {pilih_tanggal.strftime('%d %B %Y')}.")
        
        for station_id, group in df_gale.groupby(["station_wmo_id", "NAME"]):
            st.subheader(f"{station_id[0]} - {station_id[1]}")
            
            # Buat tabel detail hanya dengan kolom yang diminta
            df_table = group[["jam", "sandi_gts"]].reset_index(drop=True)
            df_table.columns = ["Jam", "Sandi Synop"]
            
            # Tampilkan tabel
            st.write(df_table.to_html(index=False, escape=False), unsafe_allow_html=True)
            st.markdown("---")


# --- CSS styling (diletakkan di akhir agar berlaku untuk semua tabel) ---
st.markdown("""
    <style>
    table {
        table-layout: fixed;
        width: 100%;
    }
    th:nth-child(1), td:nth-child(1) {
        width: 80px !important;
        text-align: center;
    }
    th:nth-child(2), td:nth-child(2) {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
        text-align: left;
    }
    th:nth-child(3), td:nth-child(3) {
        width: 100px !important;
        text-align: center;
    }
    /* CSS khusus tabel ringkasan */
    .summary-table td, .summary-table th {
        text-align: center !important;
        width: 33% !important;
    }
    </style>
""", unsafe_allow_html=True)