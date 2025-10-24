import streamlit as st
import folium
from folium import plugins
from streamlit_folium import st_folium
import pandas as pd
from folium import CustomIcon
import sqlite3
from datetime import date, timedelta


# --- Konfigurasi halaman (hanya boleh dipanggil sekali di paling atas) ---
st.set_page_config(page_title="🌦️ Peta Cuaca Ekstrem", layout="wide")

from utils.ui import setup_header
setup_header()
# ==========================================================
# 🧭 1️⃣ KONFIGURASI SIDEBAR DENGAN LOGO
# ==========================================================

# st.sidebar.image("Logo_BMKG.png", caption="Dashboard Monitoring Cuaca Ekstrem")
st.sidebar.markdown("""
    <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)
#st.title("Peta Cuaca Ekstrem")

# ==========================================================
# 💾 2️⃣ PENGAMBILAN DATA
# ==========================================================

# --- Path ke database ---
db_path_akhir = "data_akhir.db"
db_path_lengkap = "data_lengkap2.db"

# --- Widget utama untuk memilih tanggal ---
pilih_tanggal = st.date_input(
    "📅 Pilih tanggal:",
    #value=date(2025, 1, 1),
    value=date.today(),
    min_value=date(2025, 1, 1),
    max_value=date(2025, 12, 31)
)

# --- 2a. Ambil data STASIUN & HEAVY RAIN dari `data_akhir.db` ---
conn_akhir = sqlite3.connect(db_path_akhir)
query_main = """
SELECT station_wmo_id, NAME, LAT, LON, Temperatur, Curah_Hujan, 
       Kecepatan_angin, Heavy_Rain, jam, sandi_gts, tanggal
FROM data_akhir
WHERE tanggal = ?
ORDER BY station_wmo_id, jam
"""

params_main = (pilih_tanggal.strftime("%Y-%m-%d"),)
df_main = pd.read_sql_query(query_main, conn_akhir, params=params_main)
# params_main = (pilih_tanggal,)
# df_main = pd.read_sql_query(query_main, conn_akhir, params=params_main)
conn_akhir.close()


# Tentukan tanggal sebelumnya dari tanggal yang dipilih
tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)

conn_lengkap = sqlite3.connect(db_path_lengkap)

# Query ini mengambil data dari jam 01-23 UTC hari sebelumnya DAN jam 00 UTC hari yang dipilih
query_gale = """
SELECT station_wmo_id, NAME, LAT, LON, jam, sandi_gts, Kecepatan_angin
FROM data_lengkap
WHERE
    -- Kondisi 1: Ambil data jam 01-23 UTC pada HARI SEBELUMNYA
    (tanggal = ? AND jam <> '00:00' AND Kecepatan_angin >= 30)
OR
    -- Kondisi 2: Ambil data jam 00 UTC pada HARI YANG DIPILIH
    (tanggal = ? AND jam = '00:00' AND Kecepatan_angin >= 30)
ORDER BY tanggal, jam, station_wmo_id
"""

# Siapkan parameter untuk query. Urutannya harus sesuai dengan tanda tanya (?)
params_gale = (
    tanggal_sebelumnya.strftime("%Y-%m-%d"), # untuk ? pertama (tanggal sebelumnya)
    pilih_tanggal.strftime("%Y-%m-%d")       # untuk ? kedua (tanggal yang dipilih)
)

df_gale = pd.read_sql_query(query_gale, conn_lengkap, params=params_gale)
conn_lengkap.close()

# Pastikan tipe data numeric untuk filtering
if not df_gale.empty:
    df_gale['Kecepatan_angin'] = pd.to_numeric(df_gale['Kecepatan_angin'], errors='coerce').fillna(0)


# ==========================================================
# 🗺️ 3️⃣ PEMBUATAN PETA
# ==========================================================

# st.markdown("<h1 style='margin-top: -30px;'>Peta Cuaca Ekstrem</h1>", unsafe_allow_html=True)

m = folium.Map(location=[-2, 118], zoom_start=5, tiles="CartoDB Voyager")

# Buat LayerGroup untuk event cuaca ekstrem
gale_layer = folium.FeatureGroup(name="Gale", show=True)
hr_layer   = folium.FeatureGroup(name="Heavy Rain", show=True)

# --- Loop 1: Plot semua stasiun dasar dan ikon Heavy Rain dari `df_main` ---
if not df_main.empty:
    # Buat satu set untuk melacak stasiun yang sudah diplot agar tidak duplikat
    plotted_stations = set()
    for _, row in df_main.iterrows():
        lat, lon, station_id = row["LAT"], row["LON"], row["station_wmo_id"]
        wind_text = "Calm" if pd.isna(row['Kecepatan_angin']) else f"{row['Kecepatan_angin']} knot"
        rain_text = "-" if pd.isna(row['Curah_Hujan']) else f"{row['Curah_Hujan']} mm/hari"
        # Hanya plot titik biru stasiun SEKALI saja
        if pd.notna(lat) and pd.notna(lon) and station_id not in plotted_stations:

            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color="blue",
                fill=True,
                fill_color="blue",
                fill_opacity=0.7,
                popup=(
                    f"<div style='font-size:10px;width:200px;'>"
                    f"<b>{row['NAME']}</b><br>"
                    f"ID Stasiun: {row['station_wmo_id']} <br>"
                    f"Temperatur: {row['Temperatur']} °C<br>"
                    f"Curah Hujan: {rain_text} <br>"
                    f"Kecepatan Angin: {wind_text}"
                    f"</div>"
                ),
                tooltip=row["NAME"]
            ).add_to(m)
            plotted_stations.add(station_id)

        # Cek dan plot ikon Heavy Rain untuk setiap kejadian
        try:
            hr_value = float(row["Heavy_Rain"])
            if hr_value > 0:
                rain_icon = CustomIcon("cloud_rain.png", icon_size=(30, 30))
                folium.Marker(
                    location=[lat, lon], icon=rain_icon,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>{station_id}<br>🌧️ Heavy Rain: {hr_value} mm/hari"
                        f"</div>"
                    )
                ).add_to(hr_layer)
        except (ValueError, TypeError):
            pass

# --- Loop 2: Plot ikon Gale dari `df_gale` ---
if not df_gale.empty:
    for _, row in df_gale.iterrows():
        lat, lon = row["LAT"], row["LON"]
        if pd.notna(lat) and pd.notna(lon):
            wind_icon = CustomIcon("wind.png", icon_size=(30, 30))
            folium.Marker(
                location=[lat, lon], icon=wind_icon,
                popup=(
                    f"<div style='font-size:10px;width:200px;'>"
                    f"<b>{row['NAME']}</b><br>{row['station_wmo_id']}<br> Gale: {row['Kecepatan_angin']} knot <br> Pukul : {row['jam']} UTC"
                    f"</div>"
                )
            ).add_to(gale_layer)

# Tambahkan layer ke peta
gale_layer.add_to(m)
hr_layer.add_to(m)
folium.LayerControl().add_to(m)

# --- TAMBAHKAN BLOK KODE INI ---
plugins.Fullscreen(
    position="topright",  # Bisa 'topleft', 'topright', 'bottomleft', 'bottomright'
    title="Lihat Fullscreen", # Teks saat mouse hover
    title_cancel="Keluar Fullscreen", # Teks saat mouse hover (dalam mode fullscreen)
    force_separate_button=True,
).add_to(m)

st_folium(m, width=None, height=500)


# ==========================================================
# 📋 4️⃣ HIGHLIGHT & TABEL DATA
# ==========================================================
tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)
st.markdown(f"""
<hr style="border: 1px solid #ccc; margin: 20px 0;">
<div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
<b>📌 Highlight:</b><br>
1. Data yang ditampilkan merupakan data dari 00.01–23.59 UTC (<b>{tanggal_sebelumnya.strftime("%d %B %Y")}</b>)<br>
2. Threshold untuk Heavy Rain adalah <b>50 mm/hari</b><br>
3. Threshold untuk Gale adalah <b>30 knot</b>
</div>
""", unsafe_allow_html=True)


# --- Tabel 1: Heavy Rain (dari df_main) ---
st.markdown("### Daftar Stasiun dengan Kejadian Heavy Rain")
df_hr = df_main.copy()
df_hr["Heavy_Rain"] = pd.to_numeric(df_hr["Heavy_Rain"], errors="coerce")
df_hr_filtered = df_hr[df_hr["Heavy_Rain"] > 0]
if not df_hr_filtered.empty:
    df_tabel_hr = df_hr_filtered[[
        "station_wmo_id", "NAME","sandi_gts" ,"Heavy_Rain"
    ]].sort_values(by=["Heavy_Rain"], ascending=False).reset_index(drop=True)
    df_tabel_hr.index = df_tabel_hr.index + 1
    #st.dataframe(df_tabel_hr, use_container_width=True)
    st.dataframe(
        df_tabel_hr,  # DataFrame Anda yang sudah di-style
        use_container_width=True,
        column_config={
            "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
            "NAME": st.column_config.Column("Nama Stasiun"),
            "sandi_gts": st.column_config.Column("Sandi GTS"),
            "Heavy_Rain": st.column_config.Column("Curah Hujan (mm/hari)")
            # Format angka (misal "35.2") sudah diatur oleh .format() Anda,
            # jadi kita hanya perlu mengganti labelnya saja.
        }
    )
    st.info(f"Total kejadian Heavy Rain (hujan ≥ 50 mm/hari) terdeteksi: {len(df_hr_filtered)} stasiun.")
else:
    st.info("Tidak ada stasiun dengan kejadian Heavy Rain pada tanggal yang dipilih.")


# --- Tabel 2: Gale (dari df_gale) ---
st.markdown("### Daftar Stasiun dengan Kejadian Gale")
if not df_gale.empty:
    df_tabel_gale = df_gale[['station_wmo_id', 'NAME', 'jam', 'sandi_gts', 'Kecepatan_angin']].rename(columns={
        'station_wmo_id': 'WMO ID Stasiun', 'NAME': 'Nama Stasiun', 'jam': 'Jam Observasi',
        'sandi_gts': 'Sandi GTS', 'Kecepatan_angin': 'Kecepatan Angin (knot)'
    }).sort_values(by="Kecepatan Angin (knot)", ascending=False).reset_index(drop=True)
    df_tabel_gale.index = df_tabel_gale.index + 1
    st.dataframe(df_tabel_gale, use_container_width=True)
    st.info(f"Total kejadian Gale (angin ≥ 30 knot) terdeteksi: {len(df_gale)} stasiun.")
else:
    st.info("Tidak ada stasiun dengan kejadian Gale pada tanggal yang dipilih.")