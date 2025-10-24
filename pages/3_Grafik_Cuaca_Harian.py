# import streamlit as st
# import sqlite3
# import pandas as pd
# import plotly.express as px
# import matplotlib.pyplot as plt
# import numpy as np
# from windrose import WindroseAxes
# from datetime import date

# # --- Konfigurasi halaman ---
# st.set_page_config(page_title="Grafik Cuaca Harian", layout="wide")
# st.title("Grafik Cuaca Harian")

# # --- Koneksi ke database SQLite ---
# db_path = "data_lengkap2.db"   # ganti sesuai path
# table_name = "data_lengkap"   # ganti sesuai tabel
# conn = sqlite3.connect(db_path)

# # --- Ambil daftar tanggal & stasiun unik ---
# tanggal_list = pd.read_sql_query(
#     f"SELECT DISTINCT tanggal FROM {table_name} ORDER BY tanggal", conn
# )["tanggal"].tolist()

# station_list = pd.read_sql_query(
#     f"SELECT DISTINCT station_wmo_id FROM {table_name} ORDER BY station_wmo_id", conn
# )["station_wmo_id"].tolist()

# # --- Widget pilih tanggal (kalender) ---
# pilih_tanggal = st.date_input(
#     "📅 Pilih tanggal:",
#     value=date(2025, 1, 1),          # default
#     min_value=date(2025, 1, 1),
#     max_value=date(2025, 12, 31)
# )

# pilih_station = st.selectbox("🏷️ Pilih Station WMO ID:", station_list)

# # --- Query data untuk stasiun & tanggal terpilih ---
# query = f"""
# SELECT station_wmo_id, NAME, jam, tanggal, sandi_gts,
#        Tekanan_Permukaan, Temperatur, Dew_Point, Kecepatan_angin, Arah_angin, Curah_Hujan_Jam
# FROM {table_name}
# WHERE tanggal = ?
#   AND station_wmo_id = ?
# ORDER BY jam
# """
# df = pd.read_sql_query(query, conn, params=(pilih_tanggal.strftime("%Y-%m-%d"), pilih_station))
# conn.close()

# # --- Pastikan jam urut ---
# df["jam"] = pd.to_datetime(df["jam"], format="%H:%M").dt.strftime("%H:%M")

# # --- Tampilkan data ---
# st.subheader(f"📍 Data Stasiun {pilih_station} pada {pilih_tanggal.strftime('%d-%m-%Y')}")
# st.dataframe(df)

# # --- Grafik Line Tekanan Permukaan ---
# st.subheader("🌡️ Tekanan Permukaan")
# fig1 = px.line(df, x="jam", y="Tekanan_Permukaan", markers=True,
#                title="Tekanan Permukaan (hPa)")
# st.plotly_chart(fig1, use_container_width=True)

# # --- Grafik Line Temperatur ---
# st.subheader("🌡️ Temperatur")
# fig2 = px.line(df, x="jam", y="Temperatur", markers=True,
#                title="Temperatur (°C)")
# st.plotly_chart(fig2, use_container_width=True)
# # --- Grafik Line Temperatur ---
# st.subheader("🌡️ Temperatur Titik Embun")
# fig3 = px.line(df, x="jam", y="Dew_Point", markers=True,
#                title="Temperatur Titik Embun (°C)")
# st.plotly_chart(fig3, use_container_width=True)

# # --- Grafik Line Kecepatan Angin ---
# st.subheader("💨 Kecepatan Angin")
# fig4 = px.line(df, x="jam", y="Kecepatan_angin", markers=True,
#                title="Kecepatan Angin (m/s)")
# st.plotly_chart(fig4, use_container_width=True)

# # --- Grafik Windrose ---
# st.subheader("🌪️ Windrose (Distribusi Arah & Kecepatan Angin)")

# if df["Arah_angin"].notna().sum() > 0 and df["Kecepatan_angin"].notna().sum() > 0:
#     fig = plt.figure(figsize=(6,6))
#     ax = WindroseAxes.from_ax(fig=fig)

#     ax.bar(
#         df["Arah_angin"].dropna(),
#         df["Kecepatan_angin"].dropna(),
#         normed=True,
#         opening=0.8,
#         edgecolor="white",
#         bins=np.arange(0, 12, 2)   # kategori kecepatan angin
#     )

#     ax.set_title(f"Windrose Stasiun {pilih_station}", fontsize=16, pad=20, fontweight='bold')
#     ax.set_xticklabels(['E', 'NE', 'N', 'NW', 'W', 'SW', 'S', 'SE'], fontsize=12)

#     ax.legend(
#         title="Kecepatan Angin (m/s)",
#         loc='upper center',
#         bbox_to_anchor=(1.2, 1.0),
#         shadow=True,
#         ncol=1
#     )

#     st.pyplot(fig)
# else:
#     st.info("⚠️ Data arah atau kecepatan angin tidak tersedia untuk tanggal ini.")


import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np
from windrose import WindroseAxes
from datetime import date

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Data Cuaca Harian", layout="wide")

from utils.ui import setup_header
setup_header()
# ==========================================================
# 🧭 1️⃣ KONFIGURASI SIDEBAR DENGAN LOGO
# ==========================================================
# --- CSS styling untuk sidebar ---
# st.markdown("""
#     <style>
#         [data-testid="stSidebar"] {
#             background-color: #f7f9fb;
#             padding-top: 0px;
#         }
#     </style>
# """, unsafe_allow_html=True)

# --- Konten Sidebar ---
# st.sidebar.image("Logo_BMKG.png", caption="Dashboard Monitoring Cuaca Ekstrem")
st.sidebar.markdown("""
    <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)

#st.title("Data Cuaca Harian per Stasiun")

# --- Koneksi ke database SQLite ---
db_path = "data_lengkap2.db"   # ganti sesuai path
table_name = "data_lengkap"    # ganti sesuai tabel
conn = sqlite3.connect(db_path)

# # --- Ambil daftar tanggal & stasiun unik ---
# tanggal_list = pd.read_sql_query(
#     f"SELECT DISTINCT tanggal FROM {table_name} ORDER BY tanggal", conn
# )["tanggal"].tolist()

# station_list = pd.read_sql_query(
#     f"SELECT DISTINCT station_wmo_id FROM {table_name} ORDER BY station_wmo_id", conn
# )["station_wmo_id"].tolist()

# name_list = pd.read_sql_query(
#     f"SELECT DISTINCT NAME FROM {table_name} ORDER BY NAME", conn
# )["NAME"].tolist()

# # --- Widget pilih tanggal & stasiun ---
# col1, col2, col3 = st.columns(3)
# with col1:
#     pilih_tanggal = st.date_input(
#         "📅 Pilih tanggal:",
#         #value=date(2025, 8, 1),
#         value=date.today(),
#         min_value=date(2025, 1, 1),
#         max_value=date(2025, 12, 31)
#     )
# with col2:
#     pilih_station = st.selectbox("🏷️ Pilih WMO ID Stasiun:", station_list)

# with col3:
#     pilih_nama = st.selectbox("🏷️ Pilih Nama Stasiun:", name_list)

# --- Ambil daftar stasiun ---
# 

# --- Ambil daftar stasiun ---
df_stasiun = pd.read_sql_query(
    f"SELECT DISTINCT station_wmo_id, NAME FROM {table_name} ORDER BY station_wmo_id", conn
)

# Buat dictionary untuk sinkronisasi dua arah
id_to_name = dict(zip(df_stasiun["station_wmo_id"], df_stasiun["NAME"]))
name_to_id = dict(zip(df_stasiun["NAME"], df_stasiun["station_wmo_id"]))

# --- Inisialisasi session_state ---
if "selected_wmo" not in st.session_state:
    st.session_state.selected_wmo = None
if "selected_name" not in st.session_state:
    st.session_state.selected_name = None

# --- Fungsi sinkronisasi dua arah ---
def update_from_wmo():
    wmo = st.session_state.selected_wmo
    st.session_state.selected_name = id_to_name.get(wmo, None)

def update_from_name():
    name = st.session_state.selected_name
    st.session_state.selected_wmo = name_to_id.get(name, None)

# --- Widget input ---
col1, col2, col3 = st.columns(3)

with col1:
    pilih_tanggal = st.date_input(
        "📅 Pilih tanggal:",
        value=date.today(),
        min_value=date(2025, 1, 1),
        max_value=date(2025, 12, 31)
    )

with col2:
    st.selectbox(
        "🏷️ Pilih WMO ID Stasiun:",
        options=[""] + list(id_to_name.keys()),
        key="selected_wmo",
        on_change=update_from_wmo
    )

with col3:
    st.selectbox(
        "🏷️ Pilih Nama Stasiun:",
        options=[""] + list(name_to_id.keys()),
        key="selected_name",
        on_change=update_from_name
    )

# --- Ambil nilai akhir yang tersinkronisasi ---
pilih_station = st.session_state.selected_wmo
pilih_nama = st.session_state.selected_name

# --- Tampilkan hasil ---
if pilih_station and pilih_nama:
    st.success(f"Stasiun terpilih: **{pilih_nama} ({pilih_station})**")
else:
    st.info("Silakan pilih salah satu: WMO ID atau Nama Stasiun.")


# --- Query data ---
query = f"""
SELECT station_wmo_id, NAME, jam, tanggal, sandi_gts,
       Tekanan_Permukaan, Temperatur, Kecepatan_angin, Arah_angin, Curah_Hujan_Jam,Dew_Point
FROM {table_name}
WHERE tanggal = ?
  AND station_wmo_id = ?
ORDER BY jam
"""
df = pd.read_sql_query(query, conn, params=(pilih_tanggal.strftime("%Y-%m-%d"), pilih_station))
conn.close()

if df.empty:
    st.warning("⚠️ Tidak ada data untuk tanggal dan stasiun yang dipilih.")
    st.stop()

# --- Pastikan jam urut ---
df["jam"] = pd.to_datetime(df["jam"], format="%H:%M").dt.strftime("%H:%M")
df.index = df.index + 1
# --- Tampilkan data utama ---
#st.subheader(f"📍 Data Stasiun {pilih_station} pada {pilih_tanggal.strftime('%d-%m-%Y')}")
#st.dataframe(df)
st.dataframe(
    df,  # DataFrame Anda yang sudah di-style
    column_config={
        "station_wmo_id": st.column_config.Column("ID Stasiun"),
        "NAME": st.column_config.Column("Nama Stasiun"),
        "jam": st.column_config.Column("Jam"),
        "tanggal": st.column_config.Column("Tanggal"),
        "sandi_gts": st.column_config.Column("Sandi GTS"),
        "Tekanan_Permukaan": st.column_config.Column("Tekanan Permukaan"),
        "Temperatur": st.column_config.Column("Temperatur"),
        "Kecepatan_angin": st.column_config.Column("Kecepatan Angin"),
        "Arah_angin": st.column_config.Column("Arah Angin"),
        "Curah_Hujan_Jam": st.column_config.Column("Curah Hujan"),
        "Dew_Point": st.column_config.Column("Titik Embun"),
        # Format angka (misal "35.2") sudah diatur oleh .format() Anda,
        # jadi kita hanya perlu mengganti labelnya saja.
    }
)

# --- Buat tabs untuk grafik ---
tab1, tab2, tab3, tab4, tab5= st.tabs([
    "Tekanan Permukaan",
    "Temperatur Permukaan",
    "Temperatur Titik Embun",
    "Kecepatan Angin",
    "Windrose"
])

# --- TAB 1: Tekanan Permukaan ---
with tab1:
    st.subheader("Tekanan Permukaan")
    fig1 = px.bar(df, x="jam", y="Tekanan_Permukaan",
                   title="Tekanan Permukaan (hPa)")
    # Tambahkan baris ini untuk mengatur sumbu Y
    fig1.update_yaxes(
        range=[980, 1014],  # Menetapkan rentang minimum dan maksimum
        dtick=4            # Menetapkan interval antar-label menjadi 50
    )
    st.plotly_chart(fig1, use_container_width=True)

# --- TAB 2: Temperatur ---
with tab2:
    st.subheader("Temperatur Permukaan")
    fig2 = px.line(df, x="jam", y="Temperatur", markers=True,
                   title="Temperatur (°C)")
    st.plotly_chart(fig2, use_container_width=True)
    
# --- TAB 2: Temperatur ---
with tab3:
    st.subheader("Temperatur Titik Embun")
    fig3 = px.line(df, x="jam", y="Dew_Point", markers=True,
                   title="Temperatur (°C)")
    st.plotly_chart(fig3, use_container_width=True)

# --- TAB 3: Kecepatan Angin ---
with tab4:
    st.subheader("Kecepatan Angin")
    fig4 = px.bar(df, x="jam", y="Kecepatan_angin",
                   title="Kecepatan Angin (knot)")
        # Tambahkan baris ini untuk mengatur sumbu Y
    fig4.update_yaxes(
        range=[0, 30],  # Menetapkan rentang minimum dan maksimum
        dtick=5            # Menetapkan interval antar-label menjadi 50
    )
    st.plotly_chart(fig4, use_container_width=True)

# --- TAB 4: Windrose ---
with tab5:
    st.subheader("Windrose (Distribusi Arah & Kecepatan Angin)")

    if df["Arah_angin"].notna().sum() > 0 and df["Kecepatan_angin"].notna().sum() > 0:
        fig = plt.figure(figsize=(6,6))
        ax = WindroseAxes.from_ax(fig=fig)

        ax.bar(
            df["Arah_angin"].dropna(),
            df["Kecepatan_angin"].dropna(),
            normed=True,
            opening=0.8,
            edgecolor="white",
            bins=np.arange(0, 12, 2)
        )

        ax.set_title(f"Windrose Stasiun {pilih_station}", fontsize=16, pad=20, fontweight='bold')
        ax.set_xticklabels(['E', 'NE', 'N', 'NW', 'W', 'SW', 'S', 'SE'], fontsize=12)

        ax.legend(
            title="Kecepatan Angin (m/s)",
            loc='upper center',
            bbox_to_anchor=(1.2, 1.0),
            shadow=True,
            ncol=1
        )

        #st.pyplot(fig)
        st.pyplot(fig, use_container_width=True)
    else:
        st.info("⚠️ Data arah atau kecepatan angin tidak tersedia untuk tanggal ini.")
