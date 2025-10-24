import streamlit as st
import pandas as pd
import folium
from folium import plugins
from streamlit_folium import st_folium
import sqlite3
import branca.element as element

# --- Konfigurasi halaman ---
st.set_page_config(page_title="Peta Frekuensi Cuaca Ekstrem", layout="wide")

from utils.ui import setup_header
setup_header()

st.sidebar.markdown("""
    <div class="sidebar-footer" style="font-size: 12px; color: #666; text-align: center; margin-top: 400px;">
        © 2025 | BMKG Dashboard Prototype Aktualisasi Fadhilatul Istiqomah
    </div>
""", unsafe_allow_html=True)

# ==============================
# 🎨 DEFINISI WARNA & FUNGSI (Global)
# ==============================
# Kita definisikan ini di luar tab agar bisa dipakai keduanya
batas_nilai = [1, 2, 3, 4, 5, 6, 7, 8,9]
warna_diskret = [
    "#f6b11d", "#fedc21", "#fcfd5b", "#cefd9c", "#97e0a4",
    "#05c6a3", "#0c9b9a", "#016f9f", "#033b57"
]
labels = ["1", "2", "3", "4", "5", "6", "7", "8","9"]

# Fungsi Pewarnaan Diskrit
def get_color_for_value(value):
    if value == 1: return warna_diskret[0]
    if value == 2: return warna_diskret[1]
    if value == 3: return warna_diskret[2]
    if value == 4: return warna_diskret[3]
    if value == 5: return warna_diskret[4]
    if value == 6: return warna_diskret[5]
    if value == 7: return warna_diskret[6]
    if value == 8: return warna_diskret[7]
    return warna_diskret[8]

# # Fungsi Pembuat Legenda
# def create_legend_html(title_text, colors, labels_list):
#     block_width = 45
#     tip_width = 10
#     warna_balok = colors[1:-1]
#     lebar_balok_total = len(warna_balok) * block_width
#     lebar_legenda_total = lebar_balok_total + (2 * tip_width)

#     return f'''
#     <div style="
#         position: fixed; bottom: 20px; left: 18%; transform: translateX(-50%);
#         background-color: rgba(255, 255, 255, 0.9); border: 1px solid #bbb;
#         border-radius: 8px; padding: 10px 15px; z-index: 9999;
#         font-family: Arial, sans-serif; font-size: 12px; text-align: center;
#         box-shadow: 0 0 10px rgba(0,0,0,0.2);
#     ">
#         <span style="font-weight: bold; display: block; margin-bottom: 8px;">{title_text}</span>
#         <div style="display: flex; flex-direction: column; align-items: center;">
#             <div style="display: flex; align-items: center; margin-bottom: 2px;">
#                 <div style="width: 0; height: 0; border-top: 10px solid transparent;
#                             border-bottom: 10px solid transparent; border-right: {tip_width}px solid {colors[0]};"></div>
#                 {''.join([f'<div style="background-color: {color}; width: {block_width}px; height: 20px;"></div>' for color in warna_balok])}
#                 <div style="width: 0; height: 0; border-top: 10px solid transparent;
#                             border-bottom: 10px solid transparent; border-left: {tip_width}px solid {colors[-1]};"></div>
#             </div>
#             <div style="display: flex; justify-content: space-between; width: {lebar_legenda_total}px;">
#                 {''.join([f'<span>{label}</span>' for label in labels_list])}
#             </div>
#         </div>
#     </div>
#     '''

def create_legend_html(title_text, colors, labels_list):
    block_width = 45
    tip_width = 15
    warna_balok = colors
    lebar_balok_total = len(warna_balok) * block_width + tip_width  # tambahan untuk panah kanan

    return f'''
    <div style="
        position: fixed; bottom: 20px; left: 20%; transform: translateX(-50%);
        background-color: rgba(255, 255, 255, 0.9); border: 1px solid #bbb;
        border-radius: 8px; padding: 5px 15px; z-index: 9999;
        font-family: Arial, sans-serif; font-size: 12px; text-align: center;
        box-shadow: 0 0 10px rgba(0,0,0,0.2);
    ">
        <span style="font-weight: bold; display: block; margin-bottom: 8px;">{title_text}</span>
        <div style="display: flex; flex-direction: column; align-items: center;">
            <div style="display: flex; align-items: center; justify-content: center; margin-bottom: 4px;">
                <!-- Balok warna -->
                {''.join([
                    f'''
                    <div style="background-color: {warna_balok[i]}; width: {block_width}px; height: 20px;
                                border-top-left-radius: {'8px' if i==0 else '0'};
                                border-bottom-left-radius: {'8px' if i==0 else '0'};
                                border-top-right-radius: {'0' if i < len(warna_balok)-1 else '0'};
                                border-bottom-right-radius: {'0' if i < len(warna_balok)-1 else '0'};">
                    </div>
                    ''' for i in range(len(warna_balok))
                ])}
                <!-- Panah kanan -->
                <div style="width: 0; height: 0;
                            border-top: 10px solid transparent;
                            border-bottom: 10px solid transparent;
                            border-left: {tip_width}px solid {warna_balok[-1]};
                            margin-left: -1px;">
                </div>
            </div>

            <!-- Label angka di bawah tiap kotak -->
            <div style="display: flex; justify-content: center; align-items: center;">
                {''.join([
                    f'<div style="width: {block_width}px; text-align: center;">{labels_list[i]}</div>'
                    for i in range(len(labels_list))
                ])}
                <div style="width: {tip_width}px; text-align: left;"></div>
            </div>
        </div>
    </div>
    '''



# --- BUAT TAB ---
tab1, tab2 = st.tabs(["Frekuensi Heavy Rain", "Frekuensi Gale"])

# ==========================================================
# --- TAB 1: HEAVY RAIN (KODE ANDA YANG SEBELUMNYA) ---
# ==========================================================
with tab1:
    # --- Koneksi ke database ---
    db_path_hr = "data_akhir.db"
    conn_hr = sqlite3.connect(db_path_hr)
    
    # --- Ambil data ---
    query_hr = "SELECT station_wmo_id, NAME, LAT, LON, Heavy_Rain, tanggal FROM data_akhir"
    df_hr = pd.read_sql_query(query_hr, conn_hr)
    conn_hr.close()

    # --- Pastikan kolom tanggal benar ---
    df_hr["tanggal"] = pd.to_datetime(df_hr["tanggal"], errors="coerce")
    df_hr["bulan"] = df_hr["tanggal"].dt.to_period("M").astype(str)

    # --- Pilih bulan ---

    bulan_tersedia_hr = sorted(df_hr["bulan"].dropna().unique())
    bulan_pilih_hr = st.selectbox("Pilih Bulan", bulan_tersedia_hr, key="hr_select") # Key unik

    # --- Filter data per bulan ---
    df_bulan_hr = df_hr[df_hr["bulan"] == bulan_pilih_hr]

    # --- Ambil hanya baris dengan hujan (Heavy_Rain > 0) ---
    df_hujan = df_bulan_hr[df_bulan_hr["Heavy_Rain"].fillna(0) > 0]

    # --- Hitung frekuensi kejadian per stasiun ---
    frekuensi_stasiun_hr = (
        df_hujan.groupby(["station_wmo_id", "NAME", "LAT", "LON"])
        .size()
        .reset_index(name="jumlah_kejadian")
    )

    # --- Total nasional ---
    total_kejadian_hr = frekuensi_stasiun_hr["jumlah_kejadian"].sum()
    st.write(f"Total kejadian Heavy Rain di seluruh Indonesia: {int(total_kejadian_hr)} kali")

    # --- Buat peta ---
    peta_hr = folium.Map(location=[-2, 118], zoom_start=5, tiles="CartoDB Voyager")

    # --- Tambahkan titik ---
    for _, row in frekuensi_stasiun_hr.iterrows():
        val = row["jumlah_kejadian"]
        warna = get_color_for_value(val) # Pakai fungsi global
        popup_text = f"""
        <b>Stasiun:</b> {row['NAME']}<br>
        <b>Kejadian Heavy Rain:</b> {row['jumlah_kejadian']} kali<br>
        """
        folium.CircleMarker(
            location=[row["LAT"], row["LON"]], radius=7, color=warna,
            fill=True, fill_color=warna, fill_opacity=0.9,
            popup=folium.Popup(popup_text, max_width=300)
        ).add_to(peta_hr)

    # Buat legenda HTML
    legend_html_hr = create_legend_html("Total Kejadian Heavy Rain", warna_diskret, labels)
    peta_hr.get_root().html.add_child(element.Element(legend_html_hr))

    # Tambahkan tombol Fullscreen
    plugins.Fullscreen(
        position="topright", title="Lihat Fullscreen",
        title_cancel="Keluar Fullscreen", force_separate_button=True,
    ).add_to(peta_hr)

    # Tampilkan peta
    st_folium(peta_hr, width=None, height=500, key="hr_map") # Key unik

# ==========================================================
# --- TAB 2: GALE (KODE BARU YANG DIMODIFIKASI) ---
# ==========================================================
with tab2:
    # --- Koneksi ke database ---
    db_path_gale = "data_lengkap2.db" # <-- GANTI DB
    conn_gale = sqlite3.connect(db_path_gale)
    
    # --- Ambil data ---
    # <-- GANTI QUERY (asumsi tabel 'data_lengkap' dan kolom 'Kecepatan_angin')
    query_gale = "SELECT station_wmo_id, NAME, LAT, LON, Kecepatan_angin, tanggal, jam, sandi_gts FROM data_lengkap"
    df_gale = pd.read_sql_query(query_gale, conn_gale)
    conn_gale.close()

    # --- Pastikan kolom tanggal dan angin benar ---
    df_gale["tanggal"] = pd.to_datetime(df_gale["tanggal"], errors="coerce")
    df_gale["Kecepatan_angin"] = pd.to_numeric(df_gale["Kecepatan_angin"], errors="coerce") # <-- PENTING
    df_gale["bulan"] = df_gale["tanggal"].dt.to_period("M").astype(str)

    # --- Pilih bulan ---

    bulan_tersedia_gale = sorted(df_gale["bulan"].dropna().unique())
    bulan_opsi_terbatas = [b for b in bulan_tersedia_gale if b >= "2025-01"]
    bulan_pilih_gale = st.selectbox("Pilih Bulan", bulan_opsi_terbatas, key="gale_select") # Key unik

    # --- Filter data per bulan ---
    df_bulan_gale = df_gale[df_gale["bulan"] == bulan_pilih_gale]

    # --- Ambil hanya baris dengan Gale (>= 30 knot) ---
    df_gale_kejadian = df_bulan_gale[df_bulan_gale["Kecepatan_angin"].fillna(0) >= 30] # <-- GANTI LOGIKA

    # --- Hitung frekuensi kejadian per stasiun ---
    frekuensi_stasiun_gale = (
        df_gale_kejadian.groupby(["station_wmo_id", "NAME", "LAT", "LON"])
        .size()
        .reset_index(name="jumlah_kejadian") # Biarkan nama kolom sama agar kode peta bisa dipakai ulang
    )

    # --- Total nasional ---
    total_kejadian_gale = frekuensi_stasiun_gale["jumlah_kejadian"].sum()
    st.write(f"Total kejadian Gale (≥ 30 knot) di seluruh Indonesia: {int(total_kejadian_gale)} kali") # <-- GANTI TEKS

    # --- Buat peta ---
    peta_gale = folium.Map(location=[-2, 118], zoom_start=5, tiles="CartoDB Voyager")

    # --- Tambahkan titik ---
    for _, row in frekuensi_stasiun_gale.iterrows():
        val = row["jumlah_kejadian"]
        warna = get_color_for_value(val) # Pakai fungsi global
        popup_text = f"""
        <b>Stasiun:</b> {row['NAME']}<br>
        <b>Kejadian Gale:</b> {row['jumlah_kejadian']} kali<br>
        """ # <-- GANTI TEKS
        folium.CircleMarker(
            location=[row["LAT"], row["LON"]], radius=7, color=warna,
            fill=True, fill_color=warna, fill_opacity=0.9,
            popup=folium.Popup(popup_text, max_width=300)
        ).add_to(peta_gale)

    # Buat legenda HTML
    legend_html_gale = create_legend_html("Total Kejadian Gale", warna_diskret, labels) # <-- GANTI JUDUL
    peta_gale.get_root().html.add_child(element.Element(legend_html_gale))

    # Tambahkan tombol Fullscreen
    plugins.Fullscreen(
        position="topright", title="Lihat Fullscreen",
        title_cancel="Keluar Fullscreen", force_separate_button=True,
    ).add_to(peta_gale)

    # Tampilkan peta
    st_folium(peta_gale, width=None, height=500, key="gale_map") # Key unik

        # --- Tampilkan tabel histori kejadian Gale ---
    st.markdown("### 📄 Tabel Histori Kejadian Gale")

    # Ambil data rinci kejadian gale (per tanggal per stasiun)
    df_histori_gale = (
        df_gale_kejadian[["tanggal", "jam","station_wmo_id", "NAME", "sandi_gts", "Kecepatan_angin"]]
        .sort_values(by=["tanggal", "NAME"])
    )

    # Format tanggal agar lebih rapi
    df_histori_gale["tanggal"] = df_histori_gale["tanggal"].dt.strftime("%Y-%m-%d")

    # Ubah nama kolom agar lebih mudah dibaca di dashboard
    df_histori_gale = df_histori_gale.rename(columns={
        "tanggal": "Tanggal",
        "jam":"Jam",
        "station_wmo_id": "ID Stasiun",
        "NAME": "Nama Stasiun",
        "sandi_gts": "Sandi GTS",
        "Kecepatan_angin": "Kecepatan Angin (knot)"
    })

    # --- Tampilkan tabel di Streamlit ---
    if df_histori_gale.empty:
        st.info("Tidak ada kejadian Gale pada bulan ini.")
    else:

            # --- Reset index agar tampil nomor urut 1,2,3,... ---
        df_histori_gale = df_histori_gale.reset_index(drop=True)
        df_histori_gale.index = df_histori_gale.index + 1  # mulai dari 1
        df_histori_gale.index.name = "No"
        st.dataframe(df_histori_gale, use_container_width=True, height=300)
