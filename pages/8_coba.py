import streamlit as st
import pandas as pd

st.set_page_config(page_title="Tampilan Google Sheets", layout="wide")
st.title("📊 Data dari Google Sheets")

# → Ganti dengan ID dari Google Sheets-mu
SHEET_ID = "1qN8LBbEZsaZ9F4-1wtfpLnIomTNR7f9n"
# → Ganti dengan gid/tab yang ingin dibaca (dari link Google Sheets)
SHEET_GID = "1279448177"

# Membuat URL export ke format CSV
csv_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={SHEET_GID}"

@st.cache_data
def load_data(url):
    return pd.read_csv(url)

try:
    df = load_data(csv_url)
    st.success(f"Data berhasil dimuat dari sheet ID {SHEET_ID}")
    st.subheader("📋 Data Awal:")
    st.dataframe(df, use_container_width=True)
    st.write("Jumlah baris:", df.shape[0])
    st.write("Jumlah kolom:", df.shape[1])

    with st.expander("🔍 Statistik ringkas"):
        st.write(df.describe())

    with st.expander("🧭 Nama Kolom"):
        st.write(list(df.columns))

except Exception as e:
    st.error(f"Gagal memuat data: {e}")
