import streamlit as st
import pandas as pd

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
from utils.ui import setup_header
setup_header()

#st.title("Data Stasiun")

df=pd.read_excel('Stasiun.xlsx', sheet_name="data_stasiun")
df.index = df.index + 1
st.dataframe(df, use_container_width=True)


