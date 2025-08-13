import os
import streamlit as st
import pandas as pd
import snowflake.connector

st.set_page_config(page_title="Health Insights – Nascimentos", layout="wide")
st.title("Health Insights – Nascimentos (H1 2023)")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE","BOOTCAMP_WH"),
    database=os.getenv("SNOWFLAKE_DATABASE","HEALTH_INSIGHTS"),
    schema=os.getenv("SNOWFLAKE_SCHEMA","MARTS"),
)

@st.cache_data(ttl=300)
def load_data():
    q1 = """
      SELECT 
        COUNT(*) AS n_nascidos,
        ROUND(AVG(birth_weight_g),0) AS peso_medio_g,
        ROUND(AVG(is_premature)*100,2) AS pct_prematuros,
        ROUND(AVG(is_cesarean)*100,2) AS pct_cesareas
      FROM fato_nascimentos
    """
    q2 = """
      SELECT ym, COUNT(*) AS nascimentos
      FROM fato_nascimentos
      GROUP BY ym
      ORDER BY ym
    """
    q3 = """
      SELECT sex_newborn, ROUND(AVG(birth_weight_g),0) AS peso_medio_g
      FROM fato_nascimentos
      GROUP BY sex_newborn
      ORDER BY sex_newborn
    """
    with conn.cursor() as cur:
        cur.execute(q1); kpis = cur.fetchone()
        cur.execute(q2); mensal = cur.fetchall()
        cur.execute(q3); peso_sexo = cur.fetchall()
    return kpis, mensal, peso_sexo

kpis, mensal, peso_sexo = load_data()

c1,c2,c3,c4 = st.columns(4)
c1.metric("Nascidos vivos", int(kpis[0]))
c2.metric("Peso médio (g)", f"{kpis[1]:.0f}")
c3.metric("Prematuros (%)", f"{kpis[2]:.2f}")
c4.metric("Cesarianas (%)", f"{kpis[3]:.2f}")

st.subheader("Nascimentos por mês")
mensal_df = pd.DataFrame(mensal, columns=["YM","Nascimentos"])
st.line_chart(mensal_df.set_index("YM"))

st.subheader("Peso médio por sexo")
peso_df = pd.DataFrame(peso_sexo, columns=["Sexo","Peso médio (g)"])
st.bar_chart(peso_df.set_index("Sexo"))
