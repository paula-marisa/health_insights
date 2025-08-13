import os
from databricks import sql
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Health Insights — Nascimentos (DBSQL)", layout="wide")
st.title("Health Insights — Nascimentos (via Databricks SQL)")

cfg = {
    "server_hostname": os.environ["DATABRICKS_SERVER_HOSTNAME"],
    "http_path":       os.environ["DATABRICKS_HTTP_PATH"],
    "access_token":    os.environ["DATABRICKS_TOKEN"],
}

def run_query(q):
    with sql.connect(**cfg) as conn:
        return pd.read_sql(q, conn)

# bounds
b = run_query("select min(birth_date) mn, max(birth_date) mx from health_insights.silver.fed_fato")
if b.empty or pd.isna(b.loc[0,"mn"]):
    st.warning("Sem dados em health_insights.silver.fed_fato.")
    st.stop()
mn, mx = pd.to_datetime(b.loc[0,"mn"]).date(), pd.to_datetime(b.loc[0,"mx"]).date()

with st.sidebar:
    st.subheader("Filtros")
    dt_ini, dt_fim = st.date_input("Intervalo", value=(mn, mx), min_value=mn, max_value=mx)
    sexos = st.multiselect("Sexo", ["M","F","U"], default=["M","F"])

where = [f"birth_date between '{dt_ini}' and '{dt_fim}'"]
if sexos and set(sexos) != {"M","F","U"}:
    where.append("sex_newborn in (" + ",".join(f"'{s}'" for s in sexos) + ")")
clause = " where " + " and ".join(where)

# KPIs
kpis = run_query(f"""
  select
    count(*)                                 as n_nascidos,
    round(avg(birth_weight_g),0)             as peso_medio_g,
    round(avg(is_premature)*100,2)           as pct_prematuros,
    round(avg(is_cesarean)*100,2)            as pct_cesareas
  from health_insights.silver.fed_fato
  {clause}
""").iloc[0]

c1,c2,c3,c4 = st.columns(4)
c1.metric("Nascidos vivos", int(kpis.n_nascidos))
c2.metric("Peso médio (g)", f"{kpis.peso_medio_g:.0f}")
c3.metric("Prematuros (%)", f"{kpis.pct_prematuros:.2f}")
c4.metric("Cesarianas (%)", f"{kpis.pct_cesareas:.2f}")

# Séries mensais
mensal = run_query(f"""
  select ym, count(*) nascimentos
  from health_insights.silver.fed_fato
  {clause}
  group by ym order by ym
""")
st.subheader("Nascimentos por mês")
if not mensal.empty:
    st.line_chart(mensal.set_index("ym"))
else:
    st.info("Sem dados para o filtro.")

# Peso por sexo
peso = run_query(f"""
  select sex_newborn as sexo, round(avg(birth_weight_g),0) as peso_medio_g
  from health_insights.silver.fed_fato
  {clause}
  group by sex_newborn order by sex_newborn
""")
st.subheader("Peso médio por sexo")
if not peso.empty:
    st.bar_chart(peso.set_index("sexo"))
else:
    st.info("Sem dados para o filtro.")