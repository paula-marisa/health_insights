import os, json
from pathlib import Path
import pandas as pd
import streamlit as st
import numpy as np

st.set_page_config(page_title="Health Insights — 3 em 1 (Snowflake | Databricks | dbt)", layout="wide")
st.title("Health Insights — Nascimentos (3 em 1)")

# ---------- Helpers ----------
def _lower(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df.columns = [str(c).lower() for c in df.columns]
    return df

def query_snowflake(sql):
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "BOOTCAMP_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "HEALTH_INSIGHTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW_STG_MARTS"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        return _lower(pd.DataFrame(rows, columns=cols))
    finally:
        conn.close()

def query_dbsql(sql):
    from databricks import sql as dbsql
    cfg = dict(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    with dbsql.connect(**cfg) as conn:
        return _lower(pd.read_sql(sql, conn))
    
def ears_c3(s, w=3, k=3):
    roll = s.shift(1).rolling(w)
    mu, sigma = roll.mean(), roll.std(ddof=0).replace(0, 1e-9)
    return s > mu + k * sigma

def render_data_tab(run_query, table_fqn):
    # bounds
    b = run_query(f"select min(birth_date) mn, max(birth_date) mx from {table_fqn}")
    if b.empty or b.iloc[0].isna().any():
        st.warning(f"Sem dados em {table_fqn}.")
        return
    mn, mx = pd.to_datetime(b.loc[0, "mn"]).date(), pd.to_datetime(b.loc[0, "mx"]).date()

    with st.sidebar:
        st.subheader("Filtros")
        dt_ini, dt_fim = st.date_input("Intervalo", value=(mn, mx), min_value=mn, max_value=mx, key=str(table_fqn)+"_dates")
        sexos = st.multiselect("Sexo", ["M","F","U"], default=["M","F"], key=str(table_fqn)+"_sex")

    where = [f"birth_date between '{dt_ini}' and '{dt_fim}'"]
    if sexos and set(sexos) != {"M","F","U"}:
        where.append("sex_newborn in (" + ",".join(f"'{s}'" for s in sexos) + ")")
    clause = " where " + " and ".join(where)

    # KPIs
    kpi_q = f"""
      select
        count(*)                                 as n_nascidos,
        round(avg(birth_weight_g),0)             as peso_medio_g,
        round(avg(is_premature)*100,2)           as pct_prematuros,
        round(avg(is_cesarean)*100,2)            as pct_cesareas
      from {table_fqn}
      {clause}
    """
    kdf = run_query(kpi_q)
    k = kdf.iloc[0].to_dict() if not kdf.empty else {"n_nascidos":0,"peso_medio_g":0,"pct_prematuros":0.0,"pct_cesareas":0.0}

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Nascidos vivos", int(k.get("n_nascidos", 0)))
    c2.metric("Peso médio (g)", f"{float(k.get('peso_medio_g', 0)):.0f}")
    c3.metric("Prematuros (%)", f"{float(k.get('pct_prematuros', 0.0)):.2f}")
    c4.metric("Cesarianas (%)", f"{float(k.get('pct_cesareas', 0.0)):.2f}")

    # ----- Monitor de qualidade -----
    qm_df = run_query(f"""
        select 
            current_timestamp as verificado_em,
            count(*) as total_registos,
            count_if(birth_date is null) as nulos_birth_date,
            count(distinct sk_nascimento) as ids_unicos,
            (count(*) - count(distinct sk_nascimento)) as duplicados
        from {table_fqn}
    """)

    if not qm_df.empty:
        qm = qm_df.iloc[0]
        if qm['nulos_birth_date'] == 0 and qm['duplicados'] == 0:
            cor = "🟢"
        elif qm['nulos_birth_date'] < 10:
            cor = "🟡"
        else:
            cor = "🔴"
        st.metric("Qualidade dos Dados", f"{cor} {qm['verificado_em']:%Y-%m-%d}")

    # ----- Alerta epidemiológico (ex.: baixo peso) -----
    alert_df = run_query(f"""
        select ym, 
           count_if(birth_weight_g < 2500) / count(*)::float as taxa_baixo_peso
        from {table_fqn}
        {clause}
        group by ym
    o   rder by ym
    """)

    if not alert_df.empty:
        alert_df['alerta'] = ears_c3(alert_df['taxa_baixo_peso'])
    if alert_df['alerta'].any():
        st.warning("⚠️ Alerta: taxa de baixo peso acima do esperado em um ou mais meses!")

    # Mensal
    mensal = run_query(f"""
      select ym, count(*) nascimentos
      from {table_fqn}
      {clause}
      group by ym order by ym
    """)
    st.subheader("Nascimentos por mês")
    st.line_chart(mensal.set_index("ym")) if not mensal.empty else st.info("Sem dados para o filtro.")

    # Peso por sexo
    peso = run_query(f"""
      select sex_newborn as sexo, round(avg(birth_weight_g),0) as peso_medio_g
      from {table_fqn}
      {clause}
      group by sex_newborn order by sex_newborn
    """)
    st.subheader("Peso médio por sexo")
    st.bar_chart(peso.set_index("sexo")) if not peso.empty else st.info("Sem dados para o filtro.")

# ---------- Artefactos do dbt ----------
def render_dbt_tab():
    proj = Path(os.getenv("DBT_PROJECT_DIR", "."))
    target = Path(os.getenv("DBT_TARGET_DIR", proj / "target"))
    man_p = target / "manifest.json"
    res_p = target / "run_results.json"

    st.caption(f"Projeto dbt: `{proj}`")
    if not man_p.exists() and not res_p.exists():
        st.warning("Não encontrei `manifest.json` / `run_results.json`. Corre `dbt run && dbt test` e volta a carregar.")
        return

    def load_json(p):
        try:
            return json.loads(Path(p).read_text())
        except Exception:
            return None

    res = load_json(res_p)
    man = load_json(man_p)

    if res and "results" in res:
        df = pd.DataFrame([{
            "unique_id": r.get("unique_id"),
            "status": r.get("status"),
            "execution_time": r.get("execution_time"),
            "timing": r.get("timing"),
        } for r in res["results"]])
        tests = df[df["unique_id"].str.contains("^test\\.", regex=True, na=False)]
        models = df[df["unique_id"].str.contains("^model\\.", regex=True, na=False)]

        c1, c2, c3 = st.columns(3)
        c1.metric("Modelos OK", int((models["status"]=="success").sum()))
        c2.metric("Testes OK", int((tests["status"]=="success").sum()))
        c3.metric("Falhas", int((df["status"]=="error").sum()))

        st.subheader("Falhas (se existirem)")
        st.dataframe(df[df["status"]=="error"], use_container_width=True)

        st.subheader("Últimas execuções (modelos)")
        st.dataframe(models.sort_values("execution_time", ascending=False)[["unique_id","status","execution_time"]].head(20),
                     use_container_width=True)
    else:
        st.info("Sem `run_results.json` legível.")

    if man and "nodes" in man:
        st.subheader("Modelos & fontes (manifest)")
        nodes = []
        for k,v in man["nodes"].items():
            if v.get("resource_type") in ("model","source"):
                nodes.append({"id": k, "name": v.get("name"), "resource": v.get("resource_type"),
                              "package": v.get("package_name")})
        if nodes:
            st.dataframe(pd.DataFrame(nodes), use_container_width=True)

# ---------- UI principal ----------
tab_sf, tab_db, tab_dbt = st.tabs(["Snowflake (dbt marts)", "Databricks SQL (views federadas)", "dbt (artefactos)"])

with tab_sf:
    tbl_sf = f"{os.getenv('SNOWFLAKE_DATABASE','HEALTH_INSIGHTS')}.{os.getenv('SNOWFLAKE_SCHEMA','RAW_STG_MARTS')}.{os.getenv('SNOWFLAKE_FATO_TABLE','FATO_NASCIMENTOS')}"
    st.caption(f"Tabela: `{tbl_sf}`")
    render_data_tab(query_snowflake, os.getenv("SNOWFLAKE_FATO_TABLE","FATO_NASCIMENTOS"))

with tab_db:
    tbl_db = os.getenv("DATABRICKS_FED_TABLE", "health_insights.silver.fed_fato")
    st.caption(f"View: `{tbl_db}`")
    render_data_tab(query_dbsql, tbl_db)

with tab_dbt:
    render_dbt_tab()