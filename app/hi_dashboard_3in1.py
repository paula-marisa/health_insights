import os, json
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import streamlit as st
import numpy as np

st.set_page_config(page_title="Health Insights — 3 em 1 (Snowflake | Databricks | dbt)", layout="wide")
st.title("Health Insights — Nascimentos (3 em 1)")

# ========================
# Helpers base
# ========================
def _lower(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df.columns = [str(c).lower() for c in df.columns]
    return df

@st.cache_data(ttl=300, show_spinner=False)
def query_snowflake(sql: str) -> pd.DataFrame:
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "BOOTCAMP_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "HEALTH_INSIGHTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "SILVER"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        return _lower(pd.DataFrame(rows, columns=cols))
    finally:
        conn.close()

@st.cache_data(ttl=300, show_spinner=False)
def query_dbsql(sql: str) -> pd.DataFrame:
    from databricks import sql as dbsql
    cfg = dict(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    with dbsql.connect(**cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
    return _lower(pd.DataFrame(rows, columns=cols))

def ears_c3(s, w=3, k=3):
    roll = s.shift(1).rolling(w)
    mu, sigma = roll.mean(), roll.std(ddof=0).replace(0, 1e-9)
    return s > mu + k * sigma

def _split_fqn(table_fqn: str) -> Tuple[str,str,str]:
    parts = [p.strip('"') for p in table_fqn.split(".")]
    if len(parts) != 3:
        raise ValueError(f"Esperava FQN com 3 partes (DB.SCHEMA.TABLE), recebi: {table_fqn}")
    return parts[0], parts[1], parts[2]

def list_columns(table_fqn: str, run_query) -> set[str]:
    """Tenta information_schema, devolve nomes em lowercase."""
    try:
        db, sc, tb = _split_fqn(table_fqn)
    except Exception:
        return set()

    candidates = [
        f"""
        select lower(column_name) as c
        from {db}.information_schema.columns
        where lower(table_schema)=lower('{sc}') and lower(table_name)=lower('{tb}')
        """,
        """
        select lower(column_name) as c
        from information_schema.columns
        where lower(table_schema)=lower('{sc}') and lower(table_name)=lower('{tb}')
        """.format(sc=sc, tb=tb),
    ]
    for q in candidates:
        try:
            df = run_query(q)
            if df is not None and not df.empty and "c" in df.columns:
                return set(df["c"].tolist())
        except Exception:
            continue
    return set()

def first_existing(cols_set: set[str], candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c and c.lower() in cols_set:
            return c
    return None

def detect_dialect(run_query) -> str:
    """Heurística básica."""
    name = getattr(run_query, "__name__", "").lower()
    if "snowflake" in name:
        return "snowflake"
    if "dbsql" in name or "databricks" in name:
        return "databricks"
    return "generic"

# ========================
# Abstração principal
# ========================
def render_data_tab(run_query, table_fqn: str, dim_tempo_fqn: Optional[str] = None):
    dialect = detect_dialect(run_query)

    # Descobrir colunas disponíveis
    cols = list_columns(table_fqn, run_query)

    # Colunas-chave
    date_col = first_existing(cols, ["birth_date", "admission_date", "data", "date"])
    ym_col   = first_existing(cols, ["year_month_date", "ym"])
    sexo_col = first_existing(cols, ["sex_newborn", "sexo_rn", "sexo"])
    peso_col = first_existing(cols, ["birth_weight_g", "peso_nascimento_g", "peso_ao_nascer", "peso"])
    gest_col = first_existing(cols, ["gestational_weeks", "gestacao_semanas", "idade_gestacional_semanas"])
    parto_col= first_existing(cols, ["delivery_type", "tipo_parto", "tp_parto", "tipo_de_parto"])
    tempo_fk = first_existing(cols, ["fk_sk_tempo", "sk_tempo", "tempo_id"])

    # Expressões dependentes do dialeto
    if dialect == "snowflake":
        to_num = lambda c: f"try_to_number({c})"
        to_ym_from_date = lambda c: f"to_char(date_trunc('month', {c}), 'YYYY-MM')"
        to_ym_from_parts = lambda y, m: f"to_char(to_date({y}||'-'||lpad(cast({m} as string),2,'0')||'-01'),'YYYY-MM')"
        count_if = "count_if"
        now_expr = "current_timestamp"
    else:  # databricks / generic
        # Databricks SQL tem TRY_CAST, DATE_FORMAT e COUNT_IF
        to_num = lambda c: f"try_cast({c} as double)"
        to_ym_from_date = lambda c: f"date_format(date_trunc('month', {c}), 'yyyy-MM')"
        to_ym_from_parts = lambda y, m: f"format_string('%04d-%02d', {y}, {m})"
        count_if = "count_if"
        now_expr = "current_timestamp()"
        
    # ---------------- Construção de YM para séries ----------------
    # Tenta usar uma coluna já existente; se não houver, deriva de uma data;
    # em último caso usa a dim_tempo (t.ano/t.mes), se a join existir.
    if ym_col:
        ym_expr = ym_col
    elif date_col:
        ym_expr = to_ym_from_date(date_col)
    elif dim_tempo_fqn and tempo_fk:
        ym_expr = to_ym_from_parts("t.ano", "t.mes")
    else:
        ym_expr = None
        
    # Evitar ambiguidade quando existe join à dim_tempo:
    if ym_expr and ym_col and (dim_tempo_fqn and tempo_fk):
        # Se estamos a usar a coluna 'ym' existente na fato, qualifica-a como f.ym
        if ym_expr == ym_col:
            ym_expr = f"f.{ym_col}"
        

    # ---------------- Min/Max datas (fallbacks) ----------------
    b = None
    # 1) Se temos coluna de data na própria fato
    if date_col:
        b = run_query(f"select min({date_col}) mn, max({date_col}) mx from {table_fqn}")
    # 2) Senão, tenta via dim_tempo
    if (b is None or b.empty or b.iloc[0].isna().any()) and (dim_tempo_fqn and tempo_fk):
        try:
            b = run_query(f"""
                select min(t.data_dia) mn, max(t.data_dia) mx
                from {table_fqn} f
                join {dim_tempo_fqn} t on f.{tempo_fk} = t.sk_tempo
            """)
            # reusa data_dia como date_col lógico
            if not date_col:
                date_col = "t.data_dia"
        except Exception:
            b = None

    if b is None or b.empty or b.iloc[0].isna().any():
        st.warning(f"Sem dados de datas em `{table_fqn}`.")
        return None

    mn, mx = pd.to_datetime(b.loc[0, "mn"]).date(), pd.to_datetime(b.loc[0, "mx"]).date()

    # Qual coluna/expressão usar para filtrar datas?
    if date_col:
        date_expr = date_col            # já existe na fato (ex.: birth_date)
    elif dim_tempo_fqn and tempo_fk:
        date_expr = "t.data_dia"        # vem da dim_tempo
    else:
        date_expr = None
    
    # ---------------- Filtros UI ----------------
    with st.sidebar:
        st.subheader("Filtros")
        _dates = st.date_input(
            "Intervalo",
            value=(mn, mx),
            min_value=mn,
            max_value=mx,
            key=str(table_fqn) + "_dates",
        )
        # aceita 1 ou 2 valores, conforme o estado antigo do widget
        if isinstance(_dates, (list, tuple)) and len(_dates) == 2:
            dt_ini, dt_fim = _dates
        else:
            dt_ini = _dates
            dt_fim = _dates

        sexos = st.multiselect(
            "Sexo", ["M", "F", "U"], default=["M", "F"], key=str(table_fqn) + "_sex"
        )

    # ---------------- WHERE clause ----------------
    # ---------------- WHERE clause ----------------
    where = []
    if date_expr:
        where.append(f"{date_expr} between date '{dt_ini}' and date '{dt_fim}'")

    # filtro sexo (se existir)
    sexo_expr = sexo_col if sexo_col else None
    if sexos and set(sexos) != {"M", "F", "U"} and sexo_expr:
        where.append(f"{sexo_expr} in (" + ",".join(f"'{s}'" for s in sexos) + ")")

    clause = (" where " + " and ".join(where)) if where else ""

    # ---------------- KPI query (sem dependências fortes) ----------------
    peso_expr = to_num(peso_col) if peso_col else "null"
    gest_expr = to_num(gest_col) if gest_col else "null"
    parto_raw = f"coalesce({parto_col}, '')" if parto_col else "''"
    parto_lwr = f"lower({parto_raw})"

    # Regras robustas para cesariana:
    # - Texto começado por "cesa..." (cesarea/cesárea/cesariana/cesarean…)
    # - Lista de sinónimos em lower
    # - Códigos numéricos (ex.: 2,3,9) quando o campo é numérico ou string numérica
    if dialect == "snowflake":
        # Igual à query do screenshot: labels + código 1
        cesar_flag = f"""
            case
                when regexp_like(upper({parto_raw}), '^(CESAREA|CESÁREA|CESAREAN|CESARIAN|C)$') then 1
                when try_to_number({parto_raw}) in (1) then 1
                else 0
            end
        """
    else:  # databricks/generic
        cesar_flag = f"""
            case
                when upper({parto_raw}) rlike '^(CESAREA|CESÁREA|CESAREAN|CESARIAN|C)$' then 1
                when try_cast({parto_raw} as int) in (1) then 1
                else 0
            end
        """

    from_block = f"from {table_fqn} f"
    if dim_tempo_fqn and tempo_fk:
        from_block += f" left join {dim_tempo_fqn} t on f.{tempo_fk} = t.sk_tempo"

    kpi_q = f"""
        select
            count(*) as n_nascidos,
            avg({peso_expr}) as peso_medio_g,
            100.0 * avg(case when {gest_expr} is not null and {gest_expr} < 37 then 1 else 0 end) as pct_prematuros,
            100.0 * avg({cesar_flag}) as pct_cesareas
        {from_block}
        {clause}
    """
    kdf = run_query(kpi_q)
    k = kdf.iloc[0].to_dict() if kdf is not None and not kdf.empty else {
        "n_nascidos": 0, "peso_medio_g": None, "pct_prematuros": 0.0, "pct_cesareas": 0.0
    }


    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Nascidos vivos", int(k.get("n_nascidos", 0)))
    c2.metric("Peso médio (g)", "—" if (k.get("peso_medio_g") is None or pd.isna(k.get("peso_medio_g"))) else f"{float(k['peso_medio_g']):.0f}")
    c3.metric("Prematuros (%)", f"{float(k.get('pct_prematuros', 0.0)):.2f}")
    c4.metric("Cesarianas (%)", f"{float(k.get('pct_cesareas', 0.0)):.2f}")

    # ---------------- Monitor rápido de qualidade ----------------
    localidade_fk = first_existing(cols, ["fk_sk_localidade", "fk_localidade", "sk_localidade",
                                          "fk_local", "fk_municipio", "municipio_id", "fk_sk_municipio"])
    tempo_null_expr = (f"sum(case when f.{tempo_fk} is null then 1 else 0 end)" if tempo_fk else "0")
    loc_null_expr   = (f"sum(case when f.{localidade_fk} is null then 1 else 0 end)" if localidade_fk else "0")

    qm_sql = f"""
        select
            {now_expr} as verificado_em,
            count(*) as total_registos,
            {tempo_null_expr} as nulos_fk_tempo,
            {loc_null_expr} as nulos_localidade
        from {table_fqn} f
    """
    qm_df = run_query(qm_sql)
    if qm_df is not None and not qm_df.empty:
        qm = qm_df.iloc[0]
        if qm['nulos_fk_tempo'] == 0 and qm['nulos_localidade'] == 0:
            cor = "🟢"
        elif qm['nulos_fk_tempo'] < 10 and qm['nulos_localidade'] < 10:
            cor = "🟡"
        else:
            cor = "🔴"
        st.metric("Qualidade dos Dados", f"{cor} {pd.to_datetime(qm['verificado_em']).strftime('%Y-%m-%d')}")

    # ---------------- Alerta EARS (mensal) ----------------
    if ym_expr:
        try:
            if peso_col:
                if dialect == "databricks":
                    # Databricks: devolve numerador/denominador e calcula taxa em Pandas
                    alert_q = f"""
                        select {ym_expr} as ym,
                            sum(case when {peso_expr} < 2500 then 1 else 0 end) as num,
                            count(*) as den
                        {from_block}
                        {clause}
                        group by {ym_expr}
                        order by {ym_expr}
                    """
                    alert_df = run_query(alert_q)
                    if alert_df is not None and not alert_df.empty:
                        alert_df["ym"] = alert_df["ym"].astype(str)
                        alert_df["taxa"] = alert_df["num"].astype(float) / alert_df["den"].replace(0, np.nan)
                        label_alerta = "taxa de baixo peso"
                    else:
                        alert_df = None
                else:
                    alert_q = f"""
                        select {ym_expr} as ym,
                            {count_if}({peso_expr} < 2500) / nullif(count(*),0) as taxa
                        {from_block}
                        {clause}
                        group by {ym_expr}
                        order by {ym_expr}
                    """
                    alert_df = run_query(alert_q)
                    if alert_df is not None and not alert_df.empty:
                        alert_df["ym"] = alert_df["ym"].astype(str)
                        label_alerta = "taxa de baixo peso"
            else:
                if dialect == "databricks":
                    # Cesarianas no Databricks: idem, calcula taxa no Python
                    alert_q = f"""
                        select {ym_expr} as ym,
                            sum({cesar_flag}) as num,
                            count(*) as den
                        {from_block}
                        {clause}
                        group by {ym_expr}
                        order by {ym_expr}
                    """
                    alert_df = run_query(alert_q)
                    if alert_df is not None and not alert_df.empty:
                        alert_df["ym"] = alert_df["ym"].astype(str)
                        alert_df["taxa"] = alert_df["num"].astype(float) / alert_df["den"].replace(0, np.nan)
                        label_alerta = "taxa de cesarianas"
                    else:
                        alert_df = None
                else:
                    alert_q = f"""
                        select {ym_expr} as ym,
                            avg({cesar_flag}) as taxa
                        {from_block}
                        {clause}
                        group by {ym_expr}
                        order by {ym_expr}
                    """
                    alert_df = run_query(alert_q)
                    if alert_df is not None and not alert_df.empty:
                        alert_df["ym"] = alert_df["ym"].astype(str)
                        label_alerta = "taxa de cesarianas"

            if alert_df is not None and not alert_df.empty and "taxa" in alert_df.columns:
                flags = ears_c3(alert_df["taxa"].astype(float))
                if getattr(flags, "any", lambda: False)():
                    meses_alerta = alert_df.loc[flags, "ym"].astype(str).tolist()
                    st.warning(f"Alerta: {label_alerta} acima do esperado em: {', '.join(meses_alerta)}")
        except Exception as e:
            st.info(f"Não foi possível calcular o alerta ({type(e).__name__}).")

    # ---------------- Série mensal (nascimentos) ----------------
    st.subheader("Nascimentos por mês")
    if ym_expr:
        mensal = run_query(f"""
            select {ym_expr} as ym, count(*) nascimentos
            {from_block}
            {clause}
            group by {ym_expr}
            order by {ym_expr}
        """)
        if mensal is not None and not mensal.empty:
            st.line_chart(mensal.set_index("ym"))
        else:
            st.info("Sem dados para o filtro.")
    else:
        st.info("Sem coluna/expressão mensal (ym) disponível.")

    # ---------------- Peso por sexo ----------------
    st.subheader("Peso médio por sexo")
    if peso_col and sexo_col:
        peso = run_query(f"""
            select {sexo_col} as sexo, round(avg({peso_expr}), 0) as peso_medio_g
            {from_block}
            {clause}
            group by {sexo_col}
            order by {sexo_col}
        """)
        if peso is not None and not peso.empty:
            st.bar_chart(peso.set_index("sexo"))
        else:
            st.info("Sem dados para o filtro.")
    else:
        st.info("Sem coluna de peso/sexo disponível para este gráfico.")

    return None

# ========================
# Artefactos do dbt
# ========================
def render_dbt_tab():
    proj = Path(os.getenv("DBT_PROJECT_DIR", "."))
    target = Path(os.getenv("DBT_TARGET_DIR", proj / "target"))
    man_p = target / "manifest.json"
    res_p = target / "run_results.json"

    st.caption(f"Projeto dbt: `{proj}`")
    if not man_p.exists() and not res_p.exists():
        st.warning("Não encontrei `manifest.json` / `run_results.json`. Corre `dbt run && dbt test` e volta a carregar.")
        return None

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
        st.dataframe(
            models.sort_values("execution_time", ascending=False)[["unique_id","status","execution_time"]].head(20),
            use_container_width=True
        )
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

    return None

# ========================
# UI principal (tabs)
# ========================
tab_sf, tab_db, tab_dbt = st.tabs(["Snowflake (dbt marts)", "Databricks SQL (views federadas)", "dbt (artefactos)"])

with tab_sf:
    # Usa por omissão a tua view normalizada
    tbl_sf = os.getenv("SNOWFLAKE_FATO_FQN", "HEALTH_INSIGHTS.SILVER.VW_FATO_BIRTHS")
    # Se tiveres DIM_TEMPO e quiseres usar como apoio (não é obrigatório)
    dim_sf = os.getenv("SNOWFLAKE_DIM_TEMPO_FQN", "HEALTH_INSIGHTS.SILVER.DIM_TEMPO")
    st.caption(f"Fonte: `{tbl_sf}`")
    _ = render_data_tab(query_snowflake, tbl_sf, dim_sf)  # ignora saída (evita print do objeto)

with tab_db:
    # Ajusta para a tua view federada no Databricks, se existir
    tbl_db = os.getenv("DATABRICKS_FED_TABLE", "health_insights.silver.fed_fato")
    dim_db = os.getenv("DATABRICKS_DIM_TEMPO", "sf_hi.raw_stg_marts.dim_tempo")
    st.caption(f"Fonte: `{tbl_db}`")
    _ = render_data_tab(query_dbsql, tbl_db, dim_db)

with tab_dbt:
    _ = render_dbt_tab()
