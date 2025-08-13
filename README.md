# Health Insights — SINASC (Nascimentos)

Pipeline enxuta para entregar em poucas horas usando **Snowflake + dbt + Streamlit**. Inclui:

* Download oficial DATASUS (.DBC) e conversão para **CSV**
* Ingestão no **Snowflake**
* Modelagem com **dbt** (staging → intermediate → marts) com filtro para **1º semestre (H1)**
* **Dashboard Streamlit** (KPIs + gráficos)
* **Task** no Snowflake (bónus de orquestração)

> **Stack alvo:** WSL (Ubuntu) + Python 3.10+.

---

## 0) Estrutura de pastas do projeto

```bash
health_insights/
├─ datasets/
│  ├─ originais/        # .dbc baixados do DATASUS (ex.: DNRJ2022.dbc, DNRJ2023.dbc)
│  └─ csv/              # CSVs convertidos
├─ scripts/
│  └─ dbc_to_csv.py     # script de conversão Python
├─ app/
│  └─ app.py            # dashboard Streamlit
└─ dbt/health_insights/
   ├─ dbt_project.yml
   └─ models/
      ├─ staging/
      │  ├─ stg_births.sql
      │  └─ stg_births.yml
      ├─ intermediate/
      │  └─ int_births_enriched.sql
      └─ marts/
         ├─ fato_nascimentos.sql
         └─ marts.yml
      └─ exposures.yml
```

Cria as pastas:

```bash
mkdir -p ~/health_insights/{datasets/{originais,csv},scripts,app,dbt/health_insights/models/{staging,intermediate,marts}}
cd ~/health_insights
```

---

## 1) Preparar o WSL (Ubuntu) e Python

```bash
# Atualizar o sistema e instalar dependências de build
sudo apt update && sudo apt -y upgrade
sudo apt -y install python3-venv python3-dev build-essential \
    zlib1g-dev libffi-dev libssl-dev wget unzip git

# (opcional) utilitário para compressão rápida
sudo apt -y install pigz
```

Criar **venv** e instalar pacotes Python:

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install pandas pysus dbfread pyarrow
```

> O `pysus` inclui o leitor de `.dbc` (via `pyreaddbc`). As libs de build acima evitam erros na compilação.

---

## 2) Download dos datasets do DATASUS (SINASC)

Baixa as DN de **Rio de Janeiro** 2022/2023 (troca `RJ` por outra UF se quiseres):

```bash
cd ~/health_insights/datasets/originais
wget -c ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/DNRJ2022.dbc
wget -c ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/DNRJ2023.dbc
ls -lh DNRJ20*.dbc
```

---

## 3) Converter .DBC → .CSV (Python)

Cria o script `scripts/dbc_to_csv.py` com o conteúdo abaixo.

```python
# ~/health_insights/scripts/dbc_to_csv.py
from pathlib import Path
import argparse
import pandas as pd
from pysus.preprocessing.dbc import read_dbc


def coerce_date_series(s: pd.Series) -> pd.Series:
    """Tenta converter DTNASC (ou outras datas) para ISO (YYYY-MM-DD) com tolerância."""
    if s.dtype.kind in "iuf":
        s = s.astype("Int64").astype(str).str.zfill(8)
        return pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    try:
        return pd.to_datetime(s, errors="coerce", dayfirst=False, infer_datetime_format=True)
    except Exception:
        return pd.to_datetime(s.astype(str), errors="coerce", dayfirst=False)


def convert_one(in_path: Path, out_dir: Path, halfyear: str | None = None) -> Path:
    print(f"[i] Lendo: {in_path.name}")
    df = read_dbc(str(in_path), encoding="latin-1")
    df.columns = [c.upper() for c in df.columns]
    if "DTNASC" in df.columns:
        df["DTNASC"] = coerce_date_series(df["DTNASC"])  

    if halfyear and "DTNASC" in df.columns:
        if halfyear.upper() == "H1":
            df = df[(df["DTNASC"] >= "2023-01-01") & (df["DTNASC"] <= "2023-06-30")]
        elif halfyear.upper() == "H2":
            df = df[(df["DTNASC"] >= "2023-07-01") & (df["DTNASC"] <= "2023-12-31")]

    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{halfyear.lower()}" if halfyear else ""
    out_path = out_dir / (in_path.stem.lower() + f"{suffix}.csv")

    print(f"[i] A escrever CSV: {out_path.name}")
    df.to_csv(out_path, index=False)

    print(f"[✓] Linhas: {len(df):,}")
    if "DTNASC" in df.columns:
        print(f"    Intervalo DTNASC: {df['DTNASC'].min()} → {df['DTNASC'].max()}")
    return out_path


def main():
    ap = argparse.ArgumentParser(description="Converter DATASUS .DBC (SINASC) para CSV")
    ap.add_argument("--input-dir", type=Path, required=True, help="Pasta com .dbc")
    ap.add_argument("--output-dir", type=Path, required=True, help="Pasta de saída p/ .csv")
    ap.add_argument("--files", nargs="+", default=None, help="Lista de ficheiros .dbc (ex.: DNRJ2022.dbc DNRJ2023.dbc)")
    ap.add_argument("--halfyear", choices=["H1","H2"], default=None, help="Opcional: filtrar apenas H1 ou H2 (assume 2023)")
    args = ap.parse_args()

    in_dir: Path = args.input_dir
    out_dir: Path = args.output_dir
    files = args.files or [p.name for p in sorted(in_dir.glob("DN*.dbc"))]

    if not files:
        raise SystemExit("Nenhum .dbc encontrado. Use --files ou coloque-os na pasta indicada.")

    for fn in files:
        in_path = in_dir / fn
        if not in_path.exists():
            print(f"[!] Ignorado (não encontrado): {fn}")
            continue
        try:
            convert_one(in_path, out_dir, args.halfyear)
        except Exception as e:
            print(f"[x] Falhou {fn}: {e}")


if __name__ == "__main__":
    main()
```

### Executar a conversão

Ativar venv e converter tudo:

```bash
cd ~/health_insights
source venv/bin/activate

# CSV completos (2022 e 2023)
python scripts/dbc_to_csv.py \
  --input-dir datasets/originais \
  --output-dir datasets/csv \
  --files DNRJ2022.dbc DNRJ2023.dbc

# Opcional: apenas 1º semestre (H1) de 2023
python scripts/dbc_to_csv.py \
  --input-dir datasets/originais \
  --output-dir datasets/csv \
  --files DNRJ2023.dbc \
  --halfyear H1

# Verificar
ls -lh datasets/csv/
head -n 3 datasets/csv/dnrj2023.csv
```

### Plano B (fallback) se `pysus/pyreaddbc` falhar

```bash
# Instalar R e pacote read.dbc
sudo apt -y install r-base r-cran-read.dbc

# Converter via R num comando
Rscript -e 'library(read.dbc); write.csv(read.dbc("datasets/originais/DNRJ2023.dbc"), "datasets/csv/dnrj2023.csv", row.names=FALSE)'
```

---

## 4) Snowflake — criação de DB/Schema e ingestão

Abre o **Snowsight** (Worksheet) e executa por blocos:

```sql
-- Setup
USE ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE BOOTCAMP_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE OR REPLACE DATABASE HEALTH_INSIGHTS;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.RAW_STG;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.SILVER;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.MARTS;

USE WAREHOUSE BOOTCAMP_WH;
USE DATABASE HEALTH_INSIGHTS;
USE SCHEMA RAW_STG;

-- Formato CSV
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE=CSV
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER=1
  NULL_IF=('','NULL')
  EMPTY_FIELD_AS_NULL=TRUE;

-- Stage para upload via UI
CREATE OR REPLACE STAGE RAW_STAGE FILE_FORMAT = CSV_FORMAT;
```

**Upload:** Snowsight → Data → Databases → `HEALTH_INSIGHTS` → `RAW_STG` → **Stages** → `RAW_STAGE` → **Upload** → seleciona `datasets/csv/dnrj2023.csv` (e/ou `dnrj2022.csv`).

Criar tabela inferindo o schema do CSV e carregar:

```sql
-- Inferir esquema a partir do stage
CREATE OR REPLACE TABLE RAW_STG.SINASC_RAW
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(LOCATION => '@RAW_STAGE', FILE_FORMAT => 'CSV_FORMAT'))
);

-- Copiar dados
COPY INTO RAW_STG.SINASC_RAW
FROM @RAW_STAGE
PATTERN='.*dnrj.*\.csv';

-- Amostra
SELECT * FROM RAW_STG.SINASC_RAW LIMIT 10;
```

> Observação: as colunas do SINASC variam por ano/região; usaremos staging flexível no dbt.

---

## 5) dbt — projeto mínimo (H1 2023 por defeito)

### `dbt/health_insights/dbt_project.yml`

```yaml
name: 'health_insights'
version: '1.0'
config-version: 2
profile: 'health_insights'

model-paths: ['models']

vars:
  analysis_start: '2023-01-01'
  analysis_end:   '2023-06-30'

models:
  health_insights:
    +materialized: view
    staging:
      +schema: raw_stg
    intermediate:
      +schema: silver
    marts:
      +schema: marts
      +materialized: table
```

### `~/.dbt/profiles.yml` (exemplo)

```yaml
health_insights:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <o_teu_account>
      user: <o_teu_user>
      password: <a_tua_password>
      role: SYSADMIN
      database: HEALTH_INSIGHTS
      warehouse: BOOTCAMP_WH
      schema: RAW_STG
      threads: 4
```

### `models/staging/stg_births.sql`

```sql
{{ config(materialized='view') }}

with src as (
  select * from {{ source('raw_stg','sinasc_raw') }}
),
norm as (
  select
    case
      when try_to_varchar(NUMERODN) is not null then md5(try_to_varchar(NUMERODN))
      else md5(coalesce(try_to_varchar(CODMUNNASC), '') || '-' || coalesce(try_to_varchar(DTNASC), '') || '-' || coalesce(try_to_varchar(SEXO), ''))
    end as sk_birth,

    case
      when length(try_to_varchar(DTNASC)) = 10 then to_date(try_to_varchar(DTNASC))
      when length(try_to_varchar(DTNASC)) = 8  then to_date(try_to_varchar(DTNASC),'YYYYMMDD')
      else try_to_date(try_to_varchar(DTNASC))
    end as birth_date,

    case try_to_varchar(SEXO)
      when '1' then 'M'
      when '2' then 'F'
      else 'U'
    end as sex_newborn,

    try_to_number(PESO)        as birth_weight_g,
    try_to_varchar(GESTACAO)   as gestation_code,
    try_to_varchar(PARTO)      as delivery_type,
    try_to_varchar(CODMUNNASC) as municipality_code
  from src
)
select
  sk_birth, birth_date, sex_newborn, birth_weight_g, gestation_code, delivery_type, municipality_code,
  to_char(birth_date, 'YYYY-MM') as ym
from norm
where birth_date is not null
```

### `models/staging/stg_births.yml`

```yaml
version: 2

sources:
  - name: raw_stg
    database: HEALTH_INSIGHTS
    schema: RAW_STG
    tables:
      - name: SINASC_RAW

models:
  - name: stg_births
    columns:
      - name: sk_birth
        tests: [not_null, unique]
      - name: birth_date
        tests: [not_null]
      - name: sex_newborn
        tests:
          - accepted_values:
              values: ['M','F','U']
```

### `models/intermediate/int_births_enriched.sql`

```sql
{{ config(materialized='view') }}

with s as (
  select * from {{ ref('stg_births') }}
)
select
  s.*,
  case when try_to_number(birth_weight_g) >= 2500 then 0 else 1 end as is_low_weight,
  case when gestation_code in ('1','2','3','4') then 1 else 0 end as is_premature, -- <37 semanas
  case when delivery_type = '2' then 1 else 0 end as is_cesarean
from s
```

### `models/marts/fato_nascimentos.sql`

```sql
{{ config(materialized='table') }}

select
  sk_birth,
  birth_date,
  ym,
  sex_newborn,
  birth_weight_g,
  is_low_weight,
  is_premature,
  is_cesarean,
  municipality_code
from {{ ref('int_births_enriched') }}
where birth_date between to_date('{{ var("analysis_start") }}') and to_date('{{ var("analysis_end") }}')
```

### `models/marts/marts.yml`

```yaml
version: 2
models:
  - name: fato_nascimentos
    description: "Fato de nascimentos (SINASC) filtrado para o 1º semestre de 2023."
    columns:
      - name: sk_birth
        tests: [not_null, unique]
      - name: birth_weight_g
        tests:
          - dbt_utils.expression_is_true:
              expression: "birth_weight_g >= 0"
```

### `models/exposures.yml`

```yaml
version: 2
exposures:
  - name: painel_streamlit_nascimentos
    type: dashboard
    maturity: low
    url: "http://localhost:8501"
    description: "Painel Streamlit com KPIs: n.º nascidos vivos, % prematuros, peso médio, % cesarianas."
    depends_on:
      - ref('fato_nascimentos')
    owner:
      name: "Paula Rodrigues"
      email: "paula@example.com"
```

### Comandos dbt

```bash
# dentro de ~/health_insights/dbt/health_insights
pip install dbt-core dbt-snowflake dbt-extractor

dbt deps
dbt debug
dbt run -s staging+ intermediate+ marts+
dbt test
```

> Se precisares alterar o período, edita `vars.analysis_start`/`analysis_end` no `dbt_project.yml`.

---

## 6) Dashboard (Streamlit)

Cria `app/app.py`:

```python
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
```

Instalar e correr:

```bash
pip install streamlit snowflake-connector-python pandas
# credenciais (exemplo temporário para esta sessão)
export SNOWFLAKE_ACCOUNT="<acc>" \
       SNOWFLAKE_USER="<user>" \
       SNOWFLAKE_PASSWORD="<pass>" \
       SNOWFLAKE_WAREHOUSE="BOOTCAMP_WH" \
       SNOWFLAKE_DATABASE="HEALTH_INSIGHTS" \
       SNOWFLAKE_SCHEMA="MARTS"

streamlit run app/app.py
```

---

## 7) Orquestração (bónus) — Snowflake Task

Recria diariamente a tabela de fato a partir do **intermediate** (views), aplicando o mesmo filtro H1:

```sql
USE DATABASE HEALTH_INSIGHTS;
USE SCHEMA MARTS;

CREATE OR REPLACE TASK TASK_REFRESH_FATO_NASCIMENTOS
  WAREHOUSE = BOOTCAMP_WH
  SCHEDULE = 'USING CRON 0 3 * * * Europe/Lisbon'
AS
CREATE OR REPLACE TABLE FATO_NASCIMENTOS AS
SELECT *
FROM HEALTH_INSIGHTS.SILVER.INT_BIRTHS_ENRICHED
WHERE birth_date BETWEEN '2023-01-01' AND '2023-06-30';

ALTER TASK TASK_REFRESH_FATO_NASCIMENTOS RESUME;
```

---

## 8) Checklist de entrega

* [ ] **CSV gerados** em `datasets/csv/` (prints do terminal com linhas e intervalo de datas)
* [ ] **Snowflake**: DB/SCHEMAS criados, `SINASC_RAW` carregada (print do `SELECT LIMIT 10`)
* [ ] **dbt run + test** com sucesso (prints do terminal)
* [ ] **Dashboard** a mostrar 4 KPIs + 2 gráficos (screenshot)
* [ ] (Bónus) **Task** ativa (screenshot do Snowsight)
* [ ] **README** (este ficheiro) incluído no repositório `health_insights`

---

## 9) Troubleshooting rápido

* **Erro ao instalar `pysus`/`pyreaddbc`**: confirma `build-essential` e `zlib1g-dev`. Tenta reinstalar com `pip install --no-cache-dir --force-reinstall --verbose pysus`. Se persistir, usa o **fallback R** (`read.dbc`) apenas para converter.
* **Schema variando** entre anos: o `stg_births.sql` usa `try_to_*` e `case` para tolerar formatos diferentes.
* **Datas em formato numérico** (`YYYYMMDD`): o `stg_births.sql` já tenta ambos os formatos.
* **Credenciais Snowflake** no Streamlit: usa `export SNOWFLAKE_*` (ou `.env` + `python-dotenv`).

---

## 10) Licença / Notas

Este projeto é apenas demonstrativo/educacional. Os dados do SINASC são públicos e sujeitos às políticas do Ministério da Saúde (Brasil). Ajusta o período/UF conforme a tua necessidade.

---

### 4B) Upload via **SnowSQL (CLI)** — opcional

> Usa esta via se preferires linha de comando em vez do upload no Snowsight.

1. Abre uma sessão autenticada no Snowflake (substitui `<account>` e `<user>`):

```bash
snowsql -a <account> -u <user> -r SYSADMIN -w BOOTCAMP_WH -d HEALTH_INSIGHTS -s RAW_STG
```

2. A partir do `snowsql`, faz **PUT** dos CSV (ajusta o caminho, confirma com `echo $HOME` no WSL):

```sql
-- dentro do snowsql
!source
PUT file:///home/$USER/health_insights/datasets/csv/dnrj2022.csv @RAW_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///home/$USER/health_insights/datasets/csv/dnrj2023.csv @RAW_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

LIST @RAW_STAGE;
```

3. Carrega para a tabela:

```sql
COPY INTO RAW_STG.SINASC_RAW
FROM @RAW_STAGE
PATTERN='.*dnrj202(2|3)\.csv';
```

---

### 4C) Validação rápida pós‑ingestão

```sql
-- Total de linhas carregadas
SELECT COUNT(*) AS rows_loaded FROM RAW_STG.SINASC_RAW;

-- Intervalo de datas (DTNASC)
SELECT MIN(TRY_TO_DATE(DTNASC)) AS d_min,
       MAX(TRY_TO_DATE(DTNASC)) AS d_max
FROM RAW_STG.SINASC_RAW;

-- Distribuição de sexo (códigos originais: 1=M, 2=F, outros)
SELECT SEXO, COUNT(*) AS n
FROM RAW_STG.SINASC_RAW
GROUP BY 1
ORDER BY 1;
```

Se os números fizerem sentido, segue para a secção **5) dbt — projeto mínimo** e executa os comandos indicados (`dbt run` / `dbt test`).

---

### 4D) Usar a **1ª linha do CSV como nomes das colunas** (recomendado)

> Para que a tabela herde **os nomes do header** do CSV e o `COPY` faça o mapeamento por **nome**, usa `PARSE_HEADER=TRUE` e `MATCH_BY_COLUMN_NAME`.

```sql
-- Formato que lê o header como nomes de coluna
CREATE OR REPLACE FILE FORMAT CSV_FORMAT_HEADER
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  PARSE_HEADER = TRUE;   -- Não combinar com SKIP_HEADER

-- Stage (se quiseres apontar explicitamente para este formato)
CREATE OR REPLACE STAGE RAW_STAGE FILE_FORMAT = CSV_FORMAT_HEADER;

-- Criar a tabela com nomes vindos do header via INFER_SCHEMA
CREATE OR REPLACE TABLE RAW_STG.SINASC_RAW
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@RAW_STAGE',
    FILE_FORMAT => 'CSV_FORMAT_HEADER'
  ))
);

-- Carregar os dados casando por NOME de coluna (case-insensitive)
COPY INTO RAW_STG.SINASC_RAW
FROM @RAW_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT_HEADER')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*dnrj202(2|3)\.csv';

-- Verificar
SELECT * FROM RAW_STG.SINASC_RAW LIMIT 5;
```

**Notas importantes**

* `PARSE_HEADER=TRUE` faz com que a 1ª linha do CSV **seja usada como nomes de coluna**.
* Não uses `SKIP_HEADER` em conjunto com `PARSE_HEADER` (são mutuamente exclusivos).
* Se preferires copiar **por posição** (sem mapeamento por nome), cria um `FILE FORMAT` separado com `SKIP_HEADER=1` e remove `MATCH_BY_COLUMN_NAME` do `COPY`.

---

### 4E) Troubleshooting do `COPY INTO` (padrões e mapeamento por nome)

Se o `COPY INTO` não funcionar:

1. **Confirma os ficheiros no stage**

```sql
LIST @RAW_STAGE;
SELECT METADATA$FILENAME FROM @RAW_STAGE;
```

2. **Ajusta o `PATTERN`** ao nome real (ex.: `dnrj2022.csv`, `dnrj2023.csv`):

```sql
COPY INTO RAW_STG.SINASC_RAW
FROM @RAW_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT_HEADER')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN='.*dnrj202(2|3)\.csv';
```

> Se o padrão for `.*sinasc.*\.csv` e os ficheiros chamarem-se `dnrj2023.csv`, **não vai casar**.

3. **Usar header como nomes de coluna**

* Garante que o `FILE FORMAT` tem **`PARSE_HEADER=TRUE`** (não usar `SKIP_HEADER`).
* Recria a tabela com `USING TEMPLATE + INFER_SCHEMA` apontando para esse formato.

```sql
CREATE OR REPLACE FILE FORMAT CSV_FORMAT_HEADER
  TYPE=CSV
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  PARSE_HEADER=TRUE;

CREATE OR REPLACE STAGE RAW_STAGE FILE_FORMAT = CSV_FORMAT_HEADER;

CREATE OR REPLACE TABLE RAW_STG.SINASC_RAW
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@RAW_STAGE',
    FILE_FORMAT => 'CSV_FORMAT_HEADER'
  ))
);
```

4. **Verifica nomes de colunas criados**

```sql
DESCRIBE TABLE RAW_STG.SINASC_RAW;  -- deve mostrar os nomes do header do CSV
SELECT * FROM RAW_STG.SINASC_RAW LIMIT 5;
```

5. **Se ainda falhar**

* Remove o `PATTERN` e referencia o ficheiro diretamente:

```sql
COPY INTO RAW_STG.SINASC_RAW FROM @RAW_STAGE/dnrj2023.csv
  FILE_FORMAT=(FORMAT_NAME='CSV_FORMAT_HEADER')
  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
```

* Ou sobe de novo os ficheiros com nomes simples (sem espaços) e repete os passos 1–4.
