# Health Insights — SINASC (Nascimentos)

**Objetivo:** projetar e implementar uma pipeline de dados completa que ingere, transforma e modela esses dados brutos do DataSUS, tornando-os prontos para análise. Vocês devem entregar um projeto que simule uma solução de engenharia de dados real, demonstrando proficiência nas ferramentas aprendidas.

Inclui:
* Download oficial DATASUS (.DBC) e conversão para CSV
* Ingestão no Snowflake
* Modelagem com dbt (staging → intermediate → marts)
* Dashboard Streamlit (KPIs + gráficos)
* Task Snowflake (bónus: orquestração automática)

> Stack alvo: WSL (Ubuntu) + Python 3.10+.

## 0) Estrutura de pastas do projeto

```bash
health_insights/
├─ datasets/
│  ├─ originais/        # .dbc baixados do DATASUS (ex.: DNRJ2022.dbc, DNRJ2023.dbc)
│  └─ csv/              # CSVs convertidos
├─ scripts/
│  └─ converter_dbc_para_csv.py     # script de conversão Python
├─ app/
│  └─ converter_dbc_para_csv.py            # dashboard Streamlit
└─ dbt/health_insights/
   ├─ dbt_project.yml
   └─ models/
      ├─ databricks/
      │  ├─ check_conn.sql
      │  ├─ fed_fato.sql
      │  └─ fed_kpis.sql
      ├─ staging/
      │  ├─ stg_births.sql
      │  └─ stg_births.yml
      ├─ intermediate/
      │  └─ int_births_enriched.sql
      └─ marts/
         ├─ fato_nascimentos.sql
         └─ marts.yml
      └─ sources.yml
```

Cria as pastas:

```bash
mkdir -p ~/health_insights/{datasets/{originais,csv},scripts,app,dbt/health_insights/models/{databricks,staging,intermediate,marts}}
cd ~/health_insights
```

---

## 1) Preparar ambiente WSL + Python

```bash
# Atualizar o sistema e instalar dependências de build
sudo apt update && sudo apt -y upgrade
sudo apt -y install python3-venv python3-dev build-essential \
    zlib1g-dev libffi-dev libssl-dev wget unzip git
sudo apt -y install pigz   # opcional (compressão rápida)
```

Criar ambiente virtual e instalar pacotes Python:

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install pandas pysus dbfread pyarrow
```

> O pysus usa pyreaddbc, que requer as libs acima para compilar.

---

## 2) Download dos datasets do DATASUS (SINASC)

No exemplo, usaremos Rio de Janeiro (troque RJ pela sua UF):

```bash
cd ~/health_insights/datasets/originais
wget -c ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/DNRJ2022.dbc
wget -c ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/DNRJ2023.dbc
ls -lh DNRJ20*.dbc
```

---

## 3) Converter .DBC → .CSV (Python)

Script scripts/converter_dbc_para_csv.py:

```python
# ~/health_insights/scripts/converter_dbc_para_csv.py
import os
from pathlib import Path

# Usar pyreaddbc para converter .dbc -> .dbf (se existir no ambiente)
try:
    from pyreaddbc.readdbc import dbc2dbf
    HAS_PYREADDBC = True
except Exception:
    HAS_PYREADDBC = False

from dbfread import DBF
import pandas as pd

INPUT_DIR = Path("datasets/originais")
OUTPUT_DIR = Path("datasets/csv")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def convert_dbf_to_csv(dbf_path: Path, csv_path: Path):
    # tentar latin-1; se der pau, tentar iso-8859-1
    try:
        table = DBF(str(dbf_path), encoding="latin-1")
        df = pd.DataFrame(iter(table))
    except UnicodeDecodeError:
        table = DBF(str(dbf_path), encoding="iso-8859-1")
        df = pd.DataFrame(iter(table))
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"CSV gerado: {csv_path.name} ({len(df):,} linhas)")

def ensure_dbf_from_dbc(dbc_path: Path) -> Path:
    """Se existir .dbc e não existir o .dbf correspondente, converte usando pyreaddbc."""
    dbf_path = dbc_path.with_suffix(".dbf")
    if dbf_path.exists():
        return dbf_path
    if not HAS_PYREADDBC:
        raise RuntimeError(
            f"Não encontrei pyreaddbc para converter {dbc_path.name}. "
            f"No WSL, corre:  pip install pyreaddbc"
        )
    print(f"Convertendo DBC → DBF: {dbc_path.name} → {dbf_path.name}")
    dbc2dbf(str(dbc_path), str(dbf_path))
    if not dbf_path.exists() or dbf_path.stat().st_size == 0:
        raise RuntimeError(f"Falha ao gerar DBF a partir de {dbc_path.name}")
    return dbf_path

def main():
    # listar ficheiros
    dbf_files = sorted(p for p in INPUT_DIR.glob("*.dbf"))
    dbc_files = sorted(p for p in INPUT_DIR.glob("*.dbc"))

    if not dbf_files and not dbc_files:
        print(f"Nada para processar em {INPUT_DIR.resolve()}")
        return

    # garantir DBF para todos os DBC
    for dbc in dbc_files:
        try:
            ensure_dbf_from_dbc(dbc)
        except Exception as e:
            print(f"Erro a converter {dbc.name}: {e}")

    # 2) (re)carregar lista de DBFs após conversões
    dbf_files = sorted(p for p in INPUT_DIR.glob("*.dbf"))

    total = 0
    for dbf in dbf_files:
        csv_path = OUTPUT_DIR / (dbf.stem + ".csv")
        try:
            print(f"{dbf.name} → {csv_path.name}")
            convert_dbf_to_csv(dbf, csv_path)
            total += 1
        except Exception as e:
            print(f"Erro ao processar {dbf.name}: {e}")

    print(f"\nConcluído. {total} ficheiro(s) convertido(s) para CSV em {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
```

### Executar a conversão

Ativar venv e converter tudo:

```bash
cd ~/health_insights
source venv/bin/activate

# CSV completos (2022 e 2023)
# CSV completos (2022 e 2023)
python scripts/dbc_to_csv.py \
  --input-dir datasets/originais \
  --output-dir datasets/csv \
  --files DNRJ2022.dbc DNRJ2023.dbc

# Apenas 1º semestre (H1) 2023
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

## 4) Ingestão no Snowflake

Abre o **Snowsight** (Worksheet) e executa por blocos:

```sql
-- Criar DB, Schemas e Warehouse
USE ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE BOOTCAMP_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE OR REPLACE DATABASE HEALTH_INSIGHTS;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.RAW_STG;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.SILVER;
CREATE OR REPLACE SCHEMA HEALTH_INSIGHTS.MARTS;

-- Formato CSV com header
CREATE OR REPLACE FILE FORMAT CSV_FORMAT_HEADER
  TYPE=CSV
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  PARSE_HEADER=TRUE;

CREATE OR REPLACE STAGE RAW_STAGE FILE_FORMAT = CSV_FORMAT_HEADER;
```

**Upload via Snowsight:** HEALTH_INSIGHTS → RAW_STG → Stages → RAW_STAGE → Upload CSV.

Criar tabela e carregar:

```sql
CREATE OR REPLACE TABLE RAW_STG.SINASC_RAW
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@RAW_STAGE',
    FILE_FORMAT => 'CSV_FORMAT_HEADER'
  ))
);

COPY INTO RAW_STG.SINASC_RAW
FROM @RAW_STAGE
MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
PATTERN='.*dnrj202(2|3)\.csv';
```

Validação rápida:
```sql
SELECT COUNT(*) FROM RAW_STG.SINASC_RAW;
SELECT MIN(TRY_TO_DATE(DTNASC)), MAX(TRY_TO_DATE(DTNASC)) FROM RAW_STG.SINASC_RAW;
```

---

## 5) dbt — projeto mínimo (H1 2023 por defeito)

Configuração dbt_project.yml, profiles.yml, e modelos (staging, intermediate, marts) já fornecidos acima.

Execução:

```bash
cd dbt/health_insights
pip install dbt-core dbt-snowflake dbt-extractor dbt-utils
dbt deps
dbt debug
dbt run
dbt test
```

## 6) Dashboard (Streamlit)

app/hi_dashboard_3in1.py conforme código fornecido.
Instalação e execução:

```bash
pip install streamlit snowflake-connector-python
export SNOWFLAKE_ACCOUNT=...
streamlit run app/app.py
```

## 7) Orquestração (bónus) — Snowflake Task


```sql
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