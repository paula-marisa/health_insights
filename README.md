# Health Insights — SINASC (Nascimentos)

**Objetivo** 

Este repositório demonstra o desenho e a implementação de uma pipeline de dados completa para o conjunto de dados SINASC (Sistema de Informações sobre Nascidos Vivos), disponibilizado pelo portal oficial do DataSUS. O objetivo é ingerir, transformar, modelar e expor dados brutos de saúde pública, simulando uma solução de engenharia de dados de ponta a ponta. Todas as etapas foram pensadas para refletir boas práticas de engenharia, desde a coleta até a visualização, passando por automação e versionamento.

## 0) Estrutura de pastas do projeto

O projeto foi organizado para que cada componente da pipeline tenha o seu lugar. A árvore de diretórios abaixo resume a estrutura utilizada:

```bash
health_insights/
├─ datasets/
│  ├─ originais/        # .dbc baixados do DATASUS (ex.: DNRJ2022.dbc, DNRJ2023.dbc)
│  └─ csv/              # CSVs convertidos (camada bronze do data lake)
├─ scripts/
│  └─ converter_dbc_para_csv.py     # script de conversão Python
├─ app/
│  └─ hi_dashboard_3in1.py            # dashboard Streamlit
└─ dbt/health_insights/
   ├─ dbt_project.yml
   ├─ macro/
   │   └─ pick_col.sql
   ├─ seeds/
   │   ├─ refs/
   │   │  └─ ref_municipios.csv
   │   └─ schema.yml
   ├─ models/
   │   ├─ databricks/
   │   │  ├─ check_conn.sql
   │   │  ├─ fed_fato.sql
   │   │  └─ fed_kpis.sql
   │   ├─ staging/
   │   │  ├─ stg_births.sql
   │   │  └─ stg_births.yml
   │   ├─ intermediate/
   │   │  └─ int_births_enriched.sql
   │   ├─ marts/
   │   │   ├─ dim_faixa_etaria_sexo.sql
   │   │   ├─ dim_localidade.sql
   │   │   ├─ dim_mae.sql
   │   │   ├─ dim_recem_nascido.sql
   │   │   ├─ dim_tempo.sql
   │   │   ├─ fato_nascimentos.sql
   │   │   └─ marts.yml
   │   └─ sources.yml
   ├─ dbt_project.yml
   └─ packages.yml
```
> **Nota:** Os diretórios datasets/originais e datasets/csv simulam, respectivamente, a camada de ingestão (landing) e a camada bronze de um data lake. Em ambientes de produção estes ficheiros estariam num armazenamento de objetos (por exemplo S3, Azure Blob ou GCS) e seriam versionados ou particionados de acordo com o período de coleta.

## 1) Coleta e Ingestão de Dados

### Fonte de dados

Os dados utilizados são de acesso público, obtidos no portal oficial do DataSUS na área de Transferência de Arquivos (ftp.datasus.gov.br), especificamente do conjunto SINASC – Sistema de Informações sobre Nascidos Vivos.
O download foi realizado em 13/08/2025 (Europe/Lisbon), contemplando os ficheiros referentes ao estado do Rio de Janeiro (RJ) para os anos de 2022 e 2023.
A escolha do SINASC deve-se à sua relevância para a saúde pública, permitindo análises consistentes sobre nascimentos e perfil materno-infantil em um período recente.

### Simulação de Data Lake / Camada Bronze
Os ficheiros originais no formato .DBC foram descarregados para o diretório datasets/originais/ e convertidos para .CSV utilizando o script Python scripts/converter_dbc_para_csv.py.
Os arquivos convertidos foram armazenados em datasets/csv/, representando a camada bronze da arquitetura.
Em um cenário real, esta camada corresponderia ao armazenamento inicial em um data lake (por exemplo, arquivos Parquet no Amazon S3, Azure Data Lake ou Google Cloud Storage), preservando os dados brutos antes de qualquer transformação.

### Ingestão no Snowflake
Após a conversão, os CSVs foram ingeridos no Snowflake seguindo estes passos:

1. Criação da database, schemas e warehouse dedicados ao projeto.
2. Definição de um file format do tipo CSV com HEADER=TRUE.
3. Criação de um stage (RAW_STAGE) para upload dos CSVs via Snowsight.
4. Inferência automática de schema e criação da tabela RAW_STG.SINASC_RAW com base nos CSVs.
5. Execução do comando COPY INTO para carregar os ficheiros convertidos (ex.: dnrj2022.csv, dnrj2023.csv) para a tabela.

Com isso, o processo de ingestão foi comprovadamente executado na plataforma-alvo, simulando um fluxo real de recolha e armazenamento inicial de dados em ambiente de nuvem.

## 2) Transformação e Modelagem de Dados com dbt

### Objetivo
Transformar os dados brutos do DataSUS em um modelo dimensional no formato **Star Schema**, pronto para análise em saúde pública.  
O processo foi feito integralmente com o **dbt**, organizando as transformações em camadas, criando tabelas fato e dimensões, implementando testes de qualidade e gerando documentação.

---

### Modelagem Dimensional
O modelo foi desenhado em **Star Schema**, com uma tabela fato central (`fato_nascimentos`) conectada a várias dimensões que permitem análises geográficas, temporais, demográficas e clínicas.

**Tabelas criadas:**

| Tabela                  | Tipo      | Descrição resumida |
|-------------------------|-----------|--------------------|
| `fato_nascimentos`      | Fato      | Registra cada nascimento com chaves para as dimensões e medidas como peso do bebê, idade gestacional, tipo de parto, flag de prematuridade, número de consultas pré-natal e indicadores de vitalidade do recém-nascido. |
| `dim_tempo`             | Dimensão  | Calendário com dia, mês, trimestre, ano, dia da semana; permite análises temporais. |
| `dim_localidade`        | Dimensão  | Códigos de município, UF e região; permite análises geográficas. |
| `dim_faixa_etaria_sexo` | Dimensão  | Faixa etária da mãe e sexo do recém-nascido, permitindo cruzar fatores demográficos. |
| `dim_mae`               | Dimensão  | Atributos da mãe: escolaridade, estado civil, raça/cor, número de filhos, entre outros. |
| `dim_recem_nascido`     | Dimensão  | Características do recém-nascido, como sexo, peso ao nascer, Apgar no 1º e 5º minuto, e presença de anomalias congênitas. |

**Chaves:**
- Cada dimensão possui **surrogate key** (chave substituta) gerada na carga.
- A fato referencia essas chaves e inclui métricas numéricas e indicadores binários, como:
  - `peso_ao_nascer`
  - `idade_gestacional`
  - `quantidade_consultas_pre_natal`
  - `flag_parto_cesario`
  - `flag_prematuro`
  - `apgar1`
  - `apgar5`

---

### Estrutura em Camadas no dbt

1. **Staging (`models/staging/`)**
   - Normaliza e padroniza os dados brutos.
   - Ex.: `stg_births.sql` trata diferenças de schema entre anos, padroniza datas (`try_to_date`) e converte códigos para tipos adequados.
   - **Materialização:** `view` (mais rápido e fácil de inspecionar).
   
   **Comando:**
   ```bash
   dbt run -s staging
   ```

2. **Intermediate (`models/intermediate/`)**
   - Enriquece e junta tabelas de staging.
   - Ex.: int_births_enriched.sql une dados de nascimentos com códigos de localidade, atributos da mãe e informações do recém-nascido.
   - **Materialização:** `view` ou `table` dependendo do caso.

   **Comando:**
   ```bash
   dbt run -s intermediate
   ```

3. **Marts (`models/marts/`)**
   - Contém tabelas **fato** e **dimensões** finais.
   - `fato_nascimentos.sql` é **incremental** para otimizar a carga de grandes volumes.
   - Dimensões (`dim_*.sql`) são materializadas como `table` (pequenas e estáveis).

   **Comando:**
   ```bash
   dbt run -s marts
   ```

### Testes de Qualidade no dbt
Foram definidos testes no `schema.yml` e SQL para garantir a integridade dos dados.

Tipos de testes aplicados:
- **Uniqueness:** garante que IDs de dimensões são únicos.
- **Not Null:** campos críticos (datas, município, sexo).
- **Accepted Values:** validação de códigos do DataSUS (ex.: sexo ∈ {1, 2}).
- **Referential Integrity:** chaves estrangeiras da fato devem existir nas dimensões.

**Comando para executar todos os testes:**
   ```bash
   dbt test
   ```

**Comando para executar testes de um modelo específico:**
   ```bash
   dbt test -m fato_nascimentos
   ```

### Documentação
Foi gerada documentação dos modelos usando:

**Comando:**
   ```bash
   dbt docs generate
   dbt docs serve
   ```

Isso permite consultar:
- Descrições das tabelas e colunas
- Relações entre modelos
- Histórico de execuções

A documentação pode ser aberta no navegador, facilitando manutenção e entendimento por toda a equipa.

### Execução Completa
Para rodar toda a pipeline de transformação e modelagem de dados:

# Executa todos os modelos
   ```bash
   dbt run
   ```


# Executa todos os testes
   ```bash
   dbt test
   ```

# Gera e abre documentação
   ```bash
   dbt docs generate
   dbt docs serve
   ```

### Linha de Dependência (Lineage Graph)

A imagem abaixo apresenta o grafo de dependência dos modelos no dbt, evidenciando a sequência de transformações e relações entre tabelas.

![Lineage Graph](dbt\health_insights\images\lineage_graph.png)


## 3) Escolha da Plataforma

O projeto utiliza o Snowflake como data warehouse por várias razões:
1. **Escalabilidade elástica:** Snowflake permite dimensionar computação e armazenamento de forma independente, ajustando o warehouse (BOOTCAMP_WH) às cargas de trabalho.
2. **Time Travel e Zero-Copy Cloning:** funcionalidades que simplificam a gestão de versões e a recuperação de dados, úteis em experimentos com pipelines de saúde.
3. **Integração com dbt:** o conector dbt-snowflake permite compilar, testar e materializar modelos diretamente no Snowflake com mínimo overhead.
4. **Simplicidade de ingestão:** O COPY INTO com FILE FORMAT facilita o carregamento de CSVs, e stages internos eliminam dependências externas.

Embora o Databricks também seja suportado, optou-se por manter a ingestão e a modelagem no Snowflake para reduzir complexidade. Como bónus, a pasta databricks/ contém exemplos de modelos que podem ser executados em um cluster Databricks com Delta Lake, caso se deseje explorar uma arquitetura híbrida.

## 4) Orquestração e Automação

Para manter a pipeline atualizada sem intervenção manual, uma estratégia de orquestração foi delineada. No Snowflake, as Tasks podem agendar execuções de SQL ou de modelos dbt. O exemplo abaixo cria uma task para refresh diário da tabela fato, filtrando apenas nascimentos do primeiro semestre de 2023:

```sql
CREATE OR REPLACE TASK TASK_REFRESH_FATO_NASCIMENTOS
  WAREHOUSE = BOOTCAMP_WH
  SCHEDULE = 'USING CRON 0 3 * * * Europe/Lisbon'
AS
CREATE OR REPLACE TABLE HEALTH_INSIGHTS.MARTS.FATO_NASCIMENTOS AS
SELECT *
FROM HEALTH_INSIGHTS.SILVER.INT_BIRTHS_ENRICHED
WHERE DTNASC BETWEEN '2023-01-01' AND '2023-06-30';

ALTER TASK TASK_REFRESH_FATO_NASCIMENTOS RESUME;
```

Em contextos mais complexos, um orquestrador dedicado como Airflow, Dagster ou o Databricks Workflows poderia disparar a ingestão de novos arquivos FTP, acionar os jobs dbt e atualizar dashboards. O importante é definir a frequência (diária, semanal) e mapear dependências (ex.: ingestão → staging → marts → dashboard).

## 5) Dashboard e Principais Insights

Foi criado um dashboard em Streamlit (ficheiro app/hi_dashboard_3in1.py) que conecta ao Snowflake e apresenta indicadores-chave e gráficos interativos. Na figura seguinte está um exemplo de output com dados fictícios ilustrativos:


Este dashboard demonstra:
- **KPIs principais:** total de nascimentos no semestre, comparação com o ano anterior, taxa de prematuridade, taxa de cesarianas.
- **Gráfico de tendência:** número de nascimentos por mês (H1 2023), permitindo observar sazonalidades.
- **Distribuição de tipos de parto:** proporção entre partos normais, cesáreos e outros.

## 6) Inovação e Diferenciação 

Para além dos requisitos básicos, foram consideradas as seguintes inovações:

- **Mecanismo de alerta epidemiológico simples:** adicionando modelos dbt que calculam variações semanais na taxa de prematuridade ou mortalidade, é possível gerar alertas quando ultrapassam um limiar pré-definido.
- **Integração de novas fontes:** o script de ingestão foi pensado de forma modular para incorporar dados de outras bases do SUS (SIH, SIM) ou de atendimentos de emergência quase em tempo real, enriquecendo a análise de nascimentos com desfechos hospitalares.
- **Discussão sobre anonimização:** embora os dados de SINASC sejam anonimizados, scripts adicionais podem aplicar técnicas de pseudonimização (hashing de IDs, agregação de datas) para mitigar riscos em casos de bases sensíveis.
**GitOps / CI/CD para dbt:** o repositório pode ser conectado a um pipeline CI (GitHub Actions) que execute dbt run e dbt test a cada push, além de publicar a documentação dbt numa página estática.

## 7) Troubleshooting rápido

- Erro ao instalar pysus/pyreaddbc: confirme a instalação de build-essential e zlib1g-dev. Tente pip install --no-cache-dir --force-reinstall --verbose pysus. Como fallback, utilize o R com read.dbc para converter .dbc em .csv.

- Variação de schema entre anos: os modelos stg_births.sql utilizam try_to_* e CASE para tolerar formatos diferentes. Ajuste-os conforme necessário.

- Datas em formato numérico (YYYYMMDD): stg_births.sql converte automaticamente formatos mistos.

- Credenciais Snowflake no Streamlit: configure via variáveis de ambiente SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc., ou use um ficheiro .env com python-dotenv.

## 8) Licença / Notas

Este projeto tem fins educacionais. Os dados do SINASC são públicos e regidos pelas políticas do Ministério da Saúde do Brasil. Ajuste o período e a unidade federativa (UF) conforme a sua necessidade.