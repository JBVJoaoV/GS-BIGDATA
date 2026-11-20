# GS 2026.2 · CitizenTech · Big Data Architecture & Data Integration
## Alerta de Incêndios para a Defesa Civil — componente Dados

**Integrantes**
| Nome completo | RM |
|---|---|
| João Vieira | RM99805 |
| Lucas Speranzini | RM98297 |

**O produto em uma frase:** avisar o cidadão e a Defesa Civil, por estado e por dia, onde o risco de incêndio está alto, combinando focos de calor por satélite, clima e população.

**Perguntas que o MVP responde**
1. Onde o fogo está concentrado? (consulta 1)
2. O fogo está aumentando ou diminuindo? (consulta 2)
3. Em que região e período do dia o fogo aparece? (consulta 3)
4. Tempo seco aumenta o fogo? (consulta 4)
5. Quantos cidadãos precisam receber o alerta hoje? (consulta 5 — impacto)

---

## Arquitetura

```
NASA FIRMS (API, chave) ─┐
Open-Meteo (API aberta) ─┼─> Airflow 3 (DAG diário) ─> chegada/ ─> upload em lotes ─> Volume Databricks
IBGE (API aberta) ───────┘        paralelo + conferência                                      │
                                                                                               v
     MVP <── vw_mvp_risco_uf_dia <── Ouro (fato + dimensões) <── Prata (MERGE + qualidade) <── Bronze (Auto Loader)
```

| Fonte | Origem | Formato | Frequência |
|---|---|---|---|
| Focos de calor | NASA FIRMS — API `area/csv`, satélites VIIRS, retângulo do Brasil | CSV | Diária |
| Clima das 27 capitais | Open-Meteo — `/v1/forecast` (temp. máx., umidade mín., chuva, vento) | JSON | Diária |
| UFs e população | IBGE — `localidades/estados` + agregado 6579 (estimativa de população) | JSON | Diária |
| Malha das UFs | IBGE — `malhas/paises/BR?intrarregiao=UF` | GeoJSON | Diária |

## Estrutura do repositório
```
dags/         gs_coleta_queimadas.py — DAG do Airflow
databricks/   01_bronze.py · 02_prata.py · 03_ouro.sql · 04_consultas.sql
amostras/     um pouco de cada fonte
README.md
```

## Como reproduzir

### 1. Airflow (requisitos 01–03)
1. Docker Desktop aberto. Na pasta do projeto:
   ```powershell
   curl.exe -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
   mkdir dags, logs, plugins, config, chegada
   ```
2. No `docker-compose.yaml`, em `x-airflow-common → volumes`, adicionar:
   `- ${AIRFLOW_PROJ_DIR:-.}/chegada:/opt/airflow/chegada`
3. Criar `.env` com `AIRFLOW_UID=50000` e `FERNET_KEY=<chave gerada>` (fora do Git).
4. `docker compose up airflow-init` e `docker compose up -d`; abrir http://localhost:8080.
5. **Admin → Variables:** `FIRMS_MAP_KEY` = chave gratuita da NASA FIRMS (nunca no código).
6. Ativar o DAG `gs_coleta_queimadas` (`@daily`, `catchup=True` desde 01/11/2026).
   Fluxo: `inicio → [coletar_focos_firms, coletar_clima_openmeteo, coletar_ibge] → conferir_chegada`.
   Falhas disparam notificação (log e webhook opcional `NOTIFY_WEBHOOK_URL`).

### 2. Databricks Free Edition (requisitos 04–08)
1. Importar os notebooks de `databricks/` e conectar em **Serverless**.
2. `01_bronze`: rodar a 1ª célula (cria `workspace.gs` e o Volume `chegada`).
3. **Lote 1:** enviar pela interface os arquivos de 01–09/11 para `chegada/{focos,clima,ibge,ibge_malha}`; rodar com etapa `lote_1`.
4. **Lote 2:** enviar 10–19/11; rodar com etapa `lote_2`. Depois rodar com `reexecucao` sem enviar nada.
5. Rodar `02_prata`, `03_ouro` e `04_consultas`.

## Resultados

**Bronze (Auto Loader, incremental e idempotente)**
| Etapa | Focos | Arquivos |
|---|---|---|
| lote_1 | 0 → 69.181 | 0 → 9 |
| lote_2 | 69.181 → 122.835 | 9 → 19 |
| reexecucao | 122.835 → 122.835 | 19 → 19 |

**Prata — regras de qualidade (total Bronze: 122.835)**
| Regra | Barrados |
|---|---|
| R1 data, hora ou coordenadas inválidas | 0 |
| R2 FRP nulo ou ≤ 0 | 5 |
| R3 confiança baixa | 7.627 |
| R4 duplicado | 0 |
| R5 fora do Brasil (ponto fora das UFs do IBGE) | 36.852 |
| **Focos válidos (MERGE por `id_foco`)** | **78.351** |

**Ouro — modelo dimensional:** `dim_uf`, `dim_data`, `fato_focos` (1 linha por foco), `fato_clima_uf_dia` e a view `vw_mvp_risco_uf_dia`.

**Consulta de impacto (19/11/2026):** 16,5 milhões de pessoas (7,7%) vivem nos 3 estados em risco ALTO (MA, PA, RR) e 19,6 milhões (9,2%) em 5 estados em risco MÉDIO (AM, AP, CE, PI, RO).

## LGPD
A localização exata de um foco pode identificar um imóvel rural e seu proprietário (dado pessoal indireto, LGPD art. 5º, I). Na Ouro:
- **Hash:** `id_foco_hash` = SHA-256, sem coordenada legível;
- **Máscara por generalização:** latitude e longitude arredondadas para 0,1° (~11 km);
- Coordenadas exatas só na Prata, de acesso restrito. Chaves de API ficam em Airflow Variables; `.env`, `config/`, `logs/` e `chegada/` estão no `.gitignore`.

## Contrato com o MVP
O MVP consome somente `workspace.gs.vw_mvp_risco_uf_dia`: uma linha por UF e dia com `qtd_focos`, `frp_total_mw`, `focos_por_100mil_hab`, clima e `nivel_risco` (ALTO / MÉDIO / BAIXO).

## Conclusão técnica
O pipeline coleta diariamente três fontes reais no Airflow, carrega o lakehouse de forma incremental e sem duplicação, barra 36% dos registros brutos por regras de qualidade (principalmente focos de países vizinhos) e entrega ao MVP um indicador diário de risco por estado, com a população exposta como medida de impacto.
