-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Ouro — modelo dimensional, LGPD e contrato com o MVP
-- MAGIC **GS 2026.2 · CitizenTech · Big Data** — requisitos 07 e 08
-- MAGIC
-- MAGIC ```
-- MAGIC              dim_data
-- MAGIC                 │
-- MAGIC  dim_uf ── fato_focos            (1 linha por foco de incêndio)
-- MAGIC     │           │
-- MAGIC     └──── fato_clima_uf_dia      (1 linha por UF e dia)
-- MAGIC                 │
-- MAGIC        vw_mvp_risco_uf_dia  ← o que o MVP consome
-- MAGIC ```
-- MAGIC A Ouro é reconstruída a partir da Prata a cada execução (idempotente).

-- COMMAND ----------

-- MAGIC %md ## Dimensões

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_uf AS
SELECT
  cod_uf          AS sk_uf,
  sigla_uf,
  nome_uf,
  regiao,
  populacao,
  ano_populacao
FROM workspace.gs.prata_uf;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_data AS
WITH datas AS (
  SELECT data FROM workspace.gs.prata_focos
  UNION
  SELECT data FROM workspace.gs.prata_clima
)
SELECT
  CAST(date_format(data, 'yyyyMMdd') AS INT) AS sk_data,
  data,
  year(data)                                 AS ano,
  month(data)                                AS mes,
  day(data)                                  AS dia,
  dayofweek(data)                            AS dia_semana_num,
  CASE dayofweek(data)
    WHEN 1 THEN 'domingo' WHEN 2 THEN 'segunda' WHEN 3 THEN 'terça'
    WHEN 4 THEN 'quarta'  WHEN 5 THEN 'quinta'  WHEN 6 THEN 'sexta'
    ELSE 'sábado' END                        AS dia_semana,
  dayofweek(data) IN (1, 7)                  AS fim_de_semana
FROM datas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fatos
-- MAGIC
-- MAGIC **LGPD (requisito 08).** A localização exata de um foco pode identificar uma propriedade rural
-- MAGIC e, por consequência, seu proprietário: é um dado pessoal **indireto** (LGPD, art. 5º, I).
-- MAGIC Na Ouro, que é a camada consumida pelo MVP e aberta ao cidadão:
-- MAGIC - **Hash:** o identificador do foco é um SHA-256 (`id_foco_hash`), sem coordenada legível;
-- MAGIC - **Máscara por generalização:** latitude e longitude são arredondadas para 0,1° (cerca de 11 km),
-- MAGIC   suficiente para alertar a região sem apontar o imóvel;
-- MAGIC - As coordenadas exatas ficam só na Prata, de acesso restrito à equipe técnica.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.fato_focos AS
SELECT
  sha2(concat(f.id_foco, '|gs2026'), 256)            AS id_foco_hash,
  CAST(date_format(f.data, 'yyyyMMdd') AS INT)       AS sk_data,
  u.sk_uf,
  round(f.latitude, 1)                               AS latitude_aprox,
  round(f.longitude, 1)                              AS longitude_aprox,
  f.frp_mw,
  f.confianca,
  f.periodo,
  f.satelite
FROM workspace.gs.prata_focos f
JOIN workspace.gs.dim_uf u ON u.sigla_uf = f.sigla_uf;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.fato_clima_uf_dia AS
SELECT
  u.sk_uf,
  CAST(date_format(c.data, 'yyyyMMdd') AS INT) AS sk_data,
  c.temp_max_c,
  c.umidade_min_pct,
  c.chuva_mm,
  c.vento_max_kmh
FROM workspace.gs.prata_clima c
JOIN workspace.gs.dim_uf u ON u.sigla_uf = c.sigla_uf;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contrato com o MVP
-- MAGIC A tela do cidadão consome **somente** esta view: uma linha por UF e dia, com o nível de risco.
-- MAGIC
-- MAGIC Regra do nível de risco (versão inicial, ajustável):
-- MAGIC - **ALTO:** 50 ou mais focos no dia, ou 10 ou mais focos com umidade abaixo de 30% e sem chuva
-- MAGIC - **MÉDIO:** 10 ou mais focos, ou umidade abaixo de 30%
-- MAGIC - **BAIXO:** demais casos

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gs.vw_mvp_risco_uf_dia AS
WITH focos_dia AS (
  SELECT sk_uf, sk_data, count(*) AS qtd_focos, round(sum(frp_mw), 1) AS frp_total_mw
  FROM workspace.gs.fato_focos
  GROUP BY sk_uf, sk_data
)
SELECT
  d.data,
  u.sigla_uf,
  u.nome_uf,
  u.regiao,
  u.populacao,
  coalesce(f.qtd_focos, 0)                                         AS qtd_focos,
  coalesce(f.frp_total_mw, 0)                                      AS frp_total_mw,
  round(coalesce(f.qtd_focos, 0) * 100000.0 / u.populacao, 2)     AS focos_por_100mil_hab,
  c.temp_max_c,
  c.umidade_min_pct,
  c.chuva_mm,
  c.vento_max_kmh,
  CASE
    WHEN coalesce(f.qtd_focos, 0) >= 50
      OR (coalesce(f.qtd_focos, 0) >= 10 AND c.umidade_min_pct < 30 AND c.chuva_mm < 1) THEN 'ALTO'
    WHEN coalesce(f.qtd_focos, 0) >= 10 OR c.umidade_min_pct < 30                       THEN 'MÉDIO'
    ELSE 'BAIXO'
  END                                                              AS nivel_risco
FROM workspace.gs.fato_clima_uf_dia c
JOIN workspace.gs.dim_uf   u ON u.sk_uf   = c.sk_uf
JOIN workspace.gs.dim_data d ON d.sk_data = c.sk_data
LEFT JOIN focos_dia        f ON f.sk_uf   = c.sk_uf AND f.sk_data = c.sk_data;

-- COMMAND ----------

-- Conferência: o que o MVP mostra no dia mais recente
SELECT data, sigla_uf, nome_uf, qtd_focos, umidade_min_pct, chuva_mm, nivel_risco
FROM workspace.gs.vw_mvp_risco_uf_dia
WHERE data = (SELECT max(data) FROM workspace.gs.vw_mvp_risco_uf_dia)
ORDER BY qtd_focos DESC;
