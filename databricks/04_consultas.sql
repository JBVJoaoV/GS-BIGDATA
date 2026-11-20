-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas SQL que respondem às perguntas do MVP
-- MAGIC **GS 2026.2 · CitizenTech · Big Data** — requisito 07
-- MAGIC
-- MAGIC Depois de rodar cada uma, escreva no relatório 1 ou 2 frases interpretando o resultado.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Consulta 1 — Quais estados mais queimaram no período?
-- MAGIC Pergunta do MVP: *"Onde o fogo está concentrado?"*

-- COMMAND ----------

SELECT
  u.sigla_uf,
  u.nome_uf,
  count(*)                    AS qtd_focos,
  round(sum(f.frp_mw), 0)     AS frp_total_mw,
  round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_do_brasil
FROM workspace.gs.fato_focos f
JOIN workspace.gs.dim_uf u ON u.sk_uf = f.sk_uf
GROUP BY u.sigla_uf, u.nome_uf
ORDER BY qtd_focos DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Consulta 2 — O fogo está aumentando ou diminuindo?
-- MAGIC Pergunta do MVP: *"Como está a tendência nos últimos dias?"* (média móvel de 3 dias)

-- COMMAND ----------

SELECT
  d.data,
  d.dia_semana,
  count(f.id_foco_hash) AS qtd_focos,
  round(avg(count(f.id_foco_hash)) OVER (ORDER BY d.data ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0)
                        AS media_movel_3d
FROM workspace.gs.dim_data d
LEFT JOIN workspace.gs.fato_focos f ON f.sk_data = d.sk_data
GROUP BY d.data, d.dia_semana
ORDER BY d.data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Consulta 3 — Em que região e em que período do dia o fogo aparece?
-- MAGIC Pergunta do MVP: *"Quando a Defesa Civil deve reforçar o monitoramento?"*

-- COMMAND ----------

SELECT
  u.regiao,
  f.periodo,
  count(*) AS qtd_focos,
  round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY u.regiao), 1) AS pct_na_regiao,
  round(avg(f.frp_mw), 1) AS frp_medio_mw
FROM workspace.gs.fato_focos f
JOIN workspace.gs.dim_uf u ON u.sk_uf = f.sk_uf
GROUP BY u.regiao, f.periodo
ORDER BY u.regiao, f.periodo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Consulta 4 — Tempo seco aumenta o fogo?
-- MAGIC Pergunta do MVP: *"Por que meu estado está em alerta?"* — compara dias secos e úmidos

-- COMMAND ----------

SELECT
  CASE WHEN umidade_min_pct < 30 THEN '1. seco (umidade < 30%)'
       WHEN umidade_min_pct < 50 THEN '2. atenção (30% a 50%)'
       ELSE '3. úmido (≥ 50%)' END        AS condicao_do_ar,
  count(*)                               AS dias_uf_observados,
  round(avg(qtd_focos), 1)               AS media_focos_por_uf_dia,
  round(avg(temp_max_c), 1)              AS temp_max_media_c
FROM workspace.gs.vw_mvp_risco_uf_dia
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Consulta 5 — IMPACTO: quantas pessoas vivem em estados em risco?
-- MAGIC Pergunta do MVP: *"Quantos cidadãos precisam receber o alerta hoje?"*

-- COMMAND ----------

WITH ultimo_dia AS (
  SELECT * FROM workspace.gs.vw_mvp_risco_uf_dia
  WHERE data = (SELECT max(data) FROM workspace.gs.vw_mvp_risco_uf_dia)
)
SELECT
  data,
  nivel_risco,
  count(*)                                     AS qtd_ufs,
  concat_ws(', ', sort_array(collect_list(sigla_uf))) AS ufs,
  sum(populacao)                               AS populacao_exposta,
  round(100.0 * sum(populacao) / sum(sum(populacao)) OVER (), 1) AS pct_populacao_brasil
FROM ultimo_dia
GROUP BY data, nivel_risco
ORDER BY CASE nivel_risco WHEN 'ALTO' THEN 1 WHEN 'MÉDIO' THEN 2 ELSE 3 END;
