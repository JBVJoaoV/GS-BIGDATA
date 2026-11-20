# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingestão incremental com Auto Loader
# MAGIC **GS 2026.2 · CitizenTech · Big Data** — requisitos 04 e 05
# MAGIC
# MAGIC Lê os arquivos que o Airflow gravou em `chegada/` (enviados ao Volume em lotes)
# MAGIC e carrega **somente os arquivos novos** nas tabelas Bronze. Rodar de novo não duplica nada:
# MAGIC o checkpoint do Auto Loader guarda quais arquivos já foram lidos.
# MAGIC
# MAGIC | Fonte | Pasta no Volume | Formato | Tabela Bronze |
# MAGIC |---|---|---|---|
# MAGIC | NASA FIRMS | `focos/` | CSV | `bronze_focos` |
# MAGIC | Open-Meteo | `clima/` | JSON | `bronze_clima` |
# MAGIC | IBGE (UFs + população) | `ibge/` | JSON | `bronze_ibge_ufs` |
# MAGIC | IBGE (malha das UFs) | `ibge_malha/` | GeoJSON (binário) | `bronze_ibge_malha` |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Executar uma vez: schema e Volume de pouso
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gs;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("etapa", "lote_1", "Etapa (lote_1 / lote_2 / reexecucao)")
ETAPA = dbutils.widgets.get("etapa")

BASE = "/Volumes/workspace/gs/chegada"
CONTROLE = f"{BASE}/_controle"
TABELAS = ["bronze_focos", "bronze_clima", "bronze_ibge_ufs", "bronze_ibge_malha"]


def contar(rotulo: str):
    """Conta linhas e arquivos distintos de cada tabela Bronze e grava no log."""
    linhas = []
    for t in TABELAS:
        nome = f"workspace.gs.{t}"
        if spark.catalog.tableExists(nome):
            df = spark.table(nome)
            linhas.append((t, df.count(), df.select("arquivo_origem").distinct().count()))
        else:
            linhas.append((t, 0, 0))
    resultado = (
        spark.createDataFrame(linhas, "tabela string, linhas long, arquivos long")
        .withColumn("etapa", F.lit(ETAPA))
        .withColumn("momento", F.lit(rotulo))
        .withColumn("registrado_em", F.current_timestamp())
    )
    resultado.write.mode("append").saveAsTable("workspace.gs.bronze_contagens")
    display(resultado)


def carregar(pasta: str, tabela: str, formato: str, opcoes: dict | None = None):
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{tabela}")
    )
    for k, v in (opcoes or {}).items():
        leitor = leitor.option(k, v)
    df = leitor.load(f"{BASE}/{pasta}")
    if formato == "binaryFile":
        df = df.drop("modificationTime", "length")
    consulta = (
        df.withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{CONTROLE}/checkpoint_{tabela}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)          # processa o que é novo e para
        .toTable(f"workspace.gs.{tabela}")
    )
    consulta.awaitTermination()
    print(f"✔ {tabela} atualizada")

# COMMAND ----------

# MAGIC %md ## Contagem ANTES da carga

# COMMAND ----------

contar("antes")

# COMMAND ----------

# MAGIC %md ## Carga incremental (Auto Loader)

# COMMAND ----------

# Focos: CSV com cabeçalho. Bronze guarda tudo como texto (tipos são tratados na Prata).
carregar("focos", "bronze_focos", "csv", {
    "header": "true",
    "cloudFiles.schemaEvolutionMode": "rescue",
})

# Clima: cada arquivo é uma lista JSON com 27 capitais -> 27 linhas por arquivo
carregar("clima", "bronze_clima", "json", {"multiLine": "true", "cloudFiles.inferColumnTypes": "true"})

# IBGE: um objeto JSON por arquivo (estados + população)
carregar("ibge", "bronze_ibge_ufs", "json", {"multiLine": "true", "cloudFiles.inferColumnTypes": "true"})

# Malha: GeoJSON guardado como binário (as coordenadas têm profundidades diferentes)
carregar("ibge_malha", "bronze_ibge_malha", "binaryFile")

# COMMAND ----------

# MAGIC %md ## Contagem DEPOIS da carga

# COMMAND ----------

contar("depois")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência para o relatório (item 07)
# MAGIC Histórico de todas as contagens: lote 1, lote 2 e a reexecução sem arquivo novo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT etapa, momento, tabela, linhas, arquivos, registrado_em
# MAGIC FROM workspace.gs.bronze_contagens
# MAGIC ORDER BY registrado_em, tabela;
