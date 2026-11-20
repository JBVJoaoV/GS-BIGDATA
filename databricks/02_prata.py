# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — tipos, limpeza, regras de qualidade e MERGE
# MAGIC **GS 2026.2 · CitizenTech · Big Data** — requisito 06
# MAGIC
# MAGIC - Tipos corretos (double, date, timestamp, int)
# MAGIC - Nulos e inválidos tratados, duplicados removidos
# MAGIC - **MERGE pela chave** (rodar de novo não duplica)
# MAGIC - **Regras de qualidade com o total barrado** por cada uma, gravado em `prata_qualidade_log`
# MAGIC - Cada foco recebe a **UF** pelo ponto dentro do polígono do estado (malha do IBGE)

# COMMAND ----------

# MAGIC %pip install shapely --quiet

# COMMAND ----------

import json
from pyspark.sql import functions as F, Window, types as T
from shapely.geometry import shape, Point

DB = "workspace.gs"
execucao = spark.sql("SELECT current_timestamp() AS t").first().t
log_qualidade = []


def barrar(df, condicao, regra: str, descricao: str):
    """Separa as linhas que violam a regra, grava as rejeitadas e conta."""
    condicao = F.coalesce(condicao, F.lit(False))   # nulo na regra = não barra
    rejeitadas = df.filter(condicao)
    qtd = rejeitadas.count()
    log_qualidade.append((regra, descricao, qtd))
    if qtd:
        (rejeitadas.withColumn("regra", F.lit(regra))
         .withColumn("barrado_em", F.lit(execucao))
         .write.mode("append").option("mergeSchema", "true")
         .saveAsTable(f"{DB}.prata_focos_rejeitados"))
    return df.filter(~condicao)


def merge(df, tabela: str, chave: str):
    df.createOrReplaceTempView("lote_novo")
    if not spark.catalog.tableExists(f"{DB}.{tabela}"):
        df.limit(0).write.saveAsTable(f"{DB}.{tabela}")
    spark.sql(f"""
        MERGE INTO {DB}.{tabela} AS alvo
        USING lote_novo AS novo
        ON {chave}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✔ MERGE em {tabela}: {spark.table(f'{DB}.{tabela}').count()} linhas")

# COMMAND ----------

# MAGIC %md ## 1. UFs (IBGE): estados + população

# COMMAND ----------

ultimo_ibge = spark.table(f"{DB}.bronze_ibge_ufs").orderBy(F.desc("data_coleta")).limit(1)

estados = (ultimo_ibge
    .select(F.explode("estados").alias("e"))
    .select(
        F.col("e.id").cast("int").alias("cod_uf"),
        F.col("e.sigla").alias("sigla_uf"),
        F.col("e.nome").alias("nome_uf"),
        F.col("e.regiao.nome").alias("regiao"),
    ))

# A série do IBGE vem como {"2025": "46081801"}: pega o ano mais recente
populacao = (ultimo_ibge
    .select(F.explode("populacao").alias("v"))
    .select(F.explode("v.resultados").alias("r"))
    .select(F.explode("r.series").alias("s"))
    .select(
        F.col("s.localidade.id").cast("int").alias("cod_uf"),
        F.from_json(F.to_json("s.serie"), "map<string,string>").alias("serie"),
    )
    .select(
        "cod_uf",
        F.array_max(F.map_keys("serie")).cast("int").alias("ano_populacao"),
        F.col("serie")[F.array_max(F.map_keys("serie"))].cast("long").alias("populacao"),
    ))

prata_uf = estados.join(populacao, "cod_uf", "left").withColumn("atualizado_em", F.lit(execucao))
merge(prata_uf, "prata_uf", "alvo.cod_uf = novo.cod_uf")
display(spark.table(f"{DB}.prata_uf").orderBy("sigla_uf"))

# COMMAND ----------

# MAGIC %md ## 2. Clima (Open-Meteo): listas paralelas → uma linha por UF e dia

# COMMAND ----------

d = "resposta.daily"
clima = (spark.table(f"{DB}.bronze_clima")
    .select(
        "uf", "capital",
        F.explode(F.arrays_zip(
            F.col(f"{d}.time").alias("data"),
            F.col(f"{d}.temperature_2m_max").alias("temp_max_c"),
            F.col(f"{d}.relative_humidity_2m_min").alias("umidade_min_pct"),
            F.col(f"{d}.precipitation_sum").alias("chuva_mm"),
            F.col(f"{d}.wind_speed_10m_max").alias("vento_max_kmh"),
        )).alias("z"))
    .select(
        F.col("uf").alias("sigla_uf"), "capital",
        F.to_date("z.data").alias("data"),
        F.col("z.temp_max_c").cast("double").alias("temp_max_c"),
        F.col("z.umidade_min_pct").cast("double").alias("umidade_min_pct"),
        F.col("z.chuva_mm").cast("double").alias("chuva_mm"),
        F.col("z.vento_max_kmh").cast("double").alias("vento_max_kmh"),
    )
    .filter(F.col("data").isNotNull() & F.col("temp_max_c").isNotNull())
    .dropDuplicates(["sigla_uf", "data"])
    .withColumn("atualizado_em", F.lit(execucao)))

merge(clima, "prata_clima", "alvo.sigla_uf = novo.sigla_uf AND alvo.data = novo.data")

# COMMAND ----------

# MAGIC %md ## 3. Focos (NASA FIRMS): tipos, regras de qualidade, UF e MERGE

# COMMAND ----------

bruto = spark.table(f"{DB}.bronze_focos")
total_bronze = bruto.count()

focos = (bruto
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("frp_mw", F.col("frp").cast("double"))
    .withColumn("brilho_k", F.col("bright_ti4").cast("double"))
    .withColumn("data", F.to_date("acq_date"))
    .withColumn("hora_utc", F.lpad(F.col("acq_time").cast("string"), 4, "0"))
    .withColumn("detectado_em_utc",
                F.to_timestamp(F.concat_ws(" ", "acq_date", "hora_utc"), "yyyy-MM-dd HHmm"))
    .withColumn("confianca", F.when(F.col("confidence") == "h", "alta")
                              .when(F.col("confidence") == "n", "nominal")
                              .when(F.col("confidence") == "l", "baixa")
                              .otherwise(F.col("confidence")))
    .withColumn("periodo", F.when(F.col("daynight") == "D", "dia").otherwise("noite"))
    .withColumn("satelite", F.col("satellite"))
    # Chave natural do foco: mesmo ponto, mesmo instante, mesmo satélite
    .withColumn("id_foco", F.sha2(F.concat_ws("|", "latitude", "longitude", "acq_date",
                                              "hora_utc", "satellite"), 256))
    .select("id_foco", "data", "detectado_em_utc", "latitude", "longitude", "frp_mw",
            "brilho_k", "confianca", "periodo", "satelite", "arquivo_origem"))

# Regra 1: data/hora ou coordenadas ausentes ou fora do intervalo válido
focos = barrar(focos,
    F.col("data").isNull() | F.col("detectado_em_utc").isNull()
    | F.col("latitude").isNull() | F.col("longitude").isNull()
    | ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180),
    "R1_campos_invalidos", "Data, hora ou coordenadas nulas ou inválidas")

# Regra 2: potência radiativa (FRP) ausente ou não positiva
focos = barrar(focos, F.col("frp_mw").isNull() | (F.col("frp_mw") <= 0),
    "R2_frp_invalido", "FRP nulo ou menor/igual a zero")

# Regra 3: detecção de baixa confiança (provável falso positivo)
focos = barrar(focos, F.col("confianca") == "baixa",
    "R3_baixa_confianca", "Detecção com confiança baixa")

# Regra 4: duplicados dentro da carga (mesmo id_foco)
antes = focos.count()
focos = focos.dropDuplicates(["id_foco"])
log_qualidade.append(("R4_duplicado", "Mesmo foco repetido na carga", antes - focos.count()))

# COMMAND ----------

# Atribui a UF: ponto dentro do polígono do estado (malha IBGE)
malha_bin = (spark.table(f"{DB}.bronze_ibge_malha")
             .orderBy(F.desc("arquivo_origem")).select("content").first().content)
geo = json.loads(bytes(malha_bin).decode("utf-8"))
cod_para_sigla = {r.cod_uf: r.sigla_uf for r in spark.table(f"{DB}.prata_uf").collect()}
poligonos = [(cod_para_sigla.get(int(f["properties"]["codarea"])), shape(f["geometry"]))
             for f in geo["features"]]
print(f"{len(poligonos)} polígonos de UF carregados")


@F.udf(T.StringType())
def uf_do_ponto(lat, lon):
    p = Point(lon, lat)
    for sigla, poli in poligonos:
        if poli.contains(p):
            return sigla
    return None


focos = focos.withColumn("sigla_uf", uf_do_ponto("latitude", "longitude"))

# Regra 5: foco fora do território brasileiro (país vizinho ou oceano)
focos = barrar(focos, F.col("sigla_uf").isNull(),
    "R5_fora_do_brasil", "Coordenada não cai em nenhuma UF")

focos = focos.withColumn("atualizado_em", F.lit(execucao))
merge(focos, "prata_focos", "alvo.id_foco = novo.id_foco")

# COMMAND ----------

# MAGIC %md ## 4. Resultado das regras de qualidade

# COMMAND ----------

(spark.createDataFrame(log_qualidade, "regra string, descricao string, barrados long")
    .withColumn("total_bronze", F.lit(total_bronze))
    .withColumn("executado_em", F.lit(execucao))
    .write.mode("append").saveAsTable(f"{DB}.prata_qualidade_log"))

display(spark.sql(f"""
    SELECT regra, descricao, barrados, total_bronze
    FROM {DB}.prata_qualidade_log
    WHERE executado_em = (SELECT max(executado_em) FROM {DB}.prata_qualidade_log)
    ORDER BY regra
"""))
print(f"Focos válidos na Prata: {spark.table(f'{DB}.prata_focos').count()}")
