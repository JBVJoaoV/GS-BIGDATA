"""
GS 2026.2 - CitizenTech | Big Data Architecture & Data Integration
DAG de coleta: focos de incêndio (NASA FIRMS), clima (Open-Meteo) e
referência territorial (IBGE) para o Brasil inteiro.

Requisitos atendidos:
  02 - DAG agendado, um arquivo por coleta com a data no nome, em chegada/
  03 - 3 tarefas de coleta em paralelo, 1 tarefa que confere a chegada,
       MAP_KEY em Airflow Variable (nunca no código), notificação

Airflow Variables usadas:
  FIRMS_MAP_KEY        (obrigatória) chave da NASA FIRMS
  CHEGADA_DIR          (opcional)    padrão: /opt/airflow/chegada
  NOTIFY_WEBHOOK_URL   (opcional)    webhook do Discord/Teams/Slack para avisos
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pendulum
import requests
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

TIMEOUT = 60
# Satélites VIIRS (375 m), quase tempo real, em ordem de preferência
FIRMS_FONTES = ["VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT", "VIIRS_SNPP_NRT"]
BBOX_BRASIL = "-74,-34,-34,6"            # oeste,sul,leste,norte

# Capitais das 27 UFs: ponto de referência do clima de cada estado
CAPITAIS = [
    ("AC", "Rio Branco", -9.97, -67.81), ("AL", "Maceió", -9.67, -35.74),
    ("AP", "Macapá", 0.03, -51.07), ("AM", "Manaus", -3.12, -60.02),
    ("BA", "Salvador", -12.97, -38.50), ("CE", "Fortaleza", -3.73, -38.53),
    ("DF", "Brasília", -15.79, -47.88), ("ES", "Vitória", -20.32, -40.34),
    ("GO", "Goiânia", -16.69, -49.26), ("MA", "São Luís", -2.53, -44.30),
    ("MT", "Cuiabá", -15.60, -56.10), ("MS", "Campo Grande", -20.47, -54.62),
    ("MG", "Belo Horizonte", -19.92, -43.94), ("PA", "Belém", -1.46, -48.50),
    ("PB", "João Pessoa", -7.12, -34.86), ("PR", "Curitiba", -25.43, -49.27),
    ("PE", "Recife", -8.05, -34.88), ("PI", "Teresina", -5.09, -42.80),
    ("RJ", "Rio de Janeiro", -22.91, -43.17), ("RN", "Natal", -5.79, -35.21),
    ("RS", "Porto Alegre", -30.03, -51.23), ("RO", "Porto Velho", -8.76, -63.90),
    ("RR", "Boa Vista", 2.82, -60.67), ("SC", "Florianópolis", -27.60, -48.55),
    ("SP", "São Paulo", -23.55, -46.63), ("SE", "Aracaju", -10.91, -37.07),
    ("TO", "Palmas", -10.18, -48.33),
]


# ---------------------------------------------------------------- utilidades
def _chegada() -> Path:
    return Path(Variable.get("CHEGADA_DIR", default="/opt/airflow/chegada"))


def _salvar(subpasta: str, nome: str, conteudo: str) -> Path:
    destino = _chegada() / subpasta / nome
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(conteudo, encoding="utf-8")
    log.info("Gravado %s (%d bytes)", destino, destino.stat().st_size)
    return destino


def _notificar(mensagem: str) -> None:
    """Registra no log e, se houver webhook configurado, envia a mensagem."""
    log.warning("[NOTIFICAÇÃO] %s", mensagem)
    url = Variable.get("NOTIFY_WEBHOOK_URL", default="")
    if not url:
        return
    try:
        # "content" (Discord) e "text" (Slack/Teams) para servir aos três
        requests.post(url, json={"content": mensagem, "text": mensagem}, timeout=15)
    except requests.RequestException as erro:
        log.error("Falha ao enviar notificação: %s", type(erro).__name__)


def avisar_falha(context) -> None:
    ti = context["task_instance"]
    _notificar(f"❌ GS coleta: tarefa {ti.task_id} falhou na data {context['ds']}.")


# ------------------------------------------------------------------- coletas
def coletar_focos(ds: str, **_) -> None:
    """NASA FIRMS: focos de calor no retângulo do Brasil no dia ds (UTC).

    Usa a consulta por área (bbox). Pontos de países vizinhos dentro do
    retângulo são barrados na Prata (regra R5, ponto fora das UFs).
    Tenta os satélites VIIRS em ordem e fica com o primeiro que tiver focos.
    """
    chave = Variable.get("FIRMS_MAP_KEY")
    base = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    for fonte in FIRMS_FONTES:
        url = f"{base}/{chave}/{fonte}/{BBOX_BRASIL}/1/{ds}"
        try:
            r = requests.get(url, timeout=TIMEOUT)
            r.raise_for_status()
        except requests.RequestException as erro:
            # Não loga a URL: ela contém a MAP_KEY
            log.warning("FIRMS %s indisponível (%s)", fonte, type(erro).__name__)
            continue
        linhas = [l for l in r.text.strip().splitlines() if l.strip()]
        if not linhas or "latitude" not in linhas[0]:
            log.warning("FIRMS %s respondeu sem CSV válido: %s", fonte, r.text[:80])
            continue
        if len(linhas) < 2:
            log.warning("FIRMS %s sem focos em %s, tentando outro satélite", fonte, ds)
            continue
        _salvar("focos", f"focos_{ds}.csv", r.text)
        log.info("FIRMS %s: %d focos em %s", fonte, len(linhas) - 1, ds)
        return
    raise RuntimeError(f"FIRMS sem focos para {ds} em nenhum satélite (verifique a MAP_KEY).")


def coletar_clima(ds: str, **_) -> None:
    """Open-Meteo: clima diário das 27 capitais (fatores de risco de fogo)."""
    params = {
        "latitude": ",".join(str(c[2]) for c in CAPITAIS),
        "longitude": ",".join(str(c[3]) for c in CAPITAIS),
        "start_date": ds,
        "end_date": ds,
        "daily": ",".join([
            "temperature_2m_max",
            "relative_humidity_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
        ]),
        "timezone": "America/Sao_Paulo",
    }
    r = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    respostas = r.json()
    registros = [
        {"uf": uf, "capital": capital, "data_coleta": ds, "resposta": resp}
        for (uf, capital, _lat, _lon), resp in zip(CAPITAIS, respostas)
    ]
    _salvar("clima", f"clima_{ds}.json", json.dumps(registros, ensure_ascii=False))


def coletar_ibge(ds: str, **_) -> None:
    """IBGE: UFs, população estimada e malha (polígonos) das UFs."""
    estados = requests.get(
        "https://servicodados.ibge.gov.br/api/v1/localidades/estados", timeout=TIMEOUT
    )
    estados.raise_for_status()
    populacao = requests.get(
        "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1/variaveis/9324",
        params={"localidades": "N3[all]"},
        timeout=TIMEOUT,
    )
    populacao.raise_for_status()
    malha = requests.get(
        "https://servicodados.ibge.gov.br/api/v3/malhas/paises/BR",
        params={"intrarregiao": "UF", "formato": "application/vnd.geo+json", "qualidade": "minima"},
        timeout=TIMEOUT,
    )
    malha.raise_for_status()

    _salvar(
        "ibge",
        f"ibge_ufs_{ds}.json",
        json.dumps(
            {"data_coleta": ds, "estados": estados.json(), "populacao": populacao.json()},
            ensure_ascii=False,
        ),
    )
    _salvar("ibge_malha", f"malha_uf_{ds}.json", malha.text)


# --------------------------------------------------------------- conferência
def conferir_chegada(ds: str, **_) -> None:
    esperados = [
        _chegada() / "focos" / f"focos_{ds}.csv",
        _chegada() / "clima" / f"clima_{ds}.json",
        _chegada() / "ibge" / f"ibge_ufs_{ds}.json",
        _chegada() / "ibge_malha" / f"malha_uf_{ds}.json",
    ]
    faltando = [p.name for p in esperados if not p.exists() or p.stat().st_size == 0]
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios: {faltando}")

    linhas_focos = sum(1 for _ in esperados[0].open(encoding="utf-8")) - 1
    _notificar(f"✅ GS coleta {ds}: 4 arquivos em chegada/, {linhas_focos} focos no Brasil.")


# ----------------------------------------------------------------------- DAG
with DAG(
    dag_id="gs_coleta_queimadas",
    description="Coleta diária de focos de incêndio, clima e IBGE para o Brasil",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=True,               # gera os dias passados (lotes de dias diferentes)
    max_active_runs=2,
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "on_failure_callback": avisar_falha,
    },
    tags=["gs", "citizentech", "queimadas"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    focos = PythonOperator(task_id="coletar_focos_firms", python_callable=coletar_focos)
    clima = PythonOperator(task_id="coletar_clima_openmeteo", python_callable=coletar_clima)
    ibge = PythonOperator(task_id="coletar_ibge", python_callable=coletar_ibge)

    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)

    inicio >> [focos, clima, ibge] >> conferir
