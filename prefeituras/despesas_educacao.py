# %%
import requests
import pandas as pd
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

trad_mun = pd.read_sql_query(
    "SELECT id_municipio, municipio FROM municipio WHERE sigla_uf = 'RS'", engine
)

cols = [
    "exercicio",
    "periodo",
    "periodicidade",
    "id_municipio",
    "municipio",
    "anexo",
    "coluna",
    "cod_conta",
    "conta",
    "valor",
]


# Função para coletar RREO
def coletar_rreo(
    co_esfera,
    co_tipo_demonstrativo,
    anos,
    periodos,
    trad_mun,
    max_workers=20,
):
    """
    Coleta dados do RREO de forma flexível para diferentes periodicidades.

    Parâmetros:
    - co_tipo_demonstrativo: str (ex: 'RREO' ou 'RREO Simplificado')
    - periodicidade: str ('Q' ou 'S')
    - anos: list[int] (ex: [2021, 2022, 2023])
    - trad_mun: DataFrame com id_municipio e municipio
    - max_workers: int (número de threads para paralelismo)

    Retorna:
    - DataFrame consolidado com os dados
    """

    # Gerar lista de tarefas
    tasks = [
        (row["id_municipio"], ano, periodo)
        for _, row in trad_mun.iterrows()
        for ano in anos
        for periodo in periodos
    ]

    # Função interna para buscar dados
    def fetch_data(task):
        id_ente, ano, periodo = task
        url = (
            f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo?"
            f"an_exercicio={ano}&"
            f"nr_periodo={periodo}&co_tipo_demonstrativo={co_tipo_demonstrativo}&"
            f"co_esfera={co_esfera}&id_ente={id_ente}"
        )

        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get("items"):
                    df = pd.DataFrame(data["items"])
                    df["id_ente"] = id_ente
                    return df
            else:
                print(f"Erro {response.status_code} em {id_ente}-{ano}-{periodo}")
        except Exception as e:
            print(f"Falha em {id_ente}-{ano}-{periodo}: {str(e)}")

        return pd.DataFrame()

    # Executar requisições
    df_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_data, task) for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if not df.empty:
                df_list.append(df)

    # Consolidar e mergear com municípios
    if df_list:
        df_final = pd.concat(df_list, ignore_index=True)
        df_final = df_final.merge(
            trad_mun[["id_municipio", "municipio"]],
            left_on="id_ente",
            right_on="id_municipio",
            how="left",
        )
        return df_final
    else:
        return pd.DataFrame()


df_rreo_simples = coletar_rreo(
    co_esfera="M",
    co_tipo_demonstrativo="RREO Simplificado",
    anos=[2019, 2020, 2021, 2022, 2023, 2024],
    periodos=[6],
    trad_mun=trad_mun,
)

df_rreo_completa = coletar_rreo(
    co_esfera="M",
    co_tipo_demonstrativo="RREO",
    anos=[2019, 2020, 2021, 2022, 2023, 2024],
    periodos=[6],
    trad_mun=trad_mun,
)

# %%


def ajuste_despesas_educacao(df):
    return (
        df.query(
            "conta == 'Educação' & coluna == 'DESPESAS EMPENHADAS ATÉ O BIMESTRE (b)'"
        )
        .groupby(["exercicio", "id_municipio", "municipio"], as_index=False)
        .agg({"valor": "sum"})
        .rename(columns={"exercicio": "ano", "valor": "despesas_educacao"})
    )


rreo_simples = ajuste_despesas_educacao(df_rreo_simples)
rreo_completa = ajuste_despesas_educacao(df_rreo_completa)

despesas_educacao = pd.concat([rreo_simples, rreo_completa], ignore_index=True)
despesas_educacao.to_csv(
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/despesas_educacao/despesas_educacao.csv",
    index=False,
)
