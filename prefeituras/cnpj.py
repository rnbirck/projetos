# %%
import pandas as pd
from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")
trad_mun = pd.read_sql_query(
    "SELECT id_municipio, id_municipio_rf, municipio FROM municipio", engine
)
# Data
ano = "2025"
mes = "05"
dia = "12"
arquivo = range(0, 10)
caminho = f"C:/Users/rnbirck/projetos/data/cnpj/{ano}-{mes}/"
caminho_destino = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Emprego e Renda/CNPJ/"
map_situacao = {1: "NULA", 2: "ATIVA", 3: "SUSPENSA", 4: "INAPTA", 8: "BAIXADA"}


def extrair_mei_raw(caminho):
    return (
        pd.read_csv(
            caminho + "Simples.csv",
            sep=";",
            header=None,
            encoding="latin1",
            usecols=[0, 1, 4],
            engine="pyarrow",
        )
        .rename(
            columns={
                0: "cnpj",
                1: "simples",
                2: "mei",
            }
        )
        .assign(
            simples=lambda x: x.simples.map({"N": "Não", "S": "Sim"}),
            mei=lambda x: x.mei.map({"N": "Não", "S": "Sim"}),
        )
    )


def ajuste_cpnj_raw(df_raw, mei):
    return (
        df_raw.rename(
            columns={
                0: "cnpj",
                1: "situacao_cadastral",
                2: "data_situacao_cadastral",
                3: "data_inicio_atividade",
                4: "cod_subclasse",
                5: "uf",
                6: "id_municipio_rf",
            }
        )
        .query("uf == 'RS'")
        .assign(
            situacao_cadastral=lambda x: x.situacao_cadastral.map(map_situacao),
            data_inicio_atividade=lambda x: pd.to_datetime(
                x.data_inicio_atividade, format="%Y%m%d", errors="coerce"
            ),
            data_situacao_cadastral=lambda x: pd.to_datetime(
                x.data_situacao_cadastral, format="%Y%m%d", errors="coerce"
            ),
        )
        .merge(mei, how="left", on="cnpj")
        .query("simples != 'Sim' & mei != 'Sim'")
    )


def ajustar_arquivo_mei(df_raw, mei):
    return (
        df_raw.rename(
            columns={
                0: "cnpj",
                1: "situacao_cadastral",
                2: "data_situacao_cadastral",
                3: "data_inicio_atividade",
                4: "cod_subclasse",
                5: "uf",
                6: "id_municipio_rf",
            }
        )
        .query("uf == 'RS'")
        .assign(
            situacao_cadastral=lambda x: x.situacao_cadastral.map(map_situacao),
            data_inicio_atividade=lambda x: pd.to_datetime(
                x.data_inicio_atividade, format="%Y%m%d", errors="coerce"
            ),
            data_situacao_cadastral=lambda x: pd.to_datetime(
                x.data_situacao_cadastral, format="%Y%m%d", errors="coerce"
            ),
        )
        .merge(mei, how="left", on="cnpj")
        .query("simples == 'Sim' | mei == 'Sim'")
    )


def calcular_empresas_ativas_mensal(df_entrada, trad_municipios_df, ano_str, mes_str):
    """
    Calcula o número de empresas ativas mensalmente por município e subclasse.

    Args:
        df_entrada (pd.DataFrame): DataFrame com os dados das empresas.
                                   Deve conter as colunas: 'data_inicio_atividade',
                                   'data_situacao_cadastral', 'situacao_cadastral',
                                   'id_municipio_rf', 'cod_subclasse'.
        trad_municipios_df (pd.DataFrame): DataFrame para tradução de 'id_municipio_rf'
                                           para 'id_municipio'. Deve conter as colunas:
                                           'id_municipio_rf', 'id_municipio'.
        ano_str (str): Ano final para o período de análise.
        mes_str (str): Mês final para o período de análise.
    Returns:
        pd.DataFrame: DataFrame com a contagem de empresas ativas por período,
                      município e subclasse.
    """
    # Criar um DataFrame com todos os meses desde janeiro de 2021 até o presente
    data_inicio = pd.Timestamp("2021-01-31")
    # Ajuste para garantir que a data_fim seja o último dia do mês fornecido
    data_fim_calc = pd.Timestamp(f"{ano_str}-{mes_str}-01") + pd.offsets.MonthEnd(0)
    meses_ref = pd.date_range(start=data_inicio, end=data_fim_calc, freq="ME")

    # Certificar que as colunas de data estão no formato datetime
    df = df_entrada.copy()  # Trabalhar com uma cópia para evitar SettingWithCopyWarning
    df["data_inicio_atividade"] = pd.to_datetime(
        df["data_inicio_atividade"], errors="coerce"
    )
    df["data_situacao_cadastral"] = pd.to_datetime(
        df["data_situacao_cadastral"], errors="coerce"
    )

    resultados_calc = []

    for data_ref in meses_ref:
        periodo = f"{data_ref.year}_{data_ref.month:02d}"

        mask_inicio = df["data_inicio_atividade"] <= data_ref
        mask_ativas_situacao = (df["situacao_cadastral"] == "ATIVA") & mask_inicio
        mask_outras_situacao = (
            (df["situacao_cadastral"] != "ATIVA")
            & mask_inicio
            & (
                df["data_situacao_cadastral"].isnull()
                | (df["data_situacao_cadastral"] >= data_ref)
            )  # Considera nulos como ainda não baixados/suspensos/inaptos *antes* da data_ref
        )
        mask_empresas_ativas_periodo = mask_ativas_situacao | mask_outras_situacao

        contagem = (
            df[mask_empresas_ativas_periodo]
            .groupby(["id_municipio_rf", "cod_subclasse"])
            .size()
            .reset_index(name="empresas_ativas")  # Nomear a coluna diretamente
        )
        # contagem.columns = ["id_municipio_rf", "cod_subclasse", "empresas_ativas"] # Linha anterior, alternativa acima
        contagem["periodo"] = periodo
        contagem["ano_mes"] = f"{data_ref.year}-{data_ref.month:02d}"
        contagem["ano"] = data_ref.year
        contagem["mes"] = data_ref.month

        resultados_calc.append(contagem)

    resultado_final_mensal = pd.concat(resultados_calc, ignore_index=True)

    resultado_final_mensal = (
        resultado_final_mensal.sort_values(
            ["id_municipio_rf", "cod_subclasse", "periodo"]
        )
        .astype({"id_municipio_rf": str})
        .merge(trad_municipios_df, how="left", on="id_municipio_rf")
        .drop(columns=["id_municipio_rf"])
        .loc[
            :,
            [
                "id_municipio",
                "municipio",
                "cod_subclasse",
                "empresas_ativas",
                "periodo",
                "ano_mes",
                "ano",
                "mes",
            ],
        ]
    )
    return resultado_final_mensal


lista = []
for i in arquivo:
    df_raw = pd.read_csv(
        caminho + f"Estabelecimentos{i}.csv",
        sep=";",
        header=None,
        encoding="latin1",
        usecols=[0, 5, 6, 10, 11, 19, 20],
        engine="pyarrow",
    )
    lista.append(df_raw)

# Arquivo RAW CNPJ
df_raw = pd.concat(lista, ignore_index=True)
mei_raw = extrair_mei_raw(caminho)
cnpj = ajuste_cpnj_raw(df_raw, mei_raw)
mei = ajustar_arquivo_mei(df_raw, mei_raw)
resultado_mensal_cnpj = calcular_empresas_ativas_mensal(cnpj, trad_mun, ano, mes)
resultado_mensal_mei = calcular_empresas_ativas_mensal(mei, trad_mun, ano, mes)

# Salvando os resultados mensais
resultado_mensal_cnpj.to_csv(
    caminho_destino + "cnpj.csv",
    index=False,
)
resultado_mensal_mei.to_csv(
    caminho_destino + "mei.csv",
    index=False,
)
