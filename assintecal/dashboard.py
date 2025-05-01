# %%
import pandas as pd
import sidrapy as sidra
from sqlalchemy import create_engine
from bcb import Expectativas
import time

print("Iniciando o processamento...")
start_time = time.time()
# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")
expect = Expectativas()
ep = expect.get_endpoint("ExpectativasMercadoAnuais")


def funcao_tradutores(
    arquivo, sheet=None, col_to_str=None, col_z_fill=None, n_z_fill=None
):
    """
    Função para ler um arquivo Excel e converter colunas específicas para string ou preencher com zeros à esquerda.

    Parâmetros:
    arquivo (str): Caminho do arquivo Excel.
    sheet (str, opcional): Nome da planilha a ser lida. Se None, lê a primeira planilha.
    col_to_str (list, opcional): Lista de colunas a serem convertidas para string.
    col_z_fill (list, opcional): Lista de colunas a serem preenchidas com zeros à esquerda.
    n_z_fill (int, opcional): Número de zeros à esquerda a serem adicionados.

    Retorna:
    pd.DataFrame: DataFrame resultante após as operações.
    """
    # Lê o arquivo Excel
    if sheet is None:
        df = pd.read_excel(f"data/{arquivo}.xlsx", engine="calamine")
    else:
        df = pd.read_excel(f"data/{arquivo}.xlsx", engine="calamine", sheet_name=sheet)

    # Converte colunas para string
    if col_to_str is not None:
        for col in col_to_str:
            df[col] = df[col].astype(str)

    # Preenche com zeros à esquerda
    if col_z_fill is not None:
        for col in col_z_fill:
            df[col] = df[col].str.zfill(n_z_fill)
    return df


def leitor_csv(arquivo):
    columns = [
        "CO_ANO",
        "CO_MES",
        "CO_NCM",
        "CO_PAIS",
        "SG_UF_NCM",
        "QT_ESTAT",
        "VL_FOB",
    ]

    df = pd.read_csv(
        f"data/{arquivo}.csv",
        sep=";",
        encoding="ISO-8859-1",
        engine="pyarrow",
        usecols=columns,
    )
    return df


def ajuste_sh6(df, ano):
    df = (
        df.query("CO_ANO in @anos")
        .groupby(by=["CO_ANO", "CO_MES", "CO_NCM"])
        .agg({"QT_ESTAT": "sum", "VL_FOB": "sum"})
        .reset_index()
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str),
        )
        .merge(tradutor_sh6, how="left", on="CO_NCM")
        .merge(tradutor_couro, how="left", on="CO_NCM")
        .merge(tradutor_calcado, how="left", on="CO_SH6")
    )
    return df


def ajuste_comex_tipo(df, tipo, fluxo):
    df = (
        df.dropna(subset=[f"tipo_{tipo}"])
        .groupby(by=[f"tipo_{tipo}", "CO_ANO", "CO_MES"])
        .agg({"QT_ESTAT": "sum", "VL_FOB": "sum"})
        .reset_index()
        .assign(
            FLUXO=fluxo,
            SEGMENTO=str(tipo).upper(),
        )
        # .drop(columns=[f"tipo_{tipo}"])
    )
    return df


def ajuste_comex_setor(df, fluxo, tipo, tradutor):
    df = (
        df.query("CO_ANO in @anos")
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str),
            CO_PAIS=lambda x: x["CO_PAIS"].astype(str),
        )
        .merge(tradutor_sh6, how="left", on="CO_NCM")
        .groupby(by=["CO_ANO", "CO_MES", "CO_SH6", "CO_PAIS"])
        .agg({"VL_FOB": "sum"})
        .reset_index()
        .merge(tradutor, how="left", left_on="CO_SH6", right_on="id_sh6")
        .merge(tradutor_pais, how="left", left_on="CO_PAIS", right_on="id_pais")
        .merge(tradutor_sh6_desc, how="left", on="CO_SH6")
        .assign(
            FLUXO=fluxo,
        )
        .dropna(subset=[f"{tipo}"])
        .drop(columns=["id_sh6", "CO_PAIS"])
    )
    return df


def ajuste_sidra(
    df,
    lista_colunas: list,
    rename_colunas: dict,
    dicionario_taxas: dict,
    dicionario_grupo: dict,
):
    df = (
        df.loc[1:, lista_colunas]
        .rename(columns=rename_colunas)
        .assign(
            variavel=lambda x: x["variavel"].replace(dicionario_taxas),
            grupo=lambda x: x["grupo"].replace(dicionario_grupo),
            date=lambda x: pd.to_datetime(x["date"], format="%Y%m").dt.strftime(
                "%m/%Y"
            ),
            valor=lambda x: x["valor"].astype(float).round(2),
        )
        .pivot_table(index=["date", "grupo"], columns="variavel", values="valor")
        .reset_index()
    )
    return df


def ajuste_ipca(df, grupo, sufixo=""):
    df = (
        df.query("grupo == @grupo")
        .rename(
            columns={
                "taxa_mensal": f"ipca_mes{sufixo}",
                "taxa_doze_meses": f"ipca_12_meses{sufixo}",
            }
        )
        .drop(columns=["grupo"])
    )
    return df


def get_data_bcb(url, indicador):
    return (
        pd.read_json(url)
        .rename(columns={"valor": indicador})
        .assign(
            data=lambda x: pd.to_datetime(x.data, format="%d/%m/%Y"),
            mes_ano=lambda x: x.data.dt.strftime("%m/%Y"),
            ano=lambda x: x.data.dt.year,
            soma_acumulado_ano=lambda x: x.groupby("ano")[indicador].cumsum(),
            **{
                f"{indicador}_mensal": lambda x: (
                    x[indicador] / x[indicador].shift(12) - 1
                )
                * 100,
                f"{indicador}_acumulado": lambda x: (
                    x["soma_acumulado_ano"] / x["soma_acumulado_ano"].shift(12) - 1
                )
                * 100,
                f"{indicador}_mes_anterior": lambda x: (
                    x[indicador] / x[indicador].shift(1) - 1
                )
                * 100,
            },
        )
        .drop(columns=["data", "ano", "soma_acumulado_ano"])
    )


def expectativas_mercado(ano, indicador):
    nome_indicador = None  # Inicializa a variável (opcional, mas boa prática)
    if indicador == "PIB Total":
        nome_indicador = f"expectativa_pib_{str(ano)[-2:]}"
    elif indicador == "IPCA":
        nome_indicador = f"expectativa_ipca_{str(ano)[-2:]}"
    df = (
        ep.query()
        .filter(ep.Indicador == indicador, ep.DataReferencia == ano)
        .filter(ep.Data >= "2023-01-01")
        .filter(ep.baseCalculo == 0)
        .select(ep.Indicador, ep.baseCalculo, ep.Data, ep.Mediana, ep.DataReferencia)
        .collect()
    )
    df = (
        df.assign(
            Data=lambda x: pd.to_datetime(x["Data"]),
            mes_ano=lambda x: x["Data"].dt.strftime("%m/%Y"),
        )
        .sort_values(by=["Data"])
        .groupby(pd.Grouper(key="Data", freq="ME"))
        .last()
        .reset_index()[["mes_ano", "Mediana"]]
        .rename(columns={"Mediana": nome_indicador})
        .assign(
            mes=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.month,
            ano=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.year,
        )
    )
    return df


# Querys SQL para emprego
query_emprego_couro = """
SELECT a.ano, a.mes, a.cnae_2_subclasse, b.subclasse, SUM(a.saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae AS a
JOIN cei.cnae AS b ON a.cnae_2_subclasse = b.cod_subclasse
WHERE a.cnae_2_subclasse = '1510600' AND a.ano BETWEEN 2021 AND 2025
GROUP BY a.ano, a.mes, a.cnae_2_subclasse, b.subclasse
"""

query_emprego_calcado = """
SELECT a.ano, a.mes, a.cnae_2_subclasse, b.subclasse, SUM(a.saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae AS a
JOIN cei.cnae AS b ON a.cnae_2_subclasse = b.cod_subclasse
WHERE a.cnae_2_subclasse IN (
'1531902', 
'1531901', 
'1539400', 
'1533500', 
'1532700', 
'1540800'
) AND a.ano BETWEEN 2021 AND 2025
GROUP BY a.ano, a.mes, a.cnae_2_subclasse, b.subclasse
"""

# URLS
url_ibc = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24364/dados?formato=json&dataInicial=01/01/2021"
url_cambio = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.3698/dados?formato=json&dataInicial=01/01/2021"

### TRADUTORES ###
tradutor_sh6 = funcao_tradutores(
    arquivo="tradutor_sh6", col_to_str=["CO_NCM", "CO_SH6"]
)

tradutor_couro = funcao_tradutores(
    arquivo="tradutor_tipo", sheet="tradutor_couro", col_to_str=["CO_NCM"]
)

tradutor_calcado = funcao_tradutores(
    arquivo="tradutor_tipo", sheet="tradutor_calcado", col_to_str=["CO_SH6"]
)

tradutor_sh6_desc = funcao_tradutores(
    arquivo="tradutor_sh6_desc",
    col_to_str=["CO_SH6"],
    col_z_fill=["CO_SH6"],
    n_z_fill=6,
)

tradutor_vertical = funcao_tradutores(
    arquivo="tradutor_setor", sheet="vertical", col_to_str=["id_sh6"]
)

tradutor_componente = funcao_tradutores(
    arquivo="tradutor_setor", sheet="componente", col_to_str=["id_sh6"]
)

tradutor_pais = funcao_tradutores(arquivo="tradutor_pais", col_to_str=["id_pais"])

# BASES COMPLETAS
df_exp_completa = leitor_csv("EXP_COMPLETA")
df_imp_completa = leitor_csv("IMP_COMPLETA")

# BASES AJUSTADAS
anos = range(2019, 2026)
df_exp_ajustada = ajuste_sh6(df_exp_completa, anos)
df_imp_ajustada = ajuste_sh6(df_imp_completa, anos)

tipo = ["calcado", "couro"]
fluxo = ["EXP", "IMP"]

lista_df = [
    ajuste_comex_tipo(
        df=df_exp_ajustada if f == "EXP" else df_imp_ajustada,
        tipo=t,
        fluxo=f,
    )
    for t in tipo
    for f in fluxo
]

df_comex = pd.concat(lista_df, ignore_index=True).assign(
    mes_ano=lambda x: pd.to_datetime(
        x["CO_MES"].astype(str).str.zfill(2) + "/" + x["CO_ANO"].astype(str),
        format="%m/%Y",
    ).dt.strftime("%Y-%m"),
)
fluxo = ["EXP", "IMP"]

df_comex_vertical = pd.concat(
    [
        ajuste_comex_setor(
            df=df_exp_completa if f == "EXP" else df_imp_completa,
            fluxo=f,
            tipo="vertical",
            tradutor=tradutor_vertical,
        )
        for f in fluxo
    ]
)
df_comex_componente = pd.concat(
    [
        ajuste_comex_setor(
            df=df_exp_completa if f == "EXP" else df_imp_completa,
            fluxo=f,
            tipo="componente",
            tradutor=tradutor_componente,
        )
        for f in fluxo
    ]
)

# BASES SIDRA
df_producao_raw = sidra.get_table(
    table_code="8885",
    territorial_level="1",
    ibge_territorial_code="all",
    period="201901-202501",
    classification="542/56683,129200",
    variable="11602,11603",
)

df_producao = ajuste_sidra(
    df=df_producao_raw,
    lista_colunas=["V", "D2C", "D3C", "D4C"],
    rename_colunas={"V": "valor", "D2C": "date", "D3C": "variavel", "D4C": "grupo"},
    dicionario_taxas={"11602": "taxa_mensal", "11603": "taxa_acumulado"},
    dicionario_grupo={"56683": "fabricao_calcado", "129200": "curtimento_couro"},
)

df_vendas_raw = sidra.get_table(
    table_code="8882",
    territorial_level="1",
    ibge_territorial_code="all",
    period="201901-202601",
    classifications={"11046": "56734", "85": "90673"},
    variable="11709, 11710",
)

df_vendas = ajuste_sidra(
    df=df_vendas_raw,
    lista_colunas=["V", "D2C", "D3C", "D4C"],
    rename_colunas={"V": "valor", "D2C": "date", "D3C": "variavel", "D4C": "grupo"},
    dicionario_taxas={"11709": "taxa_mensal", "11710": "taxa_acumulado"},
    dicionario_grupo={"56734": "vestuario_calcados"},
)

df_ipca_raw = sidra.get_table(
    table_code="7060",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    classification="315/7169,7604",
    variable="63,2265",
)

df_ipca = (
    ajuste_sidra(
        df=df_ipca_raw,
        lista_colunas=["V", "D2C", "D3C", "D4C"],
        rename_colunas={"V": "valor", "D2C": "date", "D3C": "variavel", "D4C": "grupo"},
        dicionario_taxas={"63": "taxa_mensal", "2265": "taxa_doze_meses"},
        dicionario_grupo={"7169": "indice_geral", "7604": "calcados_acessorios"},
    )
    .rename(columns={"date": "mes_ano"})
    .assign(
        mes=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.month,
        ano=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.year,
    )
)

df_ipca_geral = ajuste_ipca(df_ipca, grupo="indice_geral", sufixo="_geral")
df_ipca_calcados = ajuste_ipca(df_ipca, grupo="calcados_acessorios", sufixo="")

df_industria_transformacao_raw = sidra.get_table(
    table_code="8888",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    classification="544/all",
    variable="11602, 11603",
)

df_industria_transformacao = (
    ajuste_sidra(
        df=df_industria_transformacao_raw,
        lista_colunas=["V", "D2C", "D3C", "D4C", "D4N"],
        rename_colunas={
            "V": "valor",
            "D2C": "date",
            "D3C": "variavel",
            "D4N": "grupo",
        },
        dicionario_taxas={"11602": "taxa_mensal", "11603": "taxa_acumulado"},
        dicionario_grupo={"544": "industria"},
    )
    .assign(
        descricao=lambda x: x["grupo"].str.replace(r"^\d+(\.\d+)?\s*", "", regex=True),
        mes=lambda x: pd.to_datetime(x["date"], format="%m/%Y").dt.month,
        ano=lambda x: pd.to_datetime(x["date"], format="%m/%Y").dt.year,
        mes_ano=lambda x: x["date"],
    )
    .query("descricao not in ['Indústria geral', 'Indústrias extrativas']")
    .drop(columns=["grupo", "date"])
)
df_taxa_desemprego_raw = sidra.get_table(
    table_code="6318",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    classification="629/all",
    variable="1641",
)

df_taxa_desemprego = (
    df_taxa_desemprego_raw.loc[1:, ["V", "D2C", "D2N", "D3N", "D4N"]]
    .rename(
        columns={
            "V": "valor",
            "D2C": "mes_ano",
            "D2N": "trimestre_movel",
            "D3N": "variavel",
            "D4N": "descricao",
        }
    )
    .assign(
        mes_ano=lambda df: pd.to_datetime(df["mes_ano"], format="%Y%m").dt.strftime(
            "%m/%Y"
        ),
        valor=lambda df: df["valor"].astype(float).round(2),
    )
    .pivot_table(
        index=["mes_ano", "trimestre_movel"], columns="descricao", values="valor"
    )
    .reset_index()
    .assign(
        mes=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.month,
        ano=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y").dt.year,
    )
)

df_emprego_couro = pd.read_sql_query(query_emprego_couro, engine).assign(
    mes_ano=lambda df: df["mes"].astype(str).str.zfill(2) + "/" + df["ano"].astype(str)
)

df_emprego_calcados = pd.read_sql_query(query_emprego_calcado, engine).assign(
    mes_ano=lambda df: df["mes"].astype(str).str.zfill(2) + "/" + df["ano"].astype(str)
)

df_ibc = get_data_bcb(url_ibc, "ibc")

df_taxa_cambio = get_data_bcb(url_cambio, "taxa_cambio")
df_taxa_cambio["media_movel_3"] = df_taxa_cambio["taxa_cambio"].rolling(3).mean()

df_expectativa_pib = pd.merge(
    expectativas_mercado(ano=2025, indicador="PIB Total"),
    expectativas_mercado(ano=2026, indicador="PIB Total"),
    on=["mes_ano", "mes", "ano"],
    how="left",
)

df_expectativa_ipca = pd.merge(
    expectativas_mercado(ano=2025, indicador="IPCA"),
    expectativas_mercado(ano=2026, indicador="IPCA"),
    on=["mes_ano", "mes", "ano"],
    how="left",
)

df_expectativa = pd.merge(
    df_expectativa_pib,
    df_expectativa_ipca,
    on=["mes_ano", "mes", "ano"],
    how="left",
)

# Salvando os DataFrames em CSV
destino = "../../OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/ASSINTECAL/Dashboard/data/"
df_emprego_calcados.to_csv(destino + "df_emprego_calcados.csv", index=False)
df_emprego_couro.to_csv(destino + "df_emprego_couro.csv", index=False)
df_producao.to_csv(destino + "df_base_producao.csv", index=False)
df_vendas.to_csv(destino + "df_base_vendas.csv", index=False)
df_comex.to_csv(destino + "df_comex_final.csv", index=False)
df_comex_vertical.to_csv(destino + "df_comex_vertical.csv", index=False)
df_comex_componente.to_csv(destino + "df_comex_componente.csv", index=False)
df_ipca_calcados.to_csv(destino + "df_ipca_calcados.csv", index=False)
df_ipca_geral.to_csv(destino + "df_ipca_geral.csv", index=False)
df_ibc.to_csv(destino + "df_ibc.csv", index=False)
df_expectativa.to_csv(destino + "df_expectativa.csv", index=False)
df_taxa_cambio.to_csv(destino + "df_tx_cambio.csv", index=False)
df_industria_transformacao.to_csv(destino + "df_ind_transf.csv", index=False)
df_taxa_desemprego.to_csv(destino + "df_tx_desemprego.csv", index=False)
end_time = time.time()
print("Arquivos Salvos")
print(f"Tempo total de execução: {end_time - start_time:.2f} segundos")
