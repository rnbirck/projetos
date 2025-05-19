# %%
import pandas as pd
import requests
from sqlalchemy import create_engine
from bcb import Expectativas
import sidrapy as sidra
import re
from bs4 import BeautifulSoup

print("Iniciando o script de conjuntura...")

expect = Expectativas()
ep = expect.get_endpoint("ExpectativasMercadoAnuais")

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")


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
                f"{indicador}_acumulado": lambda x: (
                    x["soma_acumulado_ano"] / x["soma_acumulado_ano"].shift(12) - 1
                )
                * 100,
                f"{indicador}_taxa_mensal": lambda x: (
                    x[indicador] / x[indicador].shift(12) - 1
                )
                * 100,
            },
        )
        .drop(columns=["data", "ano", "soma_acumulado_ano"])
    )


def get_expectativa_pib(ano):
    df = (
        ep.query()
        .filter(ep.Indicador == "PIB Total", ep.DataReferencia == ano)
        .filter(ep.Data >= "2023-01-01")
        .filter(ep.baseCalculo == 0)
        .select(ep.Indicador, ep.baseCalculo, ep.Data, ep.Mediana, ep.DataReferencia)
        .collect()
    )
    df = (
        df.assign(
            Data=lambda x: pd.to_datetime(x.Data),
        )
        .pipe(
            lambda x: x.assign(
                mes_ano=x.Data.dt.strftime("%m/%Y"),
            )
        )
        .sort_values(by="Data")
        .groupby(pd.Grouper(key="Data", freq="ME"))
        .last()
        .reset_index(drop=True)[["mes_ano", "Mediana"]]
        .rename(columns={"Mediana": f"expectativa_pib_{str(ano)[-2:]}"})
    )
    return df


def ajuste_sidra(
    df,
    lista_colunas: list,
    rename_colunas: dict,
    dicionario_taxas: dict,
    dicionario_grupo: dict,
):
    df = df.loc[1:, lista_colunas].rename(columns=rename_colunas)
    assign_kwargs = {
        "variavel": lambda x: x["variavel"].replace(dicionario_taxas),
        "mes_ano": lambda x: pd.to_datetime(x["date"], format="%Y%m").dt.strftime(
            "%m/%Y"
        ),
        "valor": lambda x: x["valor"].astype(float).round(2),
    }
    pivot_index_cols = ["mes_ano"]

    if "grupo" in df.columns:
        assign_kwargs["grupo"] = lambda x: x["grupo"].replace(dicionario_grupo)
        pivot_index_cols.append("grupo")

    df_final = (
        df.assign(**assign_kwargs)
        .pivot_table(index=pivot_index_cols, columns="variavel", values="valor")
        .reset_index()
    )

    return df_final


def separando_arquivos_ipca(df, categoria):
    filtro_categoria = df["grupo"] == categoria
    return (
        df[filtro_categoria]
        .rename(
            columns={
                "taxa_mensal": f"ipca_{categoria}_taxa_mensal",
                "taxa_acumulado": f"ipca_{categoria}_taxa_acumulado",
            }
        )
        .drop(columns=["grupo"])
    )


def ajuste_emprego(df, tipo_cnae, setor):
    return (
        df.assign(
            data=lambda x: pd.to_datetime(
                x["ano"].astype(str) + x["mes"].astype(str).str.zfill(2), format="%Y%m"
            ),
            mes_ano=lambda x: x["data"].dt.strftime("%m/%Y"),
        )
        .drop(columns=["ano", "mes", f"{tipo_cnae}", "data"])
        .rename(columns={"saldo_movimentacao": f"emprego_{setor}"})
    )


def ajuste_vendas_cimentos(df):
    return df.assign(
        ano=lambda x: x["date"].dt.year,
        mes_ano=lambda x: x["date"].dt.strftime("%m/%Y"),
        soma_acumulado_ano=lambda x: x.groupby("ano")["vendas_cimento_mi"].cumsum(),
        vendas_cimento_mi_acumulado=lambda x: (
            x["soma_acumulado_ano"] / x["soma_acumulado_ano"].shift(12) - 1
        )
        * 100,
        vendas_cimento_mi_taxa_mensal=lambda x: (
            x["vendas_cimento_mi"] / x["vendas_cimento_mi"].shift(12) - 1
        )
        * 100,
    ).drop(columns=["vendas_cimento_mi", "date", "ano", "soma_acumulado_ano"])


def ajuste_acos_longos(df):
    return (
        df.iloc[:, :-1]
        .T.reset_index()
        .loc[1:]
        .iloc[:, 1:]
        .rename(columns={17: "vendas_aco_longo"})
        .pipe(
            lambda x: x.assign(
                mes_ano=pd.date_range(
                    start="2013-01-01",
                    freq="ME",
                    periods=len(x),
                ),
                ano=lambda x: x["mes_ano"].dt.year,
                vendas_aco_longo=lambda x: pd.to_numeric(x["vendas_aco_longo"]),
            )
        )
        .query("mes_ano.dt.year >= 2021")
        .assign(
            mes_ano=lambda x: x["mes_ano"].dt.strftime("%m/%Y"),
            soma_acumulado_ano=lambda x: x.groupby("ano")["vendas_aco_longo"].cumsum(),
            vendas_aco_longo_acumulado=lambda x: (
                x["soma_acumulado_ano"] / x["soma_acumulado_ano"].shift(12) - 1
            )
            * 100,
            vendas_aco_longo_taxa_mensal=lambda x: (
                x["vendas_aco_longo"] / x["vendas_aco_longo"].shift(12) - 1
            )
            * 100,
        )
        .drop(columns=["vendas_aco_longo", "ano", "soma_acumulado_ano"])
    )


def ajuste_pib_construcao(df):
    return (
        df.loc[1:, ["V", "D2C", "D3C"]]
        .rename(columns={"V": "valor", "D2C": "trimestre", "D3C": "variavel"})
        .assign(
            variavel=lambda df: df["variavel"].replace(
                {
                    "6561": "pib_const_ano_anterior",
                    "6563": "pib_const_acumulado",
                    "6564": "pib_const_trim_anterior",
                }
            ),
            valor=lambda x: x["valor"].astype(float),
            mes_ano=lambda df: pd.to_datetime(
                df["trimestre"], format="%Y%m"
            ).dt.strftime("%m/%Y"),
        )
        .pipe(
            lambda x: x.assign(
                mes_ano=lambda x: x["mes_ano"]
                .str.replace("04/", "10/")
                .str.replace("02/", "04/")
                .str.replace("03/", "07/")
            )
        )
        .pivot_table(index="mes_ano", columns="variavel", values="valor")
        .reset_index()
    )


query_empregos_revestimentos = """
SELECT ano, mes, cnae_2_subclasse, SUM(saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae
WHERE cnae_2_subclasse = '2342701' AND ano BETWEEN 2021 AND 2025
GROUP BY ano, mes, cnae_2_subclasse
"""

query_empregos_loucas = """
SELECT ano, mes, cnae_2_subclasse, SUM(saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae
WHERE cnae_2_subclasse = '2349401' AND ano BETWEEN 2021 AND 2025
GROUP BY ano, mes, cnae_2_subclasse
"""

query_empregos_construcao = """
SELECT a.ano, a.mes, b.grupo_ibge, SUM(a.saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae AS a
JOIN cei.cnae AS b ON a.cnae_2_subclasse = b.cod_subclasse
WHERE a.ano BETWEEN 2021 AND 2025 AND b.grupo_ibge = 'Construção Civil'
GROUP BY a.ano, a.mes, b.grupo_ibge
"""

url_ibc = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24364/dados?formato=json&dataInicial=01/01/2021"
df_ibc = get_data_bcb(url_ibc, "ibc_br")

# Taxa de Câmbio
url_cambio = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.3698/dados?formato=json&dataInicial=01/01/2017"
df_taxa_cambio = get_data_bcb(url_cambio, "taxa_cambio")
df_taxa_cambio_capa = (
    df_taxa_cambio.assign(ano=lambda x: x["mes_ano"].str.split("/").str[1].astype(int))
    .query("ano >= 2021")
    .drop(columns=["ano"])
)

# INCC
url_incc = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.192/dados?formato=json&dataInicial=01/01/2021"
df_incc = (
    get_data_bcb(url_incc, "incc")
    .drop(columns=["incc_taxa_mensal", "incc_acumulado"])
    .assign(
        ano=lambda x: x["mes_ano"].str.split("/").str[1].astype(int),
        mes=lambda x: x["mes_ano"].str.split("/").str[0].astype(int),
        incc_acumulado=lambda x: x.groupby("ano")["incc"].transform(
            lambda x: (1 + x / 100).cumprod() - 1
        )
        * 100,
    )[["mes_ano", "incc", "incc_acumulado"]]
    .rename(columns={"incc": "incc_taxa_mensal"})
)

# SELIC
url_selic = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=01/01/2021"
df_selic = (
    pd.read_json(url_selic)
    .assign(
        data=lambda x: pd.to_datetime(x.data, format="%d/%m/%Y"),
        mes_ano=lambda x: x.data.dt.strftime("%m/%Y"),
    )
    .resample("ME", on="data")
    .last()
    .reset_index(drop=True)
    .rename(columns={"valor": "selic"})
)

# Expectativa PIB
df_expectativa_pib = get_expectativa_pib(2025)

# IPCA
lista_colunas_ipca = ["V", "D2C", "D3C", "D4C"]
rename_colunas_ipca = {"V": "valor", "D2C": "date", "D3C": "variavel", "D4C": "grupo"}
dicionario_taxas_ipca = {"63": "taxa_mensal", "69": "taxa_acumulado"}
dicionario_grupo_ipca = {
    "7169": "indice_geral",
    "12398": "revestimento_de_piso_e_parede",
}

df_ipca_raw = sidra.get_table(
    table_code="7060",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    classification="315/7169,12398",
    variable="63,69",
)

df_ipca = ajuste_sidra(
    df=df_ipca_raw,
    lista_colunas=lista_colunas_ipca,
    rename_colunas=rename_colunas_ipca,
    dicionario_taxas=dicionario_taxas_ipca,
    dicionario_grupo=dicionario_grupo_ipca,
)

df_ipca_revestimentos = separando_arquivos_ipca(
    df_ipca, categoria="revestimento_de_piso_e_parede"
)

df_ipca_geral = separando_arquivos_ipca(df_ipca, categoria="indice_geral")

# Insumos Tipicos Construção
lista_colunas_insumos = ["V", "D2C", "D3C"]
rename_colunas_insumos = {
    "V": "valor",
    "D2C": "date",
    "D3C": "variavel",
}
dicionario_taxas_insumos = {"11602": "insumos_const_mes", "11603": "insumos_const_acu"}

df_insumos_raw = sidra.get_table(
    table_code="8886",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    variable="11602, 11603",
)

df_insumos = ajuste_sidra(
    df=df_insumos_raw,
    lista_colunas=lista_colunas_insumos,
    rename_colunas=rename_colunas_insumos,
    dicionario_taxas=dicionario_taxas_insumos,
    dicionario_grupo={},
)

# Empregos
df_empregos_revestimentos_raw = pd.read_sql_query(query_empregos_revestimentos, engine)
df_empregos_revestimentos = ajuste_emprego(
    df_empregos_revestimentos_raw,
    tipo_cnae="cnae_2_subclasse",
    setor="rev_ceramicos",
)

df_empregos_loucas_raw = pd.read_sql_query(query_empregos_loucas, engine)
df_empregos_loucas = ajuste_emprego(
    df_empregos_loucas_raw, tipo_cnae="cnae_2_subclasse", setor="sanitarios"
)

df_empregos_construcao_raw = pd.read_sql_query(query_empregos_construcao, engine)
df_empregos_construcao = ajuste_emprego(
    df_empregos_construcao_raw, tipo_cnae="grupo_ibge", setor="const_civil"
)

# Vendas Cimentos
df_vendas_cimentos = (pd.read_excel("data/vendas_cimento.xlsx")).pipe(
    ajuste_vendas_cimentos
)

# Vendas Aço Longo
url_acos = "https://acobrasil.org.br/site/estatistica-mensal/"
response = requests.get(url_acos)
soup = BeautifulSoup(response.content, "html.parser")

links = soup.find_all("a", href=True)

xls_links = [link["href"] for link in links if re.search(r"\.xls$", link["href"])]

if xls_links:
    file_url = xls_links[0]
    df_acos_longos_raw = pd.read_excel(file_url)
else:
    print("Link para o arquivo XLS não encontrado.")

df_acos_longos_raw = df_acos_longos_raw
if (
    17 in df_acos_longos_raw.index
    and df_acos_longos_raw.loc[17, "Siderurgia Brasileira / Brazilian Steel Industry"]
    == "Longos / Long Products"
):
    df_acos_longos_raw = df_acos_longos_raw.loc[[17]]

df_acos_longos = ajuste_acos_longos(df_acos_longos_raw)

# PIB Construção
df_pib_construcao_raw = sidra.get_table(
    table_code="5932 ",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    variable="6561, 6563, 6564",
    classification="11255/90694",
)

df_pib_construcao = ajuste_pib_construcao(df_pib_construcao_raw)

# Merge dos dados
list_dfs = [
    df_taxa_cambio_capa,
    df_ibc,
    df_incc,
    df_selic,
    df_expectativa_pib,
    df_ipca_geral,
    df_ipca_revestimentos,
    df_insumos,
    df_empregos_revestimentos,
    df_empregos_loucas,
    df_empregos_construcao,
    df_vendas_cimentos,
    df_acos_longos,
    df_pib_construcao,
]

df_conjuntura = df_taxa_cambio.copy()
for df_in_list in list_dfs[1:]:
    df_conjuntura = pd.merge(
        df_conjuntura,
        df_in_list,
        how="left",
        on="mes_ano",
    )

df_conjuntura = df_conjuntura.melt(
    id_vars="mes_ano",
    var_name="indicador",
    value_name="valor",
).rename(columns={"mes_ano": "date"})


df_conjuntura.to_excel(
    "D:/CEI/BI_ANFACER/BI/arquivos/conjuntura.xlsx",
    index=False,
)

df_taxa_cambio.to_excel(
    "D:/CEI/BI_ANFACER/BI/arquivos/taxa_cambio.xlsx",
    index=False,
)

print("Arquivo de conjuntura gerado com sucesso!")
