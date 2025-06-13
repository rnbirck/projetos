# %%
import pandas as pd
from sqlalchemy import create_engine
from bcb import Expectativas
import sidrapy as sidra

caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/ANFACER/Boletins/0_PROJETO 2023/"
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

# URLS BCB
url_ibc = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24364/dados?formato=json&dataInicial=01/01/2021"
url_cambio = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.3698/dados?formato=json&dataInicial=01/01/2021"
url_incc = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.192/dados?formato=json&dataInicial=01/01/2021"
url_selic = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=01/01/2021"

# Querys
query_emprego_construcao = """
SELECT a.ano, a.mes, b.grupo_ibge, SUM(a.saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae AS a
JOIN cei.cnae AS b ON a.cnae_2_subclasse = b.cod_subclasse
WHERE a.ano BETWEEN 2021 AND 2025 AND b.grupo_ibge = 'Construção Civil'
GROUP BY a.ano, a.mes, b.grupo_ibge
"""
query_emprego_setor = """
SELECT ano, mes, cnae_2_subclasse, SUM(saldo_movimentacao) AS saldo_movimentacao
FROM cei.caged_cnae
WHERE cnae_2_subclasse = '2342701' AND ano BETWEEN 2021 AND 2025
GROUP BY ano, mes, cnae_2_subclasse
"""

# Colunas e dicionários para ajuste SIDRA
lista_colunas_ipca = ["V", "D2C", "D3C", "D4C"]
rename_colunas_ipca = {"V": "valor", "D2C": "date", "D3C": "variavel", "D4C": "grupo"}
dicionario_taxas_ipca = {"63": "taxa_mensal", "2265": "taxa_12meses"}
dicionario_grupo_ipca = {
    "7169": "indice_geral",
    "12398": "revestimento_de_piso_e_parede",
}

lista_colunas_insumos = ["V", "D2C", "D3C"]
rename_colunas_insumos = {
    "V": "valor",
    "D2C": "date",
    "D3C": "variavel",
}
dicionario_taxas_insumos = {"11602": "insumos_const_mes", "11603": "insumos_const_acu"}


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
        .assign(
            date=lambda x: pd.to_datetime(x["mes_ano"], format="%m/%Y"),
        )
        .sort_values(by="date")
        .drop(columns=["date"])
    )

    return df_final


def separando_arquivos_ipca(df, categoria):
    filtro_categoria = df["grupo"] == categoria
    return (
        df[filtro_categoria]
        .rename(
            columns={
                "taxa_mensal": f"ipca_{categoria}_taxa_mensal",
                "taxa_12meses": f"ipca_{categoria}_taxa_12meses",
            }
        )
        .drop(columns=["grupo"])
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


def ajuste_emprego(df_raw, col_to_drop, col_tipo):
    return (
        df_raw.assign(
            data=lambda x: pd.to_datetime(
                x["ano"].astype(str) + x["mes"].astype(str).str.zfill(2), format="%Y%m"
            )
        )
        .drop(columns=["ano", "mes", f"{col_to_drop}"])[["data", "saldo_movimentacao"]]
        .rename(columns={"saldo_movimentacao": f"{col_tipo}"})
        .sort_values(by="data")
        .assign(data=lambda x: x["data"].dt.strftime("%b/%y"))
    )


# IBC-BR
df_ibc = get_data_bcb(
    url=url_ibc,
    indicador="ibc",
)[["mes_ano", "ibc", "ibc_taxa_mensal", "ibc_acumulado"]]

# Câmbio
df_cambio = get_data_bcb(
    url=url_cambio,
    indicador="cambio",
)[["mes_ano", "cambio", "cambio_taxa_mensal", "cambio_acumulado"]]

# INCC
df_incc = get_data_bcb(url_incc, "incc")[["mes_ano", "incc"]]

# Selic
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
)[["mes_ano", "selic"]]

# IPCA
df_ipca_raw = sidra.get_table(
    table_code="7060",
    territorial_level="1",
    ibge_territorial_code="all",
    period="202101-202601",
    classification="315/7169,12398",
    variable="63,2265",
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
[
    [
        "mes_ano",
        "ipca_revestimento_de_piso_e_parede_taxa_mensal",
        "ipca_revestimento_de_piso_e_parede_taxa_12meses",
    ]
]

df_ipca_geral = separando_arquivos_ipca(df_ipca, categoria="indice_geral")[
    ["mes_ano", "ipca_indice_geral_taxa_mensal", "ipca_indice_geral_taxa_12meses"]
]

# Insumos Tipicos Construção
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
)[["mes_ano", "insumos_const_mes", "insumos_const_acu"]]

# Expectativas de mercado
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
).drop(columns=["mes", "ano"])

# Vendas de cimento MI
df_vendas_cimento = pd.read_excel("D:/CEI/BI_ANFACER/BI/data/vendas_cimento.xlsx")
df_vendas_cimento["data"] = df_vendas_cimento["date"].dt.strftime("%b/%y")
df_vendas_cimento = df_vendas_cimento[["data", "vendas_cimento_mi"]]

# Emprego
df_emprego = pd.merge(
    (
        pd.read_sql(query_emprego_construcao, con=engine).pipe(
            lambda x: ajuste_emprego(
                df_raw=x,
                col_to_drop="grupo_ibge",
                col_tipo="emprego_construcao",
            )
        )
    ),
    (
        pd.read_sql(query_emprego_setor, con=engine).pipe(
            lambda x: ajuste_emprego(
                df_raw=x,
                col_to_drop="cnae_2_subclasse",
                col_tipo="emprego_setor",
            )
        )
    ),
    on="data",
    how="left",
)

with pd.ExcelWriter(caminho + "base_conjuntura.xlsx", engine="openpyxl") as writer:
    df_ibc.to_excel(writer, sheet_name="ibc-br", index=False)
    df_cambio.to_excel(writer, sheet_name="cambio", index=False)
    df_selic.to_excel(writer, sheet_name="selic", index=False)
    df_ipca_geral.to_excel(writer, sheet_name="ipca_geral", index=False)
    df_ipca_revestimentos.to_excel(writer, sheet_name="ipca_revestimentos", index=False)
    df_expectativa.to_excel(writer, sheet_name="expectativas", index=False)
    df_insumos.to_excel(writer, sheet_name="insumos", index=False)
    df_incc.to_excel(writer, sheet_name="incc", index=False)
    df_vendas_cimento.to_excel(writer, sheet_name="vendas_cimento", index=False)
    df_emprego.to_excel(writer, sheet_name="emprego", index=False)

print("Script de conjuntura finalizado com sucesso!")
