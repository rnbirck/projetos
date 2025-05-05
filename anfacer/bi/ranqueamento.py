# %%
import pandas as pd
import numpy as np

ano_inicial = 2021
ano_final = 2024

# Arquivos
# Tradutores
tradutor_reporter = pd.read_csv("data/ranqueamento/tradutor_reporter.csv", sep=";")
tradutor_tipologia = pd.read_csv("data/ranqueamento/tradutor_tipologia.csv", sep=";")
# Arquivo TDM
tdm = (
    pd.read_csv("data/tdm.csv", delimiter=";", engine="pyarrow")
    .assign(YEAR=lambda x: x["DATE"].astype(str).str[:4].astype(int))
    .merge(tradutor_reporter, on="CTY_RPT", how="left")
    .merge(
        tradutor_reporter,
        left_on="CTY_PTN",
        right_on="CTY_RPT",
        how="left",
        suffixes=("", "_PTN"),
    )
    .rename(columns={"reporter_PTN": "partner"})
    .drop(columns=["CTY_RPT_PTN"])
)

# Arquivo GDP
ajuste_reporter_gdp = {
    "China, People's Republic of": "China",
    "Bosnia and Herzegovina": "Bosnia And Herzegovina",
    "Côte d'Ivoire": "Cote d'Ivoire",
    "Hong Kong SAR": "Hong Kong",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Korea, Republic of": "South Korea",
    "North Macedonia ": "Macedonia",
    "Macao SAR": "Macao",
    "Russian Federation": "Russia",
    "Slovak Republic": "Slovakia",
    "Türkiye, Republic of": "Turkey",
    "Taiwan Province of China": "Taiwan",
}
gdp = (
    pd.read_csv("data/ranqueamento/gdp.csv", sep=";")
    .assign(
        gdp_ano_inicial=lambda x: x[f"gdp_{ano_inicial}"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float),
        gdp_ano_final=lambda x: x[f"gdp_{ano_final}"]
        .astype(str)
        .str.replace(",", ".")
        .astype(float),
    )
    .drop(columns=[f"gdp_{ano_inicial}", f"gdp_{ano_final}"])
    .rename(
        columns={
            "reporter": "reporter_gdp",
            "gdp_ano_inicial": f"taxa_cresc_pib_{ano_inicial}",
            "gdp_ano_final": f"taxa_cresc_pib_{ano_final}",
        }
    )
    .replace({"reporter_gdp": ajuste_reporter_gdp})
)

# Arquivo Tarifas
tarifas = pd.read_csv("data/ranqueamento/tarifas.csv", sep=";")


# Funções
def imp_paises(tdm, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento de importação dos países para os anos especificados.
    """

    filtro_fluxo_imp = tdm["FLUXO"] == "Importação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])

    return (
        tdm[filtro_fluxo_imp & filtros_anos]
        .groupby(["reporter", "YEAR"], as_index=False)["VALUE"]
        .sum()
        .pivot(index="reporter", columns="YEAR", values="VALUE")
        .reset_index()
        .assign(
            taxa_cresc_imp=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                ano_inicial: f"imp_{ano_inicial}",
                ano_final: f"imp_{ano_final}",
            }
        )
    )


def preco_medio(tdm, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento do preço médio dos países para os anos especificados.
    """
    filtro_fluxo_imp = tdm["FLUXO"] == "Importação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])

    return (
        tdm[filtro_fluxo_imp & filtros_anos]
        .groupby(["reporter", "YEAR"], as_index=False)
        .agg({"VALUE": "sum", "QTY1": "sum"})
        .assign(preco_medio=lambda x: x["VALUE"] / x["QTY1"])
        .pivot(index="reporter", columns="YEAR", values="preco_medio")
        .reset_index()
        .assign(
            taxa_cresc_preco_medio=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                ano_inicial: f"preco_medio_{ano_inicial}",
                ano_final: f"preco_medio_{ano_final}",
            }
        )
        .replace({"inf": 0})
    )


def exp_brasil(tdm, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento de exportação do Brasil para os anos especificados.
    """
    filtro_fluxo_exp = tdm["FLUXO"] == "Exportação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])
    filtro_reporter_brasil = tdm["reporter"] == "Brazil"

    return (
        tdm[filtro_fluxo_exp & filtros_anos & filtro_reporter_brasil]
        .groupby(["partner", "YEAR"], as_index=False)["VALUE"]
        .sum()
        .rename(columns={"partner": "reporter"})
        .pivot(index="reporter", columns="YEAR", values="VALUE")
        .reset_index()
        .fillna(0)
        .assign(
            taxa_cresc_exp_br=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                ano_inicial: f"exp_br_{ano_inicial}",
                ano_final: f"exp_br_{ano_final}",
            }
        )
    )


def preco_medio_br(tdm, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento do preço médio do Brasil para os anos especificados.
    """
    filtro_fluxo_exp = tdm["FLUXO"] == "Exportação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])
    filtro_reporter_brasil = tdm["reporter"] == "Brazil"

    return (
        tdm[filtro_fluxo_exp & filtros_anos & filtro_reporter_brasil]
        .groupby(["partner", "YEAR"], as_index=False)
        .agg({"VALUE": "sum", "QTY1": "sum"})
        .assign(preco_medio=lambda x: x["VALUE"] / x["QTY1"])
        .rename(columns={"partner": "reporter"})
        .pivot(index="reporter", columns="YEAR", values="preco_medio")
        .reset_index()
        .fillna(0)
        .assign(
            taxa_cresc_preco_medio_br=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                ano_inicial: f"preco_medio_exp_br_{ano_inicial}",
                ano_final: f"preco_medio_exp_br_{ano_final}",
            }
        )
        .replace({"inf": 0})
    )


def imp_paises_tipologia(tdm, ano_inicial, ano_final, tipologia):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento de importação dos países por tipologia para os anos especificados.
    """

    filtro_fluxo_imp = tdm["FLUXO"] == "Importação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])

    return (
        tdm[filtro_fluxo_imp & filtros_anos]
        .merge(tradutor_tipologia, on="SH6", how="left")
        .query(f"tipologia == '{tipologia}'")
        .groupby(["reporter", "YEAR"], as_index=False)["VALUE"]
        .sum()
        .pivot(index="reporter", columns="YEAR", values="VALUE")
        .reset_index()
        .assign(
            taxa_cresc_imp=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                "taxa_cresc_imp": f"taxa_cresc_imp_{tipologia}",
                ano_inicial: f"imp_{tipologia}_{ano_inicial}",
                ano_final: f"imp_{tipologia}_{ano_final}",
            }
        )
    )


def exp_brasil_tipologia(tdm, ano_inicial, ano_final, tipologia):
    """
    Função que retorna o dataframe com os valores e taxa de crescimento de exportação do Brasil por tipologia para os anos especificados.
    """
    filtro_fluxo_exp = tdm["FLUXO"] == "Exportação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])
    filtro_reporter_brasil = tdm["reporter"] == "Brazil"

    return (
        tdm[filtro_fluxo_exp & filtros_anos & filtro_reporter_brasil]
        .merge(tradutor_tipologia, on="SH6", how="left")
        .query(f"tipologia == '{tipologia}'")
        .groupby(["partner", "YEAR"], as_index=False)["VALUE"]
        .sum()
        .rename(columns={"partner": "reporter"})
        .pivot(index="reporter", columns="YEAR", values="VALUE")
        .reset_index()
        .fillna(0)
        .assign(
            taxa_cresc_imp=lambda x: (
                (x[ano_final] / x[ano_inicial]) ** (1 / (ano_final - ano_inicial)) - 1
            )
            * 100
        )
        .rename(
            columns={
                "taxa_cresc_imp": f"taxa_cresc_exp_br_{tipologia}",
                ano_inicial: f"exp_br_{tipologia}_{ano_inicial}",
                ano_final: f"exp_br_{tipologia}_{ano_final}",
            }
        )
    )


def pib_paises(tdm, gdp):
    """
    Função que retorna o dataframe com as taxa de crescimento do PIB dos países para os anos especificados.
    """
    return (
        tdm[["reporter"]]
        .drop_duplicates()
        .merge(gdp, left_on="reporter", right_on="reporter_gdp", how="left")
        .drop(columns=["reporter_gdp"])
    )


def part_brasil(df_imp_paises, df_exp_brasil, ano_inicial, ano_final):
    return (
        pd.merge(
            df_imp_paises[["reporter", f"imp_{ano_inicial}", f"imp_{ano_final}"]],
            df_exp_brasil[["reporter", f"exp_br_{ano_inicial}", f"exp_br_{ano_final}"]],
            on="reporter",
            how="left",
        )
        .assign(
            part_brasil_ano_inicial=lambda x: (
                x[f"exp_br_{ano_inicial}"] / x[f"imp_{ano_inicial}"]
            )
            * 100,
            part_brasil_ano_final=lambda x: (
                x[f"exp_br_{ano_final}"] / x[f"imp_{ano_final}"]
            )
            * 100,
        )
        .fillna(0)
        .drop(
            columns=[
                f"exp_br_{ano_inicial}",
                f"exp_br_{ano_final}",
                f"imp_{ano_inicial}",
                f"imp_{ano_final}",
            ]
        )
        .rename(
            columns={
                "part_brasil_ano_inicial": f"part_brasil_{ano_inicial}",
                "part_brasil_ano_final": f"part_brasil_{ano_final}",
            }
        )
    )


def imp_concorrentes(tdm, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com os valores de importação dos concorrentes pelos países para os anos especificados.
    """
    correntes = ["Spain", "Italy", "India", "China", "Turkey"]
    filtro_concorrentes = tdm["partner"].isin(correntes)
    filtro_fluxo_imp = tdm["FLUXO"] == "Importação"
    filtros_anos = tdm["YEAR"].isin([ano_inicial, ano_final])
    return (
        tdm[filtro_concorrentes & filtro_fluxo_imp & filtros_anos]
        .groupby(["reporter", "YEAR"], as_index=False)["VALUE"]
        .sum()
        .pivot(index="reporter", columns="YEAR", values="VALUE")
        .reset_index()
        .rename(
            columns={
                ano_inicial: f"imp_concorrentes_{ano_inicial}",
                ano_final: f"imp_concorrentes_{ano_final}",
            }
        )
    )


def part_concorrentes(df_imp_paises, df_imp_concorrentes, ano_inicial, ano_final):
    """
    Função que retorna o dataframe com as participações dos concorrentes nos países para os anos especificados.
    """
    return (
        df_imp_paises[["reporter", f"imp_{ano_inicial}", f"imp_{ano_final}"]]
        .merge(
            df_imp_concorrentes,
            on="reporter",
            how="left",
        )
        .assign(
            part_concorrentes_ano_inicial=lambda x: (
                x[f"imp_concorrentes_{ano_inicial}"] / x[f"imp_{ano_inicial}"]
            )
            * 100,
            part_concorrentes_ano_final=lambda x: (
                x[f"imp_concorrentes_{ano_final}"] / x[f"imp_{ano_final}"]
            )
            * 100,
        )
        .fillna(0)
        .drop(
            columns=[
                f"imp_concorrentes_{ano_inicial}",
                f"imp_concorrentes_{ano_final}",
                f"imp_{ano_inicial}",
                f"imp_{ano_final}",
            ]
        )
        .rename(
            columns={
                "part_concorrentes_ano_inicial": f"part_concorrentes_{ano_inicial}",
                "part_concorrentes_ano_final": f"part_concorrentes_{ano_final}",
            }
        )
    )


# Dataframes
df_imp_paises = imp_paises(tdm, ano_inicial, ano_final)
df_preco_medio = preco_medio(tdm, ano_inicial, ano_final)
df_exp_brasil = exp_brasil(tdm, ano_inicial, ano_final)
df_preco_medio_br = preco_medio_br(tdm, ano_inicial, ano_final)
df_imp_paises_ceramica = imp_paises_tipologia(tdm, ano_inicial, ano_final, "ceramica")
df_imp_paises_porcelanato = imp_paises_tipologia(
    tdm, ano_inicial, ano_final, "porcelanato"
)
df_exp_brasil_ceramica = exp_brasil_tipologia(tdm, ano_inicial, ano_final, "ceramica")
df_exp_brasil_porcelanato = exp_brasil_tipologia(
    tdm, ano_inicial, ano_final, "porcelanato"
)
df_pib_paises = pib_paises(tdm, gdp)
df_part_brasil = part_brasil(df_imp_paises, df_exp_brasil, ano_inicial, ano_final)
df_imp_concorrentes = imp_concorrentes(tdm, ano_inicial, ano_final)
df_part_concorrentes = part_concorrentes(
    df_imp_paises, df_imp_concorrentes, ano_inicial, ano_final
)

lista_dfs = [
    df_imp_paises,
    df_preco_medio,
    df_exp_brasil,
    df_preco_medio_br,
    df_imp_paises_ceramica,
    df_imp_paises_porcelanato,
    df_exp_brasil_ceramica,
    df_exp_brasil_porcelanato,
    df_pib_paises,
    df_part_brasil,
    df_part_concorrentes,
    tarifas,
]

df_ranqueamento = lista_dfs[0]
for df in lista_dfs[1:]:
    df_ranqueamento = pd.merge(df_ranqueamento, df, on="reporter", how="left")

df_ranqueamento = df_ranqueamento.fillna(0).replace({np.inf: np.nan})

coluna = df_ranqueamento.drop(columns=["reporter"]).columns.tolist()
var = [f"var_{i + 1}" for i in range(len(coluna))]

df_classificacao = pd.DataFrame({"var": var, "coluna": coluna}).assign(
    tipo=lambda x: np.where(
        x["coluna"].str.startswith("taxa") | x["coluna"].str.startswith("tarifa"),
        "taxa",
        np.where(x["coluna"].str.startswith("part"), "participacao", "valor"),
    ),
    ordem=lambda x: np.where(
        x["coluna"].str.startswith("part_concorrentes")
        | x["coluna"].str.startswith("tarifa"),
        "invertido",
        "padrao",
    ),
)

var_rename = df_classificacao.set_index("coluna")["var"].to_dict()

df_ranqueamento_ajustado = df_ranqueamento.rename(columns=var_rename).rename(
    columns={"reporter": "id"}
)
# %%
arquivo = "data/ranqueamento/base_anfacer.xlsx"
with pd.ExcelWriter(arquivo) as writer:
    df_ranqueamento_ajustado.to_excel(
        writer, index=False, sheet_name="base_ranqueamento"
    )
    df_classificacao.to_excel(writer, index=False, sheet_name="classificacao")
