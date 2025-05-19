# %%
import pandas as pd
import numpy as np

caminho_base = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/0_bases_gerais/"
caminho_resultado = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/PROGRAMACOES/HHI/"
trad_pais = pd.read_excel(caminho_base + "trad_pais.xlsx")
trad_ncm = pd.read_excel(caminho_base + "ncm_cnae.xlsx")

filtro_uf = "SC"
anos = np.arange(2019, 2025, 1)


arquivo_exp = "../../data/EXP_COMPLETA.csv"
df_exp = pd.read_csv(
    arquivo_exp, delimiter=";", encoding="ISO-8859-1", engine="pyarrow"
)


# Funcao para calcular o HHI
def calcular_hhi(
    df: pd.DataFrame, year_col: str, category_col: str, value_col: str = "VL_FOB"
) -> pd.DataFrame:
    """
    Preprocess the HHI data by filtering, grouping, and calculating the HHI.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        year_col (str): The name of the column containing the year (ex: 'CO_ANO').
        category_col (str): The name of the column containing the category (ex: 'cnae_2dg' or 'CO_PAIS').
        value_col (str): The name of the column containing the values. Default is 'VL_FOB'.

    Returns:
        pd.DataFrame: A DataFrame with the calculated HHI values.
    """
    # IF para nomear a coluna hhi_setor ou hhi_pais
    if category_col == "cnae_2dg":
        hhi_col_name = "hhi_setor"
    elif category_col == "CO_PAIS":
        hhi_col_name = "hhi_pais"
    else:
        raise ValueError("category_col must be either 'cnae_2dg' or 'CO_PAIS'")

    df = (
        df.groupby([year_col, category_col])
        .agg({value_col: "sum"})
        .reset_index()
        .assign(
            valor_ano=lambda x: x.groupby(year_col)[value_col].transform("sum"),
            hhi_desagregado=lambda x: ((x[value_col] / x["valor_ano"]) * 100) ** 2,
            hhi=lambda x: x.groupby(year_col)["hhi_desagregado"].transform("sum"),
        )[[year_col, "hhi"]]
        .drop_duplicates()
        .rename(columns={"hhi": hhi_col_name})
        .round(2)
    )
    return df


def ajustar_dados_setor(df_exp, trad_ncm, filtro_uf, anos):
    return (
        df_exp.query(f"SG_UF_NCM == '{filtro_uf}' and CO_ANO in @anos")
        .groupby(["CO_ANO", "CO_NCM"])["VL_FOB"]
        .sum()
        .reset_index()
        .merge(trad_ncm, left_on="CO_NCM", right_on="ncm", how="left")
        .groupby(["CO_ANO", "cnae_2dg"])["VL_FOB"]
        .sum()
        .reset_index()
    )


# Agrupando dados para calcular a similaridade entre as exportacoes brasileiras e as exportacoes do estado
def calcular_agregacao_exp(df, anos, filtro_uf=None):
    """
    Agrupa os dados de exportação por ano e NCM, somando o valor FOB.
    Se um filtro de UF for fornecido, o agrupamento será feito apenas para essa UF.
    O agrupamento é utilizado para calcular a Similaridade entre as exportacoes brasileiras e as exportacoes do estado.
    """
    if filtro_uf:
        df_filtrado = df.query("CO_ANO in @anos and SG_UF_NCM == @filtro_uf")
        nome_coluna = "exp_uf"
    else:
        df_filtrado = df.query("CO_ANO in @anos")
        nome_coluna = "exp_br"

    df_agg = (
        df_filtrado.groupby(["CO_ANO", "CO_NCM"])
        .agg(**{nome_coluna: ("VL_FOB", "sum")})
        .reset_index()
    )

    return df_agg


def calcular_similaridade(df_br, df_uf, trad_ncm):
    return (
        df_br.merge(df_uf, on=["CO_ANO", "CO_NCM"], how="left")
        .fillna(0)
        .merge(trad_ncm, left_on="CO_NCM", right_on="ncm", how="left")
        .groupby(["CO_ANO", "cnae_3dg"])
        .agg(
            {
                "exp_br": "sum",
                "exp_uf": "sum",
            }
        )
        .reset_index()
        .assign(
            exp_br_total=lambda x: x.groupby("CO_ANO")["exp_br"].transform("sum"),
            exp_uf_total=lambda x: x.groupby("CO_ANO")["exp_uf"].transform("sum"),
            share_br_cnae=lambda x: x["exp_br"] / x["exp_br_total"],
            share_uf_cnae=lambda x: x["exp_uf"] / x["exp_uf_total"],
            min_share_br_uf=lambda x: np.where(
                x[["share_br_cnae", "share_uf_cnae"]].isnull().any(axis=1),
                np.nan,
                x[["share_br_cnae", "share_uf_cnae"]].min(axis=1),
            ),
        )
        .groupby("CO_ANO")
        .agg(Similaridade=("min_share_br_uf", "sum"))
        .reset_index()
    )


# HHI
df_setor = ajustar_dados_setor(
    df_exp=df_exp, trad_ncm=trad_ncm, filtro_uf=filtro_uf, anos=anos
)
hhi_setor = calcular_hhi(
    df_setor, year_col="CO_ANO", category_col="cnae_2dg", value_col="VL_FOB"
)

df_pais = (
    df_exp.query("SG_UF_NCM == @filtro_uf and CO_ANO in @anos")
    .groupby(["CO_ANO", "CO_PAIS"])["VL_FOB"]
    .sum()
    .reset_index()
)

hhi_pais = calcular_hhi(
    df_pais, year_col="CO_ANO", category_col="CO_PAIS", value_col="VL_FOB"
)

# Juntar os dois HHI
hhi = hhi_setor.merge(hhi_pais, on="CO_ANO", how="left")

# Similaridade
df_br = calcular_agregacao_exp(df_exp, anos)
df_uf = calcular_agregacao_exp(df_exp, anos, filtro_uf=filtro_uf)
df_similaridade = calcular_similaridade(df_br=df_br, df_uf=df_uf, trad_ncm=trad_ncm)
# Juntar os dois dataframes
df = (
    df_similaridade.merge(hhi, on="CO_ANO", how="left")
    .assign(
        Similaridade=lambda x: x["Similaridade"].round(2),
        hhi_setor=lambda x: x["hhi_setor"].round(2),
        hhi_pais=lambda x: x["hhi_pais"].round(2),
    )
    .rename(columns={"CO_ANO": "ano"})
    .to_excel(
        f"D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/1_hhi_similaridade/{filtro_uf}_hhi_similaridade.xlsx",
        index=False,
    )
)
