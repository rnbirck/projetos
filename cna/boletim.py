# %%
import pandas as pd
import time

script_start_time = time.time()

print("Script iniciado")

ultimo_ano = 2025
tradutor_agro = pd.read_excel("data/tradutor_agrostat.xlsx")
tradutor_pais = pd.read_excel("data/tradutor_pais.xlsx")
tradutor_ue = pd.read_excel("data/tradutor_ue.xlsx")

df_exp_completa = pd.read_csv(
    "../data/EXP_COMPLETA.csv", sep=";", engine="pyarrow"
).query("CO_ANO > 2009")


df_imp_completa = pd.read_csv(
    "../data/IMP_COMPLETA.csv", sep=";", engine="pyarrow"
).query("CO_ANO > 2009")


def ajuste_mes_ncm_pais(df, tradutor_agro, tradutor_pais):
    return (
        df.groupby(["CO_ANO", "CO_MES", "CO_NCM", "CO_PAIS"], as_index=False)
        .agg({"KG_LIQUIDO": "sum", "VL_FOB": "sum"})
        .merge(
            tradutor_agro[["NCM", "Setores", "Produtos"]],
            left_on="CO_NCM",
            right_on="NCM",
            how="left",
        )
        .merge(tradutor_pais[["CO_PAIS", "NO_PAIS"]], on="CO_PAIS", how="left")
        .drop(columns=["NCM"])
    )


def ajuste_comex_total(df):
    return df.groupby(["CO_ANO", "CO_MES"], as_index=False).agg(
        {"KG_LIQUIDO": "sum", "VL_FOB": "sum"}
    )


def ajuste_comex_agro(df):
    return (
        df.dropna(subset=["Produtos"])
        .groupby(["CO_ANO", "CO_MES"], as_index=False)
        .agg({"KG_LIQUIDO": "sum", "VL_FOB": "sum"})
    )


def ajuste_produto_ncm_pais(df, tradutor_ue):
    return (
        df.dropna(subset=["Produtos"])
        .query("CO_ANO >= 2023")
        .merge(tradutor_ue, left_on="NO_PAIS", right_on="PAIS", how="left")[
            [
                "CO_ANO",
                "CO_MES",
                "CO_NCM",
                "NO_PAIS",
                "Setores",
                "Produtos",
                "VL_FOB",
                "KG_LIQUIDO",
                "BLOCO",
            ]
        ]
        .assign(BLOCO=lambda x: x.BLOCO.replace({"UE": "União Europeia - UE"}))
        .rename(
            columns={"CO_NCM": "NCM", "NO_PAIS": "Países", "BLOCO": "BlocoEconômico"}
        )
    )


setores_interesse = [
    "CAFÉ",
    "FRUTAS (INCLUI NOZES E CASTANHAS)",
    "CACAU E SEUS PRODUTOS",
    "PESCADOS",
    "PRODUTOS APICOLAS",
]


def ajuste_setores_interesse(df_exp_mes_ncm_pais, df_exp_total, setores_interesse):
    return (
        df_exp_mes_ncm_pais.dropna(subset=["Produtos"])
        .query(f"Setores in {setores_interesse}")
        .groupby(["CO_ANO", "CO_MES", "Setores"], as_index=False)
        .agg({"VL_FOB": "sum"})
        .pivot_table(
            index=["CO_ANO", "CO_MES"],
            columns="Setores",
            values="VL_FOB",
            fill_value=0,
        )
        .reset_index()
        .merge(
            df_exp_total[["CO_ANO", "CO_MES", "VL_FOB"]],
            on=["CO_ANO", "CO_MES"],
            how="left",
        )
        .rename(columns={"VL_FOB": "VL_FOB_AGRO"})
    )


# Exportacao

df_exp_mes_ncm_pais = ajuste_mes_ncm_pais(
    df=df_exp_completa, tradutor_agro=tradutor_agro, tradutor_pais=tradutor_pais
)
df_exp_total = ajuste_comex_total(df_exp_completa)

df_exp_agro = ajuste_comex_agro(df_exp_mes_ncm_pais)

# ARQUIVO EXP_MES_TOTAL_AGRO
df_exp_mes_total_agro = pd.merge(
    df_exp_total,
    df_exp_agro,
    on=["CO_ANO", "CO_MES"],
    how="left",
    suffixes=("_TOTAL", "_AGRO"),
).query("CO_ANO >= 2018")

# ARQUIVO EXP_PRODUTO_NCM_PAIS

df_exp_produto_ncm_pais = ajuste_produto_ncm_pais(
    df=df_exp_mes_ncm_pais, tradutor_ue=tradutor_ue
)

# ARQUIVO EXP_AJUSTE_2010_ULTIMO_ANO
df_exp_setores_interesse = ajuste_setores_interesse(
    df_exp_mes_ncm_pais=df_exp_mes_ncm_pais,
    df_exp_total=df_exp_total,
    setores_interesse=setores_interesse,
)

# ARQUIVO IMP_MES_TOTAL_AGRO
df_imp_mes_ncm_pais = ajuste_mes_ncm_pais(
    df=df_imp_completa, tradutor_agro=tradutor_agro, tradutor_pais=tradutor_pais
)

df_imp_total = ajuste_comex_total(df_imp_completa)

df_imp_agro = ajuste_comex_agro(df_imp_mes_ncm_pais)

df_imp_mes_total_agro = pd.merge(
    df_imp_total,
    df_imp_agro,
    on=["CO_ANO", "CO_MES"],
    how="left",
    suffixes=("_TOTAL", "_AGRO"),
)

# SALVANDO OS ARQUIVOS XLSX
caminho = "C:/Users/rnbirck/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/CNA/Boletim Balança Comercial/RODAR/"

df_exp_mes_total_agro.to_excel(caminho + "EXP_MES_TOTAL_AGRO.xlsx", index=False)
df_exp_produto_ncm_pais.to_excel(caminho + "EXP_PRODUTO_NCM_PAIS.xlsx", index=False)
nome_arquivo_setores_interesse = f"EXP_AJUSTE_2010_{ultimo_ano}.xlsx"
df_exp_setores_interesse.to_excel(caminho + nome_arquivo_setores_interesse, index=False)
df_imp_mes_total_agro.to_excel(caminho + "IMP_MES_TOTAL_AGRO.xlsx")

print("Script finalizado, arquivos salvos")
script_end_time = time.time()
total_elapsed_time = script_end_time - script_start_time
print(f"Script completo executado em {total_elapsed_time:.2f} segundos.")
