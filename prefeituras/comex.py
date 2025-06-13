# %%
import pandas as pd

caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Comex/"
# Tradutores
trad_sh4 = pd.read_excel(caminho + "bases/trad_sh4.xlsx").assign(
    id_sh4=lambda x: x["id_sh4"].astype(str).str.zfill(4),
)
trad_mun = pd.read_excel(caminho + "bases/trad_mun.xlsx").astype({"id_mun": str})
trad_pais = pd.read_excel(caminho + "bases/trad_pais.xlsx")

# Base Comex
df_comex_mun_raw = pd.read_csv(
    "../data/EXP_COMPLETA_MUN.csv", sep=";", encoding="latin1", engine="pyarrow"
)

anos = range(2019, 2026)

cols_to_rename = {
    "CO_ANO": "ano",
    "CO_MES": "mes",
    "CO_MUN": "id_mun",
    "CO_PAIS": "id_pais",
    "SH4": "id_sh4",
    "VL_FOB": "vl_fob",
}


def ajuste_comex_mun(df_raw, anos, cols_to_rename):
    return (
        df_raw.query("CO_ANO in @anos and SG_UF_MUN == 'RS'")
        .groupby(["CO_ANO", "CO_MES", "CO_MUN", "CO_PAIS", "SH4", "SG_UF_MUN"])
        .agg({"VL_FOB": "sum"})
        .reset_index()
        .rename(columns=cols_to_rename)
        .assign(
            id_mun=lambda x: x["id_mun"].astype(str),
            id_sh4=lambda x: x["id_sh4"].astype(str).str.zfill(4),
        )
        .merge(trad_sh4, on="id_sh4", how="left")
        .merge(trad_mun, on="id_mun", how="left")
        .merge(trad_pais, on="id_pais", how="left")
        .assign(desc_sh4=lambda x: x["desc_sh4"].str.extract(r"^([^;]*)"))
    )


df_comex_mun = ajuste_comex_mun(df_comex_mun_raw, anos, cols_to_rename)

df_comex_mun.to_csv(caminho + "comex.csv", index=False)
