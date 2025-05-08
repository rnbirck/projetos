# %%
import pandas as pd
import importlib
import utils
import time

script_start_time = time.time()

importlib.reload(utils)

print("Script iniciado")

ano_inicial = 2017

lista_sh6_revestimentos = [
    690810,
    690710,
    690721,
    690722,
    690723,
    690730,
    690740,
    690790,
    690890,
]
lista_sh6_loucas_sanitarias = [691090]

cols_comexstat = [
    "DATE",
    "SH6",
    "SG_UF_NCM",
    "FLUXO",
    "CO_VIA",
    "CO_URF",
    "VALUE",
    "QTY1",
    "CTY_RPT",
    "CTY_PTN",
]

trad_ncm = pd.read_csv("data/comercio_exterior/tradutor_ncm.csv", sep=";")

trad_cod_pais = pd.read_csv(
    "data/comercio_exterior/tradutor_cod_pais.csv", sep=";"
).assign(CO_PAIS=lambda x: x.CO_PAIS.astype(str).str.zfill(3))

df_tdm_raw = pd.read_csv(
    "data/comercio_exterior/report.csv", sep=",", encoding="utf_16_le", engine="pyarrow"
)

df_exp_raw = pd.read_csv(
    "../../data/EXP_COMPLETA.csv",
    sep=";",
    encoding="utf_8",
    engine="pyarrow",
).query(f"CO_ANO >= {ano_inicial}")

df_imp_raw = pd.read_csv(
    "../../data/IMP_COMPLETA.csv",
    sep=";",
    encoding="utf_8",
    engine="pyarrow",
).query(f"CO_ANO >= {ano_inicial}")

# Revestimentos Ceramicos
## Arquivo TDM sem Brasil
df_tdm_sem_brasil = utils.ajuste_tdm_sem_brasil(
    df_raw=df_tdm_raw, lista_sh6=lista_sh6_revestimentos
)

## Comercio exterior do Brasil a partir do Comexstat
df_exp_comexstat = utils.ajustes_exp_comexstat(
    df=df_exp_raw,
    exp_updates_config=utils.exp_updates_config,
    lista_sh6=lista_sh6_revestimentos,
    trad_ncm=trad_ncm,
    trad_cod_pais=trad_cod_pais,
)
df_imp_comexstat = utils.ajustes_imp_comexstat(
    df=df_imp_raw,
    imp_updates_config=utils.imp_updates_config,
    lista_sh6=lista_sh6_revestimentos,
    trad_ncm=trad_ncm,
    trad_cod_pais=trad_cod_pais,
)
df_comexstat = utils.ajustes_comexstat_final(
    df_exp_comexstat=df_exp_comexstat, df_imp_comexstat=df_imp_comexstat
).pipe(utils.update_comex_25)

## Arquivos Finais (TDM com Brasil e Comexstat)
df_tdm = utils.ajustes_tdm_final(
    df_comexstat=df_comexstat, df_tdm_sem_brasil=df_tdm_sem_brasil
)

df_comexstat = df_comexstat[cols_comexstat]

## Salvando os arquivos de revestimentos ceramicos
df_comexstat.to_csv(
    "D:/CEI/BI_ANFACER/BI/arquivos/comexstat.csv",
    sep=";",
    index=False,
    encoding="utf-8",
)
df_tdm.to_csv(
    "D:/CEI/BI_ANFACER/BI/arquivos/tdm.csv", sep=";", index=False, encoding="utf-8"
)

df_comexstat.to_csv("data/comexstat.csv", sep=";", index=False, encoding="utf-8")
df_tdm.to_csv("data/tdm.csv", sep=";", index=False, encoding="utf-8")

# Loucas Sanitarias
## Arquivo TDM sem Brasil
df_tdm_sem_brasil_sanitarios = utils.ajuste_tdm_sem_brasil(
    df_tdm_raw, lista_sh6_loucas_sanitarias
)

## Comercio exterior do Brasil a partir do Comexstat
df_exp_comexstat_sanitarios = utils.ajustes_exp_comexstat(
    df_exp_raw,
    utils.exp_updates_config,
    lista_sh6_loucas_sanitarias,
    trad_ncm,
    trad_cod_pais,
)
df_imp_comexstat_sanitarios = utils.ajustes_imp_comexstat(
    df_imp_raw,
    utils.imp_updates_config,
    lista_sh6_loucas_sanitarias,
    trad_ncm,
    trad_cod_pais,
)
df_comexstat_sanitarios = utils.ajustes_comexstat_final(
    df_exp_comexstat=df_exp_comexstat_sanitarios,
    df_imp_comexstat=df_imp_comexstat_sanitarios,
).pipe(utils.update_comex_25)

## Arquivos Finais (TDM com Brasil e Comexstat)
df_tdm_sanitarios = utils.ajustes_tdm_final(
    df_comexstat=df_comexstat_sanitarios,
    df_tdm_sem_brasil=df_tdm_sem_brasil_sanitarios,
)

df_comexstat_sanitarios = df_comexstat_sanitarios[cols_comexstat]

## Salvando os arquivos de loucas sanitarias
df_comexstat_sanitarios.to_csv(
    "D:/CEI/BI_ANFACER/BI/arquivos/comexstat_loucas.csv",
    sep=";",
    index=False,
    encoding="utf-8",
)

df_tdm_sanitarios.to_csv(
    "D:/CEI/BI_ANFACER/BI/arquivos/tdm_loucas.csv",
    sep=";",
    index=False,
    encoding="utf-8",
)
df_tdm_sanitarios.to_csv("data/tdm_loucas.csv", sep=";", index=False, encoding="utf-8")
print("Script finalizado, arquivos salvos")
script_end_time = time.time()
total_elapsed_time = script_end_time - script_start_time
print(f"Script completo executado em {total_elapsed_time:.2f} segundos.")
