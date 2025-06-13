# %%
import pandas as pd

anos = range(1997, 2026)
caminho = "../data/comexstat_parcial/"

lista_exp = []
for ano in anos:
    df_exp = pd.read_csv(f"{caminho}EXP_{ano}.csv", sep=";", engine="pyarrow")
    lista_exp.append(df_exp)

df_exp_completa = pd.concat(lista_exp, ignore_index=True)

lista_imp = []
for ano in anos:
    df_imp = pd.read_csv(f"{caminho}IMP_{ano}.csv", sep=";", engine="pyarrow")
    lista_imp.append(df_imp)

df_imp_completa = pd.concat(lista_imp, ignore_index=True)

df_exp_completa.to_csv(
    "../data/EXP_COMPLETA.csv",
    sep=";",
    index=False,
    encoding="utf-8",
)

df_imp_completa.to_csv(
    "../data/IMP_COMPLETA.csv", sep=";", index=False, encoding="utf-8"
)
