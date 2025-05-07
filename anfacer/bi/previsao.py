# %%
import pandas as pd

df_prev_exp_vol = pd.read_excel("data/previsao.xlsx", sheet_name="exp_vol")
df_prev_vendas_mi = pd.read_excel("data/previsao.xlsx", sheet_name="vendas_mi")
df_prev_anual = pd.read_excel("data/previsao.xlsx", sheet_name="ano")

df_prev_trimestre = pd.concat(
    [
        df_prev_exp_vol.assign(indicador="exp_vol"),
        df_prev_vendas_mi.assign(indicador="vendas_mi"),
    ]
)

df_prev_trimestre = df_prev_trimestre.assign(
    ano=lambda x: "20" + x["trimestre"].str.slice(3, 5),
    trimestre=lambda x: x["trimestre"].str.slice(0, 2),
    data=lambda x: pd.to_datetime(
        x.apply(lambda row: f"{row['ano']}-{int(row['trimestre']) * 3 - 2}-01", axis=1)
    ),
    indicador=lambda x: x["indicador"].replace(
        {"exp_vol": "Exportação (m²)", "vendas_mi": "Vendas Mercado Interno"}
    ),
)

df_prev_anual = df_prev_anual.dropna().assign(
    indicador=lambda x: x["indicador"].replace(
        {
            "exp_vol": "Exportação (m²)",
            "vendas_mercado_interno": "Vendas Mercado Interno",
        }
    )
)

with pd.ExcelWriter(
    "D:/CEI/BI_ANFACER/BI/arquivos/previsao.xlsx", mode="w", engine="xlsxwriter"
) as writer:
    df_prev_trimestre.to_excel(writer, sheet_name="trimestre", index=False)
    df_prev_anual.to_excel(writer, sheet_name="ano", index=False)
