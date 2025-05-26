# %%
import pandas as pd

tradutor_tipologia = pd.read_csv(
    "data/ranqueamento/tradutor_tipologia.csv", engine="pyarrow", sep=";"
)
tradutor_reporter = pd.read_excel("D:/CEI/BI_ANFACER/BI/arquivos/dim/reporter.xlsx")[
    ["CTY_RPT", "nm_pais_ptbr_RPT"]
]
tdm = pd.read_csv("data/tdm.csv", engine="pyarrow", sep=";")

# %% EXP PAISES CERAMICA 2019-2023
anos = range(2019, 2024)
consulta = (
    tdm.assign(
        ano=lambda x: pd.to_datetime(x["DATE"]).dt.year,
    )
    .merge(tradutor_tipologia, how="left", on="SH6")
    .query("tipologia == 'ceramica' & ano in @anos & FLUXO == 'Exportação'")
    .groupby(["ano", "FLUXO", "CTY_RPT", "tipologia"], as_index=False)
    .agg(
        valor=("VALUE", "sum"),
    )
    .merge(tradutor_reporter, how="left", on="CTY_RPT")
    .rename(
        columns={
            "FLUXO": "fluxo",
            "nm_pais_ptbr_RPT": "reporter",
        }
    )[["ano", "fluxo", "reporter", "valor", "tipologia"]]
)

consulta.to_excel(
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/DEMANDAS EXTRAS/exp_ceramica_2019_2023.xlsx",
    index=False,
)
