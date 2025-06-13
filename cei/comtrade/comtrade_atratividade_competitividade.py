# %%
import pandas as pd
import comtradeapicall

subscription_key = "cab30bdb6ad34b5e8b7bef73cadb9b68"
sh6_abicalcados = "640110,640192,640199,640212,640219,640220,640291,640299,640312,640319,640320,640340,640351,640359,640391,640399,640411,640419,640420,640510,640520,640590"
cols = [
    "refYear",
    "reporterCode",
    "reporterDesc",
    "partnerCode",
    "partnerDesc",
    "flowDesc",
    "cmdCode",
    "cifvalue",
    "fobvalue",
    "netWgt",
]
anos = range(2019, 2024)
df_raw_ = {}
for ano in anos:
    df_raw_[ano] = comtradeapicall.getFinalData(
        subscription_key,
        typeCode="C",
        freqCode="A",
        clCode="HS",
        cmdCode=sh6_abicalcados,
        flowCode="X,M",
        reporterCode=None,
        partnerCode=None,
        partner2Code=None,
        customsCode=None,
        motCode=None,
        maxRecords=None,
        breakdownMode="classic",
        countOnly=None,
        includeDesc=True,
        period=str(ano),
    )

df_raw = pd.concat(df_raw_.values(), ignore_index=True)
df = df_raw[cols]

df.to_excel(
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/ABICALÇADOS/Oportunidade Internacional/2025/base.xlsx",
    index=False,
)
