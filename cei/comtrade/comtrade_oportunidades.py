# %%
import pandas as pd
import comtradeapicall
import os
import xlwings as xw
import time

subscription_key = "cab30bdb6ad34b5e8b7bef73cadb9b68"

pais = "China"
cod_reporter = 76  # Brasil
cod_partner = 156  # China

caminho_resultados = f"D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/PROGRAMACAO/OPORTUNIDADES_MARKET_SHARE/{pais}/ORIGINAL/"
# Colunas de interesse
cols_exp = [
    "refYear",
    "reporterCode",
    "reporterDesc",
    "flowCode",
    "partnerCode",
    "partnerDesc",
    "cmdCode",
    "fobvalue",
]
cols_imp = [
    "refYear",
    "reporterCode",
    "reporterDesc",
    "flowCode",
    "partnerCode",
    "partnerDesc",
    "cmdCode",
    "cifvalue",
]


# Função para ajustar o dataframe no formato dos arquivos oportunidades
def ajuste_df_comtrade(df, colunas):
    return (
        df[colunas]
        .loc[lambda x: x["cmdCode"].str.len() == 6]
        .assign(
            flowCode=lambda x: x["flowCode"].replace({"X": "Export", "M": "Import"})
        )
        .rename(
            columns={
                "refYear": "Year",
                "reporterCode": "Reporter Code",
                "reporterDesc": "Reporter Description",
                "partnerCode": "Partner Code",
                "partnerDesc": "Partner Description",
                "cmdCode": "Commodity Code",
                "flowCode": "Trade Flow Description",
                "fobvalue": "Value",
                "cifvalue": "Value",
            }
        )
    )


df_brasil_exp_pais_raw = comtradeapicall.getFinalData(
    subscription_key,
    typeCode="C",
    freqCode="A",
    clCode="HS",
    cmdCode="all",
    flowCode="X",
    reporterCode=cod_reporter,
    partnerCode=cod_partner,
    partner2Code=None,
    customsCode=None,
    motCode=None,
    maxRecords=None,
    breakdownMode="classic",
    countOnly=None,
    includeDesc=True,
    period="2020,2021,2022,2023",
)

df_pais_imp_brasil_raw = comtradeapicall.getFinalData(
    subscription_key,
    typeCode="C",
    freqCode="A",
    clCode="HS",
    cmdCode="all",
    flowCode="M",
    reporterCode=cod_partner,
    partnerCode=cod_reporter,
    partner2Code=None,
    customsCode=None,
    motCode=None,
    maxRecords=None,
    breakdownMode="classic",
    countOnly=None,
    includeDesc=True,
    period="2020,2021,2022,2023",
)

df_pais_imp_total_raw = comtradeapicall.getFinalData(
    subscription_key,
    typeCode="C",
    freqCode="A",
    clCode="HS",
    cmdCode="all",
    flowCode="M",
    reporterCode=cod_partner,
    partnerCode=0,
    partner2Code=None,
    customsCode=None,
    motCode=None,
    maxRecords=None,
    breakdownMode="classic",
    countOnly=None,
    includeDesc=True,
    period="2020,2021,2022,2023",
)
df_brasil_exp_total_raw = comtradeapicall.getFinalData(
    subscription_key,
    typeCode="C",
    freqCode="A",
    clCode="HS",
    cmdCode="all",
    flowCode="X",
    reporterCode=cod_reporter,
    partnerCode=0,
    partner2Code=None,
    customsCode=None,
    motCode=None,
    maxRecords=None,
    breakdownMode="classic",
    countOnly=None,
    includeDesc=True,
    period="2020,2021,2022,2023",
)

df_imp_pais_mundo_raw = comtradeapicall.getFinalData(
    subscription_key,
    typeCode="C",
    freqCode="A",
    clCode="HS",
    cmdCode="all",
    flowCode="M",
    reporterCode=cod_partner,
    partnerCode=None,
    partner2Code=None,
    customsCode=None,
    motCode=None,
    maxRecords=None,
    breakdownMode="classic",
    countOnly=None,
    includeDesc=True,
    period="2023",
)
### BRASIL EXP PAIS
df_brasil_exp_pais = ajuste_df_comtrade(df_brasil_exp_pais_raw, cols_exp)
df_brasil_exp_pais.to_excel(
    caminho_resultados + f"Brasil_Exp_{pais}.xlsx", index=False, sheet_name="Plan1"
)

### BRASIL EXP TOTAL
df_brasil_exp_total = ajuste_df_comtrade(df_brasil_exp_total_raw, cols_exp)
df_brasil_exp_total.to_excel(
    caminho_resultados + "Brasil_Exp_Total.xlsx", index=False, sheet_name="Plan1"
)

### PAIS IMP BRASIL
df_pais_imp_brasil = ajuste_df_comtrade(df_pais_imp_brasil_raw, cols_imp)
df_pais_imp_brasil.to_excel(
    caminho_resultados + f"{pais}_Imp_Brasil.xlsx", index=False, sheet_name="Plan1"
)

### PAIS IMP TOTAL
df_pais_imp_total = ajuste_df_comtrade(df_pais_imp_total_raw, cols_imp)
df_pais_imp_total.to_excel(
    caminho_resultados + f"{pais}_Imp_Total.xlsx", index=False, sheet_name="Plan1"
)

### IMP PAIS MUNDO
df_imp_pais_mundo = ajuste_df_comtrade(df_imp_pais_mundo_raw, cols_imp)
df_imp_pais_mundo.to_excel(
    caminho_resultados + f"Imp_{pais}_Mundo.xlsx", index=False, sheet_name="Plan1"
)

# Abrir os arquivos excel e salvar novamente para o SPSS ler
os.makedirs(caminho_resultados, exist_ok=True)

nomes_arquivos = [
    f"Brasil_Exp_{pais}.xlsx",
    "Brasil_Exp_Total.xlsx",
    f"{pais}_Imp_Brasil.xlsx",
    f"{pais}_Imp_Total.xlsx",
    f"Imp_{pais}_Mundo.xlsx",
]

arquivos_salvos = [os.path.join(caminho_resultados, nome) for nome in nomes_arquivos]

with xw.App(visible=False, add_book=False) as app:
    for arquivo_path in arquivos_salvos:
        wb = None
        try:
            # Abre o workbook no mesmo caminho
            wb = app.books.open(arquivo_path)
            # Salva o workbook no mesmo caminho (sobrescreve)
            wb.save()
            # Fecha o workbook
            wb.close()
            # Pequena pausa
            time.sleep(1)

        except Exception as e:
            print(f"Erro ao abrir o arquivo: {e}")
            if wb is not None:
                try:
                    wb.close()
                except Exception:
                    pass
