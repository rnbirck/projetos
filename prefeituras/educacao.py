# %%
import pandas as pd
import numpy as np

caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/Rendimento/data/"


def ajuste_taxa_rendimento_escolar(ano):
    colunas_rendimento = [
        "NU_ANO_CENSO",
        "CO_MUNICIPIO",
        "NO_MUNICIPIO",
        "NO_DEPENDENCIA",
        "1_CAT_FUN",
        "1_CAT_FUN_AI",
        "1_CAT_FUN_AF",
        "2_CAT_FUN",
        "2_CAT_FUN_AI",
        "2_CAT_FUN_AF",
        "3_CAT_FUN",
        "3_CAT_FUN_AI",
        "3_CAT_FUN_AF",
    ]
    return (
        pd.read_excel(
            caminho + f"tx_rend_municipios_{ano}.xlsx", skiprows=8, engine="calamine"
        )
        .query("SG_UF == 'RS' & NO_CATEGORIA == 'Total'")
        .melt(
            id_vars=["NU_ANO_CENSO", "CO_MUNICIPIO", "NO_MUNICIPIO", "NO_DEPENDENCIA"],
            value_vars=colunas_rendimento[4:],
            var_name="categoria",
            value_name="valor",
        )
        .assign(
            categoria=lambda x: x["categoria"]
            .str.replace("1_CAT", "taxa_aprovacao")
            .str.replace("2_CAT", "taxa_reprovacao")
            .str.replace("3_CAT", "taxa_abandono")
            .str.replace("FUN", "fundamental")
            .str.replace("AI", "anos_iniciais")
            .str.replace("AF", "anos_finais"),
            valor=lambda x: pd.to_numeric(x.valor, errors="coerce"),
            CO_MUNICIPIO=lambda x: x.CO_MUNICIPIO.astype(int).astype(str),
        )
        .rename(
            columns={
                "NU_ANO_CENSO": "ano",
                "CO_MUNICIPIO": "id_municipio",
                "NO_MUNICIPIO": "municipio",
                "NO_DEPENDENCIA": "dependencia",
            }
        )
    )


def ajuste_taxa_distorcao_idade(ano):
    colunas_distorcao = [
        "NU_ANO_CENSO",
        "CO_MUNICIPIO",
        "NO_MUNICIPIO",
        "NO_DEPENDENCIA",
        "FUN_CAT_0",
        "FUN_AI_CAT_0",
        "FUN_AF_CAT_0",
    ]
    return (
        pd.read_excel(
            caminho + f"TDI_MUNICIPIOS_{ano}.xlsx", skiprows=8, engine="calamine"
        )
        .query("SG_UF == 'RS' & NO_CATEGORIA == 'Total'")
        .melt(
            id_vars=["NU_ANO_CENSO", "CO_MUNICIPIO", "NO_MUNICIPIO", "NO_DEPENDENCIA"],
            value_vars=colunas_distorcao[4:],
            var_name="categoria",
            value_name="valor",
        )
        .assign(
            categoria=lambda x: x["categoria"]
            .str.replace("_CAT_0", "")
            .str.replace("FUN", "fundamental")
            .str.replace("AI", "anos_iniciais")
            .str.replace("AF", "anos_finais"),
        )
        .assign(
            categoria=lambda x: "taxa_distorcao_" + x["categoria"],
            valor=lambda x: pd.to_numeric(x.valor, errors="coerce"),
            CO_MUNICIPIO=lambda x: x.CO_MUNICIPIO.astype(int).astype(str),
        )
        .rename(
            columns={
                "NU_ANO_CENSO": "ano",
                "CO_MUNICIPIO": "id_municipio",
                "NO_MUNICIPIO": "municipio",
                "NO_DEPENDENCIA": "dependencia",
            }
        )
    )


# RENDIMENTO ESCOLAR
anos_rendimento = range(2018, 2024)

lista_rendimento = []
for ano in anos_rendimento:
    lista_rendimento.append(ajuste_taxa_rendimento_escolar(ano))

rendimento = pd.concat(lista_rendimento, ignore_index=True)

# Distorção Idade
anos_distorcao = range(2019, 2025)

lista_distorcao = []
for ano in anos_distorcao:
    lista_distorcao.append(ajuste_taxa_distorcao_idade(ano))

distorcao = pd.concat(lista_distorcao, ignore_index=True)

# Juntando os dois dataframes
rendimento_distorcao = pd.concat([rendimento, distorcao]).to_csv(
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/Rendimento/tx_rendimento.csv",
    index=False,
)
