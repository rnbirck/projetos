# %%
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"

# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")
trad_mun = pd.read_sql_query("SELECT id_municipio, municipio FROM municipio", engine)
caminho_censo = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/Matrículas/microdados/"
caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/Rendimento/data/"


def ajuste_censo(ano, colunas_censo):
    return (
        pd.read_csv(
            caminho_censo + f"microdados_ed_basica_{ano}.csv",
            sep=";",
            encoding="latin1",
            engine="pyarrow",
        )
        .query("SG_UF == 'RS'")[colunas_censo]
        .assign(qntd_escolas=1)
        .groupby(["NU_ANO_CENSO", "CO_MUNICIPIO", "TP_DEPENDENCIA"])
        .sum()
        .reset_index()
        .assign(
            TP_DEPENDENCIA=lambda x: x["TP_DEPENDENCIA"].map(
                {1: "federal", 2: "estadual", 3: "municipal", 4: "privada"}
            )
        )
        .rename(
            columns={
                "NU_ANO_CENSO": "ano",
                "CO_MUNICIPIO": "id_municipio",
                "TP_DEPENDENCIA": "dependencia",
                "QT_MAT_INF_CRE": "mat_infantil_creche",
                "QT_MAT_BAS": "mat_basico",
                "QT_MAT_INF": "mat_infantil",
                "QT_MAT_FUND": "mat_fundamental",
                "QT_MAT_MED": "mat_medio",
                "QT_MAT_PROF": "mat_profissional",
                "QT_MAT_EJA": "mat_eja",
                "QT_DOC_BAS": "docentes_basico",
                "QT_DOC_INF": "docentes_infantil",
                "QT_DOC_FUND": "docentes_fundamental",
                "QT_DOC_MED": "docentes_medio",
                "QT_DOC_PROF": "docentes_profissional",
                "QT_DOC_EJA": "docentes_eja",
                "QT_TUR_BAS": "turmas_basico",
                "QT_TUR_INF": "turmas_infantil",
                "QT_TUR_FUND": "turmas_fundamental",
                "QT_TUR_MED": "turmas_medio",
                "QT_TUR_PROF": "turmas_profissional",
                "QT_TUR_EJA": "turmas_eja",
            }
        )
        .astype({"id_municipio": str})
        .merge(trad_mun, how="left", on="id_municipio")
    )


def ajuste_creche():
    anos_creche = range(2018, 2025)
    coluna_id_municipio = "Código IBGE"
    colunas_anos = [f"\u200b{ano} (%)" for ano in anos_creche]
    colunas_selecionadas = [coluna_id_municipio] + colunas_anos
    return (
        pd.read_excel(caminho_censo + "taxa_matricula_creche_raw.xlsx", skiprows=2)[
            colunas_selecionadas
        ]
        .melt(
            id_vars=[coluna_id_municipio],
            var_name="ano",
            value_name="taxa_matricula_creche",
        )
        .assign(
            ano=lambda x: x["ano"].str.extract(r"(\d{4})")[0].astype(int),
            dependencia="municipal",
        )
        .rename(columns={coluna_id_municipio: "id_municipio"})
        .astype({"id_municipio": str})
    )


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


colunas_censo = [
    "NU_ANO_CENSO",
    "CO_MUNICIPIO",
    "TP_DEPENDENCIA",
    "QT_MAT_INF_CRE",
    "QT_MAT_BAS",
    "QT_MAT_INF",
    "QT_MAT_FUND",
    "QT_MAT_MED",
    "QT_MAT_PROF",
    "QT_MAT_EJA",
    "QT_DOC_BAS",
    "QT_DOC_INF",
    "QT_DOC_FUND",
    "QT_DOC_MED",
    "QT_DOC_PROF",
    "QT_DOC_EJA",
    "QT_TUR_BAS",
    "QT_TUR_INF",
    "QT_TUR_FUND",
    "QT_TUR_MED",
    "QT_TUR_PROF",
    "QT_TUR_EJA",
]
# CENSO
anos_censo = range(2018, 2025)
lista_censo = []
for ano in anos_censo:
    lista_censo.append(ajuste_censo(ano, colunas_censo))

censo = pd.concat(lista_censo, ignore_index=True)

creche = ajuste_creche()

educ_matriculas = pd.merge(
    censo, creche, how="left", on=["ano", "id_municipio", "dependencia"]
).assign(taxa_matricula_creche=lambda x: x["taxa_matricula_creche"].fillna(0))

educ_matriculas.to_csv(
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Educação/Matrículas/base_censo.csv",
    index=False,
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
