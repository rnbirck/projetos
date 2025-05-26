# %%
import pandas as pd
import utils
import importlib
from utils import tradutor_uf
from utils import tradutor_regiao

importlib.reload(utils)
caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Orbis/2025/"
caminho_resultado = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/0_resultados_investimentos/2025/"

uf_selecionada = "SC"
regiao_selecionada = "Sul"

tradutor_orbis_cnae = pd.read_excel(caminho + "tradutores/tradutor_orbis_cnae.xlsx")
tradutor_div_grupo = pd.read_excel(caminho + "tradutores/tradutor_cnae_div_grupo.xlsx")
tradutor_orbis = pd.read_excel(caminho + "tradutores/tradutor_setor_orbis.xlsx")
tradutor_pais = pd.read_excel(caminho + "tradutores/tradutor_pais.xlsx")
df_rais_raw = pd.read_excel(caminho + "rais_2023.xlsx")
df_orbis_18_22_raw = pd.read_excel(caminho + "orbis_18_22.xlsx", sheet_name="Results")
df_orbis_23_raw = pd.read_excel(caminho + "orbis_23.xlsx", sheet_name="Results")
df_orbis_24_raw = pd.read_excel(caminho + "orbis_24.xlsx", sheet_name="Results")
df_orbis_uf = pd.read_excel(caminho + "orbis_br.xlsx", sheet_name="Results")

# DF orbis
df_orbis_18_22 = utils.ajuste_orbis(df_orbis_18_22_raw)
df_orbis_23 = utils.ajuste_orbis(df_orbis_23_raw)
df_orbis_24 = utils.ajuste_orbis(df_orbis_24_raw)
df_orbis = pd.concat([df_orbis_18_22, df_orbis_23, df_orbis_24])

anos_iniciais = (2018, 2019, 2021)
anos_iniciais_coluna = "_".join([str(ano)[-2:] for ano in anos_iniciais])
anos_finais = (2022, 2023, 2024)
anos_finais_coluna = "_".join([str(ano)[-2:] for ano in anos_finais])

# Investimentos
df_investimento_br = utils.ajuste_investimento_br(
    df_orbis=df_orbis,
    anos_iniciais=anos_iniciais,
    anos_iniciais_coluna=anos_iniciais_coluna,
    anos_finais=anos_finais,
    anos_finais_coluna=anos_finais_coluna,
)

df_investimento_mundo = utils.ajuste_investimento_mundo(
    df_orbis=df_orbis,
    anos_iniciais=anos_iniciais,
    anos_iniciais_coluna=anos_iniciais_coluna,
    anos_finais=anos_finais,
    anos_finais_coluna=anos_finais_coluna,
)

df_investimento = utils.ajuste_investimento_final(
    df_investimento_mundo,
    df_investimento_br,
    anos_iniciais_coluna=anos_iniciais_coluna,
    anos_finais_coluna=anos_finais_coluna,
    tradutor_orbis=tradutor_orbis,
)

# Setores selecionados
filtro_setores_selecionados = df_investimento["setor"].to_list()

# RAIS
df_rais_divisao = utils.ajuste_rais_investimentos(
    df_rais_raw, "divisao", tradutor_div_grupo
)
df_rais_grupo = utils.ajuste_rais_investimentos(
    df_rais_raw, "grupo", tradutor_div_grupo
)
df_rais = pd.concat([df_rais_divisao, df_rais_grupo])

df_rais_uf = utils.ajuste_rais_uf(
    df_rais=df_rais,
    tradutor_orbis_cnae=tradutor_orbis_cnae,
    tradutor_orbis=tradutor_orbis,
    filtro_setores_selecionados=filtro_setores_selecionados,
    uf_selecionada=uf_selecionada,
)

# Setores
df_paises_setor = (
    pd.concat(
        [
            utils.paises_setor(df_orbis, "==", "brazil", tradutor_pais),
            utils.paises_setor(df_orbis, "!=", "mundo", tradutor_pais),
        ],
        axis=1,
    )
    .reset_index()
    .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")[
        ["setor_orbis_trad", "principais_paises_mundo", "principais_paises_brazil"]
    ]
    .rename(columns={"setor_orbis_trad": "setor"})
)
# Tabela Final Setores UF
df_tab_setores_uf = (
    df_rais_uf[
        ["sigla_uf", "setor", "participacao_massa_salarial", "participacao_vinculos"]
    ]
    .merge(df_investimento, on="setor", how="left")
    .merge(df_paises_setor, on="setor", how="left")
    .sort_values("participacao_vinculos", ascending=False)
    .reset_index(drop=True)
)
# Investimentos Anunciados ORBIS - REGIAO
filtro_setores_selecionados_uf = df_tab_setores_uf["setor"].unique()
anos_orbis = range(2019, 2025)

df_orbis_regiao = utils.ajuste_orbis_regiao(
    df_orbis_uf,
    tradutor_uf=tradutor_uf,
    tradutor_regiao=tradutor_regiao,
    regiao_selecionada=regiao_selecionada,
    anos_orbis=anos_orbis,
)

df_orbis_uf_br = utils.ajuste_orbis_regiao(
    df_orbis_uf=df_orbis_uf,
    tradutor_uf=tradutor_uf,
    tradutor_regiao=tradutor_regiao,
    anos_orbis=anos_orbis,
    regiao_selecionada=None,
)
# INVESTIMENTOS ANUNCIADOS ORBIS - UF - SETOR
df_orbis_uf_setor = utils.ajuste_orbis_uf_setor(
    df_orbis_uf=df_orbis_uf,
    tradutor_uf=tradutor_uf,
    tradutor_orbis=tradutor_orbis,
    anos_orbis=anos_orbis,
    uf_selecionada=uf_selecionada,
)
# INVESTIMENTOS ANUNCIADOS ORBIS - UF - PAISES
df_orbis_uf_pais = utils.ajuste_orbis_uf_pais(
    df_orbis_uf=df_orbis_uf,
    tradutor_uf=tradutor_uf,
    tradutor_pais=tradutor_pais,
    anos_orbis=anos_orbis,
    uf_selecionada=uf_selecionada,
)
# INVESTIMENTOS ANUNCIADOS ORBIS - UF - EMPRESAS
df_orbis_uf_empresa = utils.ajuste_orbis_uf_empresa(
    df_orbis_uf=df_orbis_uf,
    tradutor_uf=tradutor_uf,
    anos_orbis=anos_orbis,
    uf_selecionada=uf_selecionada,
)
# EMPRESAS QUE INVESTEM NO MUNDO E NAO NO BRASIL
df_empresas_nao_investem_brasil = utils.ajuste_empresas_nao_investem_brasil(
    df_orbis=df_orbis,
    tradutor_orbis=tradutor_orbis,
    anos_iniciais=anos_iniciais,
    df_tab_setores_uf=df_tab_setores_uf,
)
# EMPRESAS QUE INVESTEM NO BR E NAO INVESTEM NA UF
df_empresas_investem_brasil = utils.ajuste_empresas_investem_brasil(
    df_orbis=df_orbis,
    tradutor_orbis=tradutor_orbis,
    anos_iniciais=anos_iniciais,
)
df_empresas_investem_uf = utils.ajuste_empresas_investem_uf(
    df_orbis_uf=df_orbis_uf,
    tradutor_orbis=tradutor_orbis,
    anos_iniciais=anos_iniciais,
    uf_selecionada=uf_selecionada,
)
empresas_nao_selecionadas = utils.encontrar_empresas_nao_selecionadas(
    df_empresas_investem_brasil, df_empresas_investem_uf
)

df_empresas_nao_investem_uf = pd.DataFrame.from_dict(
    empresas_nao_selecionadas, orient="index"
).reset_index()

df_empresas_nao_investem_uf.columns = ["setor", "empresas"]
df_empresas_nao_investem_uf = df_empresas_nao_investem_uf.query(
    "setor in @filtro_setores_selecionados_uf"
).reset_index(drop=True)

# Tabela Geral UF
orbis_uf = (
    df_orbis_uf.pipe(utils.ajuste_orbis_uf)
    .merge(tradutor_uf, on="uf", how="left")
    .query("sigla_uf == @uf_selecionada")
    .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
    .merge(tradutor_pais, left_on="pais_origem", right_on="pais_eng", how="left")[
        [
            "ano",
            "uf_ajustada",
            "pais",
            "empresa",
            "setor_orbis_trad",
            "total_investimento",
        ]
    ]
)


# Salvando os resultados
with pd.ExcelWriter(
    caminho_resultado + f"{uf_selecionada}_resultados_investimentos.xlsx",
    mode="w",
    engine="openpyxl",
) as writer:
    orbis_uf.to_excel(writer, sheet_name="Investimentos UF", index=False)
    df_orbis_uf_br.to_excel(writer, sheet_name="Investimentos UF Brasil", index=False)
    df_orbis_regiao.to_excel(writer, sheet_name="Investimentos Região", index=False)
    df_orbis_uf_setor.to_excel(writer, sheet_name="Investimentos UF Setor", index=False)
    df_orbis_uf_pais.to_excel(writer, sheet_name="Investimentos UF País", index=False)
    df_orbis_uf_empresa.to_excel(
        writer, sheet_name="Investimentos UF Empresa", index=False
    )
    df_tab_setores_uf.to_excel(
        writer, sheet_name="Setores Maior Atracao Invest UF", index=False
    )
    df_empresas_nao_investem_brasil.to_excel(
        writer, sheet_name="Empresas Não Investem Brasil", index=False
    )
    df_empresas_nao_investem_uf.to_excel(
        writer, sheet_name="Empresas Não Investem UF", index=False
    )
