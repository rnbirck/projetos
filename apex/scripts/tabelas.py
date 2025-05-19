# %%
import pandas as pd
import utils
import importlib
from google.cloud import bigquery

importlib.reload(utils)
billing_project_id = "gold-braid-417822"
client = bigquery.Client(project=billing_project_id)
caminho = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/0_bases_gerais/"
caminho_resultado = "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/1_arquivos_tabela_graficos/2025/"

print("Iniciando a execução do script de tabelas")

uf_selecionada = "SC"

# Tradutores
tradutor_grupo = pd.read_excel(caminho + "trad_cuci.xlsx", engine="calamine").pipe(
    utils.ajuste_tradutores, colunas_tamanhos={"id_sh6": 6}
)
tradutor_uf_regiao = pd.read_excel(caminho + "trad_uf.xlsx", engine="calamine")
tradutor_isic = pd.read_excel(caminho + "trad_isic.xlsx", engine="calamine").pipe(
    utils.ajuste_tradutores, colunas_tamanhos={"id_sh6": 6}
)
tradutor_ncm = (
    pd.read_excel(caminho + "trad_sh6.xlsx", engine="calamine")[["id_sh6", "id_ncm"]]
    .drop_duplicates()
    .pipe(utils.ajuste_tradutores, colunas_tamanhos={"id_sh6": 6, "id_ncm": 8})
)
tradutor_via = (
    pd.read_excel(caminho + "trad_via.xlsx", engine="calamine")
    .rename(columns={"chave": "id_via", "valor": "via"})
    .drop_duplicates()
    .assign(id_via=lambda x: x["id_via"].astype(str))[["id_via", "via"]]
)
tradutor_sh4 = pd.read_excel(caminho + "trad_sh4.xlsx", engine="calamine").pipe(
    utils.ajuste_tradutores,
    colunas_tamanhos={"id_sh4": 4},
)
tradutor_sh6 = (
    pd.read_excel(caminho + "trad_sh6.xlsx", engine="calamine")[["id_sh6", "desc_sh6"]]
    .drop_duplicates()
    .pipe(
        utils.ajuste_tradutores,
        colunas_tamanhos={"id_sh6": 6},
    )
)
tradutor_mun = pd.read_excel(caminho + "trad_mun.xlsx", engine="calamine")
tradutor_pais = pd.read_excel(caminho + "trad_pais.xlsx", engine="calamine")
query_mesorregiao_mun = """
SELECT
    id_municipio,
    nome as municipio,
    nome_mesorregiao,
    sigla_uf	
FROM
    `basedosdados.br_bd_diretorios_brasil.municipio`
"""
tradutor_mesorregiao = (
    client.query(query_mesorregiao_mun).to_dataframe().drop_duplicates()
)

anos = list(range(2013, 2025))
ano_maximo = 2024
ano_minimo = 2019
df_exp_completa = (
    pd.read_csv(
        "../../data/EXP_COMPLETA.csv", sep=";", encoding="latin1", engine="pyarrow"
    )
    .query("CO_ANO in @anos")
    .reset_index(drop=True)
)

df_imp_completa = (
    pd.read_csv(
        "../../data/IMP_COMPLETA.csv", sep=";", encoding="latin1", engine="pyarrow"
    )
    .query("CO_ANO in @anos")
    .reset_index(drop=True)
)

df_exp_mun = (
    pd.read_csv(
        "../../data/EXP_COMPLETA_MUN.csv", sep=";", encoding="latin1", engine="pyarrow"
    )
    .query("CO_ANO in @anos")
    .reset_index(drop=True)
)
# Gráfico 1 - EXP e PIB
df_exp_regiao = utils.gerar_exp_regiao(
    df_exp_completa=df_exp_completa,
    tradutor_uf_regiao=tradutor_uf_regiao,
)
df_part_exp_uf_regiao = utils.gerar_part_exp_uf_regiao(
    df_exp_completa=df_exp_completa,
    tradutor_uf_regiao=tradutor_uf_regiao,
    df_exp_regiao=df_exp_regiao,
    uf_selecionada=uf_selecionada,
)
# Gráfico 2 - EXP UF e REGIAO
df_exp_uf_regiao = utils.gerar_exp_uf_regiao(
    df_exp_completa=df_exp_completa,
    tradutor_uf_regiao=tradutor_uf_regiao,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
)

df_exp_uf_historico = utils.gerar_exp_uf_historico(
    df_exp_completa=df_exp_completa,
    uf_selecionada=uf_selecionada,
)

# Gráfico 4 - EXP VIA
df_exp_via = utils.gerar_exp_via(
    df_exp_completa=df_exp_completa,
    uf_selecionada=uf_selecionada,
    tradutor_via=tradutor_via,
)

# Gráfico 5 - BALANCA COMERCIAL
df_balanca = utils.gerar_balanca_comercial(
    df_exp_completa=df_exp_completa,
    df_imp_completa=df_imp_completa,
    uf_selecionada=uf_selecionada,
)

# Tabela 1 - EXPORTACOES MUNICIPIOS
df_exp_mun_uf = utils.gerar_exp_mun_uf(
    df_exp_mun=df_exp_mun,
    uf_selecionada=uf_selecionada,
    ano_minimo=ano_minimo,
    tradutor_sh4=tradutor_sh4,
    tradutor_mun=tradutor_mun,
)

df_exp_part_mun = utils.gerar_exp_part_mun(
    df_exp_mun_uf=df_exp_mun_uf,
    ano_maximo=ano_maximo,
)

filtro_mun = df_exp_part_mun["mun"].unique()
df_exp_mun_sh4 = utils.gerar_exp_mun_sh4(
    df_exp_mun_uf=df_exp_mun_uf,
    filtro_mun=filtro_mun,
    ano_maximo=ano_maximo,
)

# Tabela 2 - EXPORTACAO MESORREGIAO
df_exp_mesorregioes = utils.gerar_exp_mesorregioes(
    df_exp_mun=df_exp_mun,
    uf_selecionada=uf_selecionada,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    tradutor_mesorregiao=tradutor_mesorregiao,
)

# Figura 2 - MACROSSETORES
df_exp_macrossetores = utils.gerar_exp_macrossetores(
    df_exp_completa=df_exp_completa,
    uf_selecionada=uf_selecionada,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    tradutor_ncm=tradutor_ncm,
    tradutor_isic=tradutor_isic,
)

# Tabela 3 - GRUPO CUCI
df_exp_grupo = utils.gerar_exp_grupo(
    df_exp_completa=df_exp_completa,
    uf_selecionada=uf_selecionada,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    tradutor_ncm=tradutor_ncm,
    tradutor_grupo=tradutor_grupo,
)

# Tabela 4 - EXPORTACAO DESTINOS
df_exp_destinos = utils.gerar_exp_destinos(
    df_exp_completa=df_exp_completa,
    uf_selecionada=uf_selecionada,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    tradutor_pais=tradutor_pais,
)

# Tabelas Auxiliares
df_tabela_auxiliar = utils.gerar_tabela_auxiliar(
    df_exp_completa=df_exp_completa,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    uf_selecionada=uf_selecionada,
    tradutor_ncm=tradutor_ncm,
    tradutor_pais=tradutor_pais,
    tradutor_via=tradutor_via,
    tradutor_grupo=tradutor_grupo,
    tradutor_sh6=tradutor_sh6,
)

df_tabela_auxiliar_uf = utils.gerar_tabela_auxiliar_uf(
    df_exp_mun=df_exp_mun,
    ano_minimo=ano_minimo,
    ano_maximo=ano_maximo,
    uf_selecionada=uf_selecionada,
    tradutor_sh4=tradutor_sh4,
    tradutor_mun=tradutor_mun,
    tradutor_pais=tradutor_pais,
    tradutor_mesorregiao=tradutor_mesorregiao,
)

# Salvando os arquivos

base_tabelas_graficos = f"{uf_selecionada}_base_tabelas_graficos.xlsx"

with pd.ExcelWriter(
    caminho_resultado + base_tabelas_graficos, engine="openpyxl"
) as writer:
    df_part_exp_uf_regiao.to_excel(
        writer, sheet_name="Participação da UF na Região", index=False
    )
    df_exp_uf_regiao.to_excel(
        writer, sheet_name="Exportação por UF e Região", index=False
    )
    df_exp_uf_historico.to_excel(
        writer, sheet_name="Histório da Exportação da UF", index=False
    )
    df_exp_via.to_excel(writer, sheet_name="Exportação por VIA", index=False)
    df_balanca.to_excel(writer, sheet_name="Balança Comercial", index=False)
    df_exp_part_mun.to_excel(writer, sheet_name="Part dos Mun em 2024", index=False)
    df_exp_mun_sh4.to_excel(writer, sheet_name="Part SH4 por Mun", index=False)
    df_exp_mesorregioes.to_excel(
        writer, sheet_name="Exportação por Mesorregião", index=False
    )
    df_exp_macrossetores.to_excel(
        writer, sheet_name="Exportação por Macrosetor", index=False
    )
    df_exp_grupo.to_excel(writer, sheet_name="Exportação por Grupo", index=False)
    df_exp_destinos.to_excel(writer, sheet_name="Exportação por Destinos", index=False)

base_tabelas_auxiliares = f"{uf_selecionada}_base_tabelas_auxiliares.xlsx"

with pd.ExcelWriter(
    caminho_resultado + base_tabelas_auxiliares, engine="openpyxl"
) as writer:
    df_tabela_auxiliar.to_excel(writer, sheet_name="Base Exp", index=False)
    df_tabela_auxiliar_uf.to_excel(writer, sheet_name="Base Exp UF", index=False)
    df_exp_mun_uf.to_excel(writer, sheet_name="Base Exp Municipios", index=False)

print("Execução do script de tabelas finalizada")
print("Arquivos salvos em: ", caminho_resultado)
