# %%
import pandas as pd
import utils
import importlib
from google.cloud import bigquery
from sqlalchemy import create_engine

importlib.reload(utils)

billing_project_id = "gold-braid-417822"
client = bigquery.Client(project=billing_project_id)

ano_maximo = 2024
ano_minimo = 2019

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

# Municípios do Rio Grande do Sul
municipios = pd.read_sql_query(utils.query_municipios, engine)
filtro_mun_rs = municipios["id_municipio"].to_list()
populacao = utils.ajustes_populacao(filtro_mun_rs)
populacao_mulher = utils.ajustes_populacao_mulher(filtro_mun_rs)

# PIB Municipios
pib = client.query(utils.query_pib).to_dataframe()
pib_per_capita = utils.ajustes_pib_per_capita(
    pib=pib, filtro_mun_rs=filtro_mun_rs, populacao=populacao
)

# Exportacao per capita
comex = pd.read_csv(
    utils.caminho_comex, engine="pyarrow", usecols=["ano", "id_municipio", "vl_fob"]
)
comex_per_capita = utils.ajustes_comex_per_capita(
    comex=comex, populacao=populacao, ano_minimo=ano_minimo, ano_maximo=ano_maximo
)

# Renda e Vinculos
renda_vinculos_raw = client.query(utils.query_renda_vinculos).to_dataframe()

# Renda Média
renda_media = utils.ajustes_renda_media(
    renda_vinculos_raw=renda_vinculos_raw, municipios=municipios
)

# Vinculos per capita
vinculos_per_capita = utils.ajustes_vinculos_per_capita(
    renda_vinculos_raw=renda_vinculos_raw, populacao=populacao
)

# Formalidade Mercado de Trabalh
formalidade_mercado_trabalho = utils.ajustes_formalidade_mercado_trabalho(
    renda_vinculos_raw=renda_vinculos_raw
)

# Geracao de Emprego per capita
caged_antigo_raw = client.query(utils.query_caged_antigo).to_dataframe()
caged_raw = pd.read_sql_query(utils.query_caged, engine)
geracao_emprego_per_capita = utils.ajustes_geracao_emprego_per_capita(
    caged_antigo_raw=caged_antigo_raw,
    caged_raw=caged_raw,
    populacao=populacao,
)

# Vulnerabilidade Social
vulnerabilidade_social = utils.ajustes_vulnerabilidade_social(
    populacao=populacao, ano_minimo=ano_minimo, ano_maximo=ano_maximo
)

# Indicadores Financeiros
indicadores_financeiros = (
    pd.read_csv(
        "data/indicadores_financeiros.csv",
        engine="pyarrow",
        sep=";",
    )
    .query(f"ano >= {ano_minimo}")
    .astype({"id_municipio": str})
)

# Indicadores de Seguranca
indicadores_seguranca = utils.ajustes_indicadores_seguranca(
    populacao=populacao, ano_minimo=ano_minimo, ano_maximo=ano_maximo
)

indicadores_violencia_contra_mulher = utils.ajustes_indicadores_violencia_mulher(
    populacao_mulher=populacao_mulher, ano_minimo=ano_minimo, ano_maximo=ano_maximo
)

# Indicadores de Saude
indicadores_saude = utils.ajustes_indicadores_saude(
    ano_minimo=ano_minimo, ano_maximo=ano_maximo, municipios=municipios
)

# Obitos evitaveis per capita * 1000
obitos_evitaveis_per_capita = utils.ajustes_obitos_evitaveis(
    populacao=populacao, municipios=municipios
)

#  SISAB
sisab = utils.ajustes_sisab(municipios=municipios)

# Taxa Cobertura de Matriculas Creche Municipal
matriculas_creche = utils.ajustes_matriculas_creche()

# Notas Saeb
notas_saeb_anos_iniciais = utils.ajustes_notas_saeb(categoria="anos_iniciais")
notas_saeb_anos_finais = utils.ajustes_notas_saeb(categoria="anos_finais")

# Rendimento Educacao
rendimento_educacao = utils.ajustes_rendimento_educacao()

# Adequação da Formação Docente
anos_docentes = range(ano_minimo, ano_maximo + 1)
adequacao_docentes = pd.concat(
    [utils.ajustes_adequacao_docentes(ano) for ano in anos_docentes]
)

# Despesas com Educacao per Capita
despesas_educacao_per_capita = utils.ajuste_despesas_educacao(populacao=populacao)

# Emissao de Gases de Efeito Estufa per Capita
emissao_gases_per_capita = utils.ajuste_emissao(
    populacao=populacao,
)

# Agua
agua_raw = client.query(utils.query_snis).to_dataframe()
agua = utils.ajuste_agua(df_raw=agua_raw, populacao=populacao)

# Residuos
residuos = utils.ajuste_residuos(
    municipios=municipios,
)

# %%
importlib.reload(utils)


# PIB PER CAPITA
df_pib_per_capita = utils.ajuste_df(pib_per_capita, "pib_per_capita")

# COMEX PER CAPITA
df_comex_per_capita = utils.ajuste_df(comex_per_capita, "exportacao_per_capita")

# RENDA MEDIA
df_renda_media = utils.ajuste_df(renda_media, "renda_media")

# VINCULOS PER CAPITA
df_vinculos_per_capita = utils.ajuste_df(vinculos_per_capita, "vinculos_per_capita")

# FORMALIDADE MERCADO DE TRABALHO
df_formalidade_mercado_trabalho = utils.ajuste_df(
    formalidade_mercado_trabalho, "formalidade_mercado_trabalho"
)

# GERACAO DE EMPREGO PER CAPITA
df_geracao_emprego_per_capita = utils.ajuste_df(
    geracao_emprego_per_capita, "geracao_emprego_per_capita"
)

# VULNERABILIDADE SOCIAL
df_vulnerabilidade_social = utils.ajuste_df(
    vulnerabilidade_social, "vulnerabilidade_social"
)

# INDICADORES FINANCEIROS
lista_indicadores_financeiros = [
    "indicador_exec_orc_corrente",
    "indicador_autonomia_fiscal",
    "indicador_endividamento",
    "indicador_despesas_pessoal",
    "indicador_investimentos",
    "indicador_disponibilidade_caixa",
    "indicador_geracao_caixa",
    "indicador_restos_a_pagar",
]
df_indicadores_financeiros = utils.ajuste_df_lista(
    indicadores_financeiros, lista_indicadores_financeiros
)

# INDICADORES DE SEGURANCA
lista_indicadores_seguranca = [
    "homicidio_doloso_per_capita",
    "furtos_per_capita",
    "roubos_per_capita",
    "roubos_veiculos_per_capita",
    "delitos_armas_per_capita",
]
df_indicadores_seguranca = utils.ajuste_df_lista(
    indicadores_seguranca, lista_indicadores_seguranca
)

# INDICADORES DE VIOLENCIA CONTRA A MULHER
lista_indicadores_violencia_mulher = [
    "ameaca_per_capita",
    "estupro_per_capita",
]
df_indicadores_violencia_mulher = utils.ajuste_df_lista(
    indicadores_violencia_contra_mulher, lista_indicadores_violencia_mulher
)

# INDICADORES DE SAUDE
lista_indicadores_saude = ["taxa_obitos_infantis", "coef_neonatal", "prop_nasc_adolesc"]
df_indicadores_saude = utils.ajuste_df_lista(indicadores_saude, lista_indicadores_saude)

# OBITOS EVITAVEIS PER CAPITA
df_obitos_evitaveis_per_capita = utils.ajuste_df(
    obitos_evitaveis_per_capita, "obitos_evitaveis_per_capita"
)

# SISAB
lista_sisab = [
    "gestantes_pre_natal_sisab",
    "gestantes_odonto_sisab",
    "gestantes_hiv_sisab",
    "mulheres_aps_sisab",
    "crianças_vacinadas_sisab",
    "diabetes_sisab",
    "hipertensao_sisab",
]
df_sisab = utils.ajuste_df_lista(sisab, lista_sisab)

# COBERTURA CRECHE MUNICIPAL
df_matriculas_creche = utils.ajuste_df(
    matriculas_creche, "taxa_cobertura_creche_municipal"
)

# NOTAS SAEB
lista_notas_saeb_anos_iniciais = ["nota_mat_anos_iniciais", "nota_port_anos_iniciais"]
lista_notas_saeb_anos_finais = ["nota_mat_anos_finais", "nota_port_anos_finais"]

df_notas_saeb_anos_iniciais = utils.ajuste_df_lista(
    notas_saeb_anos_iniciais, lista_notas_saeb_anos_iniciais
)
df_notas_saeb_anos_finais = utils.ajuste_df_lista(
    notas_saeb_anos_finais, lista_notas_saeb_anos_finais
)

# RENDIMENTO EDUCACAO
lista_rendimento_abandono_educacao = [
    "taxa_abandono_fundamental_anos_finais",
    "taxa_abandono_fundamental_anos_iniciais",
]
df_rendimento_abandono_educacao = utils.ajuste_df_lista(
    rendimento_educacao, lista_rendimento_abandono_educacao
)

lista_rendimento_distorcao_educacao = [
    "taxa_distorcao_fundamental_anos_finais",
    "taxa_distorcao_fundamental_anos_iniciais",
]
df_rendimento_distorcao_educacao = utils.ajuste_df_lista(
    rendimento_educacao, lista_rendimento_distorcao_educacao
)

# ADEQUACAO DA FORMACAO DOCENTE
df_adequacao_docentes = utils.ajuste_df(
    adequacao_docentes, "adequacao_formacao_docente"
)

# DESPESAS EDUCACAO PER CAPITA
df_despesas_educacao_per_capita = utils.ajuste_df(
    despesas_educacao_per_capita, "despesas_educacao_per_capita"
)

# EMISSAO DE GASES PER CAPITA
df_emissao_gases_per_capita = utils.ajuste_df(
    emissao_gases_per_capita, "emissao_gases_per_capita"
)

# AGUA
lista_agua = [
    "indice_perda_faturamento",
    "prop_atendimento_agua",
]
df_agua = utils.ajuste_df_lista(agua, lista_agua)

# RESIDUOS
df_residuos = utils.ajuste_df(residuos, "prop_coleta_residuos")

# %%
# Arquivos Finais

# 2024
df_2024 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2024 = [
    (df_pib_per_capita, "2021"),
    (df_comex_per_capita, "2024"),
    (df_renda_media, "2023"),
    (df_vinculos_per_capita, "2023"),
    (df_formalidade_mercado_trabalho, "2023"),
    (df_geracao_emprego_per_capita, "2024"),
    (df_vulnerabilidade_social, "2024"),
    (df_indicadores_financeiros, "2024"),
    (df_indicadores_seguranca, "2024"),
    (df_indicadores_violencia_mulher, "2024"),
    (df_indicadores_saude, "2024"),
    (df_obitos_evitaveis_per_capita, "2023"),
    (df_sisab, "2024"),
    (df_matriculas_creche, "2024"),
    (df_notas_saeb_anos_iniciais, "2023"),
    (df_notas_saeb_anos_finais, "2023"),
    (df_rendimento_distorcao_educacao, "2024"),
    (df_rendimento_abandono_educacao, "2023"),
    (df_adequacao_docentes, "2024"),
    (df_despesas_educacao_per_capita, "2024"),
    (df_emissao_gases_per_capita, "2023"),
    (df_agua, "2022"),
    (df_residuos, "2022"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2024:
    df_2024 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2024,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )


# 2023
df_2023 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2023 = [
    (df_pib_per_capita, "2020"),
    (df_comex_per_capita, "2023"),
    (df_renda_media, "2022"),
    (df_vinculos_per_capita, "2022"),
    (df_formalidade_mercado_trabalho, "2022"),
    (df_geracao_emprego_per_capita, "2023"),
    (df_vulnerabilidade_social, "2023"),
    (df_indicadores_financeiros, "2023"),
    (df_indicadores_seguranca, "2023"),
    (df_indicadores_violencia_mulher, "2023"),
    (df_indicadores_saude, "2023"),
    (df_obitos_evitaveis_per_capita, "2022"),
    (df_sisab, "2023"),
    (df_matriculas_creche, "2023"),
    (df_notas_saeb_anos_iniciais, "2022"),
    (df_notas_saeb_anos_finais, "2022"),
    (df_rendimento_distorcao_educacao, "2023"),
    (df_rendimento_abandono_educacao, "2022"),
    (df_adequacao_docentes, "2023"),
    (df_despesas_educacao_per_capita, "2023"),
    (df_emissao_gases_per_capita, "2022"),
    (df_agua, "2021"),
    (df_residuos, "2021"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2024:
    df_2023 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2023,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )

# 2022
df_2022 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2022 = [
    (df_pib_per_capita, "2019"),
    (df_comex_per_capita, "2022"),
    (df_renda_media, "2021"),
    (df_vinculos_per_capita, "2021"),
    (df_formalidade_mercado_trabalho, "2021"),
    (df_geracao_emprego_per_capita, "2022"),
    (df_vulnerabilidade_social, "2022"),
    (df_indicadores_financeiros, "2022"),
    (df_indicadores_seguranca, "2022"),
    (df_indicadores_violencia_mulher, "2022"),
    (df_indicadores_saude, "2022"),
    (df_obitos_evitaveis_per_capita, "2021"),
    (df_sisab, "2022"),
    (df_matriculas_creche, "2022"),
    (df_notas_saeb_anos_iniciais, "2021"),
    (df_notas_saeb_anos_finais, "2021"),
    (df_rendimento_distorcao_educacao, "2022"),
    (df_rendimento_abandono_educacao, "2021"),
    (df_adequacao_docentes, "2022"),
    (df_despesas_educacao_per_capita, "2022"),
    (df_emissao_gases_per_capita, "2021"),
    (df_agua, "2020"),
    (df_residuos, "2020"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2022:
    df_2022 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2022,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )

# 2021
df_2021 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2021 = [
    (df_pib_per_capita, "2018"),
    (df_comex_per_capita, "2021"),
    (df_renda_media, "2020"),
    (df_vinculos_per_capita, "2020"),
    (df_formalidade_mercado_trabalho, "2020"),
    (df_geracao_emprego_per_capita, "2021"),
    (df_vulnerabilidade_social, "2021"),
    (df_indicadores_financeiros, "2021"),
    (df_indicadores_seguranca, "2021"),
    (df_indicadores_violencia_mulher, "2021"),
    (df_indicadores_saude, "2021"),
    (df_obitos_evitaveis_per_capita, "2020"),
    (df_sisab, "2021"),
    (df_matriculas_creche, "2021"),
    (df_notas_saeb_anos_iniciais, "2020"),
    (df_notas_saeb_anos_finais, "2020"),
    (df_rendimento_distorcao_educacao, "2021"),
    (df_rendimento_abandono_educacao, "2020"),
    (df_adequacao_docentes, "2021"),
    (df_despesas_educacao_per_capita, "2021"),
    (df_emissao_gases_per_capita, "2020"),
    (df_agua, "2019"),
    (df_residuos, "2019"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2021:
    df_2021 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2021,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )

# 2020
df_2020 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2020 = [
    (df_pib_per_capita, "2017"),
    (df_comex_per_capita, "2020"),
    (df_renda_media, "2019"),
    (df_vinculos_per_capita, "2019"),
    (df_formalidade_mercado_trabalho, "2019"),
    (df_geracao_emprego_per_capita, "2020"),
    (df_vulnerabilidade_social, "2020"),
    (df_indicadores_financeiros, "2020"),
    (df_indicadores_seguranca, "2020"),
    (df_indicadores_violencia_mulher, "2020"),
    (df_indicadores_saude, "2020"),
    (df_obitos_evitaveis_per_capita, "2019"),
    (df_sisab, "2020"),
    (df_matriculas_creche, "2020"),
    (df_notas_saeb_anos_iniciais, "2019"),
    (df_notas_saeb_anos_finais, "2019"),
    (df_rendimento_distorcao_educacao, "2020"),
    (df_rendimento_abandono_educacao, "2019"),
    (df_adequacao_docentes, "2020"),
    (df_despesas_educacao_per_capita, "2020"),
    (df_emissao_gases_per_capita, "2019"),
    (df_agua, "2018"),
    (df_residuos, "2018"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2020:
    df_2020 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2020,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )

# 2019
df_2019 = municipios[["id_municipio", "municipio"]].copy()
# Lista de informações para os merges: (DataFrame, sufixo_do_ano)
dataframes_para_juntar_2019 = [
    (df_pib_per_capita, "2016"),
    (df_comex_per_capita, "2019"),
    (df_renda_media, "2018"),
    (df_vinculos_per_capita, "2018"),
    (df_formalidade_mercado_trabalho, "2018"),
    (df_geracao_emprego_per_capita, "2019"),
    (df_vulnerabilidade_social, "2019"),
    (df_indicadores_financeiros, "2019"),
    (df_indicadores_seguranca, "2019"),
    (df_indicadores_violencia_mulher, "2019"),
    (df_indicadores_saude, "2019"),
    (df_obitos_evitaveis_per_capita, "2018"),
    (df_sisab, "2019"),
    (df_matriculas_creche, "2019"),
    (df_notas_saeb_anos_iniciais, "2018"),
    (df_notas_saeb_anos_finais, "2018"),
    (df_rendimento_distorcao_educacao, "2019"),
    (df_rendimento_abandono_educacao, "2018"),
    (df_adequacao_docentes, "2019"),
    (df_despesas_educacao_per_capita, "2019"),
    (df_emissao_gases_per_capita, "2018"),
    (df_agua, "2017"),
    (df_residuos, "2017"),
]
# Loop para realizar todos os merges
for df_adicional, ano_sufixo in dataframes_para_juntar_2019:
    df_2019 = utils.realizar_merge_com_selecao_ano(
        df_principal=df_2019,
        df_adicional=df_adicional,
        coluna_chave="id_municipio",
        sufixo_ano=ano_sufixo,
    )


# Salvar os DataFrames em arquivos Excel
anos = range(ano_minimo, ano_maximo + 1)
for ano in anos:
    df_ano = eval(f"df_{ano}")
    df_ano.to_excel(
        f"resultados//Ranking_Municipios_{ano}.xlsx",
        index=False,
        engine="openpyxl",
    )
