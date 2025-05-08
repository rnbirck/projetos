# %%
import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine

billing_project_id = "gold-braid-417822"
client = bigquery.Client(project=billing_project_id)

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")


# Preciso pegar o número de empresas brasileiras, separadas por estado e por CNAE subclasse (são 6).
# Para cada cnae preciso de dois cortes: um de empresas com mais de 10 funcionários e outra com empresas de mais de 50 funcionários
# Sendo os CNAES:
# 10.65-1 Fabricação de amidos e féculas de vegetais e de óleos de milho,
# 10.91-1 Fabricação de produtos de panificação,
# 10.94-5 Fabricação de massas alimentícias,
# 28.21-6 Fabricação de aparelhos e equipamentos para instalações térmicas,
# 28.23-2 Fabricação de máquinas e aparelhos de refrigeração e ventilação para uso industrial e comercial
# 28.62-3 Fabricação de máquinas e equipamentos para as indústrias de alimentos, bebidas e fumo.

lista_cnae = ["10651", "10911", "10945", "28216", "28232", "28623"]
dicionario_cnae = {
    "10651": "Fabricação de amidos e féculas de vegetais e de óleos de milho",
    "10911": "Fabricação de produtos de panificação",
    "10945": "Fabricação de massas alimentícias",
    "28216": "Fabricação de aparelhos e equipamentos para instalações térmicas",
    "28232": "Fabricação de máquinas e aparelhos de refrigeração e ventilação para uso industrial e comercial",
    "28623": "Fabricação de máquinas e equipamentos para as indústrias de alimentos, bebidas e fumo",
}

tradutor_cnae = pd.read_sql_query("SELECT cod_subclasse, subclasse FROM cnae", engine)

# %%
# Construir o padrão para REGEXP_CONTAINS
# Isso criará uma string como: "^(10651|10911|10945|28216|28232|28623)"
regex_pattern = "^(" + "|".join(lista_cnae) + ")"


query_10_funcionarios = f"""
SELECT ano, sigla_uf, cnae_2_subclasse, COUNT(*) AS num_empresas_mais_que_10_funcionarios
FROM `basedosdados.br_me_rais.microdados_estabelecimentos`
WHERE ano = 2023
  AND quantidade_vinculos_ativos > 10
  AND indicador_rais_negativa = 0
  AND REGEXP_CONTAINS(cnae_2_subclasse, r'{regex_pattern}')
GROUP BY ano, sigla_uf, cnae_2_subclasse
"""


query_50_funcionarios = f"""
SELECT ano, sigla_uf, cnae_2_subclasse, COUNT(*) AS num_empresas_mais_que_50_funcionarios
FROM `basedosdados.br_me_rais.microdados_estabelecimentos`
WHERE ano = 2023
  AND quantidade_vinculos_ativos > 50
  AND indicador_rais_negativa = 0
  AND REGEXP_CONTAINS(cnae_2_subclasse, r'{regex_pattern}')
GROUP BY ano, sigla_uf, cnae_2_subclasse
"""

df_empresas_10_funcionarios = client.query(query_10_funcionarios).to_dataframe()
df_empresas_50_funcionarios = client.query(query_50_funcionarios).to_dataframe()

df_empresas = (
    pd.merge(
        (
            df_empresas_10_funcionarios.assign(
                cnae=lambda x: x.cnae_2_subclasse.str[:5]
            )
            .groupby(["ano", "sigla_uf", "cnae", "cnae_2_subclasse"])
            .agg({"num_empresas_mais_que_10_funcionarios": "sum"})
            .reset_index()
        ),
        (
            df_empresas_50_funcionarios.assign(
                cnae=lambda x: x.cnae_2_subclasse.str[:5]
            )
            .groupby(["ano", "sigla_uf", "cnae", "cnae_2_subclasse"])
            .agg({"num_empresas_mais_que_50_funcionarios": "sum"})
            .reset_index()
        ),
        on=["ano", "sigla_uf", "cnae", "cnae_2_subclasse"],
        how="outer",
    )
    .fillna(0)
    .assign(cnae_descricao=lambda x: x.cnae.map(dicionario_cnae))
    .merge(
        tradutor_cnae,
        left_on="cnae_2_subclasse",
        right_on="cod_subclasse",
        how="left",
    )[
        [
            "ano",
            "sigla_uf",
            "cnae",
            "cnae_descricao",
            "cod_subclasse",
            "subclasse",
            "num_empresas_mais_que_10_funcionarios",
            "num_empresas_mais_que_50_funcionarios",
        ]
    ]
)

df_empresas.to_excel("resultados/empresas_por_funcionario.xlsx", index=False)
