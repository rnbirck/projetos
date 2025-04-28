# %%
import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

billing_project_id = "gold-braid-417822"
client = bigquery.Client(project=billing_project_id)
# %%
replace_raca_cor = {
    1: 5,
    2: 1,
    4: 2,
    6: 4,
    8: 3,
    9: 9,
}

# Opcao basesdosdados:

query = """
SELECT 
    a.ano, 
    a.id_municipio,
    b.nome as municipio,
    a.cnae_2_subclasse,
    CAST(a.grau_instrucao_apos_2005 AS INT64) AS grau_instrucao,
    CAST(a.faixa_etaria AS INT64) AS faixa_etaria,
    CAST(a.raca_cor AS INT64) AS raca_cor,
    CAST(a.sexo AS INT64) AS sexo,
    COUNT(a.vinculo_ativo_3112) as qntd_vinculos
FROM `basedosdados.br_me_rais.microdados_vinculos` a
JOIN `basedosdados.br_bd_diretorios_brasil.municipio` b
ON a.id_municipio = b.id_municipio
WHERE
    a.ano >= 2019 AND
    a.vinculo_ativo_3112 = '1' AND
    a.sigla_uf = 'RS'
GROUP BY
    a.ano, 
    a.id_municipio,
    b.nome, 
    a.cnae_2_subclasse,
    a.grau_instrucao_apos_2005,
    a.faixa_etaria,

    a.raca_cor,
    a.sexo
"""
df_vinculos_raw = client.query(query).to_dataframe()
df_vinculos = df_vinculos_raw.replace({"raca_cor": replace_raca_cor})

# %%

# Opcao microdados:
df_municipios = pd.read_sql_query(
    "SELECT id_municipio_6, id_municipio, municipio FROM municipio WHERE sigla_uf = 'RS'",
    engine,
)
df_municipios["id_municipio_6"] = df_municipios["id_municipio_6"].astype(int)

filtro_rs = df_municipios["id_municipio_6"].tolist()

replace_raca_cor = {
    1: 5,
    2: 1,
    4: 2,
    6: 4,
    8: 3,
    9: 9,
}


def processar_dados_rais_vinculos(ano: int):
    """
    Processa os dados de vinculos da RAIS no RS.

    Parâmetros:
        - df_raw: DataFrame com os dados brutos da RAIS Vinculos.
        - ano: Ano de referência dos dados.
        - filtro: Lista de municípios a serem filtrados.

    """
    colunas = [
        "Município",
        "Vínculo Ativo 31/12",
        "Escolaridade após 2005",
        "CNAE 2.0 Subclasse",
        "Faixa Etária",
        "Raça Cor",
        "Sexo Trabalhador",
    ]

    filepath = f"data/rais/RAIS_VINC_PUB_SUL_{ano}.txt"
    df = (
        pd.read_csv(
            filepath,
            sep=";",
            encoding="ISO-8859-1",
            decimal=",",
            engine="pyarrow",
            usecols=colunas,
        )
        .query("`Vínculo Ativo 31/12` == 1 and `Município` in @filtro_rs")
        .groupby(
            [
                "Município",
                "CNAE 2.0 Subclasse",
                "Escolaridade após 2005",
                "Faixa Etária",
                "Raça Cor",
                "Sexo Trabalhador",
            ]
        )
        .size()
        .reset_index(name="qntd_vinculos")
        .rename(
            columns={
                "Município": "id_municipio_6",
                "CNAE 2.0 Subclasse": "cnae_2_subclasse",
                "Escolaridade após 2005": "grau_instrucao",
                "Faixa Etária": "faixa_etaria",
                "Raça Cor": "raca_cor",
                "Sexo Trabalhador": "sexo",
            }
        )
        .assign(ano=ano, raca_cor=lambda x: x["raca_cor"].replace(replace_raca_cor))
    )
    return df


anos = range(2023, 2025)

df_vinculos = (
    pd.concat(
        [processar_dados_rais_vinculos(ano=ano) for ano in anos], ignore_index=True
    )
    .merge(df_municipios, on="id_municipio_6", how="left")
    .drop(columns=["id_municipio_6"])
)
