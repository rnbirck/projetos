# %%
import pandas as pd

from sqlalchemy import create_engine

# Configuração do banco de dados
usuario = "root"
senha = "ceiunisinos"
host = "localhost"
banco = "cei"
# Criar a conexão com MySQL
engine = create_engine(f"mysql+pymysql://{usuario}:{senha}@{host}/{banco}")

df_municipios = pd.read_sql_query(
    "SELECT id_municipio_6, id_municipio, municipio FROM municipio WHERE sigla_uf = 'RS'",
    engine,
)
df_municipios["id_municipio_6"] = df_municipios["id_municipio_6"].astype(int)

filtro_rs = df_municipios["id_municipio_6"].tolist()


def processar_dados_rais_estab(ano: int):
    """
    Processa os dados de estabelecimentos da RAIS no RS.

    Parâmetros:
        - df_raw: DataFrame com os dados brutos da RAIS Estabelecimentos.
        - ano: Ano de referência dos dados.
        - filtro: Lista de municípios a serem filtrados.

    """

    colunas = [
        "Município",
        "Tamanho Estabelecimento",
        "CNAE 2.0 Subclasse",
        "Ind Rais Negativa",
        "Qtd Vínculos Ativos",
    ]
    filepath = f"data/rais/RAIS_ESTAB_PUB_SUL_{ano}.txt"
    df = (
        pd.read_csv(
            filepath,
            sep=";",
            encoding="ISO-8859-1",
            decimal=",",
            engine="pyarrow",
            usecols=colunas,
        )
        .query(
            "Município in @filtro and `Vínculo Ativo 31/12` == 1 and `Faixa Remun Dezem (SM)` != 0"
        )
        .groupby(["Município", "Tamanho Estabelecimento", "CNAE 2.0 Subclasse"])
        .size()
        .reset_index(name="qntd_estab")
        .rename(
            columns={
                "Município": "id_municipio_6",
                "Tamanho Estabelecimento": "tamanho_estabelecimento",
                "CNAE 2.0 Subclasse": "cnae_2_subclasse",
            }
        )
        .assign(ano=ano)
    )

    return df


anos = range(2019, 2025)
df_renda = (
    pd.concat([processar_dados_rais_estab(ano) for ano in anos], ignore_index=True)
    .merge(df_municipios, on="id_municipio_6", how="left")
    .drop(columns=["id_municipio_6"])
)
