# %%
# Bolsa Família
import pandas as pd
from google.cloud import bigquery

billing_project_id = "gold-braid-417822"
client = bigquery.Client(project=billing_project_id)
destino_arquivo = "../../OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Assistencia Social/"

ano = (2025,)
mes = 2

query_mun_bolsa = """
SELECT id_municipio_rf, nome as municipio
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
WHERE sigla_uf = 'RS'
"""
trad_mun_bolsa = client.query(query_mun_bolsa).to_dataframe()


def processar_dados_bolsa_familia(ano: int, mes: int, trad_mun):
    """
    Processa e atualiza os dados do Bolsa Família para o estado do Rio Grande do Sul.

    """
    mes_str = str(mes).zfill(2)
    arquivo = f"data/assistencia_social/{ano}{mes_str}_NovoBolsaFamilia.csv"
    df = (
        pd.read_csv(arquivo, sep=";", encoding="latin1", engine="pyarrow")
        .query('UF == "RS"')
        .assign(
            date=lambda x: pd.to_datetime(
                x["MÊS COMPETÊNCIA"].astype(str), format="%Y%m"
            ),
            ano=lambda x: x["date"].dt.year,
            mes=lambda x: x["date"].dt.month,
        )
        .assign(
            valor_parcela=lambda x: x["VALOR PARCELA"]
            .str.replace(",", ".")
            .astype(float),
            cod_mun=lambda x: x["CÓDIGO MUNICÍPIO SIAFI"].astype(str),
        )
        .groupby(["ano", "mes", "cod_mun"], as_index=False)
        .agg({"valor_parcela": "sum", "NIS FAVORECIDO": "nunique"})
        .rename(columns={"NIS FAVORECIDO": "num_beneficiarios"})
        .merge(trad_mun, left_on="cod_mun", right_on="id_municipio_rf")
        .drop(columns=["cod_mun", "id_municipio_rf"])
        .drop_duplicates()
    )

    return df


# dados = []

# for ano in anos:
#     for mes in meses:
#         try:
#             df = processar_dados_bolsa_familia(ano=ano, mes=mes, trad_mun=trad_mun_bolsa)
#             dados.append(df)
#         except FileNotFoundError:
#             print(f"Arquivo não encontrado para {mes}/{ano}, pulando...")
#         except Exception as e:
#             print(f"Erro ao processar {mes}/{ano}: {e}, pulando...")
# df_bolsa = pd.concat(dados, ignore_index=True)

df_bolsa = (
    pd.concat(
        [
            processar_dados_bolsa_familia(ano=ano, mes=mes, trad_mun=trad_mun_bolsa),
            pd.read_csv(destino_arquivo + "NovoBolsaFamilia/bolsa_familia.csv"),
        ]
    )
).to_csv(destino_arquivo + "NovoBolsaFamilia/bolsa_familia.csv", index=False)

# %%

query_mun_cad = """
SELECT id_municipio, id_municipio_6, nome as municipio
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
WHERE sigla_uf = 'RS'
"""

trad_mun_cad = client.query(query_mun_cad).to_dataframe()


def processar_dados_cad_raw(arquivo):
    """
    Processa um arquivo CSV com dados de cadastro, criando colunas de data e filtrando resultados.

    Esta função lê um arquivo CSV, converte uma coluna específica para formato de data,
    extrai o ano e mês em colunas separadas, e filtra os dados apenas para o estado do
    Rio Grande do Sul a partir de 2019.

    Parameters:
    -----------
    arquivo : str
        Nome do arquivo CSV a ser lido.
    caminho_bases : str
        Caminho para o diretório onde estão armazenados os arquivos CSV.

    Returns:
    --------
    pandas.DataFrame
        DataFrame contendo apenas os dados do RS a partir de 2019,
        com as colunas adicionais 'date', 'ano' e 'mes'.
    """
    return (
        pd.read_csv(f"data/assistencia_social/{arquivo}.csv", encoding="Latin1")
        .assign(
            date=lambda x: pd.to_datetime(x["Referência"], format="%m/%Y"),
            ano=lambda x: x["date"].dt.year,
            mes=lambda x: x["date"].dt.month,
        )
        .query('ano >= 2019 & UF == "RS"')
    )


def processar_dados_cad(trad_mun):
    """
    Processa os DataFrames do cadastro único para serem utilizados nos dashboards das prefeituras.

    """

    cols_to_drop = ["Referência", "date", "Unidade Territorial"]
    cols_to_rename = {
        "Código": "id_municipio_6",
        "UF": "uf",
    }

    df = (
        pd.merge(
            pd.merge(
                pd.merge(
                    pd.concat(
                        [
                            processar_dados_cad_raw("faixa_renda_antes_23")
                            .assign(
                                familias_pobreza=lambda x: x[
                                    "Quantidade de famílias em situação de extrema pobreza inscritas no Cadastro Único"
                                ]
                                + x[
                                    "Quantidade de famílias em situação de pobreza inscritas no Cadastro Único"
                                ],
                            )
                            .drop(
                                columns=[
                                    *cols_to_drop,
                                    "Quantidade de famílias em situação de extrema pobreza inscritas no Cadastro Único",
                                    "Quantidade de famílias em situação de pobreza inscritas no Cadastro Único",
                                ]
                            )
                            .rename(
                                columns={
                                    "Quantidade de famílias com renda per capita mensal até meio salário-mínimo inscritas no Cadastro Único": "familias_renda_ate_meio_salario",
                                    "Quantidade de famílias com renda per capita mensal acima de meio salário-mínimo*** inscritas no Cadastro Único": "familias_renda_ate_acima_salario",
                                    **cols_to_rename,
                                }
                            ),
                            (
                                processar_dados_cad_raw("faixa_renda_apos_23")
                                .drop(columns=cols_to_drop)
                                .rename(
                                    columns={
                                        "Quantidade de famílias em situação de pobreza, segundo a faixa do Programa Bolsa Família*, inscritas no Cadastro Único": "familias_pobreza",
                                        "Quantidade de famílias com renda per capita mensal até meio salário-mínimo (Pobreza + Baixa renda) inscritas no Cadastro Único": "familias_renda_ate_meio_salario",
                                        "Quantidade de famílias com renda per capita mensal acima de meio salário-mínimo*** inscritas no Cadastro Único": "familias_renda_ate_acima_salario",
                                        **cols_to_rename,
                                    }
                                )
                            ),
                        ]
                    ),
                    (
                        processar_dados_cad_raw("total_pessoas")
                        .drop(columns=cols_to_drop)
                        .rename(columns={**cols_to_rename})
                    ),
                    on=["id_municipio_6", "ano", "mes", "uf"],
                    how="left",
                ),
                (
                    processar_dados_cad_raw("total_familias")
                    .drop(columns=cols_to_drop)
                    .rename(columns={**cols_to_rename})
                ),
                on=["id_municipio_6", "ano", "mes", "uf"],
                how="left",
            ),
            (trad_mun.astype({"id_municipio_6": int})),
            on="id_municipio_6",
            how="left",
        )
        .drop(columns=["id_municipio_6", "uf"])
        .rename(
            columns={
                "familias_pobreza": "Quantidade de famílias em situação de pobreza, segundo a faixa do Programa Bolsa Família*, inscritas no Cadastro Único",
                "familias_renda_ate_meio_salario": "Quantidade de famílias com renda per capita mensal até meio salário-mínimo (Pobreza + Baixa renda) inscritas no Cadastro Único",
                "familias_renda_ate_acima_salario": "Quantidade de famílias com renda per capita mensal acima de meio salário-mínimo*** inscritas no Cadastro Único",
            }
        )
    )
    return df


df_cad = processar_dados_cad(trad_mun_cad).to_csv(
    destino_arquivo + "cadastro_unico.csv",
    index=False,
)
