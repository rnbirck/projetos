# %%
import pandas as pd
import numpy as np

trad_tipologia = pd.read_csv("data/ranqueamento/tradutor_tipologia.csv", sep=";")
trad_reporter = pd.read_csv("data/ranqueamento/tradutor_reporter.csv", sep=";")

mes_atual = 4
ano_inicial = 2024
ano_final = 2025
comexstat = (
    pd.read_csv("data/comexstat.csv", sep=";", encoding="utf-8", engine="pyarrow")
    .assign(
        DATE=lambda x: pd.to_datetime(x["DATE"], format="%Y%m%d"),
        ano=lambda x: x["DATE"].dt.year,
        mes=lambda x: x["DATE"].dt.month,
    )
    .query(f"ano >= {ano_inicial}")
)

# Destaques do mes - Exportacao Brasileira


def ajuste_comexstat_mes(df, mes_atual):
    filtro_exp = df["FLUXO"] == "Exportação"

    df = (
        df[filtro_exp]
        .query(f"mes == {mes_atual}")
        .merge(trad_tipologia, on="SH6", how="left")
        .groupby(["ano", "tipologia", "CTY_PTN"], as_index=False)
        .agg({"VALUE": "sum", "QTY1": "sum"})
        .merge(
            trad_reporter,
            left_on="CTY_PTN",
            right_on="CTY_RPT",
            how="left",
        )
        .drop(columns=["CTY_RPT"])
        .rename(
            columns={
                "CTY_PTN": "id",
                "reporter": "parceiro",
                "VALUE": "valor",
                "QTY1": "volume",
            }
        )
    )
    return df


def ajuste_comexstat_acumulado(df, mes_atual):
    filtro_exp = df["FLUXO"] == "Exportação"

    df = (
        df[filtro_exp]
        .query(f"mes <= {mes_atual}")
        .merge(trad_tipologia, on="SH6", how="left")
        .groupby(["ano", "tipologia", "CTY_PTN"], as_index=False)
        .agg({"VALUE": "sum", "QTY1": "sum"})
        .merge(
            trad_reporter,
            left_on="CTY_PTN",
            right_on="CTY_RPT",
            how="left",
        )
        .drop(columns=["CTY_RPT"])
        .rename(
            columns={
                "CTY_PTN": "id",
                "reporter": "parceiro",
                "VALUE": "valor",
                "QTY1": "volume",
            }
        )
    )
    return df


def processar_dados_comex(df, ano_inicial, ano_final, nome_tipologia=None):
    """
    Processa dados do Comexstat para agregar, pivotar e calcular taxas de crescimento.

    Args:
        df (pd.DataFrame): DataFrame de entrada (ex: comexstat_mes).
                                    Deve conter colunas 'ano', 'id', 'parceiro', 'valor', 'volume'.
                                    Se nome_tipologia for fornecido, deve conter também 'tipologia'.
        ano_inicial (int): Ano inicial para cálculo da taxa de crescimento.
        ano_final (int): Ano final para cálculo da taxa de crescimento.
        nome_tipologia (str, optional): Nome da tipologia para filtrar e usar como sufixo.
                                        Se None, processa todos os dados sem filtro e sem sufixo específico.
                                        Defaults to None.

    Returns:
        pd.DataFrame: DataFrame processado com valores agregados e taxas de crescimento.
    """
    df = df.copy()

    if nome_tipologia:
        df = df.query(f"tipologia == '{nome_tipologia}'")
        sufixo_coluna = f"_{nome_tipologia}"
    else:
        sufixo_coluna = ""

    df = (
        df.groupby(["ano", "id", "parceiro"], as_index=False)
        .agg({"valor": "sum", "volume": "sum"})
        .assign(
            preco_medio=lambda x: x["valor"] / x["volume"],
        )
        .pivot_table(
            index=["id", "parceiro"],
            columns="ano",
            values=["valor", "volume", "preco_medio"],
        )
        .reset_index()
    )

    novas_colunas = []
    for col_tuple in df.columns:
        if col_tuple[1] == "":
            novas_colunas.append(col_tuple[0])
        else:
            novas_colunas.append(f"{col_tuple[0]}_{col_tuple[1]}{sufixo_coluna}")
    df.columns = novas_colunas

    df = df.assign(
        **{
            f"taxa_crescimento_valor{sufixo_coluna}": lambda x: (
                (
                    x[f"valor_{ano_final}{sufixo_coluna}"]
                    / x[f"valor_{ano_inicial}{sufixo_coluna}"]
                    - 1
                )
                * 100
            ).round(2)
        },
        **{
            f"taxa_crescimento_volume{sufixo_coluna}": lambda x: (
                (
                    x[f"volume_{ano_final}{sufixo_coluna}"]
                    / x[f"volume_{ano_inicial}{sufixo_coluna}"]
                    - 1
                )
                * 100
            ).round(2)
        },
        **{
            f"taxa_crescimento_preco_medio{sufixo_coluna}": lambda x: (
                (
                    x[f"preco_medio_{ano_final}{sufixo_coluna}"]
                    / x[f"preco_medio_{ano_inicial}{sufixo_coluna}"]
                    - 1
                )
                * 100
            ).round(2)
        },
    )

    return df


# Dataframes de destaques do mes
comexstat_mes = ajuste_comexstat_mes(comexstat, mes_atual)

comexstat_mes_total = processar_dados_comex(
    df=comexstat_mes, ano_inicial=ano_inicial, ano_final=ano_final, nome_tipologia=None
)

comexstat_mes_ceramica = processar_dados_comex(
    df=comexstat_mes,
    ano_inicial=ano_inicial,
    ano_final=ano_final,
    nome_tipologia="ceramica",
)

comexstat_mes_porcelanato = processar_dados_comex(
    df=comexstat_mes,
    ano_inicial=ano_inicial,
    ano_final=ano_final,
    nome_tipologia="porcelanato",
)

df_comexstat_mes_final = (
    comexstat_mes_total.merge(
        comexstat_mes_ceramica,
        on=["id", "parceiro"],
        how="left",
    )
    .merge(
        comexstat_mes_porcelanato,
        on=["id", "parceiro"],
        how="left",
    )
    .fillna(0)
    .replace([float("inf"), -float("inf")], 0)
)

# Ajustes nas colunas e criacao do dataframe com as informacoes para classificacao
coluna = df_comexstat_mes_final.drop(columns=["id", "parceiro"]).columns.tolist()
var = [f"var_{i + 1}" for i in range(len(coluna))]

df_classificacao = pd.DataFrame({"var": var, "coluna": coluna}).assign(
    tipo=lambda x: np.where(x["coluna"].str.startswith("taxa"), "taxa", "valor"),
    ordem="padrao",
)

var_rename = df_classificacao.set_index("coluna")["var"].to_dict()

df_comexstat_mes_final = df_comexstat_mes_final.rename(columns=var_rename)

# Dataframes de destaques do acumulado do ano
comexstat_acumulado = ajuste_comexstat_acumulado(comexstat, mes_atual)

comexstat_acumulado_total = processar_dados_comex(
    df=comexstat_acumulado,
    ano_inicial=ano_inicial,
    ano_final=ano_final,
    nome_tipologia=None,
)
comexstat_acumulado_ceramica = processar_dados_comex(
    df=comexstat_acumulado,
    ano_inicial=ano_inicial,
    ano_final=ano_final,
    nome_tipologia="ceramica",
)
comexstat_acumulado_porcelanato = processar_dados_comex(
    df=comexstat_acumulado,
    ano_inicial=ano_inicial,
    ano_final=ano_final,
    nome_tipologia="porcelanato",
)
df_comexstat_acumulado_final = (
    comexstat_acumulado_total.merge(
        comexstat_acumulado_ceramica,
        on=["id", "parceiro"],
        how="left",
    )
    .merge(
        comexstat_acumulado_porcelanato,
        on=["id", "parceiro"],
        how="left",
    )
    .fillna(0)
    .replace([float("inf"), -float("inf")], 0)
)

df_comexstat_acumulado_final = df_comexstat_acumulado_final.rename(columns=var_rename)

# Salvando os arquivos
arquivo_mes = "../../cei/ranqueamento/bases/base_anfacer_destaque_mes.xlsx"
with pd.ExcelWriter(arquivo_mes) as writer:
    df_comexstat_mes_final.to_excel(writer, index=False, sheet_name="base_ranqueamento")
    df_classificacao.to_excel(writer, index=False, sheet_name="classificacao")

arquivo_acumulado = "../../cei/ranqueamento/bases/base_anfacer_destaque_acumulado.xlsx"
with pd.ExcelWriter(arquivo_acumulado) as writer:
    df_comexstat_acumulado_final.to_excel(
        writer, index=False, sheet_name="base_ranqueamento"
    )
    df_classificacao.to_excel(writer, index=False, sheet_name="classificacao")
