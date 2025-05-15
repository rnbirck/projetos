# %%
import pandas as pd

arquivo_destaque_mes = "ranqueamento_anfacer_destaque_mes.xlsx"
arquivo_destaque_acumulado = "ranqueamento_anfacer_destaque_acumulado.xlsx"
destaques_mes = pd.read_excel(
    f"../../cei/ranqueamento/resultados/{arquivo_destaque_mes}", engine="calamine"
)
destaques_acumulado = pd.read_excel(
    f"../../cei/ranqueamento/resultados/{arquivo_destaque_acumulado}", engine="calamine"
)

ano_inicial = 2024
ano_final = 2025


def processar_destaque_1(df, ano_final, nome_tipologia=None):
    """Processa o destaque 1: Paises relevantes para o Brasil com importante crescimento no preco medio e nas exportacoes."""
    if nome_tipologia:
        sufixo_coluna = f"_{nome_tipologia}"
    else:
        sufixo_coluna = ""

    filtro_nota_pais_relevante = df[f"nota_valor_{ano_final}{sufixo_coluna}"].isin(
        [3, 5]
    )
    filtro_nota_crescimento_preco_medio = df[
        f"nota_taxa_crescimento_preco_medio{sufixo_coluna}"
    ].isin([3, 5])
    filtro_nota_taxa_crescimento_valor = df[
        f"nota_taxa_crescimento_valor{sufixo_coluna}"
    ].isin([3, 5])
    filtro_taxa_crescimento_preco_medio = (
        df[f"taxa_crescimento_preco_medio{sufixo_coluna}"] > 2
    )

    df = (
        df[
            filtro_nota_pais_relevante
            & filtro_nota_crescimento_preco_medio
            & filtro_taxa_crescimento_preco_medio
            & filtro_nota_taxa_crescimento_valor
        ]
    ).assign(classificacao=f"destaque_1{sufixo_coluna}")

    return df


def processar_destaque_2(df, ano_final, nome_tipologia=None):
    """Processa o destaque 2: Paises relevantes para o Brasil com crescimentos nas exportacoes e reducao no preco medio."""
    if nome_tipologia:
        sufixo_coluna = f"_{nome_tipologia}"
    else:
        sufixo_coluna = ""

    filtro_nota_pais_relevante = df[f"nota_valor_{ano_final}{sufixo_coluna}"].isin(
        [3, 5]
    )

    filtro_nota_crescimento_exportacoes = df[
        f"nota_taxa_crescimento_valor{sufixo_coluna}"
    ].isin([3, 5])

    filtro_nota_crescimento_preco_medio = df[
        f"nota_taxa_crescimento_preco_medio{sufixo_coluna}"
    ].isin([1, -1])

    df = (
        df[
            filtro_nota_pais_relevante
            & filtro_nota_crescimento_preco_medio
            & filtro_nota_crescimento_exportacoes
        ]
    ).assign(classificacao=f"destaque_2{sufixo_coluna}")

    return df


def processar_destaque_3(df, ano_final, nome_tipologia=None):
    """Processa o destaque 3: Paises relevantes para o Brasil, com relevante preco_medio, crescendo as exportacoes"""
    if nome_tipologia:
        sufixo_coluna = f"_{nome_tipologia}"
    else:
        sufixo_coluna = ""

    filtro_nota_pais_relevante = df[f"nota_valor_{ano_final}{sufixo_coluna}"].isin(
        [3, 5]
    )
    filtro_nota_preco_medio_relevante = df[
        f"nota_preco_medio_{ano_final}{sufixo_coluna}"
    ].isin([3, 5])
    filtro_nota_crescimento_exportacoes = df[
        f"nota_taxa_crescimento_valor{sufixo_coluna}"
    ].isin([3, 5])

    df = (
        df[
            filtro_nota_pais_relevante
            & filtro_nota_preco_medio_relevante
            & filtro_nota_crescimento_exportacoes
        ]
    ).assign(classificacao=f"destaque_3{sufixo_coluna}")

    return df


def processar_destaque_4(df, ano_final, nome_tipologia=None):
    """Processa o destaque 4: Paises relevantes para o Brasil, com relevante preco_medio, diminuindo as exportacoes."""
    if nome_tipologia:
        sufixo_coluna = f"_{nome_tipologia}"
    else:
        sufixo_coluna = ""

    filtro_nota_pais_relevante = df[f"nota_valor_{ano_final}{sufixo_coluna}"].isin(
        [3, 5]
    )
    filtro_nota_preco_medio_relevante = df[
        f"nota_preco_medio_{ano_final}{sufixo_coluna}"
    ].isin([3, 5])
    filtro_nota_crescimento_exportacoes = df[
        f"nota_taxa_crescimento_valor{sufixo_coluna}"
    ].isin([-1])

    df = (
        df[
            filtro_nota_pais_relevante
            & filtro_nota_preco_medio_relevante
            & filtro_nota_crescimento_exportacoes
        ]
    ).assign(classificacao=f"destaque_4{sufixo_coluna}")

    return df


# Dataframes de destaques do mes
# Destaque 1
# Paises relevantes para o Brasil com importante crescimento no preco medio e nas exportacoes.
destaque_1_mes_total = processar_destaque_1(
    destaques_mes, ano_final, nome_tipologia=None
)
destaque_1_mes_ceramica = processar_destaque_1(
    destaques_mes, ano_final, nome_tipologia="ceramica"
)
destaque_1_mes_porcelanato = processar_destaque_1(
    destaques_mes, ano_final, nome_tipologia="porcelanato"
)

# Destaque 2
# Paises relevantes para o Brasil com crescimentos nas exportacoes e reducao no preco medio
destaque_2_mes_total = processar_destaque_2(
    destaques_mes, ano_final, nome_tipologia=None
)
destaque_2_mes_ceramica = processar_destaque_2(
    destaques_mes, ano_final, nome_tipologia="ceramica"
)
destaque_2_mes_porcelanato = processar_destaque_2(
    destaques_mes, ano_final, nome_tipologia="porcelanato"
)

# Destaque 3
# Paises relevantes para o Brasil, com relevante preco_medio, crescendo as exportacoes
destaque_3_mes_total = processar_destaque_3(
    destaques_mes, ano_final, nome_tipologia=None
)
destaque_3_mes_ceramica = processar_destaque_3(
    destaques_mes, ano_final, nome_tipologia="ceramica"
)
destaque_3_mes_porcelanato = processar_destaque_3(
    destaques_mes, ano_final, nome_tipologia="porcelanato"
)

# Destaque 4
# Paises relevantes para o Brasil, com relevante preco_medio, diminuindo as exportacoes
destaque_4_mes_total = processar_destaque_4(
    destaques_mes, ano_final, nome_tipologia=None
)
destaque_4_mes_ceramica = processar_destaque_4(
    destaques_mes, ano_final, nome_tipologia="ceramica"
)
destaque_4_mes_porcelanato = processar_destaque_4(
    destaques_mes, ano_final, nome_tipologia="porcelanato"
)

# Concatenar os dataframes de destaque
destaque_mes = pd.concat(
    [
        destaque_1_mes_total,
        destaque_1_mes_ceramica,
        destaque_1_mes_porcelanato,
        destaque_2_mes_total,
        destaque_2_mes_ceramica,
        destaque_2_mes_porcelanato,
        destaque_3_mes_total,
        destaque_3_mes_ceramica,
        destaque_3_mes_porcelanato,
        destaque_4_mes_total,
        destaque_4_mes_ceramica,
        destaque_4_mes_porcelanato,
    ]
)

# Dataframes de destaques acumulados
# Destaque 1
# Paises relevantes para o Brasil com importante crescimento no preco medio e nas exportacoes
destaque_1_acumulado_total = processar_destaque_1(
    destaques_acumulado, ano_final, nome_tipologia=None
)
destaque_1_acumulado_ceramica = processar_destaque_1(
    destaques_acumulado, ano_final, nome_tipologia="ceramica"
)
destaque_1_acumulado_porcelanato = processar_destaque_1(
    destaques_acumulado, ano_final, nome_tipologia="porcelanato"
)

# Destaque 2
# Paises relevantes para o Brasil com crescimentos nas exportacoes e reducao no preco medio
destaque_2_acumulado_total = processar_destaque_2(
    destaques_acumulado, ano_final, nome_tipologia=None
)
destaque_2_acumulado_ceramica = processar_destaque_2(
    destaques_acumulado, ano_final, nome_tipologia="ceramica"
)
destaque_2_acumulado_porcelanato = processar_destaque_2(
    destaques_acumulado, ano_final, nome_tipologia="porcelanato"
)

# Destaque 3
# Paises relevantes para o Brasil, com relevante preco_medio, crescendo as exportacoes
destaque_3_acumulado_total = processar_destaque_3(
    destaques_acumulado, ano_final, nome_tipologia=None
)
destaque_3_acumulado_ceramica = processar_destaque_3(
    destaques_acumulado, ano_final, nome_tipologia="ceramica"
)
destaque_3_acumulado_porcelanato = processar_destaque_3(
    destaques_acumulado, ano_final, nome_tipologia="porcelanato"
)

# Destaque 4
# Paises relevantes para o Brasil, com relevante preco_medio, diminuindo as exportacoes
destaque_4_acumulado_total = processar_destaque_4(
    destaques_acumulado, ano_final, nome_tipologia=None
)
destaque_4_acumulado_ceramica = processar_destaque_4(
    destaques_acumulado, ano_final, nome_tipologia="ceramica"
)
destaque_4_acumulado_porcelanato = processar_destaque_4(
    destaques_acumulado, ano_final, nome_tipologia="porcelanato"
)

# Concatenar os datafraacumulado de destaque
destaque_acumulado = pd.concat(
    [
        destaque_1_acumulado_total,
        destaque_1_acumulado_ceramica,
        destaque_1_acumulado_porcelanato,
        destaque_2_acumulado_total,
        destaque_2_acumulado_ceramica,
        destaque_2_acumulado_porcelanato,
        destaque_3_acumulado_total,
        destaque_3_acumulado_ceramica,
        destaque_3_acumulado_porcelanato,
        destaque_4_acumulado_total,
        destaque_4_acumulado_ceramica,
        destaque_4_acumulado_porcelanato,
    ]
)
caminho_bi = "D:/CEI/BI_ANFACER/BI/arquivos/"
destaque_mes.to_csv(
    caminho_bi + "destaque_mes.csv", sep=";", index=False, encoding="utf-8"
)
destaque_acumulado.to_csv(
    caminho_bi + "destaque_acumulado.csv", sep=";", index=False, encoding="utf-8"
)

# %%
