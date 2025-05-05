# %%
import pandas as pd

ano_incial = 2021
ano_final = 2024

ranqueamento = pd.read_excel(
    "data/ranqueamento/ranqueamento_anfacer.xlsx", engine="calamine"
)

tradutor_reporter = pd.read_csv("data/ranqueamento/tradutor_reporter.csv", sep=";")


def ajuste_oportunidades(df, col1, notas1, col2, notas2, pergunta):
    """
    Ajusta o DataFrame de oportunidades, renomeando colunas e filtrando dados.

    """
    filtro_exp_br = df[f"exp_br_{ano_final}"] != 0
    filtro_taxa_cresc_exp_br = df["taxa_cresc_exp_br"] != 0
    filtro_imp = df[f"imp_{ano_final}"] != 0
    return (
        df[filtro_exp_br & filtro_taxa_cresc_exp_br & filtro_imp]
        .query(f"{col1} in @notas1 & {col2} in @notas2")
        .assign(classificacao=pergunta)
        .reset_index(drop=True)
    )


# 1- Países que possuem taxa de variação de importações em US$ acima da média
# e o Brasil apresenta taxa de variação das exportações baixo da média?
pergunta_1 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br",
    notas2=[-1, 1],
    pergunta="pergunta_1",
)

# 2 - Países que possuem taxa de variação de importações em US$ acima da média
#  e o Brasil apresenta taxa de variação das exportações acima da média?

pergunta_2 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br",
    notas2=[3, 5],
    pergunta="pergunta_2",
)

# 3 - Países que possuem valor de importações em US$ acima da média
# e o Brasil apresenta de exportações abaixo da média?

pergunta_3 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_imp_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_{ano_final}",
    notas2=[-1, 1],
    pergunta="pergunta_3",
)

# 4 - Países que com taxa de variação do PIB acima da média
# e o Brasil apresenta de exportações abaixo da média?

pergunta_4 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_taxa_cresc_pib_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_{ano_final}",
    notas2=[-1, 1],
    pergunta="pergunta_4",
)

# 5 - Países que com importação de cerâmica acima da média
# e o Brasil apresenta de exportações abaixo da média?
pergunta_5 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_imp_ceramica_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_ceramica_{ano_final}",
    notas2=[-1, 1],
    pergunta="pergunta_5",
)

# 6 - Países que com importação de cerâmica acima da média
# e o Brasil apresenta de exportações acima da média?
pergunta_6 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_imp_ceramica_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_ceramica_{ano_final}",
    notas2=[3, 5],
    pergunta="pergunta_6",
)

# 7 - Países que com taxa de crescimento das importação de cerâmica acima da média
# e o Brasil apresenta de taxa de crescimento das exportações abaixo da média?
pergunta_7 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp_ceramica",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br_ceramica",
    notas2=[-1, 1],
    pergunta="pergunta_7",
)

# 14 - Países que com taxa de crescimento das importação de cerâmica acima da média
# e o Brasil apresenta de taxa de crescimento das exportações abaixo da média?
pergunta_14 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp_ceramica",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br_ceramica",
    notas2=[3, 5],
    pergunta="pergunta_14",
)

# 15 - Países que com importação de porcelanato acima da média
# e o Brasil apresenta de exportações abaixo da média?
pergunta_15 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_imp_porcelanato_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_porcelanato_{ano_final}",
    notas2=[-1, 1],
    pergunta="pergunta_15",
)

# 16 - Países que com importação de porcelanato acima da média
# e o Brasil apresenta de exportações acima da média?
pergunta_16 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_imp_porcelanato_{ano_final}",
    notas1=[3, 5],
    col2=f"nota_exp_br_porcelanato_{ano_final}",
    notas2=[3, 5],
    pergunta="pergunta_16",
)

# 17 -  Países que com taxa de crescimento das importação de porcelanato acima da média
# e o Brasil apresenta de taxa de crescimento das exportações abaixo da média?
pergunta_17 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp_porcelanato",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br_porcelanato",
    notas2=[-1, 1],
    pergunta="pergunta_17",
)


# 18 - Países que com taxa de crescimento das importação de porcelanato acima da média
# e o Brasil apresenta de taxa de crescimento das exportações abaixo da média?
pergunta_18 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_imp_porcelanato",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br_porcelanato",
    notas2=[3, 5],
    pergunta="pergunta_18",
)

# 8 - Países que possuem tarifa baixa
# e apresenta taxa de variação das exportações do Brasil abaixo da média?
pergunta_8 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_tarifa_media_brasil",
    notas1=[5, 3],
    col2="nota_taxa_cresc_exp_br",
    notas2=[-1, 1],
    pergunta="pergunta_8",
)
# 9 - Países que possuem tarifa baixa
# e apresenta taxa de variação das exportações do Brasil acima da média?
pergunta_9 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_tarifa_media_brasil",
    notas1=[5, 3],
    col2="nota_taxa_cresc_exp_br",
    notas2=[3, 5],
    pergunta="pergunta_9",
)

# 10 - Países com participação de mercado dos concorrentes estratégicos acima da média
# e participação do Brasil abaixo da média?
pergunta_10 = ajuste_oportunidades(
    df=ranqueamento,
    col1=f"nota_part_concorrentes_{ano_final}",
    notas1=[1, -1],
    col2=f"nota_part_brasil_{ano_final}",
    notas2=[3, 1, -1],
    pergunta="pergunta_10",
)

# 11 - Países com crescimento do preço médio das exportacoes brasileiras
# e crescimento acima da média das exportações brasileira?
pergunta_11 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_taxa_cresc_preco_medio_br",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br",
    notas2=[3, 5],
    pergunta="pergunta_11",
)

# 12 - Países com preço médio acima da média das exportações brasileiras
# e crescimento acima da média das exportações brasileira?
pergunta_12 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_preco_medio_exp_br_2024",
    notas1=[3, 5],
    col2="nota_taxa_cresc_exp_br",
    notas2=[3, 5],
    pergunta="pergunta_12",
)

# 13 - Países com preço médio abaixo da média das exportações brasileiras
# e crescimento acima da média das exportações brasileira?

pergunta_13 = ajuste_oportunidades(
    df=ranqueamento,
    col1="nota_preco_medio_exp_br_2024",
    notas1=[-1, 1],
    col2="nota_taxa_cresc_exp_br",
    notas2=[3, 5],
    pergunta="pergunta_13",
)

numero_perguntas = range(1, 19)
lista_perguntas = [f"pergunta_{i}" for i in numero_perguntas]
todas_colunas = ranqueamento.columns.tolist()
colunas_interesse = [coluna for coluna in todas_colunas if "nota" not in coluna] + [
    "classificacao"
]

df = (
    pd.concat([eval(pergunta) for pergunta in lista_perguntas], ignore_index=True)[
        colunas_interesse
    ]
    .merge(tradutor_reporter, how="left", left_on="id", right_on="reporter")
    .drop(columns=["reporter"])
)

df.to_csv("data/oportunidades/oportunidades.csv", sep=";", index=False)
