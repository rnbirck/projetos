# %%
import pandas as pd

caminho = "data/saude/mensal/"
substituir_mes_saude = {
    "Janeiro": "1",
    "Fevereiro": "2",
    "Março": "3",
    "Abril": "4",
    "Maio": "5",
    "Junho": "6",
    "Julho": "7",
    "Agosto": "8",
    "Setembro": "9",
    "Outubro": "10",
    "Novembro": "11",
    "Dezembro": "12",
}


def processar_saude_mensal_raw(
    nome_arquivo, caminho_base, colunas_descartar, substituir_mes
):
    # Ler o arquivo
    caminho_arquivo = f"{caminho_base}{nome_arquivo}"
    df = pd.read_excel(caminho_arquivo)

    # Remover colunas indesejadas
    df = df.drop(columns=colunas_descartar)

    # Ajustar valores na primeira linha das colunas
    df.iloc[0] = df.iloc[0].str.replace(r"/\d$", "", regex=True)

    # Identificar e remover colunas com "Total Semestre" ou "Total Ano"
    cols_to_remove = [
        col
        for col in df.columns
        if (df.iloc[1, df.columns.get_loc(col)] == "Total Semestre")
        or (df.iloc[0, df.columns.get_loc(col)] == "Total Ano")
    ]
    df = df.drop(columns=cols_to_remove)

    # Renomear colunas com base nas três primeiras linhas
    df.columns = [
        f"{ano}_{mes}_{indicador}" if idx >= 1 else indicador
        for idx, (ano, mes, indicador) in enumerate(
            zip(df.iloc[0], df.iloc[1], df.iloc[2])
        )
    ]

    # Remover as primeiras linhas desnecessárias
    df = df.iloc[3:].reset_index(drop=True)

    # Transformar o DataFrame em formato longo
    df = df.melt(
        id_vars=["Município"], var_name="ano_mes_indicador", value_name="valor"
    )

    # Filtrar dados válidos
    df = df.dropna(subset=["Município"])
    df = df[~df["Município"].str.contains("Total", case=False, na=False)]

    # Extrair ano, mês e indicador da coluna "ano_mes_indicador"
    df[["ano", "mes", "indicador"]] = df["ano_mes_indicador"].str.extract(
        r"(\d{4})_(\w+?)_(.+)"
    )
    df = df.drop(columns=["ano_mes_indicador"])
    df["valor"] = (
        df["valor"]
        .astype(str)
        .str.replace("%", "")
        .str.replace(",", ".")
        .str.replace("-", "0")
        .astype(float)
    )
    # Pivotar os dados para formato amplo
    df = df.pivot_table(
        index=["Município", "ano", "mes"], columns="indicador", values="valor"
    ).reset_index()

    # Mapear meses e criar a coluna de data
    df["mes"] = df["mes"].map(substituir_mes)

    return df


# ICSAB - arquivo icsab.xls
df_icsab = processar_saude_mensal_raw(
    "icsab.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "Nº Inter. Causa Sens.": "internacoes_icsab",
        "Nº Internações": "internacoes_totais",
        "Proporção": "prop_icsab",
    }
)

# Gravidez adolescencia - aqruivo gravidez_adolesc.xls
df_gravidez_adolesc = processar_saude_mensal_raw(
    "gravidez_adolesc.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "Nascimentos": "nascimentos",
        "Proporção": "prop_nasc_adolesc",
    }
)[["ano", "mes", "municipio", "nascimentos", "prop_nasc_adolesc"]]

# Mortalidade infantil - arquivo mort_infantil.xls
df_mort_infantil = processar_saude_mensal_raw(
    "mort_infantil.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "Óbitos": "obitos",
        "Taxa": "taxa_obitos_infantis",
    }
)[["ano", "mes", "municipio", "obitos", "taxa_obitos_infantis"]]

# Mortalidade neonatal - arquivo mort_neonatal.xls
df_mort_neonatal = processar_saude_mensal_raw(
    "mort_neonatal.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "Coeficiente": "coef_neonatal",
    }
)[["ano", "mes", "municipio", "coef_neonatal"]]

# Nascidos baixo peso - arquivo nascidos_baixo_peso.xls
df_nascidos_baixo_peso = processar_saude_mensal_raw(
    "nascidos_baixo_peso.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "Nasc Baixo Peso": "nasc_baixo_peso",
        "Proporção": "prop_nasc_baixo_peso",
    }
)[["ano", "mes", "municipio", "nasc_baixo_peso", "prop_nasc_baixo_peso"]]

# Nascidos pre natal - arquivo nascidos_maes_pre_natal.xlsx
df_nascidos_maes_pre_natal = processar_saude_mensal_raw(
    "nascidos_maes_pre_natal.xls",
    caminho,
    ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    substituir_mes_saude,
).rename(
    columns={
        "Município": "municipio",
        "7 ou mais consultas": "consultas_pre_natal",
        "Proporção": "prop_consultas_pre_natal",
    }
)[["ano", "mes", "municipio", "consultas_pre_natal", "prop_consultas_pre_natal"]]

# Obitos causas definidas - arquivo obitos_causas_definidas.xls
df_obitos_causas_definidas = (
    processar_saude_mensal_raw(
        "obitos_causas_definidas.xls",
        caminho,
        ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
        substituir_mes_saude,
    )
    .rename(
        columns={
            "Município": "municipio",
            "Causa Básica Def.": "obitos_causa_definida",
            "Não Fetais": "obitos_totais",
            "Proporção": "prop_obitos_causas_definidas",
        }
    )
    .assign(
        obitos_causa_nao_definida=lambda x: x["obitos_totais"]
        - x["obitos_causa_definida"]
    )
)

# Acidentes de trabalho - arquivo taxa_notif_acidentes_trab.xls
df_acidentes_trab = (
    processar_saude_mensal_raw(
        "taxa_notif_acidentes_trab.xls",
        caminho,
        ["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
        substituir_mes_saude,
    )[["ano", "mes", "Município", "Nº Norificações", "População"]]
    .rename(
        columns={
            "Município": "municipio",
            "Nº Norificações": "notificacoes_acidentes_trab",
            "População": "populacao",
        }
    )
    .assign(
        taxa_acidentes_trab=lambda x: x["notificacoes_acidentes_trab"]
        / x["populacao"]
        * 1000
    )
)

# DataFrame Final
df_saude_mensal = (
    pd.merge(df_icsab, df_gravidez_adolesc, on=["ano", "mes", "municipio"], how="left")
    .merge(df_mort_infantil, on=["ano", "mes", "municipio"], how="left")
    .merge(df_mort_neonatal, on=["ano", "mes", "municipio"], how="left")
    .merge(df_nascidos_baixo_peso, on=["ano", "mes", "municipio"], how="left")
    .merge(df_nascidos_maes_pre_natal, on=["ano", "mes", "municipio"], how="left")
    .merge(df_obitos_causas_definidas, on=["ano", "mes", "municipio"], how="left")
    .merge(df_acidentes_trab, on=["ano", "mes", "municipio"], how="left")
)

# Corrigir populacao de 2025 para usar valor de 2024 para cada municipio
mask_2025 = df_saude_mensal["ano"].astype(str) == "2025"
pop_2024 = (
    df_saude_mensal[df_saude_mensal["ano"].astype(str) == "2024"]
    .groupby("municipio")["populacao"]
    .last()
)
df_saude_mensal.loc[mask_2025, "populacao"] = df_saude_mensal.loc[
    mask_2025, "municipio"
].map(pop_2024)

# Recalcular colunas dependentes de populacao
df_saude_mensal["nascimentos/1000_hab"] = (
    df_saude_mensal["nascimentos"] / df_saude_mensal["populacao"] * 1000
)

df_saude_mensal.to_csv(
    "../../OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/Saúde/df_saude_mensal.csv",
    index=False,
)
