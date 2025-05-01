import numpy as np


def ajuste_tradutores(tradutor, colunas_tamanhos):
    """
    Ajusta os valores de colunas específicas de um DataFrame para string com zero à esquerda,
    mantendo todas as colunas originais.

    Parâmetros:
    tradutor (DataFrame): O DataFrame a ser ajustado.
    colunas_tamanhos (dict): Dicionário no formato {coluna: tamanho}.

    Retorna:
    DataFrame com colunas ajustadas e sem duplicatas.
    """
    return tradutor.drop_duplicates().assign(
        **{
            col: lambda x, c=col, t=tam: x[c].astype(str).str.zfill(t)
            for col, tam in colunas_tamanhos.items()
        }
    )


def calcular_soma_br_por_sh6(df, ano_inicial, ano_final):
    """Calcula o total exportado do Brasil por SH6 para um intervalo de anos"""
    anos = f"{str(ano_inicial)[-2:]}_{str(ano_final)[-2:]}"
    filtro_anos = df["CO_ANO"].between(ano_inicial, ano_final)
    return (
        df[filtro_anos]
        .groupby("CO_SH6")["VL_FOB"]
        .sum()
        .reset_index()
        .rename(columns={"VL_FOB": f"soma_br_{anos}"})
    )


def calcular_soma_br_total(df, ano_inicial, ano_final):
    """Calcula o total exportado do Brasil para um intervalo de anos"""
    filtro_anos = df["CO_ANO"].between(ano_inicial, ano_final)
    return df[filtro_anos]["VL_FOB"].sum()


def calcular_soma_uf_por_sh6(df, ano_inicial, ano_final, uf_selecionada):
    """Calcula o total exportado do Brasil por SH6 para um intervalo de anos"""
    anos = f"{str(ano_inicial)[-2:]}_{str(ano_final)[-2:]}"
    filtro_anos = df["CO_ANO"].between(ano_inicial, ano_final)
    filtro_uf = df["SG_UF_NCM"] == uf_selecionada
    return (
        df[filtro_anos & filtro_uf]
        .groupby(["CO_SH6", "SG_UF_NCM"])["VL_FOB"]
        .sum()
        .reset_index()
        .rename(columns={"VL_FOB": f"soma_uf_{anos}"})
    )


def calcular_soma_uf_total(df, ano_inicial, ano_final, uf_selecionada):
    """Calcula o total exportado da UF para um intervalo de anos"""
    anos = f"{str(ano_inicial)[-2:]}_{str(ano_final)[-2:]}"
    filtro_anos = df["CO_ANO"].between(ano_inicial, ano_final)
    filtro_uf = df["SG_UF_NCM"] == uf_selecionada
    return (
        df[filtro_anos & filtro_uf]
        .groupby("SG_UF_NCM")["VL_FOB"]
        .sum()
        .reset_index()
        .rename(columns={"VL_FOB": f"soma_uf_total_{anos}"})
    )


def calcular_vcr(
    df,
    soma_uf_18_20,
    soma_uf_21_23,
    soma_br_18_20,
    soma_br_21_23,
    soma_uf_total_18_20,
    soma_uf_total_21_23,
    df_br_total,
    uf_selecionada,
):
    """Calcula o VCR"""
    filtro_uf = df["SG_UF_NCM"] == uf_selecionada
    return (
        df[["CO_SH6", "SG_UF_NCM"]][filtro_uf]
        .drop_duplicates()
        .merge(soma_uf_18_20, on=["CO_SH6", "SG_UF_NCM"], how="left")
        .merge(soma_uf_21_23, on=["CO_SH6", "SG_UF_NCM"], how="left")
        .merge(soma_br_18_20, on="CO_SH6", how="left")
        .merge(soma_br_21_23, on="CO_SH6", how="left")
        .merge(soma_uf_total_18_20, on="SG_UF_NCM", how="left")
        .merge(soma_uf_total_21_23, on="SG_UF_NCM", how="left")
        .merge(df_br_total, how="cross")
        .assign(
            vcr_18_20=lambda x: (x["soma_uf_18_20"] / x["soma_uf_total_18_20"])
            / (x["soma_br_18_20"] / x["soma_br_total_18_20"]),
            vcr_21_23=lambda x: (x["soma_uf_21_23"] / x["soma_uf_total_21_23"])
            / (x["soma_br_21_23"] / x["soma_br_total_21_23"]),
        )
        .fillna(0)[
            [
                "SG_UF_NCM",
                "CO_SH6",
                "soma_uf_18_20",
                "soma_uf_21_23",
                "vcr_18_20",
                "vcr_21_23",
            ]
        ]
        .round(2)
    )


def ordenando_pais_exp(df, coluna, chave):
    """Ordena os países por valor exportado"""
    return (
        df.sort_values(by=coluna, ascending=False)
        .groupby([chave], as_index=False)
        .agg({"NO_PAIS": lambda x: ", ".join(x)})
    )


def paises_em_comum(row):
    """Verifica se o país está na lista de países do mapa"""
    paises_uf = row["NO_PAIS_uf"].split(", ")
    paises_mapa = row["NO_PAIS_mapa"].split(", ")
    comuns = [pais for pais in paises_uf if pais in paises_mapa]
    return ", ".join(comuns[:5])


def identificar_principais_destinos(
    df, uf_selecionada, tradutor_ncm, tradutor_paises, tradutor_grupos=None
):
    """Identifica os principais destinos de exportação da UF selecionada"""

    filtro_uf = df["SG_UF_NCM"] == uf_selecionada
    filtro_anos = df["CO_ANO"].between(2021, 2023)

    df_filtrado = (
        df[filtro_uf & filtro_anos]
        .assign(CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8))
        .merge(tradutor_ncm, on="CO_NCM", how="left")
        .groupby(["CO_SH6", "SG_UF_NCM", "CO_PAIS"], as_index=False)["VL_FOB"]
        .sum()
        .rename(columns={"VL_FOB": "exp_destino_21_22_23"})
        .merge(tradutor_paises, on="CO_PAIS", how="left")
    )

    # Se o tradutor_grupos for fornecido, faz o agrupamento adicional
    if tradutor_grupos is not None:
        df_filtrado = (
            df_filtrado.merge(tradutor_grupos, on="CO_SH6", how="left")
            .groupby(["desc_grupo", "NO_PAIS"], as_index=False)["exp_destino_21_22_23"]
            .sum()
        )

    return df_filtrado


def gerar_oportunidades(
    tipo,
    df,
    uf_selecionada=None,
    filtro_oportunidades_selecionadas=None,
    tradutor_sh6=None,
    tradutor_grupos=None,
    df_oportunidades=None,
    principais_destinos_sh6=None,
    principais_destinos_grupo=None,
    top_5_destinos_mapa_sh6=None,
    top_5_destinos_mapa_grupo=None,
):
    """Gera os dataframes com as oportunidades tradicionais"""

    if tipo == "classificadas":
        filtro_sh6 = df["CO_SH6"].isin(filtro_oportunidades_selecionadas)
        return (
            df[filtro_sh6]
            .groupby(["CO_SH6", "NO_PAIS", "classificacao_mapa"], as_index=False)[
                "vl_fob"
            ]
            .sum()
            .merge(tradutor_grupos, on="CO_SH6", how="left")
            .merge(tradutor_sh6, on="CO_SH6", how="left")
            .rename(
                columns={
                    "CO_SH6": "cod_sh6",
                    "NO_PAIS": "pais",
                    "vl_fob": "exp_br_23",
                    "NO_SH6_POR": "desc_sh6",
                }
            )[
                [
                    "cod_sh6",
                    "desc_sh6",
                    "desc_grupo",
                    "pais",
                    "exp_br_23",
                    "classificacao_mapa",
                ]
            ]
            .round(2)
        )

    elif tipo == "uf_sh6":
        filtro_sh6 = df["CO_SH6"].isin(filtro_oportunidades_selecionadas)
        filtro_uf = df["SG_UF_NCM"] == uf_selecionada
        return (
            df[filtro_uf & filtro_sh6]
            .query("CO_ANO == '2023'")
            .drop(columns=["CO_ANO", "SG_UF_NCM"])
            .merge(tradutor_sh6, on="CO_SH6", how="left")
            .merge(tradutor_grupos, on="CO_SH6", how="left")
            .merge(df_oportunidades, on="CO_SH6", how="left")
            .merge(principais_destinos_sh6, on="CO_SH6", how="left")
            .merge(top_5_destinos_mapa_sh6, on="CO_SH6", how="left")
            .rename(
                columns={
                    "CO_SH6": "cod_sh6",
                    "SG_UF_NCM": "uf",
                    "VL_FOB": "exp_uf_23",
                    "NO_SH6_POR": "desc_sh6",
                }
            )[
                [
                    "uf",
                    "cod_sh6",
                    "desc_sh6",
                    "desc_grupo",
                    "exp_uf_23",
                    "soma_uf_18_20",
                    "soma_uf_21_23",
                    "vcr_18_20",
                    "vcr_21_23",
                    "delta_vcr",
                    "paises_comuns",
                    "top_5_destinos_mapa",
                ]
            ]
        )

    elif tipo == "uf_grupo":
        filtro_sh6 = df["CO_SH6"].isin(filtro_oportunidades_selecionadas)
        filtro_uf = df["SG_UF_NCM"] == uf_selecionada
        return (
            df[filtro_uf & filtro_sh6]
            .query("CO_ANO == '2023'")
            .drop(columns=["CO_ANO", "SG_UF_NCM"])
            .merge(tradutor_grupos, on="CO_SH6", how="left")
            .merge(df_oportunidades, on="CO_SH6", how="left")
            .groupby(["desc_grupo", "SG_UF_NCM"], as_index=False)["VL_FOB"]
            .sum()
            .merge(principais_destinos_grupo, on="desc_grupo", how="left")
            .merge(top_5_destinos_mapa_grupo, on="desc_grupo", how="left")
            .rename(columns={"SG_UF_NCM": "uf", "VL_FOB": "exp_uf_23"})[
                [
                    "uf",
                    "desc_grupo",
                    "exp_uf_23",
                    "paises_comuns",
                    "top_5_destinos_mapa",
                ]
            ]
        )

    else:
        raise ValueError(
            "O parâmetro 'tipo' deve ser 'classificadas', 'uf_sh6' ou 'uf_grupo'"
        )


def gerar_oportunidades_explorar(
    df,
    filtro_quartil_oportunidades_explorar,
    filtro_tx_crescimento_oportunidades_explorar,
):
    """Gera o dataframe com as oportunidades a explorar"""

    df = df.assign(
        taxa_cresc_exp=lambda x: (x["soma_uf_21_23"] / x["soma_uf_18_20"] - 1) * 100
    )

    filtro_oport_explorar = df["soma_uf_21_23"] <= filtro_quartil_oportunidades_explorar
    filtro_tx_crescimento = (
        df["taxa_cresc_exp"] >= filtro_tx_crescimento_oportunidades_explorar
    )
    filtro_delta_vcr = df["delta_vcr"] >= 0
    cols_finais = [
        "uf",
        "cod_sh6",
        "desc_sh6",
        "desc_grupo",
        "exp_uf_23",
        "soma_uf_18_20",
        "soma_uf_21_23",
        "vcr_18_20",
        "vcr_21_23",
        "delta_vcr",
        "taxa_cresc_exp",
        "paises_comuns",
        "top_5_destinos_mapa",
    ]
    return (
        df[filtro_oport_explorar & filtro_tx_crescimento & filtro_delta_vcr]
        .query("exp_uf_23 > 1000")
        .sort_values(by="exp_uf_23", ascending=False)[cols_finais]
    )


def ajuste_rais(df, coluna, tipo):
    """Ajusta o df da RAIS para calculo do VCR dos vinculos, estabelecimentos e salarios do CNAE da UF"""

    group_cols = ["ano"] if not coluna else ["ano", coluna]

    return df.groupby(group_cols, as_index=False).agg(
        **{
            f"total_vinculos_{tipo}": ("qtd_vinculos", "sum"),
            f"total_estab_{tipo}": ("qtd_estabelecimentos", "sum"),
            f"total_salarios_{tipo}": ("soma_remuneracao", "sum"),
        }
    )


def vcr_rais(
    df,
    uf_selecionada,
    total_uf,
    total_br,
    total_cnae_br,
):
    """Calcula o VCR dos vínculos, estabelecimentos e salários do CNAE da UF"""
    filtro_uf = df["sigla_uf"] == uf_selecionada
    return (
        df[filtro_uf]
        .groupby(["ano", "sigla_uf", "cod_grupo", "grupo_desc"], as_index=False)
        .agg(
            vinculos_cnae_uf=("qtd_vinculos", "sum"),
            estab_cnae_uf=("qtd_estabelecimentos", "sum"),
            salarios_cnae_uf=("soma_remuneracao", "sum"),
        )
        .merge(total_uf, on=["ano", "sigla_uf"], how="left")
        .merge(total_br, on="ano", how="left")
        .merge(total_cnae_br, on=["ano", "cod_grupo"], how="left")
        .assign(
            VCR_estabelecimentos=lambda x: (x["estab_cnae_uf"] / x["total_estab_uf"])
            / (x["total_estab_cnae_br"] / x["total_estab_br"]),
            VCR_vinculos=lambda x: (x["vinculos_cnae_uf"] / x["total_vinculos_uf"])
            / (x["total_vinculos_cnae_br"] / x["total_vinculos_br"]),
            VCR_salarios=lambda x: (x["salarios_cnae_uf"] / x["total_salarios_uf"])
            / (x["total_salarios_cnae_br"] / x["total_salarios_br"]),
        )
        .drop(
            columns=[
                "ano",
                "total_vinculos_uf",
                "total_estab_uf",
                "total_salarios_uf",
                "total_vinculos_br",
                "total_estab_br",
                "total_salarios_br",
                "total_vinculos_cnae_br",
                "total_estab_cnae_br",
                "total_salarios_cnae_br",
            ]
        )
    )


def identificar_maiores_sh6_nao_tradicionais(df, tradutor_cnae, filtro_quartil):
    """Identifica os maiores SH6 não tradicionais"""

    filtro_quartil = df["soma_uf_21_23"] < filtro_quartil

    return (
        df[filtro_quartil]
        .rename(columns={"CO_SH6": "cod_sh6", "SG_UF_NCM": "sigla_uf"})
        .merge(tradutor_cnae, on="cod_sh6", how="left")
        .groupby("cod_grupo")
        .apply(
            lambda x: x.nlargest(5, "soma_uf_21_23", keep="all"), include_groups=False
        )
        .reset_index()
        .groupby("cod_grupo")
        .agg(cinco_maiores_sh6_nao_trad=("cod_sh6", lambda x: ", ".join(x.astype(str))))
        .reset_index()
    )


def gerar_oportunidades_potenciais(
    df,
    filtro_quartil,
    tradutor_cnae,
    df_rais_uf,
    maiores_sh6_nao_tradicionais,
    exp_oportunidades_tradicionais_cnae,
):
    """Gera o dataframde de oportunidades potenciais"""

    filtro_quartil = df["soma_uf_21_23"] < filtro_quartil
    cols_finais = [
        "grupo_desc",
        "sigla_uf",
        "estab_cnae_uf",
        "VCR_estabelecimentos",
        "vinculos_cnae_uf",
        "VCR_vinculos",
        "salarios_cnae_uf",
        "VCR_salarios",
        "cinco_maiores_sh6_nao_trad",
        "exp_oport_nao_trad",
        "exp_oport_trad",
    ]

    return (
        df[filtro_quartil]
        .rename(columns={"CO_SH6": "cod_sh6", "SG_UF_NCM": "sigla_uf"})
        .merge(tradutor_cnae, on="cod_sh6", how="left")
        .groupby(["cod_grupo", "sigla_uf"], as_index=False)
        .agg(exp_oport_nao_trad=("soma_uf_21_23", "sum"))
        .merge(df_rais_uf, on=["sigla_uf", "cod_grupo"], how="left")
        .query("VCR_estabelecimentos >= 1")
        .query("VCR_vinculos >= 0.7 or VCR_salarios >=0.7")
        .merge(maiores_sh6_nao_tradicionais, on="cod_grupo", how="left")
        .merge(exp_oportunidades_tradicionais_cnae, on="cod_grupo", how="left")
        .replace({np.nan: 0})
        .sort_values("exp_oport_trad", ascending=True)
        .reset_index(drop=True)[:30][cols_finais]
        .round(2)
    )
