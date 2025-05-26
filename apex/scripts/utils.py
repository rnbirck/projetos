import numpy as np
import pandas as pd


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


def gerar_exp_regiao(df_exp_completa, tradutor_uf_regiao):
    return (
        df_exp_completa.merge(
            tradutor_uf_regiao, left_on="SG_UF_NCM", right_on="uf", how="left"
        )
        .groupby(["CO_ANO", "regiao"], as_index=False)
        .agg(EXP_REGIAO=("VL_FOB", "sum"))
    )


def gerar_part_exp_uf_regiao(
    df_exp_completa, tradutor_uf_regiao, df_exp_regiao, uf_selecionada
):
    return (
        df_exp_completa.merge(
            tradutor_uf_regiao, left_on="SG_UF_NCM", right_on="uf", how="left"
        )
        .groupby(["CO_ANO", "regiao", "SG_UF_NCM"], as_index=False)
        .agg(EXP_UF=("VL_FOB", "sum"))
        .merge(df_exp_regiao, on=["CO_ANO", "regiao"], how="left")
        .assign(PART_EXP_REGIAO=lambda x: x["EXP_UF"] / x["EXP_REGIAO"])
        .query(f"SG_UF_NCM == '{uf_selecionada}'")
        .reset_index(drop=True)[
            ["CO_ANO", "SG_UF_NCM", "regiao", "EXP_UF", "EXP_REGIAO", "PART_EXP_REGIAO"]
        ]
    )


def gerar_exp_uf_regiao(df_exp_completa, tradutor_uf_regiao, ano_minimo, ano_maximo):
    return (
        df_exp_completa.merge(
            tradutor_uf_regiao, left_on="SG_UF_NCM", right_on="uf", how="left"
        )
        .query(f"CO_ANO in ({ano_minimo}, {ano_maximo}) & SG_UF_NCM != 'ND'")
        .groupby(["CO_ANO", "SG_UF_NCM", "nome_uf", "regiao"], as_index=False)["VL_FOB"]
        .sum()
        .pivot_table(
            index=["SG_UF_NCM", "nome_uf", "regiao"],
            columns="CO_ANO",
            values="VL_FOB",
        )
        .sort_values(ano_maximo, ascending=False)
        .reset_index()
        .assign(posicao=lambda x: x.index + 1)
    )


def gerar_exp_uf_historico(df_exp_completa, uf_selecionada):
    return (
        df_exp_completa.query(f"SG_UF_NCM == '{uf_selecionada}'")
        .groupby(["CO_ANO"], as_index=False)["VL_FOB"]
        .sum()
    )


def gerar_exp_via(df_exp_completa, uf_selecionada, tradutor_via):
    return (
        df_exp_completa.query(f"SG_UF_NCM == '{uf_selecionada}'")
        .groupby(["CO_ANO", "SG_UF_NCM", "CO_VIA"], as_index=False)["VL_FOB"]
        .sum()
        .assign(CO_VIA=lambda x: x["CO_VIA"].astype(str))
        .merge(tradutor_via, left_on="CO_VIA", right_on="id_via", how="left")
        .drop(columns=["CO_VIA"])
    )


def gerar_balanca_comercial(df_exp_completa, df_imp_completa, uf_selecionada):
    return (
        pd.concat(
            [
                df_exp_completa.query(f"SG_UF_NCM == '{uf_selecionada}'")
                .groupby(["CO_ANO", "SG_UF_NCM"], as_index=False)["VL_FOB"]
                .sum()
                .assign(FLUXO="EXP"),
                df_imp_completa.query(f"SG_UF_NCM == '{uf_selecionada}'")
                .groupby(["CO_ANO", "SG_UF_NCM"], as_index=False)["VL_FOB"]
                .sum()
                .assign(FLUXO="IMP"),
            ]
        )
        .pivot_table(index=["CO_ANO", "SG_UF_NCM"], columns="FLUXO", values="VL_FOB")
        .reset_index()
        .assign(SALDO=lambda x: x["EXP"] - x["IMP"])
    )


def gerar_exp_mun_uf(
    df_exp_mun, uf_selecionada, ano_minimo, tradutor_sh4, tradutor_mun
):
    return (
        df_exp_mun.query(f"SG_UF_MUN == '{uf_selecionada}' & CO_ANO >= {ano_minimo}")
        .groupby(["CO_ANO", "SH4", "SG_UF_MUN", "CO_MUN"], as_index=False)["VL_FOB"]
        .sum()
        .assign(
            SH4=lambda x: x["SH4"]
            .astype(str)
            .str.pad(width=4, side="left", fillchar="0")
        )
        .merge(tradutor_sh4, left_on="SH4", right_on="id_sh4", how="left")
        .merge(tradutor_mun, left_on="CO_MUN", right_on="id_mun", how="left")
    )


def gerar_exp_part_mun(df_exp_mun_uf, ano_maximo):
    return (
        df_exp_mun_uf.query(f"CO_ANO == {ano_maximo}")
        .groupby(["CO_ANO", "SG_UF_MUN", "mun"], as_index=False)
        .agg(VL_FOB_MUN=("VL_FOB", "sum"))
        .merge(
            df_exp_mun_uf.query(f"CO_ANO == {ano_maximo}")
            .groupby(["CO_ANO", "SG_UF_MUN"], as_index=False)
            .agg(VL_FOB_UF=("VL_FOB", "sum")),
            on=["CO_ANO", "SG_UF_MUN"],
            how="left",
        )
        .assign(PART_UF=lambda x: (x["VL_FOB_MUN"] / x["VL_FOB_UF"]) * 100)
        .sort_values("VL_FOB_MUN", ascending=False)
        .head(n=5)
        .drop(columns=["VL_FOB_UF"])
    )


def gerar_exp_mun_sh4(df_exp_mun_uf, ano_maximo, filtro_mun):
    return (
        df_exp_mun_uf.query(f"CO_ANO == {ano_maximo}")
        .sort_values("VL_FOB", ascending=False)
        .groupby("mun")
        .head(3)
        .reset_index(drop=True)
        .assign(
            id_desc_sh4=lambda x: x["id_sh4"] + " - " + x["desc_sh4"],
        )
        .merge(
            df_exp_mun_uf.query(f"CO_ANO == {ano_maximo}")
            .groupby(["CO_ANO", "SG_UF_MUN", "id_sh4"], as_index=False)
            .agg(VL_FOB_MUN=("VL_FOB", "sum")),
            on=["CO_ANO", "SG_UF_MUN", "id_sh4"],
        )
        .assign(PART_UF_SH4=lambda x: (x["VL_FOB"] / x["VL_FOB_MUN"]) * 100)
        .query("mun.isin(@filtro_mun)")[
            ["CO_ANO", "SG_UF_MUN", "mun", "id_desc_sh4", "PART_UF_SH4", "VL_FOB"]
        ]
        .sort_values(["mun", "VL_FOB"], ascending=[True, False])
        .reset_index(drop=True)
    )


def gerar_exp_mesorregioes(
    df_exp_mun, uf_selecionada, ano_minimo, ano_maximo, tradutor_mesorregiao
):
    return (
        df_exp_mun.query(
            f"CO_ANO in ({ano_minimo}, {ano_maximo}) & SG_UF_MUN == '{uf_selecionada}'"
        )
        .groupby(["CO_ANO", "SG_UF_MUN", "CO_MUN"], as_index=False)["VL_FOB"]
        .sum()
        .astype({"CO_MUN": str})
        .merge(
            tradutor_mesorregiao, left_on="CO_MUN", right_on="id_municipio", how="left"
        )
        .groupby(["CO_ANO", "nome_mesorregiao"], as_index=False)["VL_FOB"]
        .sum()
        .pivot_table(index="nome_mesorregiao", columns="CO_ANO", values="VL_FOB")
        .reset_index()
        .sort_values(ano_maximo, ascending=False)
    )


def gerar_exp_macrossetores(
    df_exp_completa, uf_selecionada, ano_minimo, ano_maximo, tradutor_ncm, tradutor_isic
):
    return (
        df_exp_completa.query(
            f"CO_ANO >= {ano_minimo} & CO_ANO <= {ano_maximo} & SG_UF_NCM == '{uf_selecionada}'"
        )
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8),
        )
        .merge(tradutor_ncm, left_on="CO_NCM", right_on="id_ncm", how="left")
        .merge(tradutor_isic, on="id_sh6", how="left")
        .groupby(["CO_ANO", "SG_UF_NCM", "desc_isic"], as_index=False)["VL_FOB"]
        .sum()
        .pivot_table(index="desc_isic", columns="CO_ANO", values="VL_FOB")
        .reset_index()
    )


def gerar_exp_grupo(
    df_exp_completa,
    uf_selecionada,
    ano_minimo,
    ano_maximo,
    tradutor_ncm,
    tradutor_grupo,
):
    return (
        df_exp_completa.query(
            f"CO_ANO >= {ano_minimo} & CO_ANO <= {ano_maximo} & SG_UF_NCM == '{uf_selecionada}'"
        )
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8),
        )
        .merge(tradutor_ncm, left_on="CO_NCM", right_on="id_ncm", how="left")
        .merge(tradutor_grupo, on="id_sh6", how="left")
        .groupby(["CO_ANO", "SG_UF_NCM", "desc_grupo"], as_index=False)["VL_FOB"]
        .sum()
        .pivot_table(index="desc_grupo", columns="CO_ANO", values="VL_FOB")
        .fillna(0)
        .sort_values(ano_maximo, ascending=False)
        .reset_index()
    )


def gerar_exp_destinos(
    df_exp_completa, ano_minimo, ano_maximo, uf_selecionada, tradutor_pais
):
    return (
        df_exp_completa.query(
            f"CO_ANO >= {ano_minimo} & CO_ANO <= {ano_maximo} & SG_UF_NCM == '{uf_selecionada}'"
        )
        .merge(tradutor_pais, left_on="CO_PAIS", right_on="id_pais", how="left")
        .groupby(["CO_ANO", "SG_UF_NCM", "pais"], as_index=False)["VL_FOB"]
        .sum()
        .pivot_table(index="pais", columns="CO_ANO", values="VL_FOB")
        .fillna(0)
        .sort_values(ano_maximo, ascending=False)
        .reset_index()
    )


def gerar_tabela_auxiliar(
    df_exp_completa,
    ano_minimo,
    ano_maximo,
    uf_selecionada,
    tradutor_ncm,
    tradutor_isic,
    tradutor_pais,
    tradutor_via,
    tradutor_grupo,
    tradutor_sh6,
):
    return (
        df_exp_completa.query(
            f"CO_ANO >= {ano_minimo} & CO_ANO <= {ano_maximo} & SG_UF_NCM == '{uf_selecionada}'"
        )
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8),
            CO_VIA=lambda x: x["CO_VIA"].astype(str),
        )
        .merge(tradutor_ncm, left_on="CO_NCM", right_on="id_ncm", how="left")
        .merge(tradutor_isic, on="id_sh6", how="left")
        .groupby(
            ["CO_ANO", "SG_UF_NCM", "id_sh6", "CO_PAIS", "CO_VIA", "desc_isic"],
            as_index=False,
        )
        .agg({"VL_FOB": "sum"})
        .merge(tradutor_pais, left_on="CO_PAIS", right_on="id_pais", how="left")
        .merge(tradutor_via, left_on="CO_VIA", right_on="id_via", how="left")
        .merge(tradutor_grupo, on="id_sh6", how="left")
        .merge(tradutor_sh6, on="id_sh6", how="left")[
            [
                "CO_ANO",
                "SG_UF_NCM",
                "id_sh6",
                "desc_sh6",
                "desc_grupo",
                "desc_isic",
                "pais",
                "via",
                "VL_FOB",
            ]
        ]
    )


def gerar_tabela_auxiliar_sh6_pais(
    df_exp_completa,
    anos,
    uf_selecionada,
    tradutor_ncm,
    tradutor_pais,
    tradutor_grupo,
    tradutor_sh6,
):
    return (
        df_exp_completa.query(f"CO_ANO in {anos} & SG_UF_NCM == '{uf_selecionada}'")
        .assign(
            CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8),
        )
        .merge(tradutor_ncm, left_on="CO_NCM", right_on="id_ncm", how="left")
        .groupby(["CO_ANO", "SG_UF_NCM", "id_sh6", "CO_PAIS", "CO_VIA"], as_index=False)
        .agg({"VL_FOB": "sum"})
        .merge(tradutor_pais, left_on="CO_PAIS", right_on="id_pais", how="left")
        .merge(tradutor_grupo, on="id_sh6", how="left")
        .merge(tradutor_sh6, on="id_sh6", how="left")[
            [
                "CO_ANO",
                "SG_UF_NCM",
                "id_sh6",
                "desc_sh6",
                "desc_grupo",
                "pais",
                "VL_FOB",
            ]
        ]
    )


def gerar_tabela_auxiliar_uf(
    df_exp_mun,
    ano_minimo,
    ano_maximo,
    uf_selecionada,
    tradutor_sh4,
    tradutor_mun,
    tradutor_pais,
    tradutor_mesorregiao,
):
    return (
        df_exp_mun.query(
            f"CO_ANO >= {ano_minimo} & CO_ANO <= {ano_maximo} & SG_UF_MUN == '{uf_selecionada}'"
        )
        .groupby(["CO_ANO", "SH4", "CO_PAIS", "SG_UF_MUN", "CO_MUN"], as_index=False)[
            "VL_FOB"
        ]
        .sum()
        .assign(
            SH4=lambda x: x["SH4"]
            .astype(str)
            .str.pad(width=4, side="left", fillchar="0")
        )
        .merge(tradutor_sh4, left_on="SH4", right_on="id_sh4", how="left")
        .merge(tradutor_mun, left_on="CO_MUN", right_on="id_mun", how="left")
        .merge(tradutor_pais, left_on="CO_PAIS", right_on="id_pais", how="left")
        .merge(
            tradutor_mesorregiao.astype({"id_municipio": int}),
            left_on="CO_MUN",
            right_on="id_municipio",
            how="left",
        )[
            [
                "CO_ANO",
                "SG_UF_MUN",
                "mun",
                "id_sh4",
                "desc_sh4",
                "pais",
                "nome_mesorregiao",
                "VL_FOB",
            ]
        ]
    )


# ORBIS
ufs = [
    ("Acre", "AC", "Acre"),
    ("Alagoas", "AL", "Alagoas"),
    ("Amapa", "AP", "Amapá"),
    ("Amazonas", "AM", "Amazonas"),
    ("Bahia", "BA", "Bahia"),
    ("Ceara", "CE", "Ceará"),
    ("Federal district", "DF", "Distrito Federal"),
    ("Espirito Santo", "ES", "Espírito Santo"),
    ("Goias", "GO", "Goiás"),
    ("Maranhao", "MA", "Maranhão"),
    ("Mato Grosso", "MT", "Mato Grosso"),
    ("Mato Grosso do Sul", "MS", "Mato Grosso do Sul"),
    ("Minas Gerais", "MG", "Minas Gerais"),
    ("Para", "PA", "Pará"),
    ("Paraiba", "PB", "Paraíba"),
    ("Parana", "PR", "Paraná"),
    ("Pernambuco", "PE", "Pernambuco"),
    ("Piaui", "PI", "Piauí"),
    ("Rio de Janeiro", "RJ", "Rio de Janeiro"),
    ("Rio Grande do Norte", "RN", "Rio Grande do Norte"),
    ("Rio Grande do Sul", "RS", "Rio Grande do Sul"),
    ("Rondonia", "RO", "Rondônia"),
    ("Roraima", "RR", "Roraima"),
    ("Santa Catarina", "SC", "Santa Catarina"),
    ("Sao Paulo", "SP", "São Paulo"),
    ("Sergipe", "SE", "Sergipe"),
    ("Tocantins", "TO", "Tocantins"),
]

tradutor_uf = pd.DataFrame(ufs, columns=["uf", "sigla_uf", "uf_ajustada"])

ufs_regiao = [
    ("AC", "Norte"),
    ("AL", "Nordeste"),
    ("AP", "Norte"),
    ("AM", "Norte"),
    ("BA", "Nordeste"),
    ("CE", "Nordeste"),
    ("DF", "Centro-Oeste"),
    ("ES", "Sudeste"),
    ("GO", "Centro-Oeste"),
    ("MA", "Nordeste"),
    ("MT", "Centro-Oeste"),
    ("MS", "Centro-Oeste"),
    ("MG", "Sudeste"),
    ("PA", "Norte"),
    ("PB", "Nordeste"),
    ("PR", "Sul"),
    ("PE", "Nordeste"),
    ("PI", "Nordeste"),
    ("RJ", "Sudeste"),
    ("RN", "Nordeste"),
    ("RS", "Sul"),
    ("RO", "Norte"),
    ("RR", "Norte"),
    ("SC", "Sul"),
    ("SP", "Sudeste"),
    ("SE", "Nordeste"),
    ("TO", "Norte"),
]

tradutor_regiao = pd.DataFrame(ufs_regiao, columns=["sigla_uf", "regiao"])


def ajuste_orbis(df):
    return (
        df.rename(
            columns={
                "Capital expenditure\nm USD": "investimento",
                "Destination market – Country": "pais_destino",
                "Source market – Country": "pais_origem",
                "Investing company BvD Sector primary code description": "setor",
                "Investing company name": "empresa",
                "Project status": "status",
                "Last project status date": "date",
            }
        )
        .assign(
            date=lambda x: pd.to_datetime(x["date"], format="%Y-%m-%d"),
            ano=lambda x: x["date"].dt.year,
            setor=lambda x: x["setor"].str.extract("^([^/]*)")[0].str.strip(),
        )
        .query(
            "ano >= 2015 & status == 'Completed' & investimento != 'n.a.' & pais_destino != '' & pais_origem != ''"
        )
        .assign(investimento=lambda x: x["investimento"].astype(float))
        .groupby(
            ["ano", "pais_origem", "pais_destino", "setor", "empresa"], as_index=False
        )
        .agg(total_investimento=("investimento", "sum"))
    )


def ajuste_investimento_br(
    df_orbis, anos_iniciais, anos_iniciais_coluna, anos_finais, anos_finais_coluna
):
    return (
        df_orbis.query("pais_destino == 'Brazil' & pais_origem != 'Brazil'")
        .groupby(["ano", "setor"], as_index=False)
        .agg(total_investimento_br=("total_investimento", "sum"))
        .assign(
            total_investimento_br_anos_iniciais=lambda x: np.where(
                x["ano"].isin(anos_iniciais), x["total_investimento_br"], 0
            ),
            total_investimento_br_anos_finais=lambda x: np.where(
                x["ano"].isin(anos_finais), x["total_investimento_br"], 0
            ),
        )
        .groupby(["setor"], as_index=False)
        .agg(
            total_investimento_br_anos_iniciais=(
                "total_investimento_br_anos_iniciais",
                "sum",
            ),
            total_investimento_br_anos_finais=(
                "total_investimento_br_anos_finais",
                "sum",
            ),
        )
        .sort_values("total_investimento_br_anos_finais", ascending=False)
        .reset_index(drop=True)
        .rename(
            columns={
                "total_investimento_br_anos_iniciais": f"total_investimento_br_{anos_iniciais_coluna}",
                "total_investimento_br_anos_finais": f"total_investimento_br_{anos_finais_coluna}",
            }
        )
    )


def ajuste_investimento_mundo(
    df_orbis, anos_iniciais, anos_iniciais_coluna, anos_finais, anos_finais_coluna
):
    return (
        df_orbis.query("pais_destino != 'Brazil'")
        .groupby(["ano", "setor"], as_index=False)
        .agg(total_investimento_mundo=("total_investimento", "sum"))
        .assign(
            total_investimento_mundo_anos_iniciais=lambda x: np.where(
                x["ano"].isin(anos_iniciais), x["total_investimento_mundo"], 0
            ),
            total_investimento_mundo_anos_finais=lambda x: np.where(
                x["ano"].isin(anos_finais), x["total_investimento_mundo"], 0
            ),
        )
        .groupby(["setor"], as_index=False)
        .agg(
            total_investimento_mundo_anos_iniciais=(
                "total_investimento_mundo_anos_iniciais",
                "sum",
            ),
            total_investimento_mundo_anos_finais=(
                "total_investimento_mundo_anos_finais",
                "sum",
            ),
        )
        .sort_values("total_investimento_mundo_anos_finais", ascending=False)
        .reset_index(drop=True)
        .rename(
            columns={
                "total_investimento_mundo_anos_iniciais": f"total_investimento_mundo_{anos_iniciais_coluna}",
                "total_investimento_mundo_anos_finais": f"total_investimento_mundo_{anos_finais_coluna}",
            }
        )
    )


def ajuste_investimento_final(
    df_investimento_mundo,
    df_investimento_br,
    anos_iniciais_coluna,
    anos_finais_coluna,
    tradutor_orbis,
):
    return (
        pd.merge(df_investimento_mundo, df_investimento_br, on="setor", how="left")
        .replace(np.nan, 0)
        .assign(
            part_br_anos_finais=lambda x: (
                (
                    x[f"total_investimento_br_{anos_finais_coluna}"]
                    / x[f"total_investimento_mundo_{anos_finais_coluna}"]
                )
                * 100
            ),
            part_br_18_19_21=lambda x: (
                (
                    x[f"total_investimento_br_{anos_iniciais_coluna}"]
                    / x[f"total_investimento_mundo_{anos_iniciais_coluna}"]
                )
                * 100
            ),
            tx_cresc_br=lambda x: (
                (
                    (
                        x[f"total_investimento_br_{anos_finais_coluna}"]
                        / x[f"total_investimento_br_{anos_iniciais_coluna}"]
                    )
                    - 1
                )
                * 100
            ),
            tx_cresc_mundo=lambda x: (
                (
                    (
                        x[f"total_investimento_mundo_{anos_finais_coluna}"]
                        / x[f"total_investimento_mundo_{anos_iniciais_coluna}"]
                    )
                    - 1
                )
                * 100
            ),
            total_investimento_mundo=lambda x: (
                x[f"total_investimento_mundo_{anos_finais_coluna}"].sum()
            ),
            total_investimento_br=lambda x: (
                x[f"total_investimento_br_{anos_finais_coluna}"].sum()
            ),
            vcr_br=lambda x: (
                (
                    x[f"total_investimento_br_{anos_finais_coluna}"]
                    / x["total_investimento_br"]
                )
                / (
                    x[f"total_investimento_mundo_{anos_finais_coluna}"]
                    / x["total_investimento_mundo"]
                )
            ),
            part_investimento_no_br=lambda x: (
                x[f"total_investimento_br_{anos_finais_coluna}"]
                / x["total_investimento_br"]
            ),
            crescimento_correto=lambda x: np.where(
                (x["tx_cresc_br"] < 0) & (x["tx_cresc_mundo"] < 0),
                x["tx_cresc_br"] > x["tx_cresc_mundo"],
                x["tx_cresc_br"] > x["tx_cresc_mundo"],
            ),
        )
        .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
        .query(
            '(vcr_br > 0.7 | crescimento_correto) & setor_orbis_trad != "Fabricação de Alimentos e Tabaco"'
        )[
            [
                "setor_orbis_trad",
                f"total_investimento_br_{anos_finais_coluna}",
                "part_investimento_no_br",
            ]
        ]
        .rename(
            columns={
                "setor_orbis_trad": "setor",
                f"total_investimento_br_{anos_finais_coluna}": "investimento",
            }
        )
        .sort_values("part_investimento_no_br", ascending=False)
        .reset_index(drop=True)
    )


def ajuste_orbis_uf(df):
    return (
        df.rename(
            columns={
                "Capital expenditure\nm USD": "investimento",
                "Destination market – Country": "pais_destino",
                "Destination market – Region in country": "uf",
                "Source market – Country": "pais_origem",
                "Investing company BvD Sector primary code description": "setor",
                "Investing company name": "empresa",
                "Project status": "status",
                "Last project status date": "date",
            }
        )
        .assign(
            date=lambda x: pd.to_datetime(x["date"], format="%Y-%m-%d"),
            ano=lambda x: x["date"].dt.year,
            setor=lambda x: x["setor"].str.extract("^([^/]*)")[0].str.strip(),
        )
        .query(
            "ano >= 2014 & ano != 2025 & status == 'Completed' & investimento != 'n.a.' & pais_destino != '' & pais_origem != ''"
        )
        .assign(investimento=lambda x: x["investimento"].astype(float))
        .groupby(
            ["ano", "uf", "pais_origem", "pais_destino", "setor", "empresa"],
            as_index=False,
        )
        .agg(total_investimento=("investimento", "sum"))
    )


def ajuste_rais_investimentos(df, categoria, tradutor):
    return (
        df.merge(tradutor, left_on="cod_grupo", right_on="grupo", how="left")
        .groupby([f"{categoria}", f"descricao_{categoria}", "sigla_uf"], as_index=False)
        .agg(
            soma_remuneracao_uf=("soma_remuneracao", "sum"),
            qtd_vinculos_uf=("qtd_vinculos", "sum"),
            qtd_estabelecimentos_uf=("qtd_estabelecimentos", "sum"),
        )
        .assign(
            soma_remuneracao_total_uf=lambda x: x.groupby("sigla_uf")[
                "soma_remuneracao_uf"
            ].transform("sum"),
            soma_vinculos_total_uf=lambda x: x.groupby("sigla_uf")[
                "qtd_vinculos_uf"
            ].transform("sum"),
            soma_estabelecimentos_total_uf=lambda x: x.groupby("sigla_uf")[
                "qtd_estabelecimentos_uf"
            ].transform("sum"),
            soma_remuneracao_brasil=lambda x: x.groupby(f"{categoria}")[
                "soma_remuneracao_uf"
            ].transform("sum"),
            soma_vinculos_brasil=lambda x: x.groupby(f"{categoria}")[
                "qtd_vinculos_uf"
            ].transform("sum"),
            soma_estabelecimentos_brasil=lambda x: x.groupby(f"{categoria}")[
                "qtd_estabelecimentos_uf"
            ].transform("sum"),
            soma_remuneracao_total_brasil=lambda x: x["soma_remuneracao_uf"].sum(),
            soma_vinculos_total_brasil=lambda x: x["qtd_vinculos_uf"].sum(),
            soma_estabelecimentos_total_brasil=lambda x: x[
                "qtd_estabelecimentos_uf"
            ].sum(),
            # vcr_remuneracao = lambda x: ((x['soma_remuneracao_uf'] / x['soma_remuneracao_total_uf'])/(x['soma_remuneracao_brasil'] / x['soma_remuneracao_total_brasil'])),
            # vcr_vinculos = lambda x: ((x['qtd_vinculos_uf'] / x['soma_vinculos_total_uf'])/(x['soma_vinculos_brasil'] / x['soma_vinculos_total_brasil'])),
            # vcr_estabelecimentos = lambda x: ((x['qtd_estabelecimentos_uf'] / x['soma_estabelecimentos_total_uf'])/(x['soma_estabelecimentos_brasil'] / x['soma_estabelecimentos_total_brasil'])),
            categoria=f"{categoria}",
        )
        .rename(
            columns={f"{categoria}": "cnae", f"descricao_{categoria}": "cnae_descricao"}
        )
        .reset_index(drop=True)
    )


def ajuste_rais_uf(
    df_rais,
    tradutor_orbis_cnae,
    tradutor_orbis,
    filtro_setores_selecionados,
    uf_selecionada,
):
    return (
        df_rais.merge(tradutor_orbis_cnae, on=["cnae", "categoria"], how="left")
        .dropna(subset=["setor_orbis"])
        .drop(columns=["cnae", "desc_cnae", "cnae_descricao"])
        .groupby(["setor_orbis", "categoria", "sigla_uf"], as_index=False)
        .agg(
            {
                "soma_remuneracao_uf": "sum",
                "qtd_vinculos_uf": "sum",
                "qtd_estabelecimentos_uf": "sum",
                "soma_remuneracao_total_uf": "first",
                "soma_vinculos_total_uf": "first",
                "soma_estabelecimentos_total_uf": "first",
                "soma_remuneracao_brasil": "sum",
                "soma_vinculos_brasil": "sum",
                "soma_estabelecimentos_brasil": "sum",
                "soma_remuneracao_total_brasil": "first",
                "soma_vinculos_total_brasil": "first",
                "soma_estabelecimentos_total_brasil": "first",
            }
        )
        .assign(
            participacao_remuneracao=lambda x: (
                x["soma_remuneracao_uf"] / x["soma_remuneracao_total_uf"]
            ),
            participacao_vinculos=lambda x: (
                x["qtd_vinculos_uf"] / x["soma_vinculos_total_uf"]
            ),
            participacao_estabelecimentos=lambda x: (
                x["qtd_estabelecimentos_uf"] / x["soma_estabelecimentos_total_uf"]
            ),
            vcr_remuneracao=lambda x: (
                (x["soma_remuneracao_uf"] / x["soma_remuneracao_total_uf"])
                / (x["soma_remuneracao_brasil"] / x["soma_remuneracao_total_brasil"])
            ),
            vcr_vinculos=lambda x: (
                (x["qtd_vinculos_uf"] / x["soma_vinculos_total_uf"])
                / (x["soma_vinculos_brasil"] / x["soma_vinculos_total_brasil"])
            ),
            vcr_estabelecimentos=lambda x: (
                (x["qtd_estabelecimentos_uf"] / x["soma_estabelecimentos_total_uf"])
                / (
                    x["soma_estabelecimentos_brasil"]
                    / x["soma_estabelecimentos_total_brasil"]
                )
            ),
        )
        .merge(tradutor_orbis, on="setor_orbis", how="left")
        .query(
            "vcr_remuneracao > 0.7 & vcr_vinculos > 0.7 & vcr_estabelecimentos > 0.7 & setor_orbis_trad in @filtro_setores_selecionados & sigla_uf == @uf_selecionada"
        )
        .reset_index(drop=True)
        .rename(
            columns={
                "setor_orbis_trad": "setor",
                "qtd_vinculos_uf": "vinculos",
                "qtd_estabelecimentos_uf": "estabelecimentos",
                "soma_remuneracao_uf": "massa_salarial",
                "participacao_remuneracao": "participacao_massa_salarial",
                "vcr_remuneracao": "vcr_massa_salarial",
            }
        )[
            [
                "setor",
                "sigla_uf",
                "vinculos",
                "estabelecimentos",
                "massa_salarial",
                "participacao_vinculos",
                "participacao_estabelecimentos",
                "participacao_massa_salarial",
                "vcr_vinculos",
                "vcr_estabelecimentos",
                "vcr_massa_salarial",
            ]
        ]
    )


def paises_setor(df, igualdade, destino, tradutor_pais):
    return (
        df.merge(tradutor_pais, left_on="pais_origem", right_on="pais_eng", how="left")
        .query(f"ano >= 2022 & pais_destino {igualdade} 'Brazil'")
        .groupby(["setor", "pais"], as_index=False)
        .agg({"total_investimento": "sum"})
        .groupby("setor", as_index=True)
        .apply(
            lambda x: pd.Series(
                {
                    f"principais_paises_{destino}": ", ".join(
                        x.sort_values("total_investimento", ascending=False)["pais"]
                        .head(5)
                        .tolist()
                    )
                }
            ),
            include_groups=False,
        )
    )


def ajuste_orbis_regiao(
    df_orbis_uf: pd.DataFrame,
    tradutor_uf: pd.DataFrame,
    tradutor_regiao: pd.DataFrame,
    anos_orbis: list,
    regiao_selecionada: str = None,
):
    query_string = "ano in @anos_orbis"

    if regiao_selecionada:
        query_string += f" & regiao == '{regiao_selecionada}'"

    return (
        df_orbis_uf.pipe(ajuste_orbis_uf)
        .merge(tradutor_uf, on="uf", how="left")
        .merge(tradutor_regiao, on="sigla_uf", how="left")
        .query(query_string)
        .groupby(["sigla_uf", "uf_ajustada"], as_index=False)["total_investimento"]
        .sum()
        .sort_values("total_investimento", ascending=False)
        .reset_index(drop=True)
    )


def ajuste_orbis_uf_setor(
    df_orbis_uf: pd.DataFrame,
    tradutor_uf: pd.DataFrame,
    tradutor_orbis: pd.DataFrame,
    anos_orbis: list,
    uf_selecionada: str,
):
    return (
        df_orbis_uf.pipe(ajuste_orbis_uf)
        .merge(tradutor_uf, on="uf", how="left")
        .query(f"sigla_uf == '{uf_selecionada}' & ano in @anos_orbis")
        .groupby(["sigla_uf", "setor"], as_index=False)["total_investimento"]
        .sum()
        .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
        .drop(columns=["setor", "setor_orbis"])
        .rename(columns={"setor_orbis_trad": "setor"})
        .sort_values("total_investimento", ascending=False)[
            ["sigla_uf", "setor", "total_investimento"]
        ]
    )


def ajuste_orbis_uf_pais(
    df_orbis_uf: pd.DataFrame,
    tradutor_uf: pd.DataFrame,
    tradutor_pais: pd.DataFrame,
    anos_orbis: list,
    uf_selecionada: str,
):
    return (
        df_orbis_uf.pipe(ajuste_orbis_uf)
        .merge(tradutor_uf, on="uf", how="left")
        .query(f"sigla_uf == '{uf_selecionada}' & ano in @anos_orbis")
        .merge(tradutor_pais, left_on="pais_origem", right_on="pais_eng", how="left")
        .groupby(["sigla_uf", "pais"], as_index=False)["total_investimento"]
        .sum()
        .sort_values("total_investimento", ascending=False)[
            ["sigla_uf", "pais", "total_investimento"]
        ]
    )


def ajuste_orbis_uf_empresa(
    df_orbis_uf: pd.DataFrame,
    tradutor_uf: pd.DataFrame,
    uf_selecionada: str,
    anos_orbis: list,
):
    return (
        df_orbis_uf.pipe(ajuste_orbis_uf)
        .merge(tradutor_uf, on="uf", how="left")
        .query(f"sigla_uf == '{uf_selecionada}' & ano in @anos_orbis")
        .groupby(["sigla_uf", "empresa"], as_index=False)["total_investimento"]
        .sum()
        .sort_values("total_investimento", ascending=False)
        .reset_index(drop=True)
    )


def ajuste_empresas_nao_investem_brasil(
    df_orbis: pd.DataFrame,
    tradutor_orbis: pd.DataFrame,
    anos_iniciais: list,
    df_tab_setores_uf: pd.DataFrame,
):
    return (
        df_orbis.query('ano in @anos_iniciais & pais_destino != "Brazil"')
        .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
        .groupby(["setor_orbis_trad", "empresa"], as_index=False)
        .agg(total_investimento=("total_investimento", "sum"))
        .groupby("setor_orbis_trad", as_index=False)
        .apply(
            lambda x: pd.Series(
                {
                    "principais_empresas_nao_investem_brasil": ", ".join(
                        x.sort_values("total_investimento", ascending=False)["empresa"]
                        .head(5)
                        .tolist()
                    )
                }
            ),
            include_groups=False,
        )
        .merge(
            df_tab_setores_uf[["setor"]],
            left_on="setor_orbis_trad",
            right_on="setor",
            how="left",
        )
        .dropna()[["setor", "principais_empresas_nao_investem_brasil"]]
        .reset_index(drop=True)
    )


def ajuste_empresas_investem_brasil(df_orbis, tradutor_orbis, anos_iniciais):
    return (
        df_orbis.query('ano in @anos_iniciais & pais_destino == "Brazil"')
        .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
        .groupby(["setor_orbis_trad", "empresa"], as_index=False)
        .agg(total_investimento=("total_investimento", "sum"))
        .groupby("setor_orbis_trad")
        .apply(
            lambda x: pd.Series(
                {
                    "principais_empresas_investem_brasil": ", ".join(
                        x.sort_values("total_investimento", ascending=False)[
                            "empresa"
                        ].tolist()
                    )
                }
            ),
            include_groups=False,
        )
        .rename(columns={"setor_orbis_trad": "setor"})
        .reset_index()
    )


def ajuste_empresas_investem_uf(
    df_orbis_uf, tradutor_orbis, anos_iniciais, uf_selecionada
):
    return (
        df_orbis_uf.pipe(ajuste_orbis_uf)
        .merge(tradutor_uf, on="uf", how="left")
        .query(f"sigla_uf == '{uf_selecionada}' & ano in @anos_iniciais")
        .merge(tradutor_orbis, left_on="setor", right_on="setor_orbis", how="left")
        .groupby(["setor_orbis_trad", "empresa"], as_index=False)["total_investimento"]
        .sum()
        .groupby("setor_orbis_trad")
        .apply(
            lambda x: pd.Series(
                {
                    "principais_empresas_investem_uf": ", ".join(
                        x.sort_values("total_investimento", ascending=False)[
                            "empresa"
                        ].tolist()
                    )
                }
            ),
            include_groups=False,
        )
        .rename(columns={"setor_orbis_trad": "setor"})
        .reset_index()
    )


def encontrar_empresas_nao_selecionadas(
    df_empresas_investem_brasil, df_empresas_investem_uf
):
    resultado = {}
    for setor in df_empresas_investem_brasil.index:
        empresas_brasil = set(
            df_empresas_investem_brasil.loc[
                setor, "principais_empresas_investem_brasil"
            ].split(", ")
        )
        if setor in df_empresas_investem_uf.index:
            empresas_selecionada = set(
                df_empresas_investem_uf.loc[
                    setor, "principais_empresas_investem_uf"
                ].split(", ")
            )
            empresas_nao_selecionadas = list(empresas_brasil - empresas_selecionada)
            resultado[setor] = ", ".join(
                empresas_nao_selecionadas[:5]
            )  # Selecionar as 3 primeiras e juntar na mesma coluna
        else:
            resultado[setor] = ", ".join(
                list(empresas_brasil)[:5]
            )  # Selecionar as 3 primeiras e juntar na mesma coluna
    return resultado
