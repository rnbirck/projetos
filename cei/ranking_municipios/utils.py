import pandas as pd

caminho_prefeituras = (
    "D:/OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/Prefeituras/"
)
# Queries
# Municipios do Rio Grande do Sul
query_municipios = """
SELECT
    id_municipio,
    id_municipio_6,
    municipio
FROM municipio
WHERE sigla_uf = 'RS'"""

query_pib = """
SELECT 
    ano, 
    id_municipio, 
    pib
FROM `basedosdados.br_ibge_pib.municipio`
WHERE ano >= 2015
"""

query_renda_vinculos = """
SELECT
    ano,
    id_municipio,
    SUM(valor_remuneracao_dezembro) AS soma_salario_nominal,
    COUNT(vinculo_ativo_3112) AS qntd_vinculos
FROM `basedosdados.br_me_rais.microdados_vinculos`
WHERE ano >= 2018 AND sigla_uf = 'RS' AND vinculo_ativo_3112 = '1'
GROUP BY ano, id_municipio
"""

query_snis = """
SELECT
    ano,
    id_municipio,
    populacao_atendida_agua,
    indice_perda_faturamento,
FROM `basedosdados.br_mdr_snis.municipio_agua_esgoto`
WHERE ano >= 2015 AND sigla_uf = 'RS'
"""

query_caged_antigo = """
SELECT
    ano,
    id_municipio,
    SUM(saldo_movimentacao) AS saldo_movimentacao
FROM `basedosdados.br_me_caged.microdados_antigos`
WHERE ano = 2019 AND sigla_uf = 'RS'
GROUP BY ano, id_municipio
"""

query_caged = """
SELECT
    ano,
    id_municipio,
    SUM(saldo_movimentacao) AS saldo_movimentacao
FROM caged_prefeituras
WHERE ano < 2025 AND sigla_uf = 'RS'
GROUP BY ano, id_municipio
"""

# Caminhos para arquivos
caminho_populacao = caminho_prefeituras + "Demografia_PIB/populacao_estimada.csv"
caminho_comex = caminho_prefeituras + "Comex/comex.csv"
caminho_renda = caminho_prefeituras + "Emprego e Renda/RAIS/VINCULOS/rais_renda_rs.csv"
caminho_vulnerabilidade_social = (
    caminho_prefeituras + "Assistencia Social/cadastro_unico.csv"
)
caminho_seguranca = caminho_prefeituras + "Segurança/seguranca.csv"
caminho_saude = caminho_prefeituras + "Saúde/df_saude_mensal.csv"
caminho_sisab = caminho_prefeituras + "Saúde/base_sisab_dash.csv"
caminho_matriculas_creche = caminho_prefeituras + "Educação/Matrículas/base_censo.csv"
caminho_notas_saeb = caminho_prefeituras + "Educação/Notas/saeb_ideb_municipios.csv"
caminho_rendimento_educacao = (
    caminho_prefeituras + "Educação/Rendimento/tx_rendimento.csv"
)
caminho_docentes = caminho_prefeituras + "Educação/adequacao_docente/"
caminho_despesas_educacao = (
    caminho_prefeituras + "Educação/despesas_educacao/despesas_educacao.csv"
)
caminho_emissao = caminho_prefeituras + "Meio Ambiente/SEEG/emissao.csv"
caminho_coleta = caminho_prefeituras + "Meio Ambiente/coleta/coleta.csv"


# ETL para a construção da base de dados
def ajustes_populacao(filtro_mun_rs):
    return (
        pd.read_csv(caminho_populacao)
        .astype({"id_municipio": str})
        .query(f"id_municipio in {filtro_mun_rs}")
        .groupby(["ano", "id_municipio", "municipio"], as_index=False)
        .agg({"populacao": "sum"})
    )


def ajustes_populacao_mulher(filtro_mun_rs):
    return (
        pd.read_csv(caminho_populacao, engine="pyarrow")
        .astype({"id_municipio": str})
        .query(f"sexo == 'Mulher' & id_municipio in {filtro_mun_rs}")
        .groupby(["ano", "id_municipio"], as_index=False)
        .agg({"populacao": "sum"})
        .rename(columns={"populacao": "populacao_mulher"})
    )


def ajustes_pib_per_capita(pib, filtro_mun_rs, populacao):
    return (
        pib.query(f"id_municipio in {filtro_mun_rs}")
        .merge(populacao, on=["id_municipio", "ano"], how="left")
        .assign(pib_per_capita=lambda x: (x["pib"] / x["populacao"]))
        .drop(columns=["pib", "populacao"])
    )


def ajustes_comex_per_capita(comex, populacao, ano_minimo, ano_maximo):
    return (
        comex.query(f"ano >= {ano_minimo} and ano <= {ano_maximo}")
        .groupby(["ano", "id_municipio"], as_index=False)
        .agg({"vl_fob": "sum"})
        .astype({"id_municipio": str})
        .merge(
            populacao,
            on=["ano", "id_municipio"],
            how="outer",
        )
        .assign(exportacao_per_capita=lambda x: (x["vl_fob"] / x["populacao"]))
        .fillna(0)
        .drop(columns=["vl_fob", "populacao"])
    )


def ajustes_renda_media(renda_vinculos_raw, municipios):
    return renda_vinculos_raw.merge(municipios, on="id_municipio", how="left").assign(
        renda_media=lambda x: (x["soma_salario_nominal"] / x["qntd_vinculos"])
    )[["ano", "id_municipio", "municipio", "renda_media"]]


def ajustes_vinculos_per_capita(renda_vinculos_raw, populacao):
    return (
        renda_vinculos_raw.merge(populacao, on=["id_municipio", "ano"], how="left")
        .assign(vinculos_per_capita=lambda x: (x["qntd_vinculos"] / x["populacao"]))
        .drop(columns=["qntd_vinculos", "populacao", "soma_salario_nominal"])
    )


def ajustes_formalidade_mercado_trabalho(renda_vinculos_raw):
    filtro_idada_pea = [
        "15 a 19 anos",
        "20 a 24 anos",
        "25 a 29 anos",
        "30 a 34 anos",
        "35 a 39 anos",
        "40 a 44 anos",
        "45 a 49 anos",
        "50 a 54 anos",
        "55 a 59 anos",
        "60 a 64 anos",
        "65 a 69 anos",
    ]

    return (
        pd.read_csv(caminho_populacao)
        .query(f"faixa_etaria in {filtro_idada_pea}")
        .groupby(["ano", "id_municipio", "municipio"], as_index=False)
        .agg({"populacao": "sum"})
        .rename(columns={"populacao": "pea"})
        .astype({"id_municipio": str})
        .merge(
            renda_vinculos_raw[["ano", "id_municipio", "qntd_vinculos"]],
            on=["ano", "id_municipio"],
            how="right",
        )
        .assign(formalidade_mercado_trabalho=lambda x: (x["qntd_vinculos"] / x["pea"]))[
            ["ano", "id_municipio", "municipio", "formalidade_mercado_trabalho"]
        ]
    )


def ajustes_geracao_emprego_per_capita(caged_raw, caged_antigo_raw, populacao):
    return (
        pd.concat([caged_antigo_raw, caged_raw])
        .merge(populacao, how="left", on=["id_municipio", "ano"])
        .assign(
            geracao_emprego_per_capita=lambda df: (
                df["saldo_movimentacao"] / df["populacao"]
            )
            * 1000
        )
        .drop(columns=["saldo_movimentacao", "populacao"])
    )


def ajustes_vulnerabilidade_social(populacao, ano_minimo, ano_maximo):
    return (
        pd.read_csv(caminho_vulnerabilidade_social, engine="pyarrow")
        .query(f"ano >= {ano_minimo} and ano <= {ano_maximo} and mes == 12")[
            [
                "ano",
                "id_municipio",
                "Quantidade total de pessoas inscritas no Cadastro Único",
            ]
        ]
        .rename(
            columns={
                "Quantidade total de pessoas inscritas no Cadastro Único": "qtd_cad_unico"
            }
        )
        .astype({"id_municipio": str})
        .merge(
            populacao,
            how="left",
            on=["ano", "id_municipio"],
        )
        .assign(
            vulnerabilidade_social=lambda x: x["qtd_cad_unico"] / x["populacao"],
        )
        .drop(columns=["qtd_cad_unico", "populacao"])
    )


def ajustes_indicadores_seguranca(populacao, ano_minimo, ano_maximo):
    return (
        pd.read_csv(caminho_seguranca, engine="pyarrow")[
            [
                "id_municipio",
                "municipio",
                "Ano",
                "Mês",
                " Homicídio  Doloso",
                " Furtos",
                " Roubos",
                " Roubo de Veículo",
                " Delitos Relacionados à Armas e Munições",
            ]
        ]
        .rename(
            columns={
                "Ano": "ano",
                "Mês": "mes",
                " Homicídio  Doloso": "homicidio_doloso",
                " Furtos": "furtos",
                " Roubos": "roubos",
                " Roubo de Veículo": "roubo_de_veiculo",
                " Delitos Relacionados à Armas e Munições": "delitos_armas",
            }
        )
        .groupby(
            ["ano", "id_municipio", "municipio"],
            as_index=False,
        )
        .agg(
            {
                "homicidio_doloso": "sum",
                "furtos": "sum",
                "roubos": "sum",
                "roubo_de_veiculo": "sum",
                "delitos_armas": "sum",
            }
        )
        .astype({"id_municipio": str})
        .query(f"ano >= {ano_minimo} and ano <= {ano_maximo}")
        .merge(populacao, on=["id_municipio", "ano", "municipio"], how="left")
        .assign(
            homicidio_doloso_per_capita=lambda x: (
                x["homicidio_doloso"] / x["populacao"]
            )
            * 1000,
            furtos_per_capita=lambda x: (x["furtos"] / x["populacao"]) * 1000,
            roubos_per_capita=lambda x: (x["roubos"] / x["populacao"]) * 1000,
            roubos_veiculos_per_capita=lambda x: (
                x["roubo_de_veiculo"] / x["populacao"]
            )
            * 1000,
            delitos_armas_per_capita=lambda x: (x["delitos_armas"] / x["populacao"])
            * 1000,
        )
        .drop(
            columns=[
                "homicidio_doloso",
                "furtos",
                "roubos",
                "roubo_de_veiculo",
                "delitos_armas",
                "populacao",
            ]
        )
    )


def ajustes_indicadores_violencia_mulher(populacao_mulher, ano_minimo, ano_maximo):
    return (
        pd.read_csv(
            caminho_seguranca,
            engine="pyarrow",
        )[["id_municipio", "municipio", "Ano", "Mês", "Ameaça", "Estupro"]]
        .rename(
            columns={
                "Ano": "ano",
                "Mês": "mes",
                "Ameaça": "ameaca",
                "Estupro": "estupro",
            }
        )
        .query(f"ano >= {ano_minimo} and ano <= {ano_maximo}")
        .groupby(["ano", "id_municipio", "municipio"], as_index=False)
        .agg(
            {
                "ameaca": "sum",
                "estupro": "sum",
            }
        )
        .astype({"id_municipio": str})
        .merge(populacao_mulher, on=["id_municipio", "ano"], how="left")
        .assign(
            ameaca_per_capita=lambda x: (x["ameaca"] / x["populacao_mulher"]) * 1000,
            estupro_per_capita=lambda x: (x["estupro"] / x["populacao_mulher"]) * 1000,
        )
        .drop(columns=["populacao_mulher", "ameaca", "estupro"])
    )


def ajustes_indicadores_saude(ano_minimo, ano_maximo, municipios):
    return (
        pd.concat(
            [
                (
                    pd.read_csv(
                        caminho_saude,
                        engine="pyarrow",
                    )
                    .query(f"ano >= {ano_minimo} and ano <= {ano_maximo}")
                    .groupby(
                        ["ano", "municipio"],
                        as_index=False,
                    )
                    .agg(
                        {
                            "taxa_obitos_infantis": "mean",
                            "coef_neonatal": "mean",
                            "prop_nasc_adolesc": "mean",
                        }
                    )
                ),
                (
                    pd.read_csv(
                        "../../prefeituras/data/saude/mensal_antigo/df_saude_mensal.csv",
                        engine="pyarrow",
                    )
                    .groupby(
                        ["ano", "municipio"],
                        as_index=False,
                    )
                    .agg(
                        {
                            "taxa_obitos_infantis": "mean",
                            "coef_neonatal": "mean",
                            "prop_nasc_adolesc": "mean",
                        }
                    )
                ),
            ]
        )
        .assign(
            municipio=lambda x: x["municipio"].replace(
                "Santana do Livramento", "Sant'Ana do Livramento"
            ),
        )
        .merge(municipios[["id_municipio", "municipio"]], on="municipio", how="left")
    )


def ajustes_obitos_evitaveis(populacao, municipios):
    return (
        pd.read_csv(
            "data/obitos_evitaveis.csv", engine="pyarrow", sep=";", encoding="utf-8"
        )
        .assign(
            id_municipio_6=lambda x: x["Municipio"].str.slice(0, 6),
        )
        .merge(municipios, on="id_municipio_6", how="left")
        .drop(columns=["Municipio", "Total", "id_municipio_6"])
        .melt(
            id_vars=["id_municipio", "municipio"],
            var_name="ano",
            value_name="obitos_evitaveis",
        )
        .assign(obitos_evitaveis=lambda x: x["obitos_evitaveis"].replace("-", 0))
        .astype({"ano": int, "id_municipio": str, "obitos_evitaveis": int})
        .merge(populacao, on=["id_municipio", "municipio", "ano"], how="left")
        .assign(
            obitos_evitaveis_per_capita=lambda x: (
                x["obitos_evitaveis"] / x["populacao"]
            )
            * 1000
        )
        .drop(columns=["populacao", "municipio", "obitos_evitaveis"])
    )


def ajustes_sisab(municipios):
    return (
        pd.read_csv(
            caminho_sisab,
            engine="pyarrow",
        )
        .astype({"cod_mun": str})
        .merge(municipios, left_on="cod_mun", right_on="id_municipio_6", how="left")
        .assign(
            ano=lambda x: x["periodo"].str.slice(0, 4).astype(int),
        )
        .groupby(["ano", "id_municipio"], as_index=False)
        .agg(
            {
                "gestantes_pre_natal": "mean",
                "gestantes_odonto": "mean",
                "gestantes_hiv": "mean",
                "mulheres_aps": "mean",
                "crianças_vacinadas": "mean",
                "diabetes": "mean",
                "hipertensao": "mean",
            }
        )
        .rename(
            columns=lambda col_name: f"{col_name}_sisab"
            if col_name not in ["ano", "id_municipio"]
            else col_name
        )
    )


def ajustes_matriculas_creche():
    return (
        pd.read_csv(
            caminho_matriculas_creche,
            engine="pyarrow",
        )
        .query("dependencia == 'municipal'")[
            ["ano", "id_municipio", "taxa_matricula_creche"]
        ]
        .rename(columns={"taxa_matricula_creche": "taxa_cobertura_creche_municipal"})
        .astype({"id_municipio": str})
    )


def ajustes_notas_saeb(categoria):
    return (
        pd.read_csv(
            caminho_notas_saeb,
            engine="pyarrow",
        )
        .query(f"rede == 'Pública' & indicador != 'ideb' & categoria == '{categoria}'")
        .pivot_table(
            index=["id_municipio", "ano"],
            columns="indicador",
            values="valor",
        )
        .reset_index()
        .rename(
            columns={
                "nota_mat": f"nota_mat_{categoria}",
                "nota_port": f"nota_port_{categoria}",
            }
        )
        .astype({"id_municipio": str})
    )


def ajustes_rendimento_educacao():
    categorias_rendimento = [
        "taxa_distorcao_fundamental_anos_iniciais",
        "taxa_distorcao_fundamental_anos_finais",
        "taxa_abandono_fundamental_anos_iniciais",
        "taxa_abandono_fundamental_anos_finais",
    ]

    return (
        pd.read_csv(
            caminho_rendimento_educacao,
            engine="pyarrow",
        )
        .query(f"dependencia == 'Total' & categoria in {categorias_rendimento}")
        .pivot_table(
            index=["id_municipio", "ano"],
            columns="categoria",
            values="valor",
        )
        .reset_index()
        .astype({"id_municipio": str})
    )


def ajustes_adequacao_docentes(ano):
    return (
        pd.read_excel(
            caminho_docentes + f"AFD_MUNICIPIOS_{ano}.xlsx",
            engine="calamine",
            skiprows=10,
        )
        .query(
            f"NU_ANO_CENSO == {ano} & SG_UF == 'RS' & NO_DEPENDENCIA == 'Total' & NO_CATEGORIA == 'Total'"
        )
        .assign(
            id_municipio=lambda x: x["CO_MUNICIPIO"].astype(int).astype(str),
            adequacao_formacao_docente=lambda x: pd.to_numeric(
                x["FUN_CAT_1"].replace("--", "")
            ),
        )[["NU_ANO_CENSO", "id_municipio", "adequacao_formacao_docente"]]
        .rename(
            columns={
                "NU_ANO_CENSO": "ano",
            }
        )
        .reset_index(drop=True)
    )


def ajuste_despesas_educacao(populacao):
    return (
        pd.read_csv(caminho_despesas_educacao, engine="pyarrow")
        .drop(columns=["municipio"])
        .astype({"id_municipio": str})
        .merge(populacao, on=["ano", "id_municipio"], how="left")
        .assign(
            despesas_educacao_per_capita=lambda x: x["despesas_educacao"]
            / x["populacao"],
            ano=lambda x: x["ano"].astype(int),
        )
        .drop(columns=["populacao", "despesas_educacao"])
    )


def ajuste_emissao(populacao):
    return (
        pd.read_csv(caminho_emissao, engine="pyarrow")
        .astype({"id_municipio": "str"})
        .groupby(["ano", "id_municipio"], as_index=False)
        .agg({"emissao": "sum"})
        .merge(populacao, on=["ano", "id_municipio"], how="left")
        .assign(emissao_gases_per_capita=lambda x: x["emissao"] / x["populacao"])
        .drop(columns=["populacao", "emissao"])
        .dropna(subset=["municipio"])
    )


def ajuste_agua(df_raw, populacao):
    return (
        df_raw.merge(populacao, how="left", on=["id_municipio", "ano"])
        .assign(
            prop_atendimento_agua=lambda x: x["populacao_atendida_agua"]
            / x["populacao"],
        )
        .drop(columns=["populacao", "populacao_atendida_agua"])
    )


def ajuste_residuos(municipios):
    return (
        pd.read_csv(
            caminho_coleta,
            sep=";",
            encoding="cp1252",
            engine="python",
        )
        .drop(columns=["Município", "Estado"])
        .set_axis(["id_municipio_6", "ano", "prop_coleta_residuos"], axis="columns")
        .assign(
            id_municipio_6=lambda x: x["id_municipio_6"].astype(str),
            ano=lambda x: x["ano"].astype(int).astype(str),
            prop_coleta_residuos=lambda x: pd.to_numeric(
                x["prop_coleta_residuos"].str.replace(",", ".")
            ),
        )
        .merge(
            municipios[["id_municipio", "id_municipio_6"]],
            how="right",
            on="id_municipio_6",
        )
        .drop(columns=["id_municipio_6"])
    )


def ajuste_df(df, coluna_valor):
    return (
        df.pivot_table(
            index=["id_municipio"],
            columns=["ano"],
            values=[coluna_valor],
        )
        .reset_index()
        .pipe(
            lambda df_pipe: df_pipe.set_axis(
                [
                    c1 if (c1 == "id_municipio" and c2 == "") else f"{c1}_{c2}"
                    for c1, c2 in df_pipe.columns
                ],
                axis=1,
            )
        )
    )


def ajuste_df_lista(df, lista):
    df_pivotado = df.pivot_table(index="id_municipio", columns="ano", values=lista)

    novas_colunas = []
    for nome_indicador, ano_valor in df_pivotado.columns:
        novas_colunas.append(f"{nome_indicador}_{ano_valor}")
    df_pivotado.columns = novas_colunas

    df_final = df_pivotado.reset_index()

    return df_final


def selecionar_colunas_para_merge(
    df_origem: pd.DataFrame, coluna_chave: str, sufixo_ano: str
) -> list:
    colunas_selecionadas = [coluna_chave]

    # Adiciona outras colunas que correspondem ao critério
    for col in df_origem.columns:
        if (
            isinstance(col, str)
            and col.endswith(str(sufixo_ano))
            and col != coluna_chave
        ):
            colunas_selecionadas.append(col)

    return colunas_selecionadas


def realizar_merge_com_selecao_ano(
    df_principal: pd.DataFrame,
    df_adicional: pd.DataFrame,
    coluna_chave: str,
    sufixo_ano: str,
    tipo_merge: str = "left",
) -> pd.DataFrame:
    colunas_do_df_adicional = selecionar_colunas_para_merge(
        df_adicional, coluna_chave, sufixo_ano
    )

    df_adicional_subset = df_adicional[colunas_do_df_adicional]

    df_merged = df_principal.merge(df_adicional_subset, on=coluna_chave, how=tipo_merge)
    return df_merged
