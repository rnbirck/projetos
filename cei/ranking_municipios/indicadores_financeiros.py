# %%
import pandas as pd

base = pd.read_csv("data/base_indicadores.csv", engine="pyarrow")


def ajuste_indicador_exec_orc_corrente(base):
    filtro_cod_conta_exec_orc_corrente = base["cod_conta"].isin(
        [
            "ReceitasCorrentes",
            "ReceitasIntraOrcamentarias",
            "DespesasCorrentes",
            "DespesasIntraOrcamentarias",
        ]
    )
    filtro_coluna_exec_orc_corrente = base["coluna"].isin(
        ["Até o Bimestre (c)", "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)"]
    )

    return (
        base[filtro_cod_conta_exec_orc_corrente & filtro_coluna_exec_orc_corrente][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            DespesasIntraOrcamentarias=lambda x: x["DespesasIntraOrcamentarias"].fillna(
                0
            ),
            ReceitasIntraOrcamentarias=lambda x: x["ReceitasIntraOrcamentarias"].fillna(
                0
            ),
        )
        .assign(
            indicador_exec_orc_corrente=lambda x: (
                (x["DespesasCorrentes"] + x["DespesasIntraOrcamentarias"])
                / (x["ReceitasCorrentes"] + x["ReceitasIntraOrcamentarias"])
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_exec_orc_corrente",
            ]
        ]
    )


def ajuste_indicador_autonomia_fiscal(base):
    filtro_cod_conta_autonomia_fiscal = base["cod_conta"].isin(
        [
            "RREO3TransferenciasCorrentes",
            "DeducaoDeReceitaParaFormacaoDoFUNDEB",
            "RREO3ReceitaCorrenteLiquida",
        ]
    )

    return (
        base[filtro_cod_conta_autonomia_fiscal][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            indicador_autonomia_fiscal=lambda x: (
                (
                    x["RREO3TransferenciasCorrentes"]
                    - x["DeducaoDeReceitaParaFormacaoDoFUNDEB"]
                )
                / x["RREO3ReceitaCorrenteLiquida"]
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_autonomia_fiscal",
            ]
        ]
    )


def ajuste_indicador_endividamento(base):
    filtro_cod_conta_endividamento = base["cod_conta"] == "PercentualDaDCLSobreARCL"

    return (
        base[filtro_cod_conta_endividamento][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            indicador_endividamento=lambda x: (x["PercentualDaDCLSobreARCL"] / 100)
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_endividamento",
            ]
        ]
    )


def ajuste_indicador_despesas_pessoal(base):
    filtro_cod_conta_desp_pessoal = base["cod_conta"] == "DespesaComPessoalTotal"
    return (
        base[filtro_cod_conta_desp_pessoal][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            indicador_despesas_pessoal=lambda x: (x["DespesaComPessoalTotal"] / 100)
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_despesas_pessoal",
            ]
        ]
    )


def ajuste_indicador_investimentos(base):
    filtro_cod_conta_investimentos = base["cod_conta"].isin(
        ["Investimentos", "InversoesFinanceiras", "RREO3ReceitaCorrenteLiquida"]
    )
    filtro_coluna_investimentos = base["coluna"].isin(
        ["DESPESAS LIQUIDADAS ATÉ O BIMESTRE (h)", "TOTAL (ÚLTIMOS 12 MESES)"]
    )

    return (
        base[filtro_cod_conta_investimentos & filtro_coluna_investimentos][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            InversoesFinanceiras=lambda x: x["InversoesFinanceiras"].fillna(0),
        )
        .assign(
            indicador_investimentos=lambda x: (
                (x["Investimentos"] + x["InversoesFinanceiras"])
                / x["RREO3ReceitaCorrenteLiquida"]
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_investimentos",
            ]
        ]
    )


def ajuste_indicador_disponibilidade_caixa(base):
    filtro_cod_conta_disponibilidade_caixa = base["cod_conta"].isin(
        ["DisponibilidadeDeCaixaLiquidaAposRP", "RREO3ReceitaCorrenteLiquida"]
    )
    filtro_conta_disponibilidade_caixa = base["conta"].isin(
        [
            "RECEITA CORRENTE LÍQUIDA (III) = (I - II)",
            "TOTAL (IV) = (I + II + III)",
            "TOTAL (III) = (I + II)",
        ]
    )

    return (
        base[
            filtro_cod_conta_disponibilidade_caixa & filtro_conta_disponibilidade_caixa
        ][["ano", "id_municipio", "municipio", "cod_conta", "valor"]]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            indicador_disponibilidade_caixa=lambda x: (
                x["DisponibilidadeDeCaixaLiquidaAposRP"]
                / x["RREO3ReceitaCorrenteLiquida"]
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_disponibilidade_caixa",
            ]
        ]
    )


def ajuste_indicador_geracao_caixa(base):
    filtro_cod_conta_geracao_de_caixa = (
        base["cod_conta"] == "DisponibilidadeDeCaixaLiquidaAposRP"
    )

    filtro_conta_geracao_de_caixa = base["conta"].isin(
        [
            "RECEITA CORRENTE LÍQUIDA (III) = (I - II)",
            "TOTAL (IV) = (I + II + III)",
            "TOTAL (III) = (I + II)",
        ]
    )

    return (
        base[filtro_cod_conta_geracao_de_caixa & filtro_conta_geracao_de_caixa][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .sort_values(["municipio", "ano"])
        .assign(
            indicador_geracao_caixa=lambda x: (
                x["DisponibilidadeDeCaixaLiquidaAposRP"]
                / x.groupby("municipio")["DisponibilidadeDeCaixaLiquidaAposRP"].shift(1)
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_geracao_caixa",
            ]
        ]
    )


def ajuste_indicador_restos_a_pagar(base):
    filtro_cod_conta_restos_a_pagar = base["cod_conta"].isin(
        ["SaldoTotal", "RREO3ReceitaCorrenteLiquida"]
    )
    filtro_conta_restos_a_pagar = base["conta"].isin(
        ["TOTAL (III) = (I + II)", "RECEITA CORRENTE LÍQUIDA (III) = (I - II)"]
    )

    return (
        base[filtro_cod_conta_restos_a_pagar & filtro_conta_restos_a_pagar][
            ["ano", "id_municipio", "municipio", "cod_conta", "valor"]
        ]
        .pivot_table(
            index=["ano", "id_municipio", "municipio"],
            columns=[
                "cod_conta",
            ],
            values="valor",
        )
        .reset_index()
        .assign(
            indicador_restos_a_pagar=lambda x: (
                x["SaldoTotal"] / x["RREO3ReceitaCorrenteLiquida"]
            )
        )[
            [
                "ano",
                "id_municipio",
                "municipio",
                "indicador_restos_a_pagar",
            ]
        ]
    )


# Indicador de Execução Orçamentária Corrente
indicador_exec_orc_corrente = ajuste_indicador_exec_orc_corrente(base)

# Indicador de Autonomia Fiscal
indicador_autonomia_fiscal = ajuste_indicador_autonomia_fiscal(base)

# Indicador Endividamento
indicador_endividamento = ajuste_indicador_endividamento(base)

# Indicador Despesas com Pessoal
indicador_despesas_pessoal = ajuste_indicador_despesas_pessoal(base)

# Indicador de Investimentos
indicador_investimentos = ajuste_indicador_investimentos(base)

# Indicador de Disponibilidade de Caixa
indicador_disponibilidade_caixa = ajuste_indicador_disponibilidade_caixa(base)

# Indicador Geracao de Caixa
indicador_geracao_caixa = ajuste_indicador_geracao_caixa(base)

# Indicador de Restos a Pagar
indicador_restos_a_pagar = ajuste_indicador_restos_a_pagar(base)

# Merge dos indicadores
indicadores_financeiros = (
    indicador_exec_orc_corrente.merge(
        indicador_autonomia_fiscal, on=["ano", "id_municipio", "municipio"], how="left"
    )
    .merge(indicador_endividamento, on=["ano", "id_municipio", "municipio"], how="left")
    .merge(
        indicador_despesas_pessoal,
        on=["ano", "id_municipio", "municipio"],
        how="left",
    )
    .merge(indicador_investimentos, on=["ano", "id_municipio", "municipio"], how="left")
    .merge(
        indicador_disponibilidade_caixa,
        on=["ano", "id_municipio", "municipio"],
        how="left",
    )
    .merge(indicador_geracao_caixa, on=["ano", "id_municipio", "municipio"], how="left")
    .merge(
        indicador_restos_a_pagar, on=["ano", "id_municipio", "municipio"], how="left"
    )
)
indicadores_financeiros.to_csv("data/indicadores_financeiros.csv", index=False, sep=";")
