# %%
import pandas as pd
import numpy as np
import utils
import importlib
import argparse

importlib.reload(utils)

# %% CONFIGURAÇÕES
parser = argparse.ArgumentParser(
    description="Processa dados de exportação para uma UF específica."
)
parser.add_argument(
    "--uf",
    type=str,
    required=True,
    help="Sigla da UF a ser processada (ex: BA, SP, MG)",
)
args = parser.parse_args()

uf_selecionada = args.uf  # <-- UF definida pelo argumento

print(f"--- Iniciando processamento para UF: {uf_selecionada} ---")
anos = range(2018, 2024)

df_exp_completa = pd.read_csv(
    "../data/EXP_COMPLETA.CSV", sep=";", encoding="utf-8", engine="pyarrow"
).query("CO_ANO in @anos")

# %% MAPA APEX
cols_mapa = [
    "SH6",
    "País",
    "Exportações de BRA para o país em Ano4 (US$)",
    "Importações totais do país em Ano4 (US$)",
    "Classificação Mapa",
]

mapa_apex = (
    pd.read_excel("../data/mapa_apex_25.xlsx", engine="calamine")[cols_mapa]
    .set_axis(
        ["CO_SH6", "NO_PAIS", "vl_fob", "imp_destino", "classificacao_mapa"], axis=1
    )
    .assign(CO_SH6=lambda x: x["CO_SH6"].astype(str).str.zfill(6))
    .drop_duplicates()
)
# %% TRADUTORES

tradutor_ncm = pd.read_excel("../data/tradutores/NCM.xlsx", engine="calamine").pipe(
    utils.ajuste_tradutores, colunas_tamanhos={"CO_NCM": 8, "CO_SH6": 6}
)

tradutor_paises = pd.read_csv(
    "../data/tradutores/PAIS.csv",
    sep=";",
    encoding="ISO-8859-1",
    on_bad_lines="skip",
    engine="pyarrow",
    usecols=["CO_PAIS", "NO_PAIS"],
)

tradutor_grupos = pd.read_excel(
    "../data/tradutores/trad_cuci.xlsx", engine="calamine"
).pipe(utils.ajuste_tradutores, colunas_tamanhos={"CO_SH6": 6})

tradutor_sh6 = pd.read_csv(
    "../data/tradutores/NCM_SH.csv",
    sep=";",
    encoding="ISO-8859-1",
    on_bad_lines="skip",
    engine="pyarrow",
).pipe(utils.ajuste_tradutores, colunas_tamanhos={"CO_SH6": 6})

tradutor_hs22_to_hs17 = pd.read_excel(
    "../data/tradutores/AJUSTE_SH6.xlsx", engine="calamine"
).pipe(utils.ajuste_tradutores, colunas_tamanhos={"HS17": 6, "HS22": 6})

tradutor_sh6_novos = pd.read_excel(
    "../data/tradutores/novos_sh_2022.xlsx", sheet_name="Novos 2022", engine="calamine"
).pipe(utils.ajuste_tradutores, colunas_tamanhos={"sh22": 6})

tradutor_cnae = (
    pd.read_excel("../data/tradutores/CNAE_SH6.xlsx", engine="calamine")[
        ["SH6", "cod_cnae"]
    ]
    .rename(columns={"SH6": "cod_sh6", "cod_cnae": "cod_grupo"})
    .pipe(utils.ajuste_tradutores, colunas_tamanhos={"cod_sh6": 6})
)

# %% FILTROS

filtro_mapa_apex = mapa_apex["CO_SH6"].unique()

filtro_sh6_novos = tradutor_sh6_novos["sh22"].unique()
# %% DF EXP
df_exp = (
    df_exp_completa.groupby(["CO_ANO", "CO_NCM", "SG_UF_NCM"], as_index=False)["VL_FOB"]
    .sum()
    .assign(CO_NCM=lambda x: x["CO_NCM"].astype(str).str.zfill(8))
    .merge(tradutor_ncm, on="CO_NCM", how="left")
    .groupby(["CO_ANO", "CO_SH6", "SG_UF_NCM"], as_index=False)["VL_FOB"]
    .sum()
    .astype({"CO_ANO": "uint16[pyarrow]"})
)
# %% SOMAS PARA CALCULO DE VCR
soma_br_18_20 = utils.calcular_soma_br_por_sh6(
    df=df_exp, ano_inicial=2018, ano_final=2020
)
soma_br_21_23 = utils.calcular_soma_br_por_sh6(
    df=df_exp, ano_inicial=2021, ano_final=2023
)
soma_uf_18_20 = utils.calcular_soma_uf_por_sh6(
    df=df_exp, ano_inicial=2018, ano_final=2020, uf_selecionada=uf_selecionada
)
soma_uf_21_23 = utils.calcular_soma_uf_por_sh6(
    df=df_exp, ano_inicial=2021, ano_final=2023, uf_selecionada=uf_selecionada
)
soma_uf_total_18_20 = utils.calcular_soma_uf_total(
    df=df_exp, ano_inicial=2018, ano_final=2020, uf_selecionada=uf_selecionada
)
soma_uf_total_21_23 = utils.calcular_soma_uf_total(
    df=df_exp, ano_inicial=2021, ano_final=2023, uf_selecionada=uf_selecionada
)
df_br_total = pd.DataFrame(
    {
        "soma_br_total_18_20": [
            utils.calcular_soma_br_total(df=df_exp, ano_inicial=2018, ano_final=2020)
        ],
        "soma_br_total_21_23": [
            utils.calcular_soma_br_total(df=df_exp, ano_inicial=2021, ano_final=2023)
        ],
    }
)
# %% FILTRO SH6 NOVOS
novos_sh6_1 = (
    df_exp.query("CO_SH6 in @filtro_sh6_novos")
    .merge(tradutor_sh6_novos, left_on="CO_SH6", right_on="sh22", how="left")
    .query("Correlação != 'n:n'")
)
filtros_novos_sh6_1 = novos_sh6_1["CO_SH6"].unique()
novos_sh6_2 = (
    df_exp.query("CO_SH6 in @filtro_sh6_novos")
    .merge(tradutor_sh6_novos, left_on="CO_SH6", right_on="sh22", how="left")
    .query("Correlação != 'n:n'")
    .merge(tradutor_hs22_to_hs17, left_on="CO_SH6", right_on="HS22", how="left")
)
sh6_HS17_selecionados = novos_sh6_2["HS17"].unique()
novos_sh6_3 = df_exp.query(
    "CO_SH6 in @sh6_HS17_selecionados & SG_UF_NCM == @uf_selecionada"
).assign(correlacao="antigos", novos="antigos", HS17=lambda x: x["CO_SH6"])

filtro_sh6_novos_2 = novos_sh6_3["CO_SH6"].unique()
filtro_sh6_novos = np.concatenate([filtro_sh6_novos, filtro_sh6_novos_2])

soma_br_18_20_novos = utils.calcular_soma_br_por_sh6(
    df=novos_sh6_3, ano_inicial=2018, ano_final=2020
)
soma_br_21_23_novos = utils.calcular_soma_br_por_sh6(
    df=novos_sh6_3, ano_inicial=2021, ano_final=2023
)
soma_uf_18_20_novos = utils.calcular_soma_uf_por_sh6(
    novos_sh6_3, 2018, 2020, uf_selecionada
)
soma_uf_21_23_novos = utils.calcular_soma_uf_por_sh6(
    novos_sh6_3, 2021, 2023, uf_selecionada
)

# %% CALCULANDO VCR
df_exp_sem_novos = df_exp.query("CO_SH6 not in @filtro_sh6_novos")

df_vcr_sem_novos = utils.calcular_vcr(
    df=df_exp_sem_novos,
    soma_uf_18_20=soma_uf_18_20,
    soma_uf_21_23=soma_uf_21_23,
    soma_br_18_20=soma_br_18_20,
    soma_br_21_23=soma_br_21_23,
    soma_uf_total_18_20=soma_uf_total_18_20,
    soma_uf_total_21_23=soma_uf_total_21_23,
    df_br_total=df_br_total,
    uf_selecionada=uf_selecionada,
)

df_vcr_novos = utils.calcular_vcr(
    df=novos_sh6_3,
    soma_uf_18_20=soma_uf_18_20_novos,
    soma_uf_21_23=soma_uf_21_23_novos,
    soma_br_18_20=soma_br_18_20_novos,
    soma_br_21_23=soma_br_21_23_novos,
    soma_uf_total_18_20=soma_uf_total_18_20,
    soma_uf_total_21_23=soma_uf_total_21_23,
    df_br_total=df_br_total,
    uf_selecionada=uf_selecionada,
)

df_vcr = (
    pd.concat([df_vcr_sem_novos, df_vcr_novos], ignore_index=True)
    .assign(delta_vcr=lambda x: x["vcr_21_23"] - x["vcr_18_20"])
    .query("soma_uf_21_23 != 0")
    .query("vcr_21_23 > 0.9 or delta_vcr > 0.6")  # valores de corte
    .query("CO_SH6 in @filtro_mapa_apex")  # garantindo que só tenha sh6 do mapa apex
    .reset_index(drop=True)
)
# %% IDENTIFICANDO OS PRINCIPAIS DESTINOS DAS OPORTUNIDADES

filtro_q50 = df_vcr["soma_uf_21_23"].quantile(0.5, interpolation="linear")

df_oportunidades = df_vcr.query("soma_uf_21_23 > @filtro_q50")

filtro_oportunidades_selecionadas = df_oportunidades["CO_SH6"].unique()

destinos_sh6_total = utils.identificar_principais_destinos(
    df=df_exp_completa,
    uf_selecionada=uf_selecionada,
    tradutor_ncm=tradutor_ncm,
    tradutor_paises=tradutor_paises,
)
destinos_grupo_total = utils.identificar_principais_destinos(
    df=df_exp_completa,
    uf_selecionada=uf_selecionada,
    tradutor_ncm=tradutor_ncm,
    tradutor_paises=tradutor_paises,
    tradutor_grupos=tradutor_grupos,
).pipe(utils.ordenando_pais_exp, coluna="exp_destino_21_22_23", chave="desc_grupo")

destinos_uf = utils.ordenando_pais_exp(
    df=destinos_sh6_total, coluna="exp_destino_21_22_23", chave="CO_SH6"
)

destinos_mapa = utils.ordenando_pais_exp(df=mapa_apex, coluna="vl_fob", chave="CO_SH6")

destinos_mapa_grupo = (
    mapa_apex.merge(tradutor_grupos, on="CO_SH6", how="left")
    .groupby(["desc_grupo", "NO_PAIS"], as_index=False)["vl_fob"]
    .sum()
    .pipe(utils.ordenando_pais_exp, coluna="vl_fob", chave="desc_grupo")
)
# Principais destinos de exportacao por SH6 da UF selecionada
principais_destinos_sh6 = (
    destinos_mapa.merge(destinos_uf, on="CO_SH6", how="left", suffixes=("_uf", "_mapa"))
    .fillna({"NO_PAIS_uf": "", "NO_PAIS_mapa": ""})
    .assign(paises_comuns=lambda x: x.apply(utils.paises_em_comum, axis=1))
    .query("CO_SH6 in @filtro_oportunidades_selecionadas")
    .drop(columns=["NO_PAIS_uf", "NO_PAIS_mapa"])
)
# Principais destinos de exportacao por sh6 no mapa de oportunidades
top_5_destinos_mapa_sh6 = destinos_mapa.assign(
    NO_PAIS=lambda x: x["NO_PAIS"].str.split(", ").apply(lambda x: ", ".join(x[:5]))
).rename(columns={"NO_PAIS": "top_5_destinos_mapa"})
# Principais destinos de exportacao por grupo da UF selecionada
principais_destinos_grupo = (
    destinos_mapa_grupo.merge(
        destinos_grupo_total, on="desc_grupo", how="left", suffixes=("_uf", "_mapa")
    )
    .fillna({"NO_PAIS_uf": "", "NO_PAIS_mapa": ""})
    .assign(paises_comuns=lambda x: x.apply(utils.paises_em_comum, axis=1))
    .drop(columns=["NO_PAIS_uf", "NO_PAIS_mapa"])
)
# Principais destinos de exportacao por grupo no mapa de oportunidades
top_5_destinos_mapa_grupo = destinos_mapa_grupo.assign(
    NO_PAIS=lambda x: x["NO_PAIS"].str.split(", ").apply(lambda x: ", ".join(x[:5]))
).rename(columns={"NO_PAIS": "top_5_destinos_mapa"})

# %% OPORTUNIDADES TRADICIONAIS

# Oportunidades identificadas para UF e Classificadas pelo Mapa
df_oportunidades_classificadas = utils.gerar_oportunidades(
    tipo="classificadas",
    df=mapa_apex,
    filtro_oportunidades_selecionadas=filtro_oportunidades_selecionadas,
    tradutor_sh6=tradutor_sh6,
    tradutor_grupos=tradutor_grupos,
)

# Oportunidades identificadas para UF por SH6
df_oportunidades_sh6 = utils.gerar_oportunidades(
    tipo="uf_sh6",
    df=df_exp,
    uf_selecionada=uf_selecionada,
    filtro_oportunidades_selecionadas=filtro_oportunidades_selecionadas,
    tradutor_sh6=tradutor_sh6,
    tradutor_grupos=tradutor_grupos,
    df_oportunidades=df_oportunidades,
    principais_destinos_sh6=principais_destinos_sh6,
    top_5_destinos_mapa_sh6=top_5_destinos_mapa_sh6,
)

# Oportunidades identificadas para UF por Grupo
df_oportunidades_grupo = utils.gerar_oportunidades(
    tipo="uf_grupo",
    df=df_exp,
    uf_selecionada=uf_selecionada,
    filtro_oportunidades_selecionadas=filtro_oportunidades_selecionadas,
    tradutor_sh6=tradutor_sh6,
    tradutor_grupos=tradutor_grupos,
    df_oportunidades=df_oportunidades,
    principais_destinos_grupo=principais_destinos_grupo,
    top_5_destinos_mapa_grupo=top_5_destinos_mapa_grupo,
)

# %% OPORTUNIDADES A EXPLORAR

filtro_quartil_oportunidades_explorar = df_oportunidades_sh6["soma_uf_21_23"].quantile(
    0.25, interpolation="linear"
)

filtro_tx_crescimento_oportunidades_explorar = (
    soma_uf_total_21_23.merge(soma_uf_total_18_20, on=["SG_UF_NCM"], how="left")
    .assign(
        tx_crescimento=lambda x: (x["soma_uf_total_21_23"] / x["soma_uf_total_18_20"])
        - 1
    )
    .loc[0, "tx_crescimento"]
)
df_oportunidades_explorar = utils.gerar_oportunidades_explorar(
    df=df_oportunidades_sh6,
    filtro_quartil_oportunidades_explorar=filtro_quartil_oportunidades_explorar,
    filtro_tx_crescimento_oportunidades_explorar=filtro_tx_crescimento_oportunidades_explorar,
)

filtro_oportunidades_explorar = df_oportunidades_explorar["cod_sh6"].unique()

df_oportunidades_explorar_classificadas = df_oportunidades_classificadas.query(
    "cod_sh6 in @filtro_oportunidades_explorar"
)
# %% OPORTUNIDADES POTENCIAIS
df_rais_raw = pd.read_excel("../data/rais_2023.xlsx", engine="calamine")
total_uf = utils.ajuste_rais(df=df_rais_raw, coluna="sigla_uf", tipo="uf")

total_br = utils.ajuste_rais(
    df=df_rais_raw,
    coluna=None,
    tipo="br",
)
total_cnae_br = utils.ajuste_rais(
    df=df_rais_raw,
    coluna="cod_grupo",
    tipo="cnae_br",
)

df_rais_uf = utils.vcr_rais(
    df=df_rais_raw,
    uf_selecionada=uf_selecionada,
    total_uf=total_uf,
    total_br=total_br,
    total_cnae_br=total_cnae_br,
)

# Identificando os 5 maiores SH6 nao tradicionais por CNAE

maiores_sh6_nao_tradicionais = utils.identificar_maiores_sh6_nao_tradicionais(
    df=df_vcr, tradutor_cnae=tradutor_cnae, filtro_quartil=filtro_q50
)

# Identificando o valor exportado dos SH6 tradicionais por CNAE

exp_oportunidades_tradicionais_cnae = (
    df_oportunidades_sh6[["cod_sh6", "soma_uf_21_23"]]
    .merge(tradutor_cnae, on="cod_sh6", how="left")
    .groupby(["cod_grupo"], as_index=False)
    .agg(exp_oport_trad=("soma_uf_21_23", "sum"))
)

# Oportunidades Potenciais

df_oportunidades_potenciais = utils.gerar_oportunidades_potenciais(
    df=df_vcr,
    filtro_quartil=filtro_q50,
    tradutor_cnae=tradutor_cnae,
    df_rais_uf=df_rais_uf,
    maiores_sh6_nao_tradicionais=maiores_sh6_nao_tradicionais,
    exp_oportunidades_tradicionais_cnae=exp_oportunidades_tradicionais_cnae,
)

# %% EXPORTANDO OS DADOS

caminho_resultado = "../../../OneDrive - Associacao Antonio Vieira/UAPP_ProjetoCEI/APEX-BRASIL/2023_Estados/Estados/0_resultados_oportunidades/2025/arquivos_apex/"
arquivo_oportunidades = f"{uf_selecionada}_oportunidades.xlsx"
print(f"Salvando resultados em: {caminho_resultado}{arquivo_oportunidades}")

with pd.ExcelWriter(
    caminho_resultado + arquivo_oportunidades, engine="xlsxwriter", mode="w"
) as writer:
    df_oportunidades_sh6.to_excel(
        writer, sheet_name="Oportunidades UF SH6", index=False
    )
    df_oportunidades_classificadas.to_excel(
        writer, sheet_name="Oportunidades Classificadas", index=False
    )
    df_oportunidades_grupo.to_excel(
        writer, sheet_name="Oportunidades UF Grupo", index=False
    )
    df_oportunidades_explorar.to_excel(
        writer, sheet_name="Oportunidades a Explorar", index=False
    )
    df_oportunidades_explorar_classificadas.to_excel(
        writer, sheet_name="Oport a Explorar Classif", index=False
    )
    df_oportunidades_potenciais.to_excel(
        writer, sheet_name="Oportunidades Potenciais", index=False
    )
print(f"--- Processamento para UF: {uf_selecionada} concluído ---")
