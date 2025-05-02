# %%
import pandas as pd
import numpy as np
import utils
import importlib

importlib.reload(utils)

print("Iniciando ranqueamento")
arquivo = "municipios"

base = pd.read_excel(
    f"bases/base_{arquivo}.xlsx",
    sheet_name="base_ranqueamento",
    engine="calamine",
).replace(0, np.nan)

classificacao = pd.read_excel(
    f"bases/base_{arquivo}.xlsx",
    sheet_name="classificacao",
    engine="calamine",
)

dicionario_tipo_classificacao = (
    classificacao[["var", "ordem"]].set_index("var")["ordem"].to_dict()
)

# Colunas Valor
colunas_valor = classificacao.query("tipo == 'valor'")["var"].tolist()

df_valor = base[["id"] + colunas_valor].copy()

df_valor_nota_1 = utils.nota_valor_1(df=df_valor, colunas=colunas_valor)

df_valor_nota_2 = utils.nota_valor_2(df=df_valor, colunas=colunas_valor)

df_valor_nota_3 = utils.nota_valor_3(df=df_valor, colunas=colunas_valor)

df_valor_notas = pd.concat(
    [
        df_valor_nota_1.set_index("id"),
        df_valor_nota_2.set_index("id"),
        df_valor_nota_3.set_index("id"),
    ],
    axis=1,
)
df_valor_final = utils.nota_final(df=df_valor_notas, colunas=colunas_valor)

# Colunas Taxa
colunas_taxa = classificacao.query("tipo == 'taxa'")["var"].tolist()

df_taxa = base[["id"] + colunas_taxa].copy()

df_taxa_nota_1 = utils.nota_taxa_1(df=df_taxa, colunas=colunas_taxa)

df_taxa_nota_2 = utils.nota_taxa_2(df=df_taxa, colunas=colunas_taxa)

df_taxa_nota_3 = utils.nota_taxa_3(df=df_taxa, colunas=colunas_taxa)

df_taxa_notas = pd.concat(
    [
        df_taxa_nota_1.set_index("id"),
        df_taxa_nota_2.set_index("id"),
        df_taxa_nota_3.set_index("id"),
    ],
    axis=1,
)

df_taxa_final = utils.nota_final(df=df_taxa_notas, colunas=colunas_taxa)

colunas_participacao = classificacao.query("tipo == 'participacao'")["var"].tolist()

df_participacao = base[["id"] + colunas_participacao].copy()

df_participacao_nota_1 = utils.nota_participacao_1(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_nota_2 = utils.nota_participacao_2(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_nota_3 = utils.nota_participacao_3(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_notas = pd.concat(
    [
        df_participacao_nota_1.set_index("id"),
        df_participacao_nota_2.set_index("id"),
        df_participacao_nota_3.set_index("id"),
    ],
    axis=1,
)

df_participacao_final = utils.nota_final(
    df=df_participacao_notas, colunas=colunas_participacao
)

# Concatenando o DataFrame base com os DataFrames de notas
df_variaveis_notas = pd.concat(
    [
        base.set_index("id"),
        df_valor_final,
        df_taxa_final,
        df_participacao_final,
    ],
    axis=1,
).reset_index()

# Ajustando as colunas de notas para as colunas de variaveis invertidas

notas_invertidas = {5: -1, 3: 1, 1: 1, -1: 5, np.nan: np.nan}
prefixo_nota = "nota_"
colunas_nota_para_inverter = []

for coluna in df_variaveis_notas.columns:
    if coluna.startswith(prefixo_nota):
        var_original = coluna[len(prefixo_nota) :]
        if dicionario_tipo_classificacao.get(var_original) == "invertido":
            colunas_nota_para_inverter.append(coluna)

for coluna in colunas_nota_para_inverter:
    df_variaveis_notas[coluna] = df_variaveis_notas[coluna].map(notas_invertidas)

# Separando todas as colunas de var
todas_colunas_base = classificacao["var"].tolist()

# Ordenando o DataFrame com as variaveis e notas
df_reordenada = utils.ordenar_df_com_notas(
    df=df_variaveis_notas,
    colunas_base=todas_colunas_base,
    sort_key_func=utils.sort_key,
    id_col="id",
)

# Renomeando e criando o dataframe final
df_final = utils.renomear_colunas_mapeadas(
    df=df_reordenada, map_df=classificacao, map_from_col="var", map_to_col="coluna"
)

arquivo_saida = f"resultados/ranqueamento_{arquivo}.xlsx"
with pd.ExcelWriter(arquivo_saida) as writer:
    df_final.to_excel(writer, index=False, sheet_name="ranqueamento")

print(f"Ranqueamento finalizado. Arquivo salvo em: {arquivo_saida}")
