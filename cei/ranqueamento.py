# %%
import pandas as pd
import numpy as np
import math
from scipy.stats import kstest, norm
from scipy import stats

base = pd.read_excel(
    "data/ranqueamento/base.xlsx", sheet_name="base_ranqueamento", engine="calamine"
).replace(0, np.nan)

classificacao = pd.read_excel(
    "data/ranqueamento/base.xlsx", sheet_name="classificacao", engine="calamine"
)
colunas_valor = classificacao.query("tipo == 'valor'")["var"].tolist()

df_valor = base[["id"] + colunas_valor].copy()

# Transformando todas as colunas em numericas
df_valor[colunas_valor] = df_valor[colunas_valor].apply(pd.to_numeric, errors="coerce")

# Transformando todas as colunas em logaritmo natural
df_valor[colunas_valor] = df_valor[colunas_valor].map(
    lambda x: np.log(x) if x > 0 else np.nan
)


# %%
def nota_valor_1(df, colunas):
    df = df.copy()
    p_values = {}

    for col in colunas:
        if col in df.columns:
            data = df[col].dropna()
            if len(data) > 0:
                # Realizando o teste de Kolmogorov-Smirnov
                z_scores = (data - data.mean()) / data.std(ddof=0)
                p_value = kstest(z_scores, "norm")[1]  # retorno: estatística, p-valor
                p_values[col] = p_value

    for col in colunas:
        if col in df.columns and col in p_values and p_values[col] >= 0.05:
            mean = df[col].mean()
            std = df[col].std(ddof=0)

            new_col = f"nota_{col}_1"
            df[new_col] = np.nan

            df.loc[df[col] < (mean - std), new_col] = -1
            df.loc[(df[col] >= (mean - std)) & (df[col] < mean), new_col] = 1
            df.loc[(df[col] >= mean) & (df[col] < (mean + std)), new_col] = 3
            df.loc[df[col] >= (mean + std), new_col] = 5
    return df


df_valor_nota_1 = nota_valor_1(df_valor, colunas_valor)


def nota_valor_2(df, colunas):
    df = df.copy()

    for col in colunas:
        if col not in df.columns:
            continue

        # Calcula z-score
        mean = df[col].mean()
        std = df[col].std(ddof=0)
        df[f"Z_{col}"] = (df[col] - mean) / std

        # Identifica outliers
        outlier_col = f"outlier_{col}"
        df[outlier_col] = 0
        df.loc[df[f"Z_{col}"] < -3, outlier_col] = 1
        df.loc[df[f"Z_{col}"] > 3, outlier_col] = 2

        # Filtro para teste de normalidade
        filtered = df[(df[f"Z_{col}"] > -3) & (df[f"Z_{col}"] < 3) & df[col].notna()]
        if filtered.empty:
            continue

        # Teste de normalidade
        z_scores_filtered = (filtered[col] - filtered[col].mean()) / filtered[col].std(
            ddof=0
        )
        p_value = kstest(z_scores_filtered, "norm")[1]

        if p_value >= 0.05:
            # Estatísticas para classificacao
            mean = df[col].mean()
            std = df[col].std(ddof=0)
            nota_col = f"nota_{col}_2"

            df[nota_col] = np.nan

            # Outliers
            df.loc[df[outlier_col] == 1, nota_col] = -1
            df.loc[df[outlier_col] == 2, nota_col] = 5

            # Valores normais
            normal_mask = df[outlier_col] == 0
            df.loc[normal_mask & (df[col] < (mean - std)), nota_col] = -1
            df.loc[
                normal_mask & (df[col] >= (mean - std)) & (df[col] < mean), nota_col
            ] = 1
            df.loc[
                normal_mask & (df[col] >= mean) & (df[col] < (mean + std)), nota_col
            ] = 3
            df.loc[normal_mask & (df[col] >= (mean + std)), nota_col] = 5

    return df


df_valor_nota_2 = nota_valor_2(df_valor_nota_1, colunas_valor)

outlier_flags = {
    col.replace("outlier_", ""): col
    for col in df_valor_nota_2.columns
    if col.startswith("outlier_")
}


def nota_valor_3(df, colunas, outlier_flags=None):
    df = df.copy()

    for col in colunas:
        if col not in df.columns:
            continue

        col_ln = col  # já está em log
        nota_col = f"nota_{col}_3"

        # Se houver flag de outlier, usá-la
        if outlier_flags and col in outlier_flags:
            outlier_col = outlier_flags[col]
            df_valid = df[df[outlier_col] == 0]
        else:
            df_valid = df.copy()

        # Calcular quartis sem outliers
        q25 = df_valid[col_ln].quantile(0.25)
        q50 = df_valid[col_ln].quantile(0.50)
        q75 = df_valid[col_ln].quantile(0.75)

        df[nota_col] = np.nan

        # Atribuição de notas conforme os intervalos dos quartis
        df.loc[df[col_ln] <= q25, nota_col] = -1
        df.loc[(df[col_ln] > q25) & (df[col_ln] <= q50), nota_col] = 1
        df.loc[(df[col_ln] > q50) & (df[col_ln] <= q75), nota_col] = 3
        df.loc[df[col_ln] > q75, nota_col] = 5

        # Casos com valor original mas falha no log -> nota -1
        original_col = col.replace("_ln", "")
        if original_col in df.columns:
            df.loc[df[original_col].notna() & df[col_ln].isna(), nota_col] = -1

    return df


df_valor_nota_3 = nota_valor_3(
    df_valor_nota_2, colunas_valor, outlier_flags=outlier_flags
)

cols_nota = [col for col in df_valor_nota_3.columns if col.startswith("nota_")]
df_valor_nota_3[cols_nota] = df_valor_nota_3[cols_nota].fillna(0)
df_valor_nota_3


def nota_valor_final(df, colunas):
    df = df.copy()

    for col in colunas:
        col_1 = f"nota_{col}_1"
        col_2 = f"nota_{col}_2"
        col_3 = f"nota_{col}_3"
        final_col = f"nota_{col}_final"

        for c in [col_1, col_2, col_3]:
            if c not in df.columns:
                df[c] = 0

        df[final_col] = df[[col_1, col_2, col_3]].apply(
            lambda row: next((x for x in row if x != 0), 0), axis=1
        )

    return df


df_valor_final = nota_valor_final(df_valor_nota_3, colunas_valor)

colunas_finais = ["id"] + [
    col
    for col in df_valor_final.columns
    if col.startswith("var_") or col.endswith("_final")
]

df_valor_final = df_valor_final[colunas_finais]

df_valor_final.to_excel(
    "data/ranqueamento/base_ranqueamento_final.xlsx",
    index=False,
    engine="openpyxl",
)

# %%

colunas_variacao = classificacao.query("tipo == d'variacao'")["var"].tolist()

df_variacao = base[["id"] + colunas_variacao].copy()

for col in colunas_variacao:
    # Encontra o mínimo da coluna
    min_val = df_variacao[col].min()

    # Verificar se a coluna inteira é NaN
    if pd.isna(min_val):
        # Cria colunas resultantes com NaN e pula para a próxima
        df_variacao[f"{col}_min"] = np.nan
        df_variacao[f"{col}_min_ln"] = np.nan
        continue

    # Calcula o valor absoluto do mínimo
    abs_min = np.abs(min_val)

    # Calcula a coluna deslocada (abs_min + valor original)
    shifted_col_name = f"{col}_min"
    df_variacao[shifted_col_name] = abs_min + df_variacao[col]

    # Calcula o logaritmo natural da coluna deslocada
    log_col_name = f"{col}_min_ln"
    # Suprime avisos de log(0) e calcula o log
    with np.errstate(divide="ignore"):
        df_variacao[log_col_name] = np.log(df_variacao[shifted_col_name])

    # Substitui -inf (resultado de log(0)) por NaN
    df_variacao[log_col_name] = df_variacao[log_col_name].replace(-np.inf, np.nan)
