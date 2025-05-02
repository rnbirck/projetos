import numpy as np
import pandas as pd
import warnings
import re
from statsmodels.stats.diagnostic import lilliefors
from typing import List, Callable, Set


def nota_valor_1(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_1) para colunas de valor, baseadas na distribuição normal
    da coluna transformada por logaritmo natural.

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários (log, teste KS, média, sd).

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list): Uma lista de nomes das colunas de *valor* originais
                           a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_1'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        log_col_name = f"{col}_ln"

        # Lidar com coluna original toda NaN

        numeric_col_check = pd.to_numeric(df[col], errors="coerce")
        if numeric_col_check.isnull().all():
            if log_col_name not in df.columns:
                df[log_col_name] = np.nan
            prob_ks_col = f"Prob_KS_{log_col_name}"
            mean_col = f"{log_col_name}_mean"
            sd_col = f"{log_col_name}_sd"
            mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
            mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"
            nota_col = f"nota_{col}_1"
            for c_name in [
                prob_ks_col,
                mean_col,
                sd_col,
                mean_minus_sd_col,
                mean_plus_sd_col,
                nota_col,
            ]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            nota_cols_final.append(nota_col)
            if log_col_name in df.columns and df[log_col_name].isnull().all():
                try:
                    del df[log_col_name]
                except KeyError:
                    pass
            continue

        # Calcular log, tratando 0 e negativos como NaN
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df[log_col_name] = np.log(numeric_col_check.where(numeric_col_check > 0))
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # Teste de Normalidade (Lilliefors)
        prob_ks_col = f"Prob_KS_{log_col_name}"
        data_for_ks = df[log_col_name].dropna()
        p_value = np.nan
        if len(data_for_ks) >= 5:
            try:
                ks_stat, p_value = lilliefors(data_for_ks, dist="norm")
            except Exception:
                p_value = np.nan
        df[prob_ks_col] = p_value

        # Média e Desvio Padrão
        mean_col = f"{log_col_name}_mean"
        sd_col = f"{log_col_name}_sd"
        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)
        df[mean_col] = col_mean
        df[sd_col] = col_sd

        # Limites Mean +/- SD
        mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
        mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"
        if pd.notna(col_mean) and pd.notna(col_sd):
            df[mean_minus_sd_col] = col_mean - col_sd
            df[mean_plus_sd_col] = col_mean + col_sd
        else:
            df[mean_minus_sd_col] = np.nan
            df[mean_plus_sd_col] = np.nan

        # Cálculo da Nota (_1)
        nota_col = f"nota_{col}_1"
        nota_cols_final.append(nota_col)
        df[nota_col] = np.nan

        # Condicao para valores originais que eram não-NaN mas <= 0 (resultando em log NaN)
        cond_zero_or_neg = numeric_col_check.notna() & df[log_col_name].isna()

        p_val_check = df[prob_ks_col].fillna(0) >= 0.05

        cond1 = p_val_check & (df[log_col_name] < df[mean_minus_sd_col])
        cond2 = (
            p_val_check
            & (df[log_col_name] >= df[mean_minus_sd_col])
            & (df[log_col_name] < df[mean_col])
        )
        cond3 = (
            p_val_check
            & (df[log_col_name] >= df[mean_col])
            & (df[log_col_name] < df[mean_plus_sd_col])
        )
        cond4 = p_val_check & (df[log_col_name] >= df[mean_plus_sd_col])

        conditions = [cond_zero_or_neg, cond1, cond2, cond3, cond4]

        choices = [-1, -1, 1, 3, 5]

        df[nota_col] = np.select(conditions, choices, default=np.nan)

        cols_to_delete = [
            log_col_name,
            prob_ks_col,
            mean_col,
            sd_col,
            mean_minus_sd_col,
            mean_plus_sd_col,
        ]
        for c_del in cols_to_delete:
            if c_del in df.columns:
                try:
                    del df[c_del]
                except KeyError:
                    pass

    # Selecionar colunas finais
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_valor_2(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_2) para colunas de valor, considerando outliers (+/- 3 SD)
    e realizando teste K-S em dados filtrados.

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list): Uma lista de nomes das colunas de *valor* originais
                           a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_2'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        log_col_name = f"{col}_ln"

        # Lidar com coluna original toda NaN
        numeric_col_check = pd.to_numeric(df[col], errors="coerce")
        if numeric_col_check.isnull().all():
            if log_col_name not in df.columns:
                df[log_col_name] = np.nan
            outlier_col = f"outlier_{col}"
            prob_ks_col = f"Prob_KS_{log_col_name}"
            mean_col = f"{log_col_name}_mean"
            sd_col = f"{log_col_name}_sd"
            mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
            mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"
            nota_col = f"nota_{col}_2"
            for c_name in [
                outlier_col,
                prob_ks_col,
                mean_col,
                sd_col,
                mean_minus_sd_col,
                mean_plus_sd_col,
                nota_col,
            ]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            nota_cols_final.append(nota_col)
            if log_col_name in df.columns and df[log_col_name].isnull().all():
                try:
                    del df[log_col_name]
                except KeyError:
                    pass
            continue

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df[log_col_name] = np.log(numeric_col_check.where(numeric_col_check > 0))
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # Média e Desvio Padrão Gerais
        mean_col = f"{log_col_name}_mean"
        sd_col = f"{log_col_name}_sd"
        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)
        df[mean_col] = col_mean
        df[sd_col] = col_sd

        # Cálculo do Z-Score
        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0:
            temp_z_score_col = (df[log_col_name] - col_mean) / col_sd

        # Identificação de Outliers (Threshold +/- 3)
        outlier_col = f"outlier_{col}"
        df[outlier_col] = np.nan
        df.loc[df[log_col_name].notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -3, outlier_col] = 1
        df.loc[temp_z_score_col > 3, outlier_col] = 2

        # Filtragem TEMPORÁRIA para K-S (Z entre -3 e 3)
        ks_filter_mask = df[outlier_col] == 0
        data_for_ks_filtered = df.loc[ks_filter_mask, log_col_name]

        # Realizar Teste K-S (Lilliefors) nos dados filtrados
        p_value_filtered = np.nan
        if len(data_for_ks_filtered) >= 5:
            try:
                ks_stat, p_value_filtered = lilliefors(
                    data_for_ks_filtered, dist="norm"
                )
            except Exception:
                p_value_filtered = np.nan
        prob_ks_col = f"Prob_KS_{log_col_name}"
        df[prob_ks_col] = p_value_filtered

        # Calcular Limites (usando média/dp gerais)
        mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
        mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"
        if pd.notna(col_mean) and pd.notna(col_sd):
            df[mean_minus_sd_col] = col_mean - col_sd
            df[mean_plus_sd_col] = col_mean + col_sd
        else:
            df[mean_minus_sd_col] = np.nan
            df[mean_plus_sd_col] = np.nan

        #  Cálculo da Nota (_2)
        nota_col = f"nota_{col}_2"
        nota_cols_final.append(nota_col)
        df[nota_col] = np.nan

        cond_zero_or_neg = numeric_col_check.notna() & df[log_col_name].isna()

        p_val_check = df[prob_ks_col].fillna(0) >= 0.05

        condA = p_val_check & (df[outlier_col] == 1)
        condB = p_val_check & (df[outlier_col] == 2)
        condC = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[log_col_name] < df[mean_minus_sd_col])
        )
        condD = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[log_col_name] >= df[mean_minus_sd_col])
            & (df[log_col_name] < df[mean_col])
        )
        condE = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[log_col_name] >= df[mean_col])
            & (df[log_col_name] < df[mean_plus_sd_col])
        )
        condF = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[log_col_name] >= df[mean_plus_sd_col])
        )

        conditions = [cond_zero_or_neg, condA, condB, condC, condD, condE, condF]
        choices = [-1, -1, 5, -1, 1, 3, 5]

        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza de Colunas Intermediárias
        cols_to_delete = [
            log_col_name,
            mean_col,
            sd_col,
            outlier_col,
            prob_ks_col,
            mean_minus_sd_col,
            mean_plus_sd_col,
        ]
        for c_del in cols_to_delete:
            if c_del in df.columns:
                try:
                    del df[c_del]
                except KeyError:
                    pass

    #  Selecionar colunas finais
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_valor_3(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_3) para colunas de valor, baseadas nos quartis da
    coluna logaritmizada, calculados após filtrar outliers (+/- 3 SD).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list): Uma lista de nomes das colunas de *valor* originais
                           a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_3'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        # Calcular Log Natural
        log_col_name = f"{col}_ln"

        # Lidar com coluna original toda NaN ou com valores não positivos
        if df[col].isnull().all() or (df[col] <= 0).any():
            if log_col_name not in df.columns:
                df[log_col_name] = np.nan
            # Criar colunas dependentes como NaN
            outlier_col = f"outlier_{col}"
            nota_col = f"nota_{col}_3"
            for c_name in [outlier_col, nota_col]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            nota_cols_final.append(nota_col)
            if log_col_name in df.columns and df[log_col_name].isnull().all():
                del df[log_col_name]
            continue

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df[log_col_name] = np.log(df[col].where(df[col] > 0))
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # Calcular Outliers (Threshold +/- 3 SD)
        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)

        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0:
            temp_z_score_col = (df[log_col_name] - col_mean) / col_sd

        outlier_col = f"outlier_{col}"
        df[outlier_col] = np.nan
        df.loc[df[log_col_name].notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -3, outlier_col] = 1  # Threshold -3
        df.loc[temp_z_score_col > 3, outlier_col] = 2  # Threshold +3

        # Filtrar e Calcular Quartis
        quartile_filter_mask = (df[outlier_col] == 0) & df[log_col_name].notna()
        data_for_quartiles = df.loc[quartile_filter_mask, log_col_name]

        k25, k50, k75 = np.nan, np.nan, np.nan  # Valores padrão
        if not data_for_quartiles.empty:
            quantiles = data_for_quartiles.quantile([0.25, 0.50, 0.75])
            if 0.25 in quantiles.index:
                k25 = quantiles.loc[0.25]
            if 0.50 in quantiles.index:
                k50 = quantiles.loc[0.50]
            if 0.75 in quantiles.index:
                k75 = quantiles.loc[0.75]

        # Cálculo da Nota (_3)
        nota_col = f"nota_{col}_3"
        nota_cols_final.append(nota_col)
        df[nota_col] = np.nan

        # Condição especial: Original não nulo, mas _ln é nulo
        cond_missing = df[col].notna() & df[log_col_name].isna()

        # Condições baseadas nos quartis (só aplicáveis se quartis foram calculados)
        cond1 = pd.notna(k25) & (df[log_col_name] <= k25)
        cond2 = (
            pd.notna(k25)
            & pd.notna(k50)
            & (df[log_col_name] > k25)
            & (df[log_col_name] <= k50)
        )
        cond3 = (
            pd.notna(k50)
            & pd.notna(k75)
            & (df[log_col_name] > k50)
            & (df[log_col_name] <= k75)
        )
        cond4 = pd.notna(k75) & (df[log_col_name] > k75)

        conditions = [cond_missing, cond1, cond2, cond3, cond4]
        choices = [-1, -1, 1, 3, 5]
        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza de Colunas Intermediárias
        cols_to_delete = [log_col_name, outlier_col]
        for c_del in cols_to_delete:
            if c_del in df.columns:
                try:
                    del df[c_del]
                except KeyError:
                    pass

    # Selecionar colunas finais
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_taxa_1(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_1) para colunas de variação, baseadas na distribuição
    normal da coluna transformada (shift para positivo + log).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list[str]): Uma lista de nomes das colunas de *variação* originais
                             a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_1'). Notas não atribuídas
                      pelas condições (incluindo p-valor KS < 0.05)
                      permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []
    for col in colunas:
        # Encontra o mínimo da coluna
        min_val = df[col].min()

        if df[col].isnull().all():
            # Criar colunas resultantes com NaN
            shifted_col_name = f"{col}_min"
            log_col_name = f"{col}_min_ln"
            prob_ks_col = f"Prob_KS_{log_col_name}"
            mean_col = f"{log_col_name}_mean"
            sd_col = f"{log_col_name}_sd"
            mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
            mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"
            nota_col = f"nota_{col}_1"
            for c in [
                shifted_col_name,
                log_col_name,
                prob_ks_col,
                mean_col,
                sd_col,
                mean_minus_sd_col,
                mean_plus_sd_col,
                nota_col,
            ]:
                if c not in df.columns:  # Evitar sobrescrever se rodar duas vezes
                    df[c] = np.nan
            nota_cols_final.append(nota_col)  # Adicionar à lista de notas
            continue  # Pular para a próxima coluna

        abs_min = np.abs(min_val) if pd.notna(min_val) else 0

        shifted_col_name = f"{col}_min"
        df[shifted_col_name] = abs_min + df[col]

        log_col_name = f"{col}_min_ln"
        # Suprime avisos de log(0 ou negativo) e calcula o log
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df[log_col_name] = np.log(df[shifted_col_name])
            # Substitui -inf (resultado de log(0)) ou +inf por NaN
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # --- Teste K-S (Equivalente a NPAR TESTS e manipulação OMS) ---
        prob_ks_col = f"Prob_KS_{log_col_name}"
        data_for_ks = df[log_col_name].dropna()

        p_value = np.nan  # Valor padrão caso o teste não possa ser realizado

        # O teste K-S e Lilliefors precisam de um número mínimo de pontos
        if len(data_for_ks) >= 5:  # Um mínimo razoável, ajuste se necessário
            ks_stat, p_value = lilliefors(data_for_ks, dist="norm")
        else:
            print(
                f"  Aviso: Não há dados suficientes ({len(data_for_ks)}) para teste K-S em {log_col_name}."
            )
        # Adiciona o p-valor como uma coluna constante para todas as linhas (como MATCH FILES)
        df[prob_ks_col] = p_value
        # O comando LAG do SPSS provavelmente não é necessário aqui, pois o p-valor é constante.

        # Média e Desvio Padrão
        mean_col = f"{log_col_name}_mean"
        sd_col = f"{log_col_name}_sd"
        mean_minus_sd_col = f"{log_col_name}_mean_menos_sd"
        mean_plus_sd_col = f"{log_col_name}_mean_mais_sd"

        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)  # ddof=1 para desvio padrão amostral

        df[mean_col] = col_mean
        df[sd_col] = col_sd

        # Calcula os limites apenas se a média e o desvio padrão puderem ser calculados
        if pd.notna(col_mean) and pd.notna(col_sd):
            df[mean_minus_sd_col] = col_mean - col_sd
            df[mean_plus_sd_col] = col_mean + col_sd
        else:
            print(
                f"  Aviso: Média ou Desvio Padrão não puderam ser calculados para {log_col_name}."
            )
            df[mean_minus_sd_col] = np.nan
            df[mean_plus_sd_col] = np.nan

        # Cálculo da Nota
        nota_col = f"nota_{col}_1"
        nota_cols_final.append(nota_col)  # Adiciona à lista de colunas de nota

        # Inicializar a coluna de nota com NaN
        df[nota_col] = np.nan

        # Condições
        # Condição 1: p-valor >= 0.05 (Normal) E log_val < mean - sd
        cond1 = (df[prob_ks_col] >= 0.05) & (df[log_col_name] < df[mean_minus_sd_col])
        # Condição 2: p-valor >= 0.05 (Normal) E mean - sd <= log_val < mean
        cond2 = (
            (df[prob_ks_col] >= 0.05)
            & (df[log_col_name] >= df[mean_minus_sd_col])
            & (df[log_col_name] < df[mean_col])
        )
        # Condição 3: p-valor >= 0.05 (Normal) E mean <= log_val < mean + sd
        cond3 = (
            (df[prob_ks_col] >= 0.05)
            & (df[log_col_name] >= df[mean_col])
            & (df[log_col_name] < df[mean_plus_sd_col])
        )
        # Condição 4: p-valor >= 0.05 (Normal) E log_val >= mean + sd
        cond4 = (df[prob_ks_col] >= 0.05) & (df[log_col_name] >= df[mean_plus_sd_col])
        # Condição 5: p-valor >= 0.05 (Normal) E valor original não é nulo E log_val é nulo
        cond5 = (
            (df[prob_ks_col] >= 0.05) & (df[col].notna()) & (df[log_col_name].isna())
        )

        conditions = [
            cond5,  # Verificar esta condição primeiro
            cond1,
            cond2,
            cond3,
            cond4,
        ]
        choices = [
            -1,  # Valor para cond5
            -1,  # Valor para cond1
            1,  # Valor para cond2
            3,  # Valor para cond3
            5,  # Valor para cond4
        ]

        # Aplicar as condições. O default é NaN (onde nenhuma condição é atendida)
        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Recodificar os NaNs restantes para 0
        # df[nota_col] = df[nota_col].fillna(0)

    # Selecionar as colunas a serem mantidas
    cols_to_keep_final = []
    cols_to_keep_final.append("id")
    # Adicionar apenas as colunas de nota que foram realmente geradas
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    df = df[cols_to_keep_final].copy()

    return df


def nota_taxa_2(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_2) para colunas de variação (otimizado para performance).
    Considera outliers e realiza teste K-S em dados filtrados.

    Assume que a coluna 'id' existe no DataFrame de entrada.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados e a coluna 'id'.
        colunas (list): Uma lista de nomes de colunas de variação a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_2'). Notas não atribuídas
                      pelas condições são recodificadas para 0.
    """
    # Trabalhar em cópia apenas das colunas necessárias + id
    cols_to_use = ["id"] + colunas
    cols_existentes = [c for c in cols_to_use if c in df.columns]
    if "id" not in cols_existentes and len(cols_existentes) > 0:
        if "id" in df.columns:
            cols_existentes.insert(0, "id")

    if not cols_existentes or (len(cols_existentes) == 1 and "id" in cols_existentes):
        return pd.DataFrame({"id": df["id"]}) if "id" in df.columns else pd.DataFrame()

    df = df[cols_existentes].copy()
    nota_cols_final = []

    for col in colunas:
        if col not in df.columns:
            continue

        nota_col = f"nota_{col}_2"
        nota_cols_final.append(nota_col)

        # Shift e Log (calculados como Series)
        min_val = df[col].min()
        log_col_series = pd.Series(np.nan, index=df.index)

        if not df[col].isnull().all():
            abs_min = np.abs(min_val) if pd.notna(min_val) else 0
            temp_shifted_col = abs_min + df[col]
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                log_col_series = np.log(temp_shifted_col)
            log_col_series = log_col_series.replace([np.inf, -np.inf], np.nan)

        #  Cálculos Intermediários
        col_mean = np.nan
        col_sd = np.nan
        mean_minus_sd = np.nan
        mean_plus_sd = np.nan
        p_value_filtered = np.nan
        z_score_series = pd.Series(np.nan, index=df.index)
        outlier_series = pd.Series(np.nan, index=df.index)

        if not log_col_series.isnull().all():
            col_mean = log_col_series.mean()
            col_sd = log_col_series.std(ddof=1)

            if pd.notna(col_mean) and pd.notna(col_sd):
                mean_minus_sd = col_mean - col_sd
                mean_plus_sd = col_mean + col_sd

            if pd.notna(col_sd) and col_sd != 0:
                z_score_series = (log_col_series - col_mean) / col_sd

            outlier_series.loc[log_col_series.notna()] = 0
            outlier_series.loc[z_score_series < -2] = 1
            outlier_series.loc[z_score_series > 2] = 2

            # Filtragem TEMPORÁRIA para K-S
            ks_filter_mask = (
                log_col_series.notna() & (z_score_series > -3) & (z_score_series < 3)
            )
            data_for_ks_filtered = log_col_series[ks_filter_mask]

            if len(data_for_ks_filtered) >= 5:
                try:
                    ks_stat, p_value_filtered = lilliefors(
                        data_for_ks_filtered, dist="norm"
                    )
                except Exception:
                    p_value_filtered = np.nan

        #  Cálculo da Nota (_2)
        df[nota_col] = np.nan

        p_val_check_scalar = pd.notna(p_value_filtered) and (p_value_filtered >= 0.05)

        if p_val_check_scalar:
            condA = outlier_series == 1
            condB = outlier_series == 2
            condG = df[col].notna() & log_col_series.isna()
            condC = (
                (outlier_series == 0)
                & pd.notna(mean_minus_sd)
                & (log_col_series < mean_minus_sd)
            )
            condD = (
                (outlier_series == 0)
                & pd.notna(mean_minus_sd)
                & pd.notna(col_mean)
                & (log_col_series >= mean_minus_sd)
                & (log_col_series < col_mean)
            )
            condE = (
                (outlier_series == 0)
                & pd.notna(col_mean)
                & pd.notna(mean_plus_sd)
                & (log_col_series >= col_mean)
                & (log_col_series < mean_plus_sd)
            )
            condF = (
                (outlier_series == 0)
                & pd.notna(mean_plus_sd)
                & (log_col_series >= mean_plus_sd)
            )

            conditions = [condA, condB, condG, condC, condD, condE, condF]
            choices = [-1, 5, -1, -1, 1, 3, 5]

            df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Recodificar os NaNs restantes para 0
        df[nota_col] = df[nota_col].fillna(0)

    #  Selecionar colunas finais
    cols_to_keep_final = []
    id_col_name = "id"
    if id_col_name in df.columns:
        cols_to_keep_final.append(id_col_name)
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_taxa_3(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_3) baseadas nos quartis das colunas de variação,
    calculados após filtrar outliers (identificados internamente).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários (log, z-score, outlier).

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list): Uma lista de nomes das colunas de variação *originais*
                           a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_3'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        min_val = df[col].min()
        log_col_name = f"{col}_min_ln"  # Nome da coluna de log

        # Lidar com coluna original toda NaN
        if df[col].isnull().all():
            outlier_col = f"outlier_{col}"
            nota_col = f"nota_{col}_3"
            for c_name in [log_col_name, outlier_col, nota_col]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            nota_cols_final.append(nota_col)
            continue

        abs_min = np.abs(min_val) if pd.notna(min_val) else 0

        temp_shifted_col = abs_min + df[col]  # Série temporária

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            df[log_col_name] = np.log(temp_shifted_col)
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # Calcular Outliers
        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)

        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0:
            temp_z_score_col = (df[log_col_name] - col_mean) / col_sd

        outlier_col = f"outlier_{col}"
        df[outlier_col] = np.nan
        df.loc[df[log_col_name].notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -2, outlier_col] = 1
        df.loc[temp_z_score_col > 2, outlier_col] = 2

        # Filtrar e Calcular Quartis
        quartile_filter_mask = (df[outlier_col] == 0) & df[log_col_name].notna()
        data_for_quartiles = df.loc[quartile_filter_mask, log_col_name]

        k25, k50, k75 = np.nan, np.nan, np.nan
        if not data_for_quartiles.empty:
            quantiles = data_for_quartiles.quantile([0.25, 0.50, 0.75])
            if 0.25 in quantiles.index:
                k25 = quantiles.loc[0.25]
            if 0.50 in quantiles.index:
                k50 = quantiles.loc[0.50]
            if 0.75 in quantiles.index:
                k75 = quantiles.loc[0.75]

        #  Cálculo da Nota (_3)
        nota_col = f"nota_{col}_3"
        nota_cols_final.append(nota_col)
        df[nota_col] = np.nan

        cond_missing = df[col].notna() & df[log_col_name].isna()
        cond1 = pd.notna(k25) & (df[log_col_name] <= k25)
        cond2 = (
            pd.notna(k25)
            & pd.notna(k50)
            & (df[log_col_name] > k25)
            & (df[log_col_name] <= k50)
        )
        cond3 = (
            pd.notna(k50)
            & pd.notna(k75)
            & (df[log_col_name] > k50)
            & (df[log_col_name] <= k75)
        )
        cond4 = pd.notna(k75) & (df[log_col_name] > k75)

        conditions = [cond_missing, cond1, cond2, cond3, cond4]
        choices = [-1, -1, 1, 3, 5]
        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza
        if log_col_name in df.columns:
            del df[log_col_name]
        if outlier_col in df.columns:
            del df[outlier_col]

    # Selecionar colunas finais
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_participacao_1(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_1) para colunas de participação, baseadas na distribuição
    normal da coluna original (tratando 0s como missing para stats).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list[str]): Uma lista de nomes das colunas de *participação*
                             originais a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_1'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        # Criar uma série temporária para cálculos onde 0 é NaN
        col_data_for_stats = df[col].replace(0, np.nan)

        # Nome da coluna de nota
        nota_col = f"nota_{col}_1"
        nota_cols_final.append(nota_col)

        if col_data_for_stats.isnull().all():
            # Criar colunas dependentes como NaN
            prob_ks_col = f"Prob_KS_{col}"  # Sem _ln
            mean_col = f"{col}_mean"
            sd_col = f"{col}_sd"
            mean_minus_sd_col = f"{col}_mean_menos_sd"
            mean_plus_sd_col = f"{col}_mean_mais_sd"
            for c_name in [
                prob_ks_col,
                mean_col,
                sd_col,
                mean_minus_sd_col,
                mean_plus_sd_col,
                nota_col,
            ]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            continue
        # Teste de Normalidade (Lilliefors)
        prob_ks_col = f"Prob_KS_{col}"
        data_for_ks = col_data_for_stats.dropna()
        p_value = np.nan
        if len(data_for_ks) >= 5:
            try:
                ks_stat, p_value = lilliefors(data_for_ks, dist="norm")
            except Exception:
                p_value = np.nan
        # Adicionar p-valor como coluna constante
        df[prob_ks_col] = p_value

        # Média e Desvio Padrão ---
        mean_col = f"{col}_mean"
        sd_col = f"{col}_sd"
        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)

        df[mean_col] = col_mean
        df[sd_col] = col_sd

        # Limites Mean +/- SD
        mean_minus_sd_col = f"{col}_mean_menos_sd"
        mean_plus_sd_col = f"{col}_mean_mais_sd"
        if pd.notna(col_mean) and pd.notna(col_sd):
            df[mean_minus_sd_col] = col_mean - col_sd
            df[mean_plus_sd_col] = col_mean + col_sd
        else:
            df[mean_minus_sd_col] = np.nan
            df[mean_plus_sd_col] = np.nan

        #  Cálculo da Nota (_1) ---
        df[nota_col] = np.nan

        # Condição principal do p-valor
        p_val_check = df[prob_ks_col].fillna(0) >= 0.05

        # Condições baseadas nos limites e casos especiais (0 e 100)
        # Aplicadas à coluna original df[col]
        cond_eq_0 = p_val_check & (df[col] == 0)
        cond_eq_100 = p_val_check & (df[col] == 100)
        cond_lt_msd = p_val_check & (df[col] < df[mean_minus_sd_col])
        cond_bt_msd_m = (
            p_val_check & (df[col] >= df[mean_minus_sd_col]) & (df[col] < df[mean_col])
        )
        cond_bt_m_psd = (
            p_val_check & (df[col] >= df[mean_col]) & (df[col] < df[mean_plus_sd_col])
        )
        cond_gt_psd = p_val_check & (df[col] >= df[mean_plus_sd_col])

        # Ordem: Casos especiais (0, 100) primeiro, depois as bandas
        conditions = [
            cond_eq_0,  # Valor 0 -> Nota 5
            cond_eq_100,  # Valor 100 -> Nota -1
            cond_lt_msd,  # Abaixo de mean-sd -> Nota 5 (Invertido!)
            cond_bt_msd_m,  # Entre mean-sd e mean -> Nota 3 (Invertido!)
            cond_bt_m_psd,  # Entre mean e mean+sd -> Nota 1 (Invertido!)
            cond_gt_psd,  # Acima de mean+sd -> Nota -1 (Invertido!)
        ]
        choices = [
            5,  # Nota para 0
            -1,  # Nota para 100
            -1,  # Nota para < mean-sd
            1,  # Nota para >= mean-sd & < mean
            3,  # Nota para >= mean & < mean+sd
            5,  # Nota para >= mean+sd
        ]
        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza de Colunas Intermediárias
        cols_to_delete = [
            prob_ks_col,
            mean_col,
            sd_col,
            mean_minus_sd_col,
            mean_plus_sd_col,
        ]
        for c_del in cols_to_delete:
            if c_del in df.columns:
                try:
                    del df[c_del]
                except KeyError:
                    pass
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_participacao_2(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_2) para colunas de participação, considerando outliers
    (+/- 3 SD) e teste K-S em dados filtrados (sem 0s e sem outliers).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list[str]): Uma lista de nomes das colunas de *participação*
                             originais a serem processadas.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_2'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        # Preparar dados para Stats (0 = NaN)
        col_data_for_stats = df[col].replace(0, np.nan)

        # Nome da coluna de nota
        nota_col = f"nota_{col}_2"
        nota_cols_final.append(nota_col)

        # Lidar com coluna original (sem 0s) toda NaN
        if col_data_for_stats.isnull().all():
            # Criar colunas dependentes como NaN
            outlier_col = f"outlier_{col}"
            prob_ks_col = f"Prob_KS_{col}"
            mean_col = f"{col}_mean"
            sd_col = f"{col}_sd"
            mean_minus_sd_col = f"{col}_mean_menos_sd"
            mean_plus_sd_col = f"{col}_mean_mais_sd"
            for c_name in [
                outlier_col,
                prob_ks_col,
                mean_col,
                sd_col,
                mean_minus_sd_col,
                mean_plus_sd_col,
                nota_col,
            ]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            continue

        # Média e Desvio Padrão Gerais
        mean_col = f"{col}_mean"
        sd_col = f"{col}_sd"
        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)

        df[mean_col] = col_mean
        df[sd_col] = col_sd

        # Cálculo do Z-Score
        # Calculado sobre os dados com 0s como NaN
        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0:
            temp_z_score_col = (col_data_for_stats - col_mean) / col_sd

        # Identificação de Outliers (Threshold +/- 3)
        outlier_col = f"outlier_{col}"
        df[outlier_col] = np.nan
        df.loc[df[col].notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -3, outlier_col] = 1
        df.loc[temp_z_score_col > 3, outlier_col] = 2

        # Filtragem TEMPORÁRIA para K-S
        # Filtro: não missing na original E Z-score entre -3 e 3
        ks_filter_mask = (
            col_data_for_stats.notna()
            & (temp_z_score_col > -3)
            & (temp_z_score_col < 3)
        )
        data_for_ks_filtered = col_data_for_stats[ks_filter_mask]  # Usa dados com 0=NaN

        # Realizar Teste K-S (Lilliefors) nos dados filtrados
        p_value_filtered = np.nan
        if len(data_for_ks_filtered) >= 5:
            try:
                ks_stat, p_value_filtered = lilliefors(
                    data_for_ks_filtered, dist="norm"
                )
            except Exception:
                p_value_filtered = np.nan

        # Aplica o p-valor (do teste filtrado) a TODAS as linhas
        prob_ks_col = f"Prob_KS_{col}"
        df[prob_ks_col] = p_value_filtered

        # -Calcular Limites (usando média/dp gerais, com 0=NaN)
        mean_minus_sd_col = f"{col}_mean_menos_sd"
        mean_plus_sd_col = f"{col}_mean_mais_sd"
        if pd.notna(col_mean) and pd.notna(col_sd):
            df[mean_minus_sd_col] = col_mean - col_sd
            df[mean_plus_sd_col] = col_mean + col_sd
        else:
            df[mean_minus_sd_col] = np.nan
            df[mean_plus_sd_col] = np.nan

        # Cálculo da Nota (_2) ---
        # Comparações usam a coluna original (0 é 0)
        df[nota_col] = np.nan

        p_val_check = df[prob_ks_col].fillna(0) >= 0.05

        # Condições específicas para nota_participacao_2
        cond_eq_0 = p_val_check & (df[col] == 0)
        cond_eq_100 = p_val_check & (df[col] == 100)
        cond_outlier_1 = p_val_check & (df[outlier_col] == 1)  # Outlier inferior Z<-3
        cond_outlier_2 = p_val_check & (df[outlier_col] == 2)  # Outlier superior Z>3
        # Se não for outlier (==0), aplicar banding
        cond_band_1 = (
            p_val_check & (df[outlier_col] == 0) & (df[col] < df[mean_minus_sd_col])
        )
        cond_band_2 = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[col] >= df[mean_minus_sd_col])
            & (df[col] < df[mean_col])
        )
        cond_band_3 = (
            p_val_check
            & (df[outlier_col] == 0)
            & (df[col] >= df[mean_col])
            & (df[col] < df[mean_plus_sd_col])
        )
        cond_band_4 = (
            p_val_check & (df[outlier_col] == 0) & (df[col] >= df[mean_plus_sd_col])
        )

        # Ordem: Casos Especiais (0, 100), Outliers (1, 2), Banding (para 0)
        conditions = [
            cond_eq_0,  # Nota 5 se 0
            cond_eq_100,  # Nota -1 se 100
            cond_outlier_1,  # Nota 5 se outlier Z<-3
            cond_outlier_2,  # Nota -1 se outlier Z>3
            cond_band_1,  # Nota 5 se não outlier e < mean-sd
            cond_band_2,  # Nota 3 se não outlier e >= mean-sd & < mean
            cond_band_3,  # Nota 1 se não outlier e >= mean & < mean+sd
            cond_band_4,  # Nota -1 se não outlier e >= mean+sd
        ]
        choices = [
            5,  # Nota para 0
            -1,  # Nota para 100
            5,  # Nota para outlier Z<-3
            -1,  # Nota para outlier Z>3
            5,  # Nota para banda 1
            3,  # Nota para banda 2
            1,  # Nota para banda 3
            -1,  # Nota para banda 4
        ]
        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza de Colunas Intermediárias
        cols_to_delete = [
            mean_col,
            sd_col,
            outlier_col,
            prob_ks_col,
            mean_minus_sd_col,
            mean_plus_sd_col,
        ]
        for c_del in cols_to_delete:
            if c_del in df.columns:
                try:
                    del df[c_del]
                except KeyError:
                    pass

    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_participacao_3(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_3) para colunas de participação, baseadas nos quartis
    da coluna original, calculados após filtrar outliers (+/- 3 SD).

    Assume que a coluna 'id' existe no DataFrame de entrada.
    Realiza todos os cálculos intermediários necessários.

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados originais e a coluna 'id'.
        colunas (list[str]): Lista dos nomes das colunas de participação originais.

    Returns:
        pd.DataFrame: Um DataFrame contendo a coluna 'id' e as colunas
                      de nota calculadas ('nota_col_3'). Notas não atribuídas
                      pelas condições permanecerão como NaN.
    """
    df = df.copy()  # Trabalhar em uma cópia
    nota_cols_final = []

    # Escala de notas padrão (direta)
    choices = [-1, 1, 3, 5]

    for col in colunas:
        # Preparar dados para Stats (0 = NaN) ---
        col_data_for_stats = df[col].replace(0, np.nan)

        # Nome da coluna de nota
        nota_col = f"nota_{col}_3"
        nota_cols_final.append(nota_col)

        # Lidar com coluna original (sem 0s) toda NaN
        if col_data_for_stats.isnull().all():
            outlier_col = f"outlier_{col}"
            for c_name in [outlier_col, nota_col]:
                if c_name not in df.columns:
                    df[c_name] = np.nan
            continue

        # Calcular Outliers (Threshold +/- 3 SD) ---
        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)

        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0:
            temp_z_score_col = (col_data_for_stats - col_mean) / col_sd

        outlier_col = f"outlier_{col}"
        df[outlier_col] = np.nan
        df.loc[df[col].notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -3, outlier_col] = 1
        df.loc[temp_z_score_col > 3, outlier_col] = 2

        # Filtrar e Calcular Quartis ---
        quartile_filter_mask = (df[outlier_col] == 0) & df[col].notna()
        data_for_quartiles = df.loc[quartile_filter_mask, col]

        k25, k50, k75 = np.nan, np.nan, np.nan
        if not data_for_quartiles.empty:
            quantiles = data_for_quartiles.quantile([0.25, 0.50, 0.75])
            if 0.25 in quantiles.index:
                k25 = quantiles.loc[0.25]
            if 0.50 in quantiles.index:
                k50 = quantiles.loc[0.50]
            if 0.75 in quantiles.index:
                k75 = quantiles.loc[0.75]

        # Cálculo da Nota (_3) ---
        df[nota_col] = np.nan  # Inicializar com NaN

        # Condições baseadas nos quartis
        # A comparação é feita com a coluna original (df[col])
        cond1 = pd.notna(k25) & (df[col] <= k25)
        cond2 = pd.notna(k25) & pd.notna(k50) & (df[col] > k25) & (df[col] <= k50)
        cond3 = pd.notna(k50) & pd.notna(k75) & (df[col] > k50) & (df[col] <= k75)
        cond4 = pd.notna(k75) & (df[col] > k75)

        conditions = [cond1, cond2, cond3, cond4]
        # choices já definido como [-1, 1, 3, 5] no início da função

        df[nota_col] = np.select(conditions, choices, default=np.nan)

        # Limpeza de Colunas Intermediárias
        if outlier_col in df.columns:
            try:
                del df[outlier_col]
            except KeyError:
                pass
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])

    return df[cols_to_keep_final].copy()


def nota_final(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula a nota final consolidada para cada variável base, pegando a
    primeira nota não nula e diferente de zero de _1, _2, _3.

    Assume que as colunas de nota (_1, _2, _3) e 'id' existem no DataFrame.

    Args:
        df (pd.DataFrame): DataFrame contendo 'id' e as colunas de nota.
        colunas (list): Lista dos nomes base das variáveis para as quais
                             calcular a nota final.

    Returns:
        pd.DataFrame: DataFrame contendo 'id' e as colunas de nota final
                      calculadas.
                      Se nenhuma nota _1, _2, ou _3 for válida, o valor
                      será NaN.
    """
    df = df.copy()
    nota_final_cols = []

    for base_col in colunas:
        nota_1_col = f"nota_{base_col}_1"
        nota_2_col = f"nota_{base_col}_2"
        nota_3_col = f"nota_{base_col}_3"
        cols_to_check = [nota_1_col, nota_2_col, nota_3_col]

        final_nota_col = f"nota_{base_col}"
        nota_final_cols.append(final_nota_col)

        # Verificar se as colunas de nota existem no DataFrame
        existing_cols = [col for col in cols_to_check if col in df.columns]

        # Inicializar a coluna final com NaN
        df[final_nota_col] = np.nan

        # Se nenhuma das colunas de nota existir, pular para a próxima base_col
        if not existing_cols:
            continue  # A coluna final permanecerá como NaN

        # Iterar pelas colunas existentes na ordem definida (1, 2, 3)
        # e preencher a nota final com o primeiro valor válido encontrado
        for nota_col in existing_cols:
            # Condição: valor não é NaN E valor é diferente de 0
            is_valid = df[nota_col].notna() & (df[nota_col] != 0)

            # Condição: a nota final ainda não foi preenchida (é NaN)
            is_target_still_nan = df[final_nota_col].isna()

            # Onde ambas as condições são verdadeiras, preencher a nota final
            fill_mask = is_valid & is_target_still_nan
            df.loc[fill_mask, final_nota_col] = df[nota_col]

    # Selecionar colunas finais para retornar
    cols_to_keep = []
    if "id" in df.columns:
        cols_to_keep.append("id")
    cols_to_keep.extend([nc for nc in nota_final_cols if nc in df.columns])

    return df[cols_to_keep].copy()


def calcular_notas_bloco(
    df_variaveis_notas: pd.DataFrame, dicionario_bloco: dict
) -> pd.DataFrame:
    """
    Calcula as notas finais para blocos temáticos.

    Baseia-se na média das notas das variáveis individuais (`nota_var_X`)
    pertencentes a cada bloco, conforme definido em `dicionario_bloco`.
    A média é então categorizada em uma nota final (-1, 1, 3, 5) usando a
    média e o desvio padrão da coluna de médias do bloco.

    Args:
        df_variaveis_notas (pd.DataFrame): DataFrame contendo pelo menos
                                            as colunas 'id', 'municipio',
                                            e as colunas 'nota_var_X'.
        dicionario_bloco (dict): Dicionário mapeando nomes de variáveis base
                                 ('var_X') para seus respectivos nomes de bloco
                                 (e.g., {'var_1': 'Socioeconômica'}).

    Returns:
        pd.DataFrame: Uma cópia do DataFrame original com colunas adicionais
                      representando a média calculada (`media_bloco_...`) e
                      a nota final categorizada (`nota_bloco_...`) para cada
                      bloco encontrado. Retorna o DataFrame original (cópia)
                      se o dicionario_bloco for inválido ou vazio.
    """
    if not dicionario_bloco or not isinstance(dicionario_bloco, dict):
        return df_variaveis_notas

    df_result = df_variaveis_notas.copy()

    bloco_para_notas_cols = {}
    colunas_de_nota_existentes = [
        c for c in df_result.columns if c.startswith("nota_var_")
    ]
    prefixo_chave = "nota_"

    for col_nota in colunas_de_nota_existentes:
        var_original_key = col_nota[len(prefixo_chave) :]
        bloco = dicionario_bloco.get(var_original_key)
        if bloco:
            if bloco not in bloco_para_notas_cols:
                bloco_para_notas_cols[bloco] = []
            bloco_para_notas_cols[bloco].append(col_nota)

    # Calcular a média e 3. Categorizar para cada bloco
    blocos_processados = 0
    for bloco, lista_cols_nota in bloco_para_notas_cols.items():
        # --- Clean block name (unchanged) ---
        bloco_clean_name = bloco.lower()
        bloco_clean_name = re.sub(r"[áàâãä]", "a", bloco_clean_name)
        bloco_clean_name = re.sub(r"[éèêë]", "e", bloco_clean_name)
        bloco_clean_name = re.sub(r"[íìîï]", "i", bloco_clean_name)
        bloco_clean_name = re.sub(r"[óòôõö]", "o", bloco_clean_name)
        bloco_clean_name = re.sub(r"[úùûü]", "u", bloco_clean_name)
        bloco_clean_name = re.sub(r"[ç]", "c", bloco_clean_name)
        bloco_clean_name = re.sub(r"[\s-]+", "_", bloco_clean_name)
        bloco_clean_name = re.sub(r"[^\w_]+", "", bloco_clean_name)
        bloco_clean_name = re.sub(r"_+", "_", bloco_clean_name).strip("_")

        media_col = f"media_bloco_{bloco_clean_name}"
        nota_final_col = f"nota_bloco_{bloco_clean_name}"

        cols_existentes_neste_bloco = [
            c for c in lista_cols_nota if c in df_result.columns
        ]

        # Calcular a média das notas para o bloco
        try:
            df_result[media_col] = df_result[cols_existentes_neste_bloco].mean(
                axis=1, skipna=True
            )
        except Exception:
            if media_col in df_result.columns:
                del df_result[media_col]
            continue

        # Calcular a nota final para o bloco com base na média e desvio padrão
        try:
            # Calcular a média e o desvio padrão das médias para este bloco
            media_geral_bloco = df_result[media_col].mean()
            sd_geral_bloco = df_result[media_col].std(ddof=1)

            # Limite inferior e superior baseados na média e desvio padrão
            # Tratar casos onde o desvio padrão pode ser NaN ou zero
            if (
                pd.isna(media_geral_bloco)
                or pd.isna(sd_geral_bloco)
                or sd_geral_bloco == 0
            ):
                print(
                    f"Aviso: Média ou SD da coluna '{media_col}' é NaN. Notas para '{nota_final_col}' serão NaN."
                )
                df_result[nota_final_col] = np.nan
            else:
                limite_inf = media_geral_bloco - sd_geral_bloco
                limite_sup = media_geral_bloco + sd_geral_bloco

                # Define as condicoes com base nos limites
                cond1 = df_result[media_col] < limite_inf
                cond2 = (df_result[media_col] >= limite_inf) & (
                    df_result[media_col] < media_geral_bloco
                )
                cond3 = (df_result[media_col] >= media_geral_bloco) & (
                    df_result[media_col] < limite_sup
                )
                cond4 = df_result[media_col] >= limite_sup

                conditions = [cond1, cond2, cond3, cond4]
                choices = [-1, 1, 3, 5]

                # Use default=np.nan para linhas onde media_col era originalmente NaN
                df_result[nota_final_col] = np.select(
                    conditions, choices, default=np.nan
                )

            blocos_processados += 1

        except Exception:
            # Limpeza de colunas criadas potencialmente se ocorrer erro no meio do cálculo
            if nota_final_col in df_result.columns:
                del df_result[nota_final_col]
            # Manter media_col como foi calculada com sucesso antes deste bloco try
            continue

    return df_result


def sort_key(col_name):
    match = re.match(r"([a-zA-Z_]+)(\d+)$", col_name)
    if match:
        prefix = match.group(1)
        number = int(match.group(2))
        return (prefix, number)
    return (col_name, 0)


def ordenar_df_com_notas(
    df: pd.DataFrame,
    colunas_base: List[str],
    sort_key_func: Callable,
    id_col: str = "id",
) -> pd.DataFrame:
    """
    Reordena as colunas de um DataFrame para o formato
    [id, outras_colunas_sem_bloco, base, nota_base, ..., colunas_com_bloco].

    Args:
        df (pd.DataFrame): DataFrame de entrada (geralmente combinado).
        colunas_base (List[str]): Lista dos nomes base das colunas (ex: 'var_1').
        sort_key_func (Callable): Função usada para ordenar as listas de colunas.
        id_col (str, optional): Nome da coluna de identificação. Defaults to 'id'.

    Returns:
        pd.DataFrame: DataFrame com colunas reordenadas.
    """
    colunas_existentes_set = set(df.columns)
    nova_ordem_colunas = []

    # Adiciona coluna ID se existir
    if id_col in colunas_existentes_set:
        nova_ordem_colunas.append(id_col)
        id_set = {id_col}
    else:
        id_set = set()

    # Identifica o conjunto de todas as colunas base e de nota *existentes*
    base_e_nota_existentes_set: Set[str] = set()
    for base_col in colunas_base:
        nota_col = f"nota_{base_col}"
        if base_col in colunas_existentes_set:
            base_e_nota_existentes_set.add(base_col)
        if nota_col in colunas_existentes_set:
            base_e_nota_existentes_set.add(nota_col)

    colunas_restantes_total = (
        colunas_existentes_set - id_set - base_e_nota_existentes_set
    )

    # 4. Divide essas colunas restantes em aquelas com "bloco" e aquelas sem
    colunas_restantes_sem_bloco = []
    colunas_bloco = []
    for col in colunas_restantes_total:
        if "bloco" in col.lower():
            colunas_bloco.append(col)
        else:
            colunas_restantes_sem_bloco.append(col)

    # 5. Ordena as colunas restantes sem bloco e as adiciona após o ID
    colunas_restantes_sem_bloco_sorted = sorted(
        colunas_restantes_sem_bloco, key=sort_key_func
    )
    nova_ordem_colunas.extend(colunas_restantes_sem_bloco_sorted)

    # 6. Ordena as colunas base e adiciona os pares base/nota
    todas_colunas_base_ordenadas = sorted(colunas_base, key=sort_key_func)
    for base_col in todas_colunas_base_ordenadas:
        nota_col = f"nota_{base_col}"

        if (
            base_col in base_e_nota_existentes_set
            and base_col not in nova_ordem_colunas
        ):
            nova_ordem_colunas.append(base_col)

        # Adiciona a coluna nota se existir e não tiver sido adicionada
        if (
            nota_col in base_e_nota_existentes_set
            and nota_col not in nova_ordem_colunas
        ):
            nova_ordem_colunas.append(nota_col)

    # 7. Ordena as colunas bloco e as adiciona ao final
    colunas_bloco_sorted = sorted(colunas_bloco, key=sort_key_func)
    nova_ordem_colunas.extend(colunas_bloco_sorted)

    # 8. Verificação final para garantir que todas as colunas existem e retorna o DataFrame reordenado
    cols_existentes_na_ordem_final = [
        col for col in nova_ordem_colunas if col in colunas_existentes_set
    ]

    return df[cols_existentes_na_ordem_final]


def renomear_colunas_mapeadas(
    df: pd.DataFrame,
    map_df: pd.DataFrame,
    map_from_col: str = "var",
    map_to_col: str = "coluna",
) -> pd.DataFrame:
    """
    Renomeia colunas do DataFrame baseadas em um mapeamento de outro DataFrame.
    Procura por padrões 'map_from_col_value' e 'nota_map_from_col_value'.

    Args:
        df (pd.DataFrame): DataFrame cujas colunas serão renomeadas.
        map_df (pd.DataFrame): DataFrame contendo o mapeamento.
        map_from_col (str, optional): Nome da coluna em map_df com os nomes atuais. Defaults to 'var'.
        map_to_col (str, optional): Nome da coluna em map_df com os nomes novos. Defaults to 'coluna'.

    Returns:
        pd.DataFrame: DataFrame com colunas renomeadas.
    """
    # Cria o dicionário de mapeamento, tratando possíveis NaNs
    map_df_clean = map_df.dropna(subset=[map_from_col, map_to_col])
    mapping_dict = pd.Series(
        map_df_clean[map_to_col].values, index=map_df_clean[map_from_col]
    ).to_dict()

    rename_mapping = {}

    base_pattern_regex = re.compile(r"^(var_\d+)$")
    nota_pattern_regex = re.compile(r"^nota_(var_\d+)$")

    for current_col in df.columns:
        match_nota = nota_pattern_regex.match(current_col)
        match_base = base_pattern_regex.match(current_col)

        if match_nota:
            var_part = match_nota.group(1)
            new_base_name = mapping_dict.get(var_part)
            if new_base_name:
                rename_mapping[current_col] = f"nota_{new_base_name}"
            else:
                rename_mapping[current_col] = current_col
        elif match_base:
            var_part = match_base.group(1)
            new_base_name = mapping_dict.get(var_part)
            if new_base_name:
                rename_mapping[current_col] = new_base_name
            else:
                rename_mapping[current_col] = current_col
        else:
            rename_mapping[current_col] = current_col

    return df.rename(columns=rename_mapping)
