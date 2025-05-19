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
        log_col_name = f"{col}_ln"
        outlier_col = f"outlier_{col}"
        nota_col = f"nota_{col}_3"
        nota_cols_final.append(nota_col)

        # Tentar converter para numérico, tratando erros
        numeric_col_check = pd.to_numeric(df[col], errors="coerce")

        # Calcular log, tratando 0 e negativos como NaN
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            # Usar numeric_col_check que já tratou não-números como NaN
            df[log_col_name] = np.log(numeric_col_check.where(numeric_col_check > 0))
        df[log_col_name] = df[log_col_name].replace([np.inf, -np.inf], np.nan)

        # Calcular Outliers (Threshold +/- 3 SD) - SOMENTE se houver dados no log
        col_mean = df[log_col_name].mean()
        col_sd = df[log_col_name].std(ddof=1)

        temp_z_score_col = pd.Series(np.nan, index=df.index)
        # Calcular Z-score apenas se sd for válido e não zero
        if pd.notna(col_sd) and col_sd != 0 and pd.notna(col_mean):
            temp_z_score_col = (df[log_col_name] - col_mean) / col_sd

        # Inicializar coluna outlier com NaN
        df[outlier_col] = np.nan
        # Marcar como 0 (não outlier) apenas onde o log NÃO é NaN
        df.loc[df[log_col_name].notna(), outlier_col] = 0
        # Marcar outliers onde Z-score foi calculado e excede o threshold
        df.loc[temp_z_score_col < -3, outlier_col] = 1  # Threshold -3
        df.loc[temp_z_score_col > 3, outlier_col] = 2  # Threshold +3

        # Filtrar e Calcular Quartis
        # Considerar apenas não-outliers (0) e onde log não é NaN
        quartile_filter_mask = (df[outlier_col] == 0) & df[log_col_name].notna()
        data_for_quartiles = df.loc[quartile_filter_mask, log_col_name]

        k25, k50, k75 = np.nan, np.nan, np.nan  # Valores padrão
        if (
            not data_for_quartiles.empty and len(data_for_quartiles.dropna()) >= 4
        ):  # Precisa de pontos suficientes para quartis
            try:
                quantiles = data_for_quartiles.quantile([0.25, 0.50, 0.75])

                k25 = quantiles.get(0.25, np.nan)
                k50 = quantiles.get(0.50, np.nan)
                k75 = quantiles.get(0.75, np.nan)
            except Exception:
                k25, k50, k75 = np.nan, np.nan, np.nan

        # Cálculo da Nota (_3)
        df[nota_col] = np.nan  # Inicializa a coluna de nota

        # Condição especial: Original não nulo, mas _ln é nulo
        cond_missing = numeric_col_check.notna() & df[log_col_name].isna()

        # Condições baseadas nos quartis (só aplicáveis se quartis foram calculados e log não é NaN)
        # Checar se os quartis não são NaN antes de usar na condição
        cond1 = pd.notna(k25) & df[log_col_name].notna() & (df[log_col_name] <= k25)
        cond2 = (
            pd.notna(k25)
            & pd.notna(k50)
            & df[log_col_name].notna()
            & (df[log_col_name] > k25)
            & (df[log_col_name] <= k50)
        )
        cond3 = (
            pd.notna(k50)
            & pd.notna(k75)
            & df[log_col_name].notna()
            & (df[log_col_name] > k50)
            & (df[log_col_name] <= k75)
        )
        cond4 = pd.notna(k75) & df[log_col_name].notna() & (df[log_col_name] > k75)
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
    # Garantir que apenas colunas de nota realmente criadas sejam mantidas
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])
    existing_cols_to_keep = [col for col in cols_to_keep_final if col in df.columns]

    return df[existing_cols_to_keep].copy()


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
    Calcula notas (_1) para colunas de participação,
    - KS, Média, SD calculados ignorando zeros.
    - TODAS as notas (0, 100, bandas) são atribuídas SOMENTE se p-valor KS >= 0.05.
    - Nota 0 -> -1
    - Nota 100 -> 5
    - Notas Bandas: -1, 1, 3, 5
    - Usa np.select para atribuição de notas.

    Args:
        df (pd.DataFrame): DataFrame com dados originais e 'id'.
        colunas (list[str]): Nomes das colunas de participação.

    Returns:
        pd.DataFrame: DataFrame com 'id' e 'nota_col_1' calculadas.
    """
    df_result = df[["id"]].copy()

    for col in colunas:
        nota_col = f"nota_{col}_1"

        numeric_col_check = pd.to_numeric(df[col], errors="coerce")
        col_data_for_stats = numeric_col_check.replace(
            0, np.nan
        )  # Zeros como NaN para stats

        # --- Cálculos Stats (sem zeros) ---
        data_for_ks = col_data_for_stats.dropna()
        p_value = np.nan
        if len(data_for_ks) >= 5:
            try:
                ks_stat, p_value = lilliefors(data_for_ks, dist="norm")
            except Exception:
                p_value = np.nan
        prob_ks_check = (p_value >= 0.05) if pd.notna(p_value) else False

        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)

        # Calcular limites apenas se mean e sd forem válidos
        mean_minus_sd = np.nan
        mean_plus_sd = np.nan
        is_sd_zero = pd.notna(col_sd) and col_sd == 0
        is_sd_positive = pd.notna(col_sd) and col_sd > 0

        if is_sd_positive and pd.notna(col_mean):
            mean_minus_sd = col_mean - col_sd
            mean_plus_sd = col_mean + col_sd
        elif is_sd_zero and pd.notna(col_mean):  # Caso sd=0
            mean_minus_sd = col_mean
            mean_plus_sd = col_mean

        # --- Definição das Condições para np.select ---
        # Aplicadas à coluna numérica original
        # Só avaliar se os limites são válidos
        if pd.notna(mean_minus_sd):
            # Condições Base (sem incluir o check de p-valor ainda)
            base_cond_eq_0 = numeric_col_check == 0
            base_cond_eq_100 = numeric_col_check == 100
            base_cond_sd_zero_at_mean = is_sd_zero & (numeric_col_check == col_mean)
            # Condições de banda válidas apenas se sd > 0
            base_cond_lt_msd = is_sd_positive & (numeric_col_check < mean_minus_sd)
            base_cond_bt_msd_m = (
                is_sd_positive
                & (numeric_col_check >= mean_minus_sd)
                & (numeric_col_check < col_mean)
            )
            base_cond_bt_m_psd = (
                is_sd_positive
                & (numeric_col_check >= col_mean)
                & (numeric_col_check < mean_plus_sd)
            )
            base_cond_gt_psd = is_sd_positive & (numeric_col_check >= mean_plus_sd)

            # Combinar com prob_ks_check e ordenar por prioridade SPSS (0, 100, sd=0, bandas)
            conditions = [
                prob_ks_check & base_cond_eq_0,  # Nota -1
                prob_ks_check & base_cond_eq_100,  # Nota 5
                prob_ks_check
                & base_cond_sd_zero_at_mean,  # Nota 1 (assumido para sd=0)
                prob_ks_check & base_cond_lt_msd,  # Nota -1
                prob_ks_check & base_cond_bt_msd_m,  # Nota 1
                prob_ks_check & base_cond_bt_m_psd,  # Nota 3
                prob_ks_check & base_cond_gt_psd,  # Nota 5
            ]
            choices = [-1, 5, 1, -1, 1, 3, 5]  # Notas correspondentes
            default_value = np.nan  # Se nenhuma condição for True (inclui p<0.05)

            # Calcular e atribuir a nota ao DataFrame resultado
            df_result[nota_col] = np.select(conditions, choices, default=default_value)

        else:  # Caso onde mean ou sd não puderam ser calculados
            df_result[nota_col] = np.nan

    return df_result


def nota_participacao_2(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_2) para colunas de participação
    - Média, SD, Z-score calculados INCLUINDO zeros.
    - KS Test feito em dados não-missing e não-outliers (Z entre -3 e 3).
    - TODAS as notas (0, 100, outliers, bandas) são atribuídas SOMENTE se p-valor KS >= 0.05.
    - Nota 0 -> -1
    - Nota 100 -> 5
    - Nota Outlier Z<-3 -> -1
    - Nota Outlier Z>3 -> 5
    - Notas Bandas: -1, 1, 3, 5
    - Usa np.select para atribuição de notas.

    Args:
        df (pd.DataFrame): DataFrame com dados originais e 'id'.
        colunas (list[str]): Nomes das colunas de participação.

    Returns:
        pd.DataFrame: DataFrame com 'id' e 'nota_col_2' calculadas.
    """
    df_result = df[["id"]].copy()

    for col in colunas:
        nota_col = f"nota_{col}_2"

        numeric_col_check = pd.to_numeric(df[col], errors="coerce")
        col_data_for_stats = numeric_col_check

        # Pular se coluna numérica for toda NaN
        if col_data_for_stats.isnull().all():
            df_result[nota_col] = np.nan
            continue

        # --- Cálculos Stats (COM zeros) ---
        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)

        # Calcular Z-Score (COM zeros)
        temp_z_score_col = pd.Series(np.nan, index=df.index)
        # Z-score é NaN se sd=NaN, sd=0, mean=NaN ou valor=NaN
        if pd.notna(col_sd) and col_sd != 0 and pd.notna(col_mean):
            mask_notna = col_data_for_stats.notna()
            temp_z_score_col.loc[mask_notna] = (
                col_data_for_stats[mask_notna] - col_mean
            ) / col_sd

        # --- KS Test (em dados não-missing, não-outliers) ---
        # Filtro SPSS: not missing(...) and (Z > -3 and Z < 3)
        # Equivalente a Z-score não ser NaN e estar entre -3 e 3
        ks_filter_mask = temp_z_score_col.between(
            -3, 3, inclusive="neither"
        )  # 'neither' = exclusivo
        # Teste KS feito nos dados originais (com zeros) filtrados
        data_for_ks_filtered = col_data_for_stats[ks_filter_mask]

        p_value_filtered = np.nan
        if len(data_for_ks_filtered.dropna()) >= 5:
            try:
                ks_stat, p_value_filtered = lilliefors(
                    data_for_ks_filtered.dropna(), dist="norm"
                )
            except Exception:
                p_value_filtered = np.nan
        prob_ks_check = (
            (p_value_filtered >= 0.05) if pd.notna(p_value_filtered) else False
        )

        # --- Calcular Limites (usando Mean/SD com zeros) ---
        mean_minus_sd = np.nan
        mean_plus_sd = np.nan
        # Calcular limites apenas se Média e SD são válidos
        if pd.notna(col_mean) and pd.notna(col_sd):
            # Se sd=0, os limites serão iguais à média
            mean_minus_sd = col_mean - col_sd
            mean_plus_sd = col_mean + col_sd

        # --- Definição das Condições Base para np.select ---
        # Aplicadas à coluna numérica original e ao Z-score calculado
        base_cond_eq_0 = numeric_col_check == 0
        base_cond_eq_100 = numeric_col_check == 100
        # Definir condições de outlier diretamente do Z-score
        # Precisamos checar se Z não é NaN antes da comparação
        base_cond_outlier_1 = temp_z_score_col.notna() & (temp_z_score_col < -3)
        base_cond_outlier_2 = temp_z_score_col.notna() & (temp_z_score_col > 3)
        # Condição para ser um não-outlier válido (Z existe e está entre -3 e 3)
        is_valid_non_outlier_z = ks_filter_mask

        # Condições de banda (aplicadas apenas se for não-outlier válido E limites válidos)
        base_cond_band_1 = pd.Series(False, index=df.index)
        base_cond_band_2 = pd.Series(False, index=df.index)
        base_cond_band_3 = pd.Series(False, index=df.index)
        base_cond_band_4 = pd.Series(False, index=df.index)

        if pd.notna(mean_minus_sd):
            base_cond_band_1 = is_valid_non_outlier_z & (
                numeric_col_check < mean_minus_sd
            )
            base_cond_band_2 = (
                is_valid_non_outlier_z
                & (numeric_col_check >= mean_minus_sd)
                & (numeric_col_check < col_mean)
            )
            base_cond_band_3 = (
                is_valid_non_outlier_z
                & (numeric_col_check >= col_mean)
                & (numeric_col_check < mean_plus_sd)
            )
            base_cond_band_4 = is_valid_non_outlier_z & (
                numeric_col_check >= mean_plus_sd
            )

        # --- Atribuição da Nota via np.select (replicando IFs e prioridade SPSS) ---
        conditions = [
            prob_ks_check & base_cond_eq_0,  # P1: 0 (-1)
            prob_ks_check & base_cond_eq_100,  # P2: 100 (5)
            prob_ks_check & base_cond_outlier_1,  # P3: Outlier Low (-1)
            prob_ks_check & base_cond_outlier_2,  # P4: Outlier High (5)
            prob_ks_check & base_cond_band_1,  # P5: Band 1 (-1)
            prob_ks_check & base_cond_band_2,  # P6: Band 2 (1)
            prob_ks_check & base_cond_band_3,  # P7: Band 3 (3)
            prob_ks_check & base_cond_band_4,  # P8: Band 4 (5)
        ]
        choices = [-1, 5, -1, 5, -1, 1, 3, 5]  # Notas correspondentes
        default_value = (
            np.nan
        )  # Se nenhuma condição for True (inclui p<0.05, NaNs originais)

        # Calcular e atribuir a nota ao DataFrame resultado
        df_result[nota_col] = np.select(conditions, choices, default=default_value)

    return df_result


def nota_participacao_3(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    """
    Calcula notas (_3) para colunas de participação,
    - Usa outlier flag calculado como em nota_2 (Z-score INCLUINDO zeros).
    - Calcula quartis sobre dados não-outliers (pode incluir zeros se não forem outliers).
    - Atribui notas (-1, 1, 3, 5) baseado SOMENTE nas bandas de quartis.
    - Outliers não recebem nota (NaN). 0 e 100 recebem nota baseada na banda em que caem.
    - NENHUMA dependência de teste KS.

    Args:
        df (pd.DataFrame): DataFrame com dados originais e 'id'.
        colunas (list[str]): Nomes das colunas de participação.

    Returns:
        pd.DataFrame: DataFrame com 'id' e 'nota_col_3' calculadas.

    """
    df = df.copy()
    nota_cols_final = []

    for col in colunas:
        nota_col = f"nota_{col}_3"
        nota_cols_final.append(nota_col)

        numeric_col_check = pd.to_numeric(df[col], errors="coerce")
        col_data_for_stats = numeric_col_check

        outlier_col = f"outlier_{col}"
        df[nota_col] = np.nan
        df[outlier_col] = np.nan

        # Pular se coluna numérica for toda NaN
        if col_data_for_stats.isnull().all():
            continue

        # Recalcular Outliers
        col_mean = col_data_for_stats.mean()
        col_sd = col_data_for_stats.std(ddof=1)
        temp_z_score_col = pd.Series(np.nan, index=df.index)
        if pd.notna(col_sd) and col_sd != 0 and pd.notna(col_mean):
            mask_notna = col_data_for_stats.notna()
            temp_z_score_col.loc[mask_notna] = (
                col_data_for_stats[mask_notna] - col_mean
            ) / col_sd
        df.loc[temp_z_score_col.notna(), outlier_col] = 0
        df.loc[temp_z_score_col < -3, outlier_col] = 1
        df.loc[temp_z_score_col > 3, outlier_col] = 2
        # Valores onde Z não pôde ser calculado (NaN original) terão outlier_col = NaN

        # Calcular Quartis (sobre não-outliers, pode incluir zeros)
        quartile_filter_mask = (df[outlier_col] == 0) & numeric_col_check.notna()
        data_for_quartiles = numeric_col_check[quartile_filter_mask]

        k25, k50, k75 = np.nan, np.nan, np.nan
        if not data_for_quartiles.empty and len(data_for_quartiles.dropna()) >= 4:
            try:
                # Usar interpolação linear, que é comum e similar a algumas opções do SPSS
                quantiles = data_for_quartiles.quantile(
                    [0.25, 0.50, 0.75], interpolation="linear"
                )
                k25 = quantiles.get(0.25, np.nan)
                k50 = quantiles.get(0.50, np.nan)
                k75 = quantiles.get(0.75, np.nan)
            except Exception:
                k25, k50, k75 = np.nan, np.nan, np.nan

        # Aplicar a TODOS os valores não-NaN na coluna original.
        # Outliers (que não estão no calculo de K) e NaNs originais ficarão NaN.

        # Verificar se K foram calculados antes de definir condições
        if pd.notna(k25) and pd.notna(k50) and pd.notna(k75):
            cond_q1 = numeric_col_check <= k25
            # Garantir exclusividade entre <=k25 e >k25
            cond_q2 = (numeric_col_check > k25) & (numeric_col_check <= k50)
            cond_q3 = (numeric_col_check > k50) & (numeric_col_check <= k75)
            # >k75 cobre o resto dos valores não-NaN
            cond_q4 = numeric_col_check > k75

            # Ordem é importante se houver sobreposição (não deve haver com > e <=)
            conditions = [cond_q1, cond_q2, cond_q3, cond_q4]
            choices = [-1, 1, 3, 5]

            # Aplicar np.select. O default=np.nan lida com NaNs originais
            # e com os outliers (que não satisfarão nenhuma condição se K são válidos)
            df[nota_col] = np.select(conditions, choices, default=np.nan)
        # else: Se os K não puderam ser calculados, a nota permanece NaN.

        del df[outlier_col]

    # Selecionar colunas finais
    cols_to_keep_final = []
    if "id" in df.columns:
        cols_to_keep_final.append("id")
    cols_to_keep_final.extend([nc for nc in nota_cols_final if nc in df.columns])
    existing_cols_to_keep = [col for col in cols_to_keep_final if col in df.columns]

    return df[existing_cols_to_keep].copy()


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
        media_col = f"media_bloco_{bloco}"
        nota_final_col = f"nota_bloco_{bloco}"

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


def mapear_colunas_para_blocos_excel(
    df_para_formatar, df_classificacao, nome_col_descricoes
):
    """
    Mapeia colunas do df_para_formatar para blocos definidos em df_classificacao.
    Retorna um dicionário {nome_bloco: [lista_de_colunas_do_bloco_presentes_em_df_para_formatar]}.
    """
    colunas_por_bloco = {}

    if not isinstance(df_para_formatar, pd.DataFrame) or not isinstance(
        df_classificacao, pd.DataFrame
    ):
        return colunas_por_bloco
    if (
        "var" not in df_classificacao.columns
        or nome_col_descricoes not in df_classificacao.columns
    ):
        return colunas_por_bloco

    map_var_to_descricao = df_classificacao.set_index("var")[
        nome_col_descricoes
    ].to_dict()

    if (
        "bloco" in df_classificacao.columns
        and not df_classificacao["bloco"].dropna().empty
    ):
        blocos_unicos = df_classificacao["bloco"].dropna().unique()

        for bloco_nome in blocos_unicos:
            lista_cols_neste_bloco = []
            vars_no_bloco = df_classificacao[df_classificacao["bloco"] == bloco_nome][
                "var"
            ].tolist()

            for var_original in vars_no_bloco:
                if var_original in map_var_to_descricao:
                    descricao_col_name = map_var_to_descricao[var_original]
                    if descricao_col_name in df_para_formatar.columns:
                        lista_cols_neste_bloco.append(descricao_col_name)

                    nome_nota_final = f"nota_{descricao_col_name}"
                    if nome_nota_final in df_para_formatar.columns:
                        lista_cols_neste_bloco.append(nome_nota_final)

            # Adicionar colunas de média e nota do bloco se existirem
            media_bloco_col = f"media_bloco_{bloco_nome}"
            if media_bloco_col in df_para_formatar.columns:
                lista_cols_neste_bloco.append(media_bloco_col)

            nota_bloco_col = f"nota_bloco_{bloco_nome}"
            if nota_bloco_col in df_para_formatar.columns:
                lista_cols_neste_bloco.append(nota_bloco_col)

            if (
                lista_cols_neste_bloco
            ):  # Adiciona apenas se encontrou colunas para o bloco
                colunas_por_bloco[bloco_nome] = sorted(
                    list(set(lista_cols_neste_bloco))
                )
    return colunas_por_bloco


def definir_cores_para_blocos_excel(colunas_por_bloco, paleta_cores=None):
    """Cria um dicionário mapeando nomes de blocos para cores."""
    if paleta_cores is None:
        paleta_cores = [
            "#FFFFE0",
            "#ADD8E6",
            "#FFB6C1",
            "#90EE90",
            "#FFDAB9",
            "#E6E6FA",
            "#AFEEEE",
            "#F0E68C",
        ]
    if (
        not colunas_por_bloco or not paleta_cores
    ):  # Se não há blocos ou cores, retorna mapa vazio
        return {}

    return {
        bloco: paleta_cores[i % len(paleta_cores)]
        for i, bloco in enumerate(colunas_por_bloco.keys())
    }


def iniciar_excel_e_escrever_dados(
    df_para_salvar, nome_arquivo_saida, nome_planilha="ranqueamento"
):
    """Inicia o ExcelWriter e escreve os dados do DataFrame, sem cabeçalho, a partir da segunda linha."""
    try:
        writer = pd.ExcelWriter(nome_arquivo_saida, engine="xlsxwriter")
        df_para_salvar.to_excel(
            writer, index=False, sheet_name=nome_planilha, header=False, startrow=1
        )
        workbook = writer.book
        worksheet = writer.sheets[nome_planilha]
        return writer, workbook, worksheet
    except Exception as e:
        print(f"ERRO CRÍTICO ao iniciar ExcelWriter ou escrever dados: {e}")
        return None, None, None


def formatar_cabecalhos_e_colunas_excel(
    worksheet, workbook, df_para_formatar, colunas_por_bloco, mapa_cores_blocos
):
    """Aplica formatação de cabeçalho, cor de fundo de bloco e largura às colunas."""
    if (
        worksheet is None
        or workbook is None
        or not isinstance(df_para_formatar, pd.DataFrame)
    ):
        return

    default_header_format = workbook.add_format(
        {
            "bold": True,
            "text_wrap": True,
            "valign": "top",
            "bg_color": "#F0F0F0",
            "pattern": 1,
            "border": 1,
            "align": "center",
        }
    )
    col_idx_map = {
        col_name: idx for idx, col_name in enumerate(df_para_formatar.columns)
    }

    for col_name, col_idx in col_idx_map.items():
        header_text = str(col_name)
        header_format_to_apply = default_header_format
        current_data_cell_format = None

        if colunas_por_bloco:
            for bloco_nome, colunas_do_bloco in colunas_por_bloco.items():
                if col_name in colunas_do_bloco:
                    background_color = mapa_cores_blocos.get(bloco_nome, "#FFFFFF")
                    header_format_to_apply = workbook.add_format(
                        {
                            "bold": True,
                            "text_wrap": True,
                            "valign": "top",
                            "border": 1,
                            "align": "center",
                            "font_color": "black",
                            "bg_color": background_color,
                            "pattern": 1,
                        }
                    )
                    current_data_cell_format = workbook.add_format(
                        {"bg_color": background_color, "pattern": 1}
                    )
                    break

        worksheet.write(0, col_idx, header_text, header_format_to_apply)

        max_len = len(header_text)
        if (
            col_name in df_para_formatar.columns
            and not df_para_formatar[col_name].empty
        ):
            try:
                col_max_len = df_para_formatar[col_name].astype(str).map(len).max()
                if pd.notna(col_max_len):
                    max_len = max(max_len, int(col_max_len))
            except Exception:
                pass

        adjusted_width = min(max(10, max_len + 3), 40)
        worksheet.set_column(col_idx, col_idx, adjusted_width, current_data_cell_format)
