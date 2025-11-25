import pandas as pd

def metric_null_percentage(df: pd.DataFrame, col: str) -> float:
    """Porcentaje de valores nulos en una columna."""
    if col not in df.columns:
        return 1.0
    return float(df[col].isna().mean())


def metric_unique_values(df: pd.DataFrame, col: str) -> int:
    """Número de valores únicos en una columna."""
    if col not in df.columns:
        return 0
    return int(df[col].nunique())


def metric_duplicates(df: pd.DataFrame, subset: list[str]) -> int:
    """Número de filas duplicadas según un subconjunto de columnas."""
    return int(df.duplicated(subset=subset).sum())


def check_numeric_range(df: pd.DataFrame, col: str, min_val=None, max_val=None) -> bool:
    """Comprueba si todos los valores numéricos están en un rango."""
    if col not in df.columns:
        return False

    series = df[col].dropna()
    if series.empty:
        return True

    if min_val is not None and (series < min_val).any():
        return False
    if max_val is not None and (series > max_val).any():
        return False

    return True


def check_required_columns(df: pd.DataFrame, required_cols: list[str]) -> dict:
    """Indica si cada columna requerida existe en el DataFrame."""
    return {col: (col in df.columns) for col in required_cols}
