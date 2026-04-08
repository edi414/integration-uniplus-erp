import pandas as pd

def clean_dataframe_nans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui de forma segura todos os valores nulos (NaN, NaT) do Pandas
    por None nativo do Python. Isso previne que o Pandas reconverta None
    de volta para float('nan') em colunas numéricas, garantindo que o PostgreSQL
    salve o dado como um verdadeiro NULL.
    """
    result = df.copy()
    result = result.astype(object).where(pd.notnull(result), None)
    return result
