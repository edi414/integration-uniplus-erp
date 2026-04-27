import pandas as pd

def clean_dataframe_nans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui de forma segura todos os valores nulos (NaN, NaT, 'NaN' string) do Pandas
    por None nativo do Python. Isso previne que o Pandas reconverta None
    de volta para float('nan') em colunas numéricas, garantindo que o PostgreSQL
    salve o dado como um verdadeiro NULL.
    """
    result = df.copy()
    result = result.astype(object).where(pd.notnull(result), None)
    # Captura 'NaN' string que pode surgir de conversões intermediárias com MySQL
    result = result.replace('NaN', None)
    return result
