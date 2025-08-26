import pandas as pd
from handlers.db_connection import DatabaseConnection
from handlers.query_loader import get_etl_query, get_etl_config
from handlers.log_handler import setup_logger
from typing import Dict, Optional


class ContasAPagarETL:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_connection = DatabaseConnection(source_config)
        self.target_connection = DatabaseConnection(target_config)
        self.logger = setup_logger("contas_a_pagar_etl", log_file="logs/contas_a_pagar_etl.log")

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            self.logger.info(f"Transformando {len(df)} registros")

            # As colunas já vêm com os nomes finais a partir da query
            # Garantir tipos e normalizações

            # Campos numéricos
            numeric_columns = ["valor", "saldo"]
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Campo inteiro (parcela)
            if "parcela" in df.columns:
                df["parcela"] = pd.to_numeric(df["parcela"], errors="coerce").astype("Int64")

            # id_origem pode ser bigint - tratar valores muito grandes
            if "id_origem" in df.columns:
                df["id_origem"] = pd.to_numeric(df["id_origem"], errors="coerce")
                # Se exceder o limite do bigint, usar None
                max_bigint = 9223372036854775807  # 2^63 - 1
                df.loc[df["id_origem"] > max_bigint, "id_origem"] = None

            # Datas (date)
            date_columns = [
                "emissao",
                "vencimento_original",
                "vencimento",
                "entrada",
                "pagamento",
                "baixa",
            ]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    mask = pd.notna(df[col])
                    df.loc[mask, col] = df.loc[mask, col].dt.date

            # Timestamp (registro)
            if "registro" in df.columns:
                df["registro"] = pd.to_datetime(df["registro"], errors="coerce")

            # Campos de texto: normalizar strings e None
            text_columns = [
                "tipo",
                "documento",
                "razao_social",
                "status",
                "historico",
                "codigo_barras",
                "codigo_digitado",
            ]
            for col in text_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
                    df.loc[df[col] == "None", col] = None

            # Garantir presença e ordem das colunas da tabela de destino
            columns = [
                "id_origem",
                "tipo",
                "documento",
                "parcela",
                "razao_social",
                "status",
                "valor",
                "saldo",
                "emissao",
                "vencimento_original",
                "vencimento",
                "entrada",
                "pagamento",
                "baixa",
                "registro",
                "historico",
                "codigo_barras",
                "codigo_digitado",
            ]

            existing_columns = [c for c in columns if c in df.columns]
            result = df[existing_columns]

            # Limpeza final de NaT
            for col in result.columns:
                if str(result[col].dtype).startswith("datetime64"):
                    result[col] = result[col].replace({pd.NaT: None})

            return result

        except Exception as e:
            self.logger.error(f"Erro na transformação: {str(e)}")
            raise

    def extract_data(self) -> pd.DataFrame:
        try:
            self.logger.info("Iniciando extração de dados de contas_a_pagar")
            query = get_etl_query("contas_a_pagar")
            result = self.source_connection.get_data(query)
            self.logger.info(f"Extraídos {len(result)} registros")
            return result
        except Exception as e:
            self.logger.error(f"Erro na extração: {str(e)}")
            raise

    def load_data(self, df: pd.DataFrame) -> None:
        try:
            self.logger.info(f"Carregando {len(df)} registros com UPSERT")
            config = get_etl_config("contas_a_pagar")
            table_name = config.get("table", "contas_a_pagar")
            schema = config.get("schema", "public")
            unique_columns = config.get(
                "unique_columns",
                [
                    "tipo",
                    "documento",
                    "id_origem",
                    "parcela",
                    "vencimento_original",
                    "registro",
                ],
            )

            self.target_connection.upsert(
                table_name=table_name,
                data=df,
                unique_columns=unique_columns,
                schema=schema,
            )

            self.logger.info("UPSERT concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no carregamento (UPSERT): {str(e)}")
            raise

    def run_etl(self) -> dict:
        try:
            self.logger.info("Iniciando processo ETL de contas_a_pagar")
            raw = self.extract_data()
            if raw.empty:
                self.logger.warning("Nenhum dado retornado da origem")
                return {"processed": 0}

            transformed = self.transform_data(raw)
            self.load_data(transformed)
            self.logger.info(f"ETL concluído. Processados {len(transformed)} registros.")
            return {"processed": len(transformed)}
        except Exception as e:
            self.logger.error(f"Falha no ETL: {str(e)}")
            raise


