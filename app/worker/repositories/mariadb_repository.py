import mysql.connector
from app.worker.config import config
from app.worker.utils.logger import get_logger

logger = get_logger()

class MariaDBRepository:
    """
    Handler para conexão e manipulação direta com MariaDB (G3Tech).
    Utiliza as transações cirúrgicas solicitadas pelo usuário.
    """
    def __init__(self):
        self.config = {
            'host': config.MARIADB_HOST,
            'port': config.MARIADB_PORT,
            'user': config.MARIADB_USER,
            'password': config.MARIADB_PASSWORD,
            'database': config.MARIADB_DB,
            'autocommit': False  # Importante para controle manual de transação
        }
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            logger.info(f"Conectado ao MariaDB {config.MARIADB_HOST} com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao conectar no MariaDB: {str(e)}")
            self.connection = None

    def get_id_produto_by_gtin(self, ean: str):
        """Busca o ID interno na tabela produto usando o GTIN."""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        if not self.connection:
            return None

        try:
            cursor = self.connection.cursor()
            query = "SELECT ID FROM produto WHERE gtin = %s AND EXCLUIDO = 0 LIMIT 1"
            cursor.execute(query, (ean,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Erro ao buscar ID do produto EAN {ean}: {str(e)}")
            return None

    def update_price_surgical(self, id_produto: int, new_price: float):
        """
        Executa a atualização de preço conforme SQL cirúrgico.
        Garante que 2 linhas sejam afetadas antes do COMMIT.
        """
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        if not self.connection:
            return False

        cursor = self.connection.cursor()
        try:
            # Comando explícito conforme pedido
            cursor.execute("START TRANSACTION;")
            
            update_sql = """
            UPDATE estoque_produto AS est
            INNER JOIN preco_produto_tipo_pagamento AS pdt 
                ON est.id_produto = pdt.id_produto
            SET 
                est.VALOR_VENDA_PRINCIPAL = %s,
                pdt.preco = %s
            WHERE est.id_produto = %s
              AND est.id_filial = 1 
              AND est.ATIVO = 1 
              AND pdt.id_tipopagamento = 1 
              AND pdt.id_emitente = 1;
            """
            cursor.execute(update_sql, (new_price, new_price, id_produto))
            
            # Validação cirúrgica: 2 linhas afetadas (estoque + preço)
            if cursor.rowcount == 2:
                self.connection.commit()
                logger.info(f"COMMIT: Preço atualizado para ID {id_produto} no MariaDB ({new_price})")
                return True
            else:
                self.connection.rollback()
                logger.warning(f"ROLLBACK: Update ID {id_produto} afetou {cursor.rowcount} linhas (esperado 2).")
                return False

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Erro fatal no update cirúrgico ID {id_produto}: {str(e)}")
            return False
        finally:
            cursor.close()

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Conexão com MariaDB fechada.")
