import pymysql
from app.worker.config import config
from app.worker.utils.logger import get_logger

logger = get_logger()

class MariaDBRepository:
    """
    Handler para conexão e manipulação direta com MariaDB.
    Será utilizado no futuro para substituir a atualização via API.
    """
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=config.MARIADB_HOST,
                port=config.MARIADB_PORT,
                user=config.MARIADB_USER,
                password=config.MARIADB_PASSWORD,
                database=config.MARIADB_DB,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info("Conectado ao MariaDB com sucesso.")
        except Exception as e:
            logger.error(f"Erro ao conectar no MariaDB: {str(e)}")

    def update_product_price(self, ean: str, new_price: float):
        """
        No futuro, o service usará esse método para atualizar
        diretamente no banco de dados.
        """
        if not self.connection:
            self.connect()
        
        if not self.connection:
            return False

        try:
            with self.connection.cursor() as cursor:
                # Exemplo: atualizando usando parameterized queries para evitar SQL injection
                sql = "UPDATE precos_api SET preco = %s WHERE ean = %s"
                cursor.execute(sql, (new_price, ean))
                
            self.connection.commit()
            logger.info(f"Preço do produto EAN {ean} atualizado direto no db para {new_price}")
            return True
        except Exception as e:
            logger.error(f"Erro ao atualizar produto no DB. EAN: {ean}. Erro: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info("Conexão com MariaDB fechada.")
