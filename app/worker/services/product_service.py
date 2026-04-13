import logging
from app.worker.services.uniplus_api import UniplusAPI
from app.worker.repositories.mariadb_repository import MariaDBRepository
from app.worker.utils.logger import get_logger, log_event_to_file

logger = get_logger()

class ProductService:
    def __init__(self):
        self.api = UniplusAPI()
        self.db = MariaDBRepository()

    def process_price_update(self, ean: str, raw_payload: dict):
        """
        Interpreta o evento, busca o ID interno no G3 e aplica o update cirúrgico.
        """
        # 1. Obter novo preço do payload
        new_price = raw_payload.get("preco")
        if new_price is None:
            logger.error(f"Evento sem campo 'preco' para EAN {ean}")
            return False

        # Converter preço de string (ex: '150,00') para float se necessário
        if isinstance(new_price, str):
            try:
                new_price = float(new_price.replace(',', '.'))
            except ValueError:
                logger.error(f"Formato de preço inválido: {new_price} para EAN {ean}")
                return False

        # 2. Obter ID do produto no MariaDB G3 (Local)
        product_id = self.db.get_id_produto_by_gtin(ean)

        if not product_id:
            message = f"Processamento cancelado: ID do produto não encontrado no MariaDB para EAN {ean}"
            logger.warning(message)
            log_event_to_file(message, level=logging.WARNING)
            return False

        logger.info(f"Iniciando transação cirúrgica para EAN {ean} (ID: {product_id}) -> Novo Preço: {new_price}")
        
        # 3. Executar Transação SQL Cirúrgica
        success = self.db.update_price_surgical(product_id, new_price)
        
        if success:
            msg = f"SUCESSO: EAN {ean} atualizado via DB G3 (Cirúrgico). Novo Preço: {new_price}"
            logger.info(msg)
            log_event_to_file(msg, level=logging.INFO)
            return True
        else:
             msg = f"FALHA: Erro na transação cirúrgica para EAN {ean}."
             logger.error(msg)
             log_event_to_file(msg, level=logging.ERROR)
             return False


    def _apply_business_rules(self, raw_payload: dict, codigo: int, ean: str) -> dict:
        """
        Garante que regras de impostos, lucro e casas decimais são respeitadas,
        além de adicionar chave `codigo` e `ean` que a API obrigatoriamente espera.
        """
        payload = dict(raw_payload) # cópia
        payload["codigo"] = codigo
        payload["ean"] = ean

        # Tratamento de regras do Uniplus
        if "situacaoTributariaSN" in payload and (payload["situacaoTributariaSN"] == "" or payload["situacaoTributariaSN"] is None):
            payload["situacaoTributariaSN"] = "102"
        
        if "tributacaoSN" in payload and (payload["tributacaoSN"] == "" or payload["tributacaoSN"] is None):
            payload["tributacaoSN"] = "102"

        if "lucroBruto" in payload:
            lucro_bruto = str(payload["lucroBruto"])
            partes = lucro_bruto.split('.')
            parte_inteira = partes[0].replace('-', '')
            if len(parte_inteira) > 3:
                payload["lucroBruto"] = "0"
                logger.info(f"Regra aplicada: LucroBruto zero para EAN {ean} (excedeu 3 digitos)")

        if "aliquotaICMS" in payload and payload["aliquotaICMS"] is not None:
             try:
                payload["aliquotaICMS"] = round(float(payload["aliquotaICMS"]), 2)
             except ValueError:
                 pass

        if "casasDecimais" in payload and payload["casasDecimais"] is not None:
            payload["casasDecimais"] = 0

        return payload

# uv run python -m app.worker.worker