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
        Recebe a intenção de atualizar via payload cru fornecido pelo n8n,
        limpa/valida o modelo de dados e envia para a API.
        No futuro, trocará a API pelo 'self.db.update_product_price()'.
        """
        # Obter código do produto local ou via API
        product_code = self.api.get_product_code_by_ean(ean)

        if not product_code:
            message = f"Processamento cancelado: Código do produto não encontrado para EAN {ean}"
            logger.warning(message)
            log_event_to_file(message, level=logging.WARNING)
            return False

        # Verifica e limpa o payload do N8N seguindo a regra original
        processed_payload = self._apply_business_rules(raw_payload, product_code, ean)

        if not processed_payload:
             message = f"Falha ao processar as regras de negócio para EAN {ean}"
             logger.error(message)
             log_event_to_file(message, level=logging.ERROR)
             return False

        # Montar estrutura final consumida pela API
        final_payload = {"produto": processed_payload}

        logger.info(f"Tentando atualizar o produto EAN {ean} via API")
        
        # O N8N já envia o payload total com as alterações (incluído o novo preço)
        success = self.api.update_product(final_payload)
        
        # Futuro handler via DB seria engatilhado aqui:
        # success = self.db.update_product_price(ean, processed_payload.get('preco'))

        if success:
            msg = f"Sucesso na atualização de preço do EAN {ean} - payload processado: {processed_payload}"
            logger.info(msg)
            log_event_to_file(msg, level=logging.INFO)
            return True
        else:
             msg = f"Falha na atualização de preço via API para EAN {ean}."
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
