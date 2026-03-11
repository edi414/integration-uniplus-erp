import json
import logging
from app.worker.services.product_service import ProductService
from app.worker.utils.logger import get_logger, log_event_to_file

logger = get_logger()

# Inicializa o serviço uma vez para reutilizar conexões API
product_service = ProductService()

def handle_price_update(event_data: dict):
    """
    Interpreta o evento 'update_price' consumido do Redis
    e repassa para o Service processá-lo.
    """
    logger.info(f"Produto recebido na fila! Payload: {json.dumps(event_data, indent=2)}")
    
    # Validações básicas de formato
    ean = event_data.get("ean")
    
    if not ean:
        msg = f"Evento descartado: Faltou enviar a chave 'ean' no payload. Dado puro: {event_data}"
        logger.error(msg)
        log_event_to_file(msg, level=logging.ERROR)
        return False
        
    log_event_to_file(f"Iniciando processamento para EAN: {ean}")
    
    # Consideramos que o n8n vai mandar o corpo do produto todo dentro do evento
    # Se ele mandar as infos na raiz, o próprio payload é a base.
    # Ex: { "type": "update_price", "ean": "123", "preco": 10.0, "nome": "abacate"... }
    
    # Repassa pro service lidar com lógica
    success = product_service.process_price_update(ean, raw_payload=event_data)
    
    if success:
        log_event_to_file(f"EAN {ean} processado integralmente com SUCESSO. Fim de evento.")
    else:
        log_event_to_file(f"EAN {ean} falhou durante processamento.")
        
    return success
