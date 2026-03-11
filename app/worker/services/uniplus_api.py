import requests
import base64
import time
import logging
from urllib3.exceptions import InsecureRequestWarning
from app.worker.config import config
from app.worker.utils.logger import get_logger, log_event_to_file

# Suppress insecure request warnings
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = get_logger()

class UniplusAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False # Replicando a falta de verify=False no script antigo
        
        self.access_token = None
        self.token_expires_at = 0

    def _get_auth_header(self):
        credentials = f"{config.UNIPLUS_API_CLIENT_ID}:{config.UNIPLUS_API_CLIENT_SECRET}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"

    def get_token(self):
        """Obtém token, reaproveitando se não estiver expirado."""
        if self.access_token and time.time() < self.token_expires_at:
            return self.access_token

        endpoint = f"{config.UNIPLUS_API_BASE_URL}/oauth/token"
        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {"grant_type": "client_credentials"}

        try:
            # Timeout set to avoid hanging forever
            response = self.session.post(endpoint, headers=headers, data=data, timeout=10)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            
            # Renovação segura: consideramos que expira alguns segundos antes
            self.token_expires_at = time.time() + expires_in - 60
            
            logger.info("Novo token de acesso Uniplus gerado/renovado.")
            return self.access_token

        except Exception as e:
            logger.error(f"Falha ao gerar o token de acesso: {str(e)}")
            log_event_to_file(f"Erro Crítico: Falha ao gerar o token de acesso à Uniplus -> {str(e)}", level=logging.ERROR)
            return None

    def get_product_code_by_ean(self, ean: str):
        """Consulta o endpoint de EANs e obtém o código do produto."""
        token = self.get_token()
        if not token:
            return None

        endpoint = f"{config.UNIPLUS_API_BASE_URL}/public-api/v1/eans/{ean}"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = self.session.get(endpoint, headers=headers, timeout=10)
            if response.status_code == 200:
                produto_data = response.json()
                codigo = produto_data.get('produto')
                logger.info(f"Obtido código do produto para o EAN {ean}: {codigo}")
                return codigo
            else:
                logger.error(f"Falha ao obter o código do produto para o EAN {ean}. Status: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Erro na requisição EAN -> {ean}: {str(e)}")
            return None

    def update_product(self, payload: dict):
        """Envia requisição PUT para atualizar o produto completo."""
        token = self.get_token()
        if not token:
            return False

        endpoint = f"{config.UNIPLUS_API_BASE_URL}/public-api/v1/produtos"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            # Aqui 'payload' já deve vir formatado corretamente ex: {"produto": {...}}
            response = self.session.put(endpoint, headers=headers, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"Produto atualizado com sucesso. Endpoint: {endpoint}")
                return True
            else:
                logger.error(f"Falha ao atualizar o produto. Status code: {response.status_code} Resposta: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Erro ao realizar a requisição PUT na Uniplus: {str(e)}")
            return False
