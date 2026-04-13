import os
import gzip
import base64
import tempfile
import requests
import logging
import urllib3
from lxml import etree
from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from typing import Dict, Optional

# Suppress insecure request warnings for SEFAZ mTLS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NFeHandler:
    """
    Handler modularizado para comunicação com a SEFAZ (NFeDistribuicaoDFe).
    Gerencia mTLS com certificado A1 (.pfx) e download de XMLs completos.
    """
    def __init__(self, pfx_path: str, pfx_senha: str, cnpj_interessado: str):
        self.pfx_path = pfx_path
        self.pfx_senha = pfx_senha
        self.cnpj_interessado = cnpj_interessado
        self.url_distribuicao = "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx"
        self.logger = logging.getLogger("nfe_handler")

    def _extrair_pem_do_pfx(self) -> tuple[str, str]:
        """Extrai chave privada e certificado do .pfx para uso mTLS."""
        with open(self.pfx_path, "rb") as f:
            pfx_data = f.read()

        chave_privada, certificado, _ = pkcs12.load_key_and_certificates(
            pfx_data,
            self.pfx_senha.encode("utf-8"),
            backend=default_backend()
        )

        cert_pem = certificado.public_bytes(Encoding.PEM)
        key_pem = chave_privada.private_bytes(
            Encoding.PEM,
            PrivateFormat.TraditionalOpenSSL,
            NoEncryption()
        )

        tmp_cert = tempfile.NamedTemporaryFile(delete=False, suffix="_cert.pem")
        tmp_key = tempfile.NamedTemporaryFile(delete=False, suffix="_key.pem")

        tmp_cert.write(cert_pem)
        tmp_key.write(key_pem)
        tmp_cert.close()
        tmp_key.close()

        return tmp_cert.name, tmp_key.name

    def _montar_envelope_dist_dfe(self, cnpj: str, cuf_autor: str, chave_nfe: str) -> str:
        """Monta o envelope SOAP para NFeDistribuicaoDFe por chave."""
        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
            'xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">'
            '<soap12:Header>'
            '<nfeCabecMsg xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">'
            f'<cUF>{cuf_autor}</cUF>'
            '<versaoDados>1.01</versaoDados>'
            '</nfeCabecMsg>'
            '</soap12:Header>'
            '<soap12:Body>'
            '<nfeDistDFeInteresse xmlns="http://www.portalfiscal.inf.br/nfe/wsdl/NFeDistribuicaoDFe">'
            '<nfeDadosMsg>'
            '<distDFeInt xmlns="http://www.portalfiscal.inf.br/nfe" versao="1.01">'
            f'<tpAmb>1</tpAmb>'
            f'<cUFAutor>{cuf_autor}</cUFAutor>'
            f'<CNPJ>{cnpj}</CNPJ>'
            '<consChNFe>'
            f'<chNFe>{chave_nfe}</chNFe>'
            '</consChNFe>'
            '</distDFeInt>'
            '</nfeDadosMsg>'
            '</nfeDistDFeInteresse>'
            '</soap12:Body>'
            '</soap12:Envelope>'
        )

    def _descompactar_doc_zip(self, doc_zip_b64: str) -> str:
        """Descompacta base64+gzip da SEFAZ."""
        compressed = base64.b64decode(doc_zip_b64)
        return gzip.decompress(compressed).decode("utf-8")

    def download_xml(self, chave_nfe: str) -> Optional[str]:
        """Tenta baixar o XML completo da NF-e via SEFAZ."""
        cert_pem, key_pem = None, None
        try:
            cuf_autor = chave_nfe[0:2]
            cert_pem, key_pem = self._extrair_pem_do_pfx()
            envelope = self._montar_envelope_dist_dfe(self.cnpj_interessado, cuf_autor, chave_nfe)
            
            headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
            
            response = requests.post(
                self.url_distribuicao,
                data=envelope.encode("utf-8"),
                headers=headers,
                cert=(cert_pem, key_pem),
                timeout=30,
                verify=False
            )

            if response.status_code != 200:
                self.logger.error(f"Erro HTTP {response.status_code} para chave {chave_nfe}")
                return None

            root = etree.fromstring(response.content)
            ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
            ret = root.find(".//nfe:retDistDFeInt", ns)

            if ret is not None:
                cStat = ret.findtext("nfe:cStat", namespaces=ns)
                if cStat == "138":
                    lote = ret.find("nfe:loteDistDFeInt", ns)
                    docs = lote.findall("nfe:docZip", ns)
                    for doc in docs:
                        if "procNFe" in doc.get("schema", ""):
                            return self._descompactar_doc_zip(doc.text)
            
            self.logger.warning(f"XML não localizado na SEFAZ: {chave_nfe} (cStat {cStat if ret is not None else '??'})")
            return None

        except Exception as e:
            self.logger.error(f"Erro fatal no download {chave_nfe}: {str(e)}")
            return None
        finally:
            if cert_pem and os.path.exists(cert_pem): os.unlink(cert_pem)
            if key_pem and os.path.exists(key_pem): os.unlink(key_pem)
