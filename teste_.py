"""
Download do XML completo de NF-e via NFeDistribuicaoDFe (Ambiente Nacional)
Autenticação via mTLS com certificado digital A1 (.pfx)

O serviço NFeDistribuicaoDFe é o ÚNICO que retorna o XML completo da nota.
O NFeConsultaProtocolo retorna apenas o protocolo de autorização.

Dependências:
    pip install requests lxml cryptography
"""

import os
import gzip
import base64
import tempfile
import requests
from lxml import etree
from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend


# ─── CONFIGURAÇÕES ──────────────────────────────────────────────────────────────

PFX_PATH = "/Users/edivaldo/Library/CloudStorage/GoogleDrive-edivaldo414@gmail.com/My Drive/git_personal/integration-uniplus-erp/CERTIFICADO MERCADO POPULAR.pfx"
PFX_SENHA = "Mecont292"
CHAVE_NFE = "26260324150377000195550010059140681363282161"

# CNPJ do CERTIFICADO DIGITAL (destinatário/comprador)
# NÃO é o CNPJ da chave (que é do emissor/fornecedor)
CNPJ = "08935303000108"

# cUF do autor (posições 0-1 da chave = UF do emissor)
CUF_AUTOR = CHAVE_NFE[0:2]

# Ambiente Nacional - Produção (AN)
URL_DISTRIBUICAO = "https://www1.nfe.fazenda.gov.br/NFeDistribuicaoDFe/NFeDistribuicaoDFe.asmx"

# ────────────────────────────────────────────────────────────────────────────────


def extrair_pem_do_pfx(pfx_path: str, senha: str) -> tuple[str, str]:
    """
    Extrai chave privada e certificado do .pfx e salva como arquivos .pem temporários.
    Retorna (caminho_cert_pem, caminho_key_pem).
    """
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    chave_privada, certificado, _ = pkcs12.load_key_and_certificates(
        pfx_data,
        senha.encode("utf-8"),
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


def montar_envelope_dist_dfe(cnpj: str, cuf_autor: str, chave_nfe: str) -> str:
    """
    Monta o envelope SOAP para NFeDistribuicaoDFe - consulta por chave.
    """
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


def descompactar_doc_zip(doc_zip_b64: str) -> str:
    """Descompacta o conteúdo base64+gzip retornado pela SEFAZ."""
    compressed = base64.b64decode(doc_zip_b64)
    return gzip.decompress(compressed).decode("utf-8")


def download_xml_nfe(pfx_path: str, pfx_senha: str, chave_nfe: str, cnpj: str, cuf_autor: str) -> dict:
    """
    Baixa o XML completo da NF-e via NFeDistribuicaoDFe.
    """
    cert_pem, key_pem = None, None

    try:
        # 1. Extrai certificado
        cert_pem, key_pem = extrair_pem_do_pfx(pfx_path, pfx_senha)

        # 2. Monta envelope SOAP
        envelope = montar_envelope_dist_dfe(cnpj, cuf_autor, chave_nfe)

        # 3. Requisição com mTLS
        headers = {
            "Content-Type": "application/soap+xml; charset=utf-8",
        }

        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        print(f"📡 Enviando requisição para: {URL_DISTRIBUICAO}")
        
        response = requests.post(
            URL_DISTRIBUICAO,
            data=envelope.encode("utf-8"),
            headers=headers,
            cert=(cert_pem, key_pem),
            timeout=60,
            verify=False
        )

        print(f"📥 HTTP Status: {response.status_code}")

        if response.status_code != 200:
            return {
                "sucesso": False,
                "erro": f"HTTP Error {response.status_code}",
                "resposta_bruta": response.text
            }

        # 4. Parse da resposta
        root = etree.fromstring(response.content)

        ns = {
            "soap": "http://www.w3.org/2003/05/soap-envelope",
            "nfe": "http://www.portalfiscal.inf.br/nfe"
        }

        # Busca o retDistDFeInt
        ret = root.find(".//nfe:retDistDFeInt", ns)

        if ret is None:
            return {
                "sucesso": False,
                "erro": "retDistDFeInt não encontrado na resposta",
                "resposta_bruta": response.text
            }

        cStat = ret.findtext("nfe:cStat", namespaces=ns)
        xMotivo = ret.findtext("nfe:xMotivo", namespaces=ns)

        print(f"📋 cStat: {cStat} - {xMotivo}")

        resultado = {
            "sucesso": False,
            "cStat": cStat,
            "xMotivo": xMotivo,
            "xmls": [],
            "resposta_bruta": response.text
        }

        # cStat 138 = Documento(s) localizado(s)
        if cStat == "138":
            resultado["sucesso"] = True

            # Busca os lotes de documentos
            lote = ret.find("nfe:loteDistDFeInt", ns)
            if lote is not None:
                docs = lote.findall("nfe:docZip", ns)
                print(f"📦 {len(docs)} documento(s) encontrado(s)")

                for i, doc in enumerate(docs):
                    schema = doc.get("schema", "desconhecido")
                    nsu = doc.get("NSU", "?")
                    
                    # O conteúdo é base64 de um gzip
                    doc_b64 = doc.text
                    if doc_b64:
                        try:
                            xml_descompactado = descompactar_doc_zip(doc_b64)

                            # Formata o XML com indentação para legibilidade
                            try:
                                from xml.dom.minidom import parseString
                                dom = parseString(xml_descompactado)
                                xml_formatado = dom.toprettyxml(indent="  ", encoding=None)
                                # Remove linhas em branco extras geradas pelo minidom
                                lines = [line for line in xml_formatado.split('\n') if line.strip()]
                                xml_formatado = '\n'.join(lines) + '\n'
                            except Exception:
                                xml_formatado = xml_descompactado  # fallback: salva sem formatar

                            resultado["xmls"].append({
                                "nsu": nsu,
                                "schema": schema,
                                "xml": xml_formatado
                            })

                            # Determina o tipo de arquivo pelo schema
                            if "procNFe" in schema:
                                sufixo = "procNFe"
                            elif "resNFe" in schema:
                                sufixo = "resNFe"
                            elif "resEvento" in schema:
                                sufixo = "resEvento"
                            elif "procEventoNFe" in schema:
                                sufixo = "procEventoNFe"
                            else:
                                sufixo = f"doc{i}"

                            nome_arquivo = f"nfe_{chave_nfe}_{sufixo}.xml"
                            with open(nome_arquivo, "w", encoding="utf-8") as f:
                                f.write(xml_formatado)
                            print(f"  ✅ [{sufixo}] NSU={nsu} → salvo em: {nome_arquivo}")

                        except Exception as e:
                            print(f"  ⚠️  Erro ao descompactar doc NSU={nsu}: {e}")

        return resultado

    finally:
        if cert_pem and os.path.exists(cert_pem):
            os.unlink(cert_pem)
        if key_pem and os.path.exists(key_pem):
            os.unlink(key_pem)


# ─── EXECUÇÃO ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"🔍 Consultando XML completo da NF-e")
    print(f"   Chave: {CHAVE_NFE}")
    print(f"   CNPJ:  {CNPJ}")
    print(f"   UF:    {CUF_AUTOR} (PE)")
    print(f"📜 Certificado: {PFX_PATH}")
    print(f"🌐 Serviço: NFeDistribuicaoDFe (Ambiente Nacional)\n")

    resultado = download_xml_nfe(PFX_PATH, PFX_SENHA, CHAVE_NFE, CNPJ, CUF_AUTOR)

    if resultado["sucesso"]:
        print(f"\n{'='*60}")
        print(f"✅ Sucesso! {len(resultado['xmls'])} documento(s) baixado(s)")
        print(f"{'='*60}")

        for doc in resultado["xmls"]:
            print(f"\n📄 Schema: {doc['schema']} | NSU: {doc['nsu']}")
            print(f"   Tamanho: {len(doc['xml'])} bytes")
            # Mostra preview do XML
            preview = doc["xml"][:500]
            print(f"   Preview:\n{preview}...")
    else:
        print(f"\n{'='*60}")
        print(f"❌ Falha na consulta")
        print(f"   cStat:   {resultado.get('cStat')}")
        print(f"   xMotivo: {resultado.get('xMotivo') or resultado.get('erro')}")
        print(f"{'='*60}")
        if resultado.get("resposta_bruta"):
            print(f"\n--- Resposta bruta (primeiros 1000 chars) ---")
            print(resultado["resposta_bruta"][:1000])