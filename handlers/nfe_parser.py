import xml.etree.ElementTree as ET
import random
import string
from datetime import datetime
from typing import Dict, List, Optional
import logging

class NFeParser:
    """
    Handler especializado em realizar o parsing de XMLs de NF-e (v4.00)
    Garantindo 100% de paridade com a lógica do process_xml_files.py.
    """
    
    NAMESPACE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
    
    @staticmethod
    def _generate_random_id(length: int = 7) -> str:
        """Gera um ID numérico aleatório conforme padrão legado."""
        return ''.join(random.choices(string.digits, k=length))

    @staticmethod
    def _get_text(element: ET.Element, path: str, namespaces: Dict = None) -> Optional[str]:
        """Helper para extrair texto de um elemento com segurança."""
        if element is None:
            return None
        found = element.find(path, namespaces or NFeParser.NAMESPACE)
        return found.text if found is not None else None

    def parse_summary(self, xml_content: str) -> Dict:
        """
        Extrai informações de cabeçalho da NFe para a tabela nfes_informations.
        Implementação fiel ao input_summary_nfe do script original.
        """
        root = ET.fromstring(xml_content)
        
        # Chave NFe (Atributo Id do elemento infNFe)
        infNFe = root.find('.//nfe:infNFe', self.NAMESPACE)
        chave = infNFe.attrib.get('Id', '').replace('NFe', '') if infNFe is not None else None
        
        # Emitente (Fornecedor) - get_fornecedor
        emit = root.find('.//nfe:emit', self.NAMESPACE)
        cnpj_fornecedor = self._get_text(emit, 'nfe:CNPJ') or self._get_text(emit, 'nfe:CPF')
        nome_fornecedor = self._get_text(emit, 'nfe:xNome')
        fantasia_fornecedor = self._get_text(emit, 'nfe:xFant') or ""
        cep_fornecedor = self._get_text(emit, './/nfe:CEP')
        
        # Destinatário (Cliente) - get_cliente
        dest = root.find('.//nfe:dest', self.NAMESPACE)
        cnpj_cliente = self._get_text(dest, 'nfe:CNPJ')
        
        # Valores (get_value_nota)
        # Prioriza informações de faturamento (fat) se disponíveis
        fat = root.find('.//nfe:cobr/nfe:fat', self.NAMESPACE)
        v_orig = self._get_text(fat, 'nfe:vOrig')
        v_desc = self._get_text(fat, 'nfe:vDesc')
        v_liq = self._get_text(fat, 'nfe:vLiq')
        
        # Fallback para totais da nota se fatura estiver ausente
        if v_orig is None:
            total = root.find('.//nfe:total/nfe:ICMSTot', self.NAMESPACE)
            v_orig = self._get_text(total, 'nfe:vNF')
        
        # Pagamento (vPag)
        pag = root.find('.//nfe:pag/nfe:detPag', self.NAMESPACE)
        v_pag = self._get_text(pag, 'nfe:vPag')
        
        # Duplicata (get_pagamentos) - Pega a primeira duplicata encontrada
        dup = root.find('.//nfe:cobr/nfe:dup', self.NAMESPACE)
        d_venc = self._get_text(dup, 'nfe:dVenc')
        v_dup = self._get_text(dup, 'nfe:vDup')

        return {
            'id': self._generate_random_id(),
            'data_registro': datetime.now().strftime('%Y-%m-%d'),
            'chave_nfe': chave,
            'CPNJ_FORNECEDOR': cnpj_fornecedor,
            'NOME_FORNECEDOR': nome_fornecedor,
            'NOME_FANTASIA_FORNECEDOR': fantasia_fornecedor,
            'CEP_FORNECEDOR': cep_fornecedor,
            'CNPJ_CLIENTE': cnpj_cliente,
            'VALOR_TOTAL': v_orig,
            'VALOR_DESCONTO': v_desc,
            'VALOR_LIQUIDO': v_liq,
            'VALOR_PAGAMENTO': v_pag,
            'DATA_VENCIMENTO_DUP': d_venc,
            'VALOR_DUPLICADA': v_dup
        }

    def parse_items(self, xml_content: str) -> List[Dict]:
        """
        Extrai itens da NFe para a tabela precificacao.
        Implementação fiel ao input_nivel_produto do script original.
        """
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//nfe:infNFe', self.NAMESPACE)
        chave = infNFe.attrib.get('Id', '').replace('NFe', '') if infNFe is not None else None
        
        items = []
        # det_element no script original usa root.iter('{http://www.portalfiscal.inf.br/nfe}det')
        for det in root.findall('.//nfe:det', self.NAMESPACE):
            prod = det.find('nfe:prod', self.NAMESPACE)
            
            # Dados básicos do produto (prod_element)
            item = {
                'id': self._generate_random_id(),
                'chave_nfe': chave,
                'COD': self._get_text(prod, 'nfe:cProd'),
                'EAN': self._get_text(prod, 'nfe:cEAN'),
                'EAN_trib': self._get_text(prod, 'nfe:cEANTrib'),
                'NCM': self._get_text(prod, 'nfe:NCM'),
                'CEST': self._get_text(prod, 'nfe:CEST'),
                'CFOP': self._get_text(prod, 'nfe:CFOP'),
                'SALE_TYPE': self._get_text(prod, 'nfe:uCom'),
                'VALOR_UN_trib': self._get_text(prod, 'nfe:vUnTrib'),
                'DESCRICAO': self._get_text(prod, 'nfe:xProd'),
                'QUANTIDADE': float(self._get_text(prod, 'nfe:qCom') or 0),
                'VALOR_TOTAL': float(self._get_text(prod, 'nfe:vProd') or 0),
                'VALOR_DESCONTO': float(self._get_text(prod, 'nfe:vDesc') or 0),
                'quantidade_tributada': self._get_text(prod, 'nfe:qTrib'),
                'unidade_tributada': self._get_text(prod, 'nfe:uTrib'),
            }
            
            # Impostos (imposto_element)
            imposto = det.find('nfe:imposto', self.NAMESPACE)
            
            # ICMS (get_imposto_icms dinâmico)
            # A lógica original percorre os filhos de nfe:ICMS e busca subtags como ICMS00, ICMS10, etc.
            icms_container = imposto.find('nfe:ICMS', self.NAMESPACE)
            if icms_container is not None:
                # O script busca por subtags que começam com ICMS (ex: ICMS00)
                for icms_xx in icms_container:
                    # Itera sobre os elementos dentro do ICMSxx (ex: vBC, pICMS)
                    for field in icms_xx:
                        tag = field.tag.split('}')[1] if '}' in field.tag else field.tag
                        if tag == 'CST' or tag == 'CSOSN':
                            item['CST_ICMS'] = field.text
                        elif tag == 'vBC':
                            item['BASE_CALCULO_ICMS'] = float(field.text or 0)
                        elif tag == 'pICMS':
                            item['ALIQ_ICMS'] = float(field.text or 0)
                        elif tag == 'vICMS':
                            item['VALOR_ICMS'] = float(field.text or 0)
                        elif tag == 'vBCST':
                            item['VB_ICMS_ST'] = float(field.text or 0)
                        elif tag == 'pICMSST':
                            item['ICMS_ST'] = float(field.text or 0)
                        elif tag == 'vICMSST':
                            item['V_ICMS_ST'] = float(field.text or 0)
            
            # IPI (get_imposto_ipi)
            ipi_trib = imposto.find('.//nfe:IPI/nfe:IPITrib', self.NAMESPACE)
            if ipi_trib is not None:
                item['CST_IPI'] = self._get_text(ipi_trib, 'nfe:CST')
                item['BASE_CALCULO_IPI'] = float(self._get_text(ipi_trib, 'nfe:vBC') or 0)
                item['ALIQ_IPI'] = float(self._get_text(ipi_trib, 'nfe:pIPI') or 0)
                item['VALOR_IPI'] = float(self._get_text(ipi_trib, 'nfe:vIPI') or 0)
            
            # PIS (get_imposto_pis)
            pis_aliq = imposto.find('.//nfe:PIS/nfe:PISAliq', self.NAMESPACE) or imposto.find('.//nfe:PIS/nfe:PISOutr', self.NAMESPACE)
            if pis_aliq is not None:
                item['CST_PIS'] = self._get_text(pis_aliq, 'nfe:CST')
                item['BASE_CALCULO_PIS'] = float(self._get_text(pis_aliq, 'nfe:vBC') or 0)
                item['ALIQ_PIS'] = float(self._get_text(pis_aliq, 'nfe:pPIS') or 0)
                item['VALOR_PIS'] = float(self._get_text(pis_aliq, 'nfe:vPIS') or 0)
                
            # COFINS (get_imposto_cofins)
            cofins_aliq = imposto.find('.//nfe:COFINS/nfe:COFINSAliq', self.NAMESPACE) or imposto.find('.//nfe:COFINS/nfe:COFINSOutr', self.NAMESPACE)
            if cofins_aliq is not None:
                item['CST_COFINS'] = self._get_text(cofins_aliq, 'nfe:CST')
                item['BASE_CALCULO_COFINS'] = float(self._get_text(cofins_aliq, 'nfe:vBC') or 0)
                item['ALIQ_COFINS'] = float(self._get_text(cofins_aliq, 'nfe:pCOFINS') or 0)
                item['VALOR_COFINS'] = float(self._get_text(cofins_aliq, 'nfe:vCOFINS') or 0)

            # Cálculos finais de Negócio (df_report no script original)
            # PRECO_COMPRA = VALOR_TOTAL + VALOR_IPI + V_ICMS_ST
            item['PRECO_COMPRA'] = item['VALOR_TOTAL'] + item.get('VALOR_IPI', 0) + item.get('V_ICMS_ST', 0)
            
            # PRECO_MIN = (PRECO_COMPRA * 1.15) / QUANTIDADE (round 2)
            if item['QUANTIDADE'] > 0:
                item['PRECO_MIN'] = round((item['PRECO_COMPRA'] * 1.15) / item['QUANTIDADE'], 2)
            else:
                item['PRECO_MIN'] = 0
            
            # RESPONSE (vê se é UN)
            item['RESPONSE'] = 'Ok' if item['SALE_TYPE'] == 'UN' else 'Atenção: revisar o preço mínimo calculado'
            
            # Outros campos extras
            item['qtd_embalagem'] = None
            
            items.append(item)
            
        return items
