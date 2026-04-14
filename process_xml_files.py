import pandas as pd
import xml.etree.ElementTree as ET
import sqlite3
import time
import os
import shutil
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import psycopg2
import random
import string

# Pasta de origem dos arquivos .xml
source_folder = r"G:\Meu Drive"
dest_folder = r"G:\Meu Drive\Reports\Notas_fiscais\xmls\leitura_summary"

## Função que processa os XML a nível NFE

def input_summary_nfe(xml_file):

    def existing_nfe_bd(chave_nfe):
        # Conectar ao banco PostgreSQL
        postgres_conn = psycopg2.connect(
            "postgresql://mercado:26829441ed@35.188.206.246:5432/banco_mercado"
        )
        postgres_cursor = postgres_conn.cursor()

        # Buscar os dados existentes no banco PostgreSQL
        print(f"SELECT chave_nfe FROM nfes_informations where chave_nfe = '{chave_nfe}'")
        postgres_cursor.execute(f"SELECT chave_nfe FROM nfes_informations WHERE chave_nfe = '{chave_nfe}'")
        existing_rows = postgres_cursor.fetchall()
        df_postgres = pd.DataFrame(existing_rows)

        if df_postgres.empty:
            return True
        else:
            return False

    def obter_id_xml(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Define o namespace
        namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

        # Obtém o elemento infNFe
        infNFe_element = root.find('nfe:NFe/nfe:infNFe', namespace)

        if infNFe_element is not None:
            # Obtém o valor do atributo Id
            id_xml = infNFe_element.attrib.get('Id')
            id_xml = str(id_xml).replace('NFe', '')
            return id_xml
        else:
            return None

    def get_value_nota(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        values = []

        # Encontre o elemento vNF que contém o valor total
        valor_total_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}cobr/{http://www.portalfiscal.inf.br/nfe}fat/{http://www.portalfiscal.inf.br/nfe}vOrig")
        if valor_total_element is not None:
            valor_total = valor_total_element.text
            values.append(valor_total)
        else:
            values.append(valor_total_element)
        valor_discount_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}cobr/{http://www.portalfiscal.inf.br/nfe}fat/{http://www.portalfiscal.inf.br/nfe}vDesc")
        if valor_discount_element is not None:
            valor_discount = valor_discount_element.text
            values.append(valor_discount)
        else:
            values.append(valor_discount_element)
        valor_liquido_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}cobr/{http://www.portalfiscal.inf.br/nfe}fat/{http://www.portalfiscal.inf.br/nfe}vLiq")
        if valor_liquido_element is not None:
            valor_liq = valor_liquido_element.text
            values.append(valor_liq)
        else:
            values.append(valor_liquido_element)
        valor_pagamento_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}pag/{http://www.portalfiscal.inf.br/nfe}detPag/{http://www.portalfiscal.inf.br/nfe}vPag")
        if valor_pagamento_element is not None:
            valor_pag = valor_pagamento_element.text
            values.append(valor_pag)
        else:
            values.append(valor_pagamento_element)

        return values

    def get_fornecedor(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        values = []

        # Encontre o elemento xNome dentro do emit (fornecedor)
        fornecedor_cnpj = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}CNPJ")
        if fornecedor_cnpj is not None:
            fornecedor = fornecedor_cnpj.text
            values.append(fornecedor)
        else:
            fornecedor_cpf = root.find(
                ".//{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}CPF")
            if fornecedor_cpf is not None:
                fornecedor_cpf_v = fornecedor_cpf.text
                values.append(fornecedor_cpf_v)
            else:
                values.append(fornecedor_cpf)

        xNome_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}xNome")
        if xNome_element is not None:
            fornecedor = xNome_element.text
            values.append(fornecedor)
        else:
            values.append(xNome_element)
        nome_fantasia = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}xFant")
        if nome_fantasia is not None:
            fornecedor = nome_fantasia.text
            values.append(fornecedor)
        else:
            x = ''
            values.append(x)
        cep_fornecedor = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}emit/{http://www.portalfiscal.inf.br/nfe}enderEmit/{http://www.portalfiscal.inf.br/nfe}CEP")
        if cep_fornecedor is not None:
            fornecedor = cep_fornecedor.text
            values.append(fornecedor)
        else:
            values.append(cep_fornecedor)

        return values

    def get_cliente(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Encontre o elemento xNome dentro do dest (cliente)
        xCNPJ_element = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}dest/{http://www.portalfiscal.inf.br/nfe}CNPJ")

        if xCNPJ_element is not None:
            cliente = xCNPJ_element.text
            return cliente
        else:
            return None

    def get_pagamentos(xml_file):
        tree = ET.parse(xml_file)
        root = tree.getroot()

        values = []

        # Encontre o elemento xNome dentro do emit (fornecedor)
        data_vencimento_dup = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}cobr/{http://www.portalfiscal.inf.br/nfe}dup/{http://www.portalfiscal.inf.br/nfe}dVenc")
        if data_vencimento_dup is not None:
            data_vencimento_dup = data_vencimento_dup.text
            values.append(data_vencimento_dup)
        else:
            values.append(data_vencimento_dup)
        valor_duplicada = root.find(
            ".//{http://www.portalfiscal.inf.br/nfe}cobr/{http://www.portalfiscal.inf.br/nfe}dup/{http://www.portalfiscal.inf.br/nfe}vDup")
        if valor_duplicada is not None:
            valor_duplicada = valor_duplicada.text
            values.append(valor_duplicada)
        else:
            values.append(valor_duplicada)

        return values

    informations_nfe = {
        'chave_nfe': [obter_id_xml(xml_file)],
        'CPNJ_FORNECEDOR': [get_fornecedor(xml_file)[0]],
        'NOME_FORNECEDOR': [get_fornecedor(xml_file)[1]],
        'NOME_FANTASIA_FORNECEDOR': [get_fornecedor(xml_file)[2]],
        'CEP_FORNECEDOR': [get_fornecedor(xml_file)[3]],
        'CNPJ_CLIENTE': [get_cliente(xml_file)],
        'VALOR_TOTAL': [get_value_nota(xml_file)[0]],
        'VALOR_DESCONTO': [get_value_nota(xml_file)[1]],
        'VALOR_LIQUIDO': [get_value_nota(xml_file)[2]],
        'VALOR_PAGAMENTO': [get_value_nota(xml_file)[3]],
        'DATA_VENCIMENTO_DUP': [get_pagamentos(xml_file)[0]],
        'VALOR_DUPLICADA': [get_pagamentos(xml_file)[1]]
    }

    informations_nfe_df = pd.DataFrame(informations_nfe)
    url_database = "postgresql://mercado:26829441ed@35.188.206.246:5432/banco_mercado"
    table_name = "nfes_informations"
    columns_deep = [
        'id',
        'data_registro',
        'chave_nfe',
        'CPNJ_FORNECEDOR',
        'NOME_FORNECEDOR',
        'NOME_FANTASIA_FORNECEDOR',
        'CEP_FORNECEDOR',
        'CNPJ_CLIENTE',
        'VALOR_TOTAL',
        'VALOR_DESCONTO',
        'VALOR_LIQUIDO',
        'VALOR_PAGAMENTO',
        'DATA_VENCIMENTO_DUP',
        'VALOR_DUPLICADA'
    ]

    current_date = datetime.now().strftime('%Y-%m-%d')
    informations_nfe_df['data_registro'] = current_date

    def generate_random_id(length=7):
        return ''.join(random.choices(string.digits, k=length))

    informations_nfe_df['id'] = [generate_random_id() for _ in range(len(informations_nfe_df))]
    informations_nfe_df = informations_nfe_df.reindex(columns=columns_deep)

    chave_nfe = informations_nfe_df['chave_nfe'][0]
    print(chave_nfe)

    if existing_nfe_bd(chave_nfe):
        try:
            conn = psycopg2.connect(url_database)
            print("Conexão ao banco de dados PostgreSQL bem sucedida.")
            cur = conn.cursor()

            # Insira linha por linha
            total_rows = len(informations_nfe_df)
            print(f'Número de linhas no dataframe de information_NFE: {total_rows}')
            print(f'Dataframe, information_nfe_df: {informations_nfe_df}')
            with tqdm(total=total_rows, desc="Inserindo linhas no banco de dados") as pbar:
                for index, row in informations_nfe_df.iterrows():
                    row_data = [row[column] for column in columns_deep]
                    cur.execute(
                        f"INSERT INTO {table_name} ({', '.join(columns_deep)}) VALUES ({', '.join(['%s'] * len(columns_deep))})",
                        row_data
                    )
                    conn.commit()
                    pbar.update(1)

            print('Inserção no banco de dados finalizada.')

        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao trabalhar com o banco de dados PostgreSQL:", error)

        finally:
            if conn is not None:
                conn.close()

    else:
        print(f'A chave_nfe: {chave_nfe}, já foi processada.')

    # # importação no banco
    # print('Começando a importação no banco')
    # conn = sqlite3.connect(r"G:\Meu Drive\Database\relatorios_uniplus.sqlite")
    # informations_nfe_df.to_sql('deep_notas_fiscais', conn, if_exists='append', index=False)
    # print('Importação no banco finalizada')
    # conn.close()

## Função que processa os XML a nível de produto

def input_nivel_produto(xml_file):
    def validar_versao(xml_file):
        # Faz a leitura do arquivo XML
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Define o namespace
        namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

        # Verifica a versão do XML
        if root.attrib.get('versao') == '4.00' and root.find('nfe:NFe', namespace) is not None:
            return True
        else:
            return False

    def existing_nfe_bd(chave_nfe):
        # Conectar ao banco PostgreSQL
        postgres_conn = psycopg2.connect(
            "postgresql://mercado:26829441ed@35.188.206.246:5432/banco_mercado"
        )
        postgres_cursor = postgres_conn.cursor()

        # Buscar os dados existentes no banco PostgreSQL
        postgres_cursor.execute(f"SELECT chave_nfe FROM precificacao where chave_nfe = '{chave_nfe}'")
        existing_rows = postgres_cursor.fetchall()
        df_postgres = pd.DataFrame(existing_rows)

        if df_postgres.empty:
            return True
        else:
            return False

    if validar_versao(xml_file):

        print("O XML é da versão 4.0")

        def tabulate_xml(xml_file):
            # Faz a leitura do arquivo XML
            tree = ET.parse(xml_file)
            root = tree.getroot()

            def obter_id_xml(xml_file):
                tree = ET.parse(xml_file)
                root = tree.getroot()

                # Define o namespace
                namespace = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

                # Obtém o elemento infNFe
                infNFe_element = root.find('nfe:NFe/nfe:infNFe', namespace)

                if infNFe_element is not None:
                    # Obtém o valor do atributo Id
                    id_xml = infNFe_element.attrib.get('Id')
                    return id_xml
                else:
                    return None

            # Cria uma lista para armazenar os dados dos produtos
            products = []

            # Função para obter as informações de impostos
            def get_imposto_icms(imposto_element):
                icms_data = {}  # Lista para armazenar os dados de todos os ICMSxx encontrados

                # Encontra todos os elementos ICMS dentro de imposto_element
                icms_element = imposto_element.find('.//{http://www.portalfiscal.inf.br/nfe}ICMS')
                if icms_element is not None:
                    print(f"Encontrado ICMS Element: {icms_element}")
                    for filho in icms_element:
                        if filho is not None:
                            # Remove o namespace da tag
                            tag_sem_namespace = filho.tag.split('}')[1] if '}' in filho.tag else filho.tag
                            if tag_sem_namespace is not None:
                                # Verifica se a tag do elemento filho está no formato esperado, como 'ICMS00', 'ICMS10', etc.
                                if tag_sem_namespace.startswith('ICMS') and len(tag_sem_namespace) == 6:
                                    print(f"Encontrado ICMSxx: {tag_sem_namespace}")
                                    # Dicionário para armazenar os valores do ICMSxx atual
                                    icms_xx = icms_element.find('.//{http://www.portalfiscal.inf.br/nfe}'+f'{tag_sem_namespace}')
                                    print(icms_xx)
                                    if icms_xx is not None:
                                        print(icms_xx)
                                        # Itera sobre os filhos de ICMS00
                                        for elemento_filho in icms_xx:
                                            if elemento_filho is not None:
                                            # Remove o namespace da tag
                                                tag_filho_sem_namespace = elemento_filho.tag.split('}')[
                                                    1] if '}' in elemento_filho.tag else elemento_filho.tag
                                                valor_filho = elemento_filho.text if elemento_filho.text is not None else ''
                                                print(f"Tag do filho: {tag_filho_sem_namespace}, Valor: {valor_filho}")
                                                if elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}CST':
                                                    icms_data['CST_ICMS'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}vBC':
                                                    icms_data['BASE_CALCULO_ICMS'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}pICMS':
                                                    icms_data['ALIQ_ICMS'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}vICMS':
                                                    icms_data['VALOR_ICMS'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}vBCST':
                                                    icms_data['VB_ICMS_ST'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}pICMSST':
                                                    icms_data['ICMS_ST'] = elemento_filho.text
                                                elif elemento_filho.tag == '{http://www.portalfiscal.inf.br/nfe}vICMSST':
                                                    icms_data['V_ICMS_ST'] = elemento_filho.text
                return icms_data

            def get_imposto_ipi(imposto_element):
                ipi_data = {}
                ipi_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}IPI/{http://www.portalfiscal.inf.br/nfe}IPITrib')

                if ipi_element is not None:
                    for child in ipi_element:
                        tag = child.tag
                        if tag == '{http://www.portalfiscal.inf.br/nfe}CST':
                            ipi_data['CST_IPI'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vBC':
                            ipi_data['BASE_CALCULO_IPI'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}pIPI':
                            ipi_data['ALIQ_IPI'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vIPI':
                            ipi_data['VALOR_IPI'] = child.text

                return ipi_data

            def get_imposto_pis(imposto_element):
                pis_data = {}
                pis_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}PIS/{http://www.portalfiscal.inf.br/nfe}PISAliq')

                if pis_element is not None:
                    for child in pis_element:
                        tag = child.tag
                        if tag == '{http://www.portalfiscal.inf.br/nfe}CST':
                            pis_data['CST_PIS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vBC':
                            pis_data['BASE_CALCULO_PIS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}pPIS':
                            pis_data['ALIQ_PIS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vPIS':
                            pis_data['VALOR_PIS'] = child.text

                return pis_data

            def get_imposto_cofins(imposto_element):
                cofins_data = {}
                cofins_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}COFINS/{http://www.portalfiscal.inf.br/nfe}COFINSAliq')

                if cofins_element is not None:
                    for child in cofins_element:
                        tag = child.tag
                        if tag == '{http://www.portalfiscal.inf.br/nfe}CST':
                            cofins_data['CST_COFINS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vBC':
                            cofins_data['BASE_CALCULO_COFINS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}pCOFINS':
                            cofins_data['ALIQ_COFINS'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vCOFINS':
                            cofins_data['VALOR_COFINS'] = child.text

                return cofins_data

            # Itera sobre os elementos de <det> e tabula os produtos
            for det_element in root.iter('{http://www.portalfiscal.inf.br/nfe}det'):
                product_data = {}
                print(det_element)

                # Obtém as informações do produto
                prod_element = det_element.find('{http://www.portalfiscal.inf.br/nfe}prod')
                if prod_element is not None:
                    for child in prod_element:
                        tag = child.tag
                        if tag == '{http://www.portalfiscal.inf.br/nfe}cProd':
                            product_data['COD'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}cEAN':
                            product_data['EAN'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}cEANTrib':
                            product_data['EAN_trib'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}NCM':
                            product_data['NCM'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}CEST':
                            product_data['CEST'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}CFOP':
                            product_data['CFOP'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}uCom':
                            product_data['SALE_TYPE'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vUnTrib':
                            product_data['VALOR_UN_trib'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}xProd':
                            product_data['DESCRICAO'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}qCom':
                            product_data['QUANTIDADE'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vProd':
                            product_data['VALOR_TOTAL'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}vDesc':
                            product_data['VALOR_DESCONTO'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}qTrib':
                            product_data['quantidade_tributada'] = child.text
                        elif tag == '{http://www.portalfiscal.inf.br/nfe}uTrib':
                            product_data['unidade_tributada'] = child.text

                # Obtém as informações de impostos
                imposto_element = det_element.find('{http://www.portalfiscal.inf.br/nfe}imposto')
                icms_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}ICMS')
                ipi_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}IPI/{http://www.portalfiscal.inf.br/nfe}IPITrib')
                pis_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}PIS/{http://www.portalfiscal.inf.br/nfe}PISAliq')
                cofins_element = imposto_element.find(
                    './/{http://www.portalfiscal.inf.br/nfe}COFINS/{http://www.portalfiscal.inf.br/nfe}COFINSAliq')

                if imposto_element is not None:
                    if icms_element is not None:
                        print('iniciando extração do ICMS')
                        product_data.update(get_imposto_icms(imposto_element))
                        print('finalizando extração do ICMS')
                    if ipi_element is not None:
                        product_data.update(get_imposto_ipi(imposto_element))
                    if pis_element is not None:
                        product_data.update(get_imposto_pis(imposto_element))
                    if cofins_element is not None:
                        product_data.update(get_imposto_cofins(imposto_element))

                xml = obter_id_xml(xml_file)
                product_data['chave_nfe'] = str(xml).replace('NFe', '')
                # Adiciona os dados do produto à lista de produtos
                products.append(product_data)
                print(product_data)

            # Cria um DataFrame com os dados dos produtos
            df = pd.DataFrame(products)
            return df

        df_produtos = tabulate_xml(xml_file)

        print('Tratamento de dados iniciado')

        print(df_produtos.columns)

        #def tratamento_

        # tratamento dos dados
        colunas = [
            'QUANTIDADE',
            'VALOR_TOTAL',
            'VALOR_UN_trib',
            'BASE_CALCULO_ICMS',
            'ALIQ_ICMS',
            'VALOR_ICMS',
            'VB_ICMS_ST'
            'ICMS_ST',
            'V_ICMS_ST',
            'BASE_CALCULO_IPI',
            'ALIQ_IPI',
            'VALOR_IPI',
            'BASE_CALCULO_PIS',
            'ALIQ_PIS',
            'VALOR_PIS',
            'BASE_CALCULO_COFINS',
            'ALIQ_COFINS',
            'VALOR_COFINS',
            'quantidade_tributada'
        ]

        # Itere pelas colunas desejadas
        for coluna in colunas:
            # Verifique se a coluna existe no DataFrame
            if coluna not in df_produtos.columns:
                # Se não existir, crie-a com valores vazios (NaN)
                df_produtos[
                    coluna] = pd.NA  # ou df_produtos[coluna] = None se estiver usando uma versão mais antiga do pandas

        # Converta as colunas para o tipo numérico
        df_produtos[colunas] = df_produtos[colunas].apply(pd.to_numeric)

        df_report = df_produtos
        print("Conversão de valores feita!")
        df_report['PRECO_COMPRA'] = df_report['VALOR_TOTAL'] + df_report['VALOR_IPI'] + df_report['V_ICMS_ST']
        df_report['PRECO_MIN'] = df_report['PRECO_COMPRA'] * 1.15
        df_report['PRECO_MIN'] = df_report['PRECO_MIN'] / df_report['QUANTIDADE']
        df_report['PRECO_MIN'] = df_report['PRECO_MIN'].round(decimals=2)
        df_report['RESPONSE'] = df_report['SALE_TYPE'].apply(
            lambda x: 'Ok' if x == 'UN' else 'Atenção: revisar o preço mínimo calculado')

        columns_raw_data = [
            'chave_nfe', 'EAN', 'EAN_trib', 'NCM', 'CEST', 'CFOP', 'SALE_TYPE',
            'VALOR_UN_trib', 'DESCRICAO', 'QUANTIDADE', 'VALOR_TOTAL', 'VALOR_DESCONTO',
            'quantidade_tributada', 'unidade_tributada',  # Novas colunas adicionadas
            'CST_ICMS', 'BASE_CALCULO_ICMS', 'ALIQ_ICMS', 'VALOR_ICMS',
            'VB_ICMS_ST', 'ICMS_ST', 'V_ICMS_ST', 'CST_IPI', 'ALIQ_IPI', 'VALOR_IPI',
            'CST_PIS', 'BASE_CALCULO_PIS', 'ALIQ_PIS', 'VALOR_PIS', 'CST_COFINS',
            'BASE_CALCULO_COFINS', 'ALIQ_COFINS', 'VALOR_COFINS', 'PRECO_COMPRA',
            'PRECO_MIN', 'RESPONSE'
        ]

        df_raw_data = df_report
        df_raw_data = df_raw_data.reindex(columns=columns_raw_data)

        print('Tratamento de dados finalizado')

        url_database = "postgresql://mercado:26829441ed@35.188.206.246:5432/banco_mercado"
        table_name = "precificacao"
        columns_raw_data = [
            'chave_nfe', 'EAN', 'EAN_trib', 'NCM', 'CEST', 'CFOP', 'SALE_TYPE', 'VALOR_UN_trib', 'DESCRICAO',
            'QUANTIDADE', 'VALOR_TOTAL', 'VALOR_DESCONTO', 'quantidade_tributada', 'unidade_tributada', 'CST_ICMS', 'BASE_CALCULO_ICMS', 'ALIQ_ICMS', 'VALOR_ICMS',
            'VB_ICMS_ST', 'ICMS_ST', 'V_ICMS_ST', 'CST_IPI', 'ALIQ_IPI', 'VALOR_IPI', 'CST_PIS', 'BASE_CALCULO_PIS',
            'ALIQ_PIS', 'VALOR_PIS', 'CST_COFINS', 'BASE_CALCULO_COFINS', 'ALIQ_COFINS', 'VALOR_COFINS',
            'PRECO_COMPRA', 'PRECO_MIN', 'RESPONSE', 'data_registro', 'id'
        ]

        # Adicione a coluna 'id' com uma sequência alfanumérica aleatória
        def generate_random_id(length=7):
            return ''.join(random.choices(string.digits, k=length))

        df_raw_data['id'] = [generate_random_id() for _ in range(len(df_raw_data))]
        df_raw_data['qtd_embalagem'] = None
        df_raw_data['id_sqlite'] = None

        columns_bd = [
            'id', 'DESCRICAO', 'EAN_trib', 'SALE_TYPE', 'QUANTIDADE',
            'VALOR_TOTAL', 'VALOR_DESCONTO', 'quantidade_tributada', 'unidade_tributada',  
            'V_ICMS_ST', 'VALOR_IPI', 'PRECO_COMPRA', 'PRECO_MIN', 'qtd_embalagem', 
            'chave_nfe', 'EAN', 'NCM', 'CEST', 'CFOP', 'VALOR_UN_trib', 'CST_ICMS',
            'BASE_CALCULO_ICMS', 'ALIQ_ICMS', 'VALOR_ICMS', 'VB_ICMS_ST', 'ICMS_ST',
            'CST_IPI', 'ALIQ_IPI', 'CST_PIS', 'BASE_CALCULO_PIS', 'ALIQ_PIS', 'VALOR_PIS',
            'CST_COFINS', 'BASE_CALCULO_COFINS', 'ALIQ_COFINS', 'VALOR_COFINS', 'RESPONSE'
        ]

        df_raw_data = df_raw_data[columns_bd]

        chave_nfe = df_raw_data['chave_nfe'][0]

        if existing_nfe_bd(chave_nfe):
            try:
                conn = psycopg2.connect(url_database)
                print("Conexão ao banco de dados PostgreSQL bem-sucedida.")
                cur = conn.cursor()

                # Insira linha por linha
                total_rows = len(df_raw_data)
                print(f'Número de linhas no dataframe de produtos {total_rows}')
                print(f'Dataframe, level products: {df_raw_data}')
                with tqdm(total=total_rows, desc="Inserindo linhas no banco de dados") as pbar:
                    for index, row in df_raw_data.iterrows():
                        row_data = [row[column] for column in columns_bd]
                        cur.execute(
                            f"INSERT INTO {table_name} ({', '.join(columns_bd)}) VALUES ({', '.join(['%s'] * len(columns_bd))})",
                            row_data
                        )
                        conn.commit()
                        pbar.update(1)

                print('Inserção no banco de dados finalizada.')

            except (Exception, psycopg2.DatabaseError) as error:
                print("Erro ao trabalhar com o banco de dados PostgreSQL:", error)
                raise

            finally:
                if conn is not None:
                    conn.close()
        else:
            print(f'A chave_nfe: {chave_nfe}, já foi processada e consta no banco.')


        # # importação no banco sqlite (descontinuado)
        # print('Começando a importação no banco')
        # conn = sqlite3.connect(r"G:\Meu Drive\Database\relatorios_uniplus.sqlite")
        # df_raw_data.to_sql('produtos_notas_fiscais', conn, if_exists='append', index=False)
        # print('Importação no banco finalizada')
        # conn.close()

    else:
        print("O XML não é da versão 4.0")

# Função que processa o arquivo e move para a pasta de destino
def process_and_move_file(file_path, dest_folder):
    try:
        # Chama a função para processar o arquivo
        file_path_str = str(file_path)
        input_summary_nfe(file_path_str)
        time.sleep(3)
        input_nivel_produto(file_path_str)

        # Move o arquivo processado para a pasta de output
        file_name = os.path.basename(file_path_str)
        dest_path = os.path.join(dest_folder, file_name)
        shutil.move(file_path_str, dest_path)
        print(f"Arquivo {file_name} processado com sucesso e movido para {dest_folder}")
    except Exception as e:
        print(f"Erro ao processar o arquivo {file_path}: {str(e)}")

def app_all_files():
    files = list(Path(source_folder).glob("*.xml"))
    # Processa cada arquivo encontrado
    for file_path in files:
        try:
            process_and_move_file(file_path, dest_folder)
        except Exception as e:
            print(f"Erro ao processar o arquivo {file_path}: {str(e)}")
            continue

funcoes_insert = [app_all_files]

for i in funcoes_insert:
    while True:
        try:
            i()
            print(f"Função {i.__name__} executada com sucesso.")
            break
        except Exception as e:
            print(f"Erro na função {i.__name__}: {e}")
            print(f"Tentando novamente em 2 segundos...")
            time.sleep(5)