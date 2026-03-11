import psycopg2
import requests
import json
import subprocess
from datetime import datetime
import logging

def update_prices():

    now = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

    # Configurar o logger
    logging.basicConfig(
        filename=f'alteracoes_produto_{now}_.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger()

    # Função para obter o token de acesso à API
    def obter_token_de_acesso():
        curl_cmd = 'curl -k -X POST -H "Authorization: Basic dW5pcGx1czpsNGd0cjFjazJyc3ByM25nY2wzZW50" -H "Content-Type: application/x-www-form-urlencoded" -d "grant_type=client_credentials" "https://localhost:8443/oauth/token"'
        output = subprocess.check_output(curl_cmd, shell=True)
        token_json = json.loads(output.decode())
        logger.info(f"Token gerado: {token_json}")
        print(token_json)
        return token_json.get('access_token')

    # Função para consultar o endpoint de EANs e obter o código do produto
    def obter_codigo_produto_por_ean(ean_tributado, token_de_acesso):
        print(ean_tributado)
        endpoint = f"https://localhost:8443/public-api/v1/eans/{ean_tributado}"
        print(endpoint)
        headers = {"Authorization": f"Bearer {token_de_acesso}"}
        response = requests.get(endpoint, headers=headers, verify=False)

        if response.status_code == 200:
            produto_data = response.json()
            logger.info(f"Obtido código do produto para o EAN tributado {ean_tributado}: {produto_data.get('produto')}")
            return produto_data.get('produto')
        else:
            logger.error(
                f"Falha ao obter o código do produto para o EAN tributado {ean_tributado}. Tentando buscar no banco de dados.")
            try:
                postgres_conn = psycopg2.connect(
                    "postgresql://postgres:G1CG2B*6d6GEDdABGcf--dA5cb*5bEf6@monorail.proxy.rlwy.net:48186/railway"
                )
                with postgres_conn.cursor() as cursor:
                    query = f"SELECT DISTINCT sku FROM precos_api WHERE ean = '{ean_tributado}'"
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        codigo_produto = int(result[0].replace('.', ''))
                        logger.info(
                            f"Código do produto obtido do banco de dados para o EAN tributado {ean_tributado}: {codigo_produto}")
                        return codigo_produto
                    else:
                        logger.warning(
                            f"Nenhum código de produto encontrado no banco de dados para o EAN tributado {ean_tributado}.")
                        return None
            except Exception as e:
                logger.error(f"Erro ao conectar ao banco de dados ou executar a query: {str(e)}")
                return None
            finally:
                postgres_conn.close()

    # Função para consultar o endpoint de produtos e obter o preço atual
    def obter_infos_atual_por_codigo_produto(codigo_produto, token_de_acesso):
        endpoint = f"https://localhost:8443/public-api/v1/produtos/{codigo_produto}"
        headers = {"Authorization": f"Bearer {token_de_acesso}"}
        response = requests.get(endpoint, headers=headers, verify=False)

        if response.status_code == 200:
            produto_data = response.json()
            current_price = produto_data.get('preco')
            current_name = produto_data.get('nome')
            current_unit = produto_data.get('unidadeMedida')
            logger.info(f"Informações atuais do produto de código {codigo_produto}: Preço: {current_price}, Nome: {current_name}, Unidade de medida: {current_unit}, Payload: {produto_data}")
            return current_price, current_name, current_unit, produto_data
        else:
            logger.error(f"Falha ao obter as informações atuais do produto de código {codigo_produto}.")
            return None

    # Função para comparar dois payloads e registrar mudanças
    def comparar_payloads(payload_old, payload_new):
        differences = {}
        for key, value_old in payload_old.items():
            value_new = payload_new.get(key)
            if value_old != value_new:
                differences[key] = {'old_value': value_old, 'new_value': value_new}

        if differences:
            logger.info("Diferenças encontradas:")
            logger.info(json.dumps(differences, indent=4))
        else:
            logger.info("Nenhuma diferença encontrada.")

    def alterar_preco_atual_por_codigo_produto(codigo_produto, token_de_acesso, new_price, name, unit, payload_current, ean_tributado):
        endpoint = f"https://localhost:8443/public-api/v1/produtos"
        headers = {
            "Authorization": f"Bearer {token_de_acesso}",
            "Content-Type": "application/json"
        }
        print(payload_current)
        payload_update = {
                        "codigo": codigo_produto,
                         "preco": new_price,
                         "nome": name,
                         "unidadeMedida": unit,
                         "ean": ean_tributado
            }

        payload_current.update(payload_update)

        if "situacaoTributariaSN" in payload_current and payload_current["situacaoTributariaSN"] == "":
            payload_current["situacaoTributariaSN"] = "102"
            logger.info(f"Chave 'situacaoTributariaSN' atualizada para 102")

        if "tributacaoSN" in payload_current and payload_current["tributacaoSN"] == "":
            payload_current["tributacaoSN"] = "102"
            logger.info(f"Chave 'tributacaoSN' atualizada para 102")

        if "lucroBruto" in payload_current:
            lucro_bruto = payload_current["lucroBruto"]
            # Verificar se lucroBruto tem mais de 3 dígitos inteiros antes do ponto decimal
            if isinstance(lucro_bruto, str):
                partes = lucro_bruto.split('.')  # Separar a parte inteira da decimal
                parte_inteira = partes[0].replace('-', '')  # Remover sinal negativo, se houver
                if len(parte_inteira) > 3:
                    payload_current["lucroBruto"] = "0"
                    logger.info(
                        f"Chave 'lucroBruto' atualizada para 0 devido ao valor exceder 3 dígitos inteiros: {lucro_bruto}")

        if "aliquotaICMS" in payload_current:
            aliquota_icms = payload_current["aliquotaICMS"]
            if aliquota_icms is not None:
                # Arredondar para duas casas decimais
                payload_current["aliquotaICMS"] = round(float(aliquota_icms), 2)
                logger.info(f"Aliquota ICMS: {payload_current['aliquotaICMS']}")

        if "casasDecimais" in payload_current:
            casas = payload_current["casasDecimais"]
            if casas is not None:
                payload_current["casasDecimais"] = 0
                logger.info(f"Casas Decimais: {payload_current['casasDecimais']}")

        logger.info(f"Payload atualizado: {payload_current}")

        payload = {"produto": payload_current}

        print(payload)

        try:
            response = requests.put(endpoint, headers=headers, json=payload, verify=False)
            logger.info("Requisição PUT enviada:")
            logger.info(f"Endpoint: {endpoint}")
            logger.info(f"Headers: {headers}")
            logger.info(f"Payload: {payload}")
            logger.info(f"Resposta recebida:")
            logger.info(f"Status code: {response.status_code}")
            logger.info(f"Resposta: {response.text}")

            if response.status_code == 200:
                logger.info(f"Preço atualizado com sucesso para o produto de código {codigo_produto}.")
            else:
                logger.error(f"Falha ao atualizar o preço para o produto de código {codigo_produto}.")
        except Exception as e:
            logger.error(f"Erro ao realizar a requisição PUT: {e}")

    # Conectar ao banco de dados no Railway
    logger.info("Conectando ao banco de dados no Railway...")
    try:
        postgres_conn = psycopg2.connect(
            "postgresql://postgres:G1CG2B*6d6GEDdABGcf--dA5cb*5bEf6@monorail.proxy.rlwy.net:48186/railway"
        )
        logger.info("Conexão bem-sucedida.")
    except Exception as e:
        logger.error(f"Falha ao conectar ao banco de dados: {e}")
        return
    cur = postgres_conn.cursor()
    cur.execute("SELECT * FROM update_precos_uniplus")
    dados = cur.fetchall()

    logger.info("Processando dados e novos preços...")

    for dado in dados:
         # Extrair informações necessárias
         ean_tributado = dado[2]
         new_price = dado[3]
         print(new_price)
         #new_price = new_price.replace('.', '').replace(',','.')
         #new_price = float(new_price)
         logger.info(f"Novo preço para EAN tributado {ean_tributado}: {new_price}")
         token_de_acesso = obter_token_de_acesso()

         # Passo 2: Obter o código do produto
         codigo_produto = obter_codigo_produto_por_ean(ean_tributado, token_de_acesso)

         if codigo_produto is not None:
             # Passo 3: Obter o preço atual do produto
             infos_current = obter_infos_atual_por_codigo_produto(codigo_produto, token_de_acesso)

             if infos_current is not None:
                 alterar_preco_atual_por_codigo_produto(codigo_produto, token_de_acesso, new_price=new_price, name=infos_current[1], unit=infos_current[2], ean_tributado=ean_tributado, payload_current=infos_current[3])
                 payload_pos = obter_infos_atual_por_codigo_produto(codigo_produto, token_de_acesso)
                 comparar_payloads(infos_current[3], payload_pos[3])

    logger.info("Limpando o banco de dados...")

    try:
        cur.execute("DELETE FROM update_precos_uniplus")
        postgres_conn.commit()
        logger.info("Banco de dados limpo.")
    except Exception as e:
        logger.error(f"Falha ao limpar o banco de dados: {e}")

    # Fechar a conexão com o banco de dados
    cur.close()
    postgres_conn.close()

    logger.info("Todas as operações foram concluídas com sucesso.")

update_prices()

# https://centraldouniplus.intelidata.inf.br/cdu/web/ferramentas/comunicacao-via-api/