import os

class NotasFiscaisXMLDownloader:
    def __init__(self, db_connection, output_dir="xml_downloads"):
        """
        db_connection: instância de conexão com o banco (deve ter método .get_data(query, params))
        output_dir: diretório onde os arquivos XML serão salvos
        """
        self.db_connection = db_connection
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def download_xml_files(self, query=None):
        """
        Consulta o banco e faz download dos arquivos XML da coluna 'arquivoxml'.
        Salva cada arquivo como <chave>.xml no diretório de saída.
        """
        if query is None:
            # Consulta padrão: busca chave e arquivoxml da tabela de notas fiscais
            query = """
                SELECT chaveacesso AS chave, arquivoxml
                FROM documentofiscalfornecedor
                WHERE arquivoxml IS NOT NULL
            """
        # Busca os dados
        df = self.db_connection.get_data(query)
        for idx, row in df.iterrows():
            chave = str(row['chave'])
            xml_bin = row['arquivoxml']
            if not chave or not xml_bin:
                continue
            # Garante extensão .xml
            filename = f"{chave}.xml"
            filepath = os.path.join(self.output_dir, filename)
            # Salva o arquivo binário como XML
            with open(filepath, "wb") as f:
                f.write(xml_bin)
        print(f"Download concluído. {len(df)} arquivos salvos em '{self.output_dir}'.")

# Exemplo de uso:
# from handlers.db_connection import DatabaseConnection
# db = DatabaseConnection(config_dict)
# downloader = NotasFiscaisXMLDownloader(db)
# downloader.download_xml_files()
