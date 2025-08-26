# Integration Uniplus ERP

Sistema de integração ETL para sincronização de dados entre banco local (Uniplus ERP) e banco destino (Cloud).

## 📁 Estrutura do Projeto

```
integration-uniplus-erp/
├── handlers/           # Manipuladores de conexão e logs
│   ├── db_connection.py
│   ├── log_handler.py
│   └── query_loader.py # Carregador de queries SQL
├── queries/            # Arquivos SQL organizados
│   ├── vendas_daily.sql
│   ├── notas_fiscais.sql
│   ├── catalogo.sql
│   ├── contas_a_pagar.sql
│   ├── icms_daily.sql
│   └── ...
├── services/           # Serviços ETL
│   ├── vendas_daily.py
│   ├── notas_fiscais.py
│   ├── catalogo.py
│   ├── contas_a_pagar.py
│   └── ...
├── settings/           # Configurações
│   ├── config_etl.json # Configurações dos ETLs
│   └── db_config.py    # Configurações de banco
└── main.py            # Exemplo de uso
```

## 🚀 Como usar

### 1. Configurar variáveis de ambiente (.env):
```bash
# Banco local (UNICO ERP)
UNICO_DB=nome_do_banco_local
UNICO_USER=usuario_local
UNICO_PASSWORD=senha_local
UNICO_HOST=localhost
UNICO_PORT=5432

# Banco destino (Cloud)
MERCADO_DB=nome_do_banco_cloud
MERCADO_USER=usuario_cloud
MERCADO_PASSWORD=senha_cloud
MERCADO_HOST=host_do_banco_cloud.com
MERCADO_PORT=5432
```

### 2. Executar ETL de Notas Fiscais:
```python
from services.notas_fiscais import NotasFiscaisETL
from settings.db_config import get_source_config, get_target_config

# Configurar conexões
source_config = get_source_config()  # Banco local
target_config = get_target_config()  # Banco destino

# Executar ETL
etl = NotasFiscaisETL(source_config, target_config)
etl.run_etl('2024-01-01')  # A partir de uma data específica
```

### 3. Executar ETL de Catálogo:
```python
from services.catalogo import CatalogoETL
from settings.db_config import get_source_config, get_target_config

# Configurar conexões
source_config = get_source_config()  # Banco local
target_config = get_target_config()  # Banco destino

# Executar ETL
etl = CatalogoETL(source_config, target_config)
etl.run_etl()  # Sincroniza todos os produtos ativos
```

### 4. Executar ETL de Vendas (automático com UPSERT):
```python
from main import run_vendas_daily_etl

# O ETL sempre processa automaticamente as datas faltantes
# baseado na tabela company_schedule usando UPSERT
summary = run_vendas_daily_etl()
print(f"Processadas: {summary['processed']}, Falharam: {summary['failed']}")

# Exibe detalhes das datas processadas
for date in summary['dates']['processed']:
    print(f"✅ {date}")
    
for failed in summary['dates']['failed']:
    print(f"❌ {failed['date']}: {failed['error']}")
```

### 5. Executar ETL de Contas a Pagar (com UPSERT):
```python
from main import run_contas_a_pagar_etl

# O ETL sincroniza todas as contas a pagar usando UPSERT
# para evitar duplicatas e manter dados atualizados
summary = run_contas_a_pagar_etl()
print(f"Registros processados: {summary['processed']}")
```

## 📝 Adicionando novos ETLs

1. **Criar arquivo SQL** em `queries/novo_etl.sql`
2. **Adicionar configuração** em `settings/config_etl.json`
3. **Criar serviço** em `services/novo_etl.py`