import os
import sentry_sdk
from dotenv import load_dotenv
from app.worker.redis_consumer import RedisConsumer
from app.worker.utils.logger import get_logger

load_dotenv()
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    send_default_pii=True,
)

logger = get_logger()

def main():
    logger.info("Inicializando sistemas do Worker Local Uniplus...")
    
    # Instancia o consumidor e inicia o loop
    consumer = RedisConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
