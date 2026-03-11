from app.worker.redis_consumer import RedisConsumer
from app.worker.utils.logger import get_logger

logger = get_logger()

def main():
    logger.info("Inicializando sistemas do Worker Local Uniplus...")
    
    # Instancia o consumidor e inicia o loop
    consumer = RedisConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
