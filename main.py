import psycopg2
import logging
from deal_loader import load_deal_info
from product_loader import load_product

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Вывод в терминал
    ]
)

logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Начало загрузки данных")
        conn = psycopg2.connect(
            dbname="dwh",
            user="postgres",
            password="k435f7634g5hf",
            host="localhost"
        )
        # cur = conn.cursor()
        # cur.execute('TRUNCATE TABLE rd.deal_info')
        # conn.commit()
        # cur.execute('TRUNCATE TABLE rd.product')
        # conn.commit()
        # Загрузка данных
        load_deal_info(conn, "deal_info.csv")
        load_product(conn, "product_info.csv")
        logger.info("Загрузка данных завершена")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто")


if __name__ == "__main__":
    main()