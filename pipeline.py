from utils import (
    Extractor, 
    CSVGenerator,
    Transformer,
    Loader
    )

from loguru import logger
import pandas as pd
import os
import sqlite3

try:
    # EXTRAÇÃO DE DADOS
    extractor = Extractor()
    products = extractor.get_products()
    users = extractor.get_users()

    if not os.path.isfile("data/vendas.csv"):
        generator = CSVGenerator(users=users, products=products)
        generator.generate_csv()

    df = pd.read_csv("data/vendas.csv", sep=",", encoding="utf-8")

    # TRANSFORMAÇÃO DE DADOS
    transformer = Transformer(users=users, products=products)
    users_df = transformer.transform_users()
    products_df = transformer.transform_products()


    # CARGA DE DADOS
    DB_PATH = "data/ecommerce.db"
    conn = sqlite3.connect(DB_PATH)
    loader = Loader(products_df=products_df, 
                    users_df=users_df, 
                    vendas_df=df, 
                    conn=conn)

    loader.create_tables()
    loader.load_products(conn)
    loader.load_users(conn)
    loader.load_vendas(conn)

    logger.info("Pipeline executado com sucesso!")

except Exception as e:
    logger.error(f"Erro ao executar o pipeline: {e}")

finally:
    logger.info("Fechando conexão com o banco de dados...")
    if conn:
        conn.close()
        logger.info("Conexão fechada com sucesso!")