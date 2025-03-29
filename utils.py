from typing import List, Dict
import requests
from loguru import logger
import os
import datetime
import csv
import random
import pandas as pd

class Extractor:
    BASE_URL = "https://fakestoreapi.com"

    def __init__(self):
        self.products = None
        self.users = None

    def _fetch_data(self, endpoint: str) -> List[Dict]:
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            logger.info(f"Carregando dados de {endpoint}...")
            response = requests.get(url, timeout=10)

            if response.status_code != 200:
                logger.error(f"Erro ao buscar dados: {response.status_code}")
                return []

            data = response.json()
            if not data:
                logger.error(f"Nenhum dado encontrado em {endpoint}")
                return []

            logger.info(f"Dados de {endpoint} carregados com sucesso!")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            return []

    def get_products(self) -> List[Dict]:
        if self.products is None:
            self.products = self._fetch_data(endpoint="products")
        return self.products or []

    def get_users(self) -> List[Dict]:
        if self.users is None:
            self.users = self._fetch_data(endpoint="users")
        return self.users or []


class CSVGenerator:
    def __init__(self, users: List[Dict], products: List[Dict]):
        self.users = users
        self.products = products

    def generate_csv(self, output_dir: str = "data", filename: str = "vendas.csv"):
        if not self.products:
            logger.error("Lista de produtos está vazia")
            raise ValueError("Nenhum produto encontrado")

        if not self.users:
            logger.error("Lista de usuários está vazia")
            raise ValueError("Nenhum usuário encontrado")

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, filename)
        headers = ["product_id", "user_id", "date", "payment_method", "quantity", "total"]

        start_date = datetime.date(2002, 8, 8)
        datas = [(start_date + datetime.timedelta(days=i)).isoformat() for i in range(1000)]
        user_ids = [user["id"] for user in self.users]
        payment_methods = ["Credit Card", "Debit Card", "Cash", "Pix"]

        try:
            with open(output_path, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()

                logger.info("Gerando vendas...")
                for _ in range(1000):
                    product = random.choice(self.products)
                    quantity = random.randint(1, 10)

                    writer.writerow(
                        {
                            "product_id": product["id"],
                            "user_id": random.choice(user_ids),
                            "date": random.choice(datas),
                            "payment_method": random.choice(payment_methods),
                            "quantity": quantity,
                            "total": quantity * product["price"],
                        }
                    )

                logger.info(f"Vendas geradas com sucesso em {output_path}!")

        except (IOError, ValueError) as e:
            logger.error(f"Erro ao gerar vendas: {e}")

class Transformer:
    def __init__(self, users: List[Dict], products: List[Dict]):
        self.users = users
        self.products = products

    def transform_products(self):
        products_df = pd.DataFrame(self.products)
        products_df['rate'] = products_df['rating'].apply(lambda x: x['rate'])
        products_df['rating_count'] = products_df['rating'].apply(lambda x: x['count'])
        products_df.drop('rating', axis=1, inplace=True)

        return products_df

    def transform_users(self):
        users_df = pd.DataFrame(self.users)
        users_df['full_name'] = users_df['name'].apply(lambda x: f"{x['firstname']} {x['lastname']}")
        users_df['full_address'] = users_df['address'].apply(lambda x: f"{x['number']} {x['street']}, {x['city']}, {x['zipcode']}")
        users_df['lat'] = users_df['address'].apply(lambda x: x['geolocation']['lat'])
        users_df['long'] = users_df['address'].apply(lambda x: x['geolocation']['long'])
        users_df.drop(columns=['name', 'address', 'password', '__v'], axis=1, inplace=True)
        
        return users_df

class Loader:
    def __init__(self, products_df, users_df, vendas_df, conn):
        self.products_df = products_df
        self.users_df = users_df
        self.vendas_df = vendas_df
        self.conn = conn

    def create_tables(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                title TEXT,
                price REAL,
                description TEXT,
                category TEXT,
                image TEXT,
                rate REAL,
                rating_count INTEGER
            );
            """)

            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                email TEXT,
                username TEXT,
                phone TEXT,
                full_name TEXT,
                full_address TEXT,
                lat REAL,
                long REAL
            );
            """)

            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                user_id INTEGER,
                date TEXT,
                payment_method TEXT,
                quantity INTEGER,
                total REAL,
                FOREIGN KEY (product_id) REFERENCES products (id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            );
            """)

    def load_products(self, conn, table_name="products"):
        self.products_df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"Produtos carregados com sucesso em {table_name}!")

    def load_users(self, conn, table_name="users"):
        self.users_df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"Usuários carregados com sucesso em {table_name}!")

    def load_vendas(self, conn, table_name="sales"):
        self.vendas_df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"Vendas carregadas com sucesso em {table_name}!")