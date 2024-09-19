import psycopg2
from psycopg2 import sql
from datetime import datetime

import re

def extract_data(filename):
    products = []
    similar_products = []
    categories_data = []
    product_categories = []
    reviews = []
    
    with open(filename, 'r') as file:
        current_product = {}
        ignore_product = False
        
        for line in file:
            line = line.strip()

            if "discontinued product" in line:
                ignore_product = True 
                current_product = {}
                continue

            if line.startswith("Id:"):
                if current_product and not ignore_product:
                    products.append(current_product)
                current_product = {}
                ignore_product = False
                current_product['id'] = int(line.split()[1])

            elif line.startswith("ASIN:"):
                current_product['asin'] = line.split()[1]

            elif line.startswith("title:"):
                current_product['title'] = line.split(": ", 1)[1]

            elif line.startswith("group:"):
                current_product['group'] = line.split()[1]

            elif line.startswith("salesrank:"):
                current_product['salesrank'] = int(line.split()[1])

            elif line.startswith("similar:"):
                similar_asins = line.split()[2:]
                for similar_asin in similar_asins:
                    similar_products.append((current_product.get('asin'), similar_asin))

            elif line.startswith("categories:"):
                num_categories = int(line.split()[1])
            elif line.startswith('|') and not ignore_product:
                category_path = line.split('|')
                for i, category in enumerate(category_path):
                    category_cleaned = re.sub(r'\[\d+\]', '', category).strip()
                    if category_cleaned:
                        categories_data.append((category_cleaned, category_path[i-1] if i > 0 else None))
                        product_categories.append((current_product.get('asin'), category_cleaned))

            elif line.startswith("reviews:"):
                total_reviews = int(line.split()[2])

                if total_reviews == 0:
                    reviews.append((current_product.get('asin'), None, None, None, None, None))
                else:
                    for _ in range(total_reviews):
                        review_line = next(file).strip()
                        review_parts = review_line.split()

                        if len(review_parts) >= 9 and re.match(r'\d{4}-\d{1,2}-\d{1,2}', review_parts[0]):
                            try:
                                date = datetime.strptime(review_parts[0], '%Y-%m-%d').date()
                                customer_id = review_parts[2]
                                rating = int(review_parts[4])
                                votes = int(review_parts[6])
                                helpful = int(review_parts[8])
                                reviews.append((current_product.get('asin'), customer_id, date, rating, votes, helpful))
                            except ValueError:
                                reviews.append((current_product.get('asin'), None, None, None, None, None))
                        else:
                            reviews.append((current_product.get('asin'), None, None, None, None, None))

        if current_product and not ignore_product:
            products.append(current_product)
    
    return products, similar_products, categories_data, product_categories, reviews

def get_db_connection(dbname, user, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def create_new_database(dbname,user,password):
    try:
        conn = get_db_connection("postgres", user, password)

        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))

        print(f"Banco de dados '{dbname}' criado.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(e)

def create_db_schema(dbname, user, password):

    try:
        conn = get_db_connection("postgres", user, password)

        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Products (
            id serial PRIMARY KEY,
            asin varchar(20) UNIQUE,
            title varchar(255),
            productGroup varchar(50),
            salesrank integer
            );

        CREATE TABLE IF NOT EXISTS SimilarProducts (
            asin varchar(20) REFERENCES Products(asin),
            similar_asin varchar(20),
            PRIMARY KEY (asin, similar_asin)
            );

        CREATE TABLE IF NOT EXISTS Categories (
            id serial PRIMARY KEY,
            category_name varchar(255) UNIQUE NOT NULL,
            parent_category_id integer REFERENCES Categories(id) ON DELETE SET NULL
            );

        CREATE TABLE IF NOT EXISTS ProductCategories (
            product_id integer REFERENCES Products(id),
            category_id integer REFERENCES Categories(id),
            PRIMARY KEY (product_id, category_id)
            );

        CREATE TABLE IF NOT EXISTS Reviews (
            id SERIAL PRIMARY KEY,
            product_id INTEGER REFERENCES Products(id),
            customer_id VARCHAR(20),
            date DATE,
            rating INTEGER,
            votes INTEGER,
            helpful INTEGER
            );
        ''')

        conn.commit()

        cursor.close()
        conn.close()

        print("Tabelas criadas com sucesso!")

    except Exception as e:
        print(e)

def insert_data_to_db(dbname, user, password, products, similar_products, categories_data, product_categories, reviews):
    try:
        conn = get_db_connection("postgres", user, password)

        cursor = conn.cursor()

        for product in products:
            cursor.execute('''
                INSERT INTO Products (asin, title, productGroup, salesrank)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (asin) DO NOTHING
            ''', (product['asin'], product.get('title'), product.get('group'), product.get('salesrank')))

        category_id_map = {}
        for category, _ in categories_data:
            cursor.execute('''
                INSERT INTO Categories (category_name)
                VALUES (%s)
                ON CONFLICT (category_name) DO NOTHING
                RETURNING id
            ''', (category,))
            result = cursor.fetchone()
            if result:
                category_id_map[category] = result[0]

        for category, parent_category in categories_data:
            if parent_category:
                category_id = category_id_map.get(category)
                parent_category_id = category_id_map.get(parent_category)
                if category_id and parent_category_id:
                    cursor.execute('''
                        UPDATE Categories
                        SET parent_category_id = %s
                        WHERE id = %s
                    ''', (parent_category_id, category_id))

        for asin, category in product_categories:
            cursor.execute('''
                SELECT id FROM Products WHERE asin = %s
            ''', (asin,))
            product_result = cursor.fetchone()
            if product_result:
                product_id = product_result[0]
                category_id = category_id_map.get(category)
                if category_id:
                    cursor.execute('''
                        INSERT INTO ProductCategories (product_id, category_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    ''', (product_id, category_id))

        for asin, similar_asin in similar_products:
            cursor.execute('''
                INSERT INTO SimilarProducts (asin, similar_asin)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            ''', (asin, similar_asin))

        for asin, customer_id, date, rating, votes, helpful in reviews:
            cursor.execute('''
                SELECT id FROM Products WHERE asin = %s
            ''', (asin,))
            product_result = cursor.fetchone()
            if product_result:
                product_id = product_result[0]
                cursor.execute('''
                    INSERT INTO Reviews (product_id, customer_id, date, rating, votes, helpful)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                ''', (product_id, customer_id, date, rating, votes, helpful))

        conn.commit()
        cursor.close()
        conn.close()

        print("Dados inseridos com sucesso!")

    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")

if __name__ == "__main__":

    user = "postgres"
    password = ""   # senha
    dbname = "postgres"     # nome do bd

    filename = "ex.txt"

    products, similar_products, categories_data, product_categories,reviews = extract_data(filename)

    create_new_database(dbname,user,password)
    create_db_schema(dbname, user, password)

    insert_data_to_db(dbname, user, password, products,similar_products,categories_data, product_categories,reviews)
