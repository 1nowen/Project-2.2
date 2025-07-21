import csv

import pandas as pd

df = pd.read_csv("product_info.csv", sep=",", encoding='cp1251')
print(f"Строк в файле product_info.csv: {len(df)}")

def load_product(conn, file_path):
    cur = conn.cursor()
    with open(file_path, 'r', encoding='cp1251') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO rd.tmp_product (
                    product_rk, product_name,
                    effective_from_date, effective_to_date
                ) VALUES (%s, %s, %s, %s)
            """, (
                int(row['product_rk']), row['product_name'],
                row['effective_from_date'], row['effective_to_date']
            ))
    conn.commit()