import csv
import pandas as pd

df = pd.read_csv("deal_info.csv", sep=",", encoding='cp1251')
print(f"Строк в файле deal_info.csv: {len(df)}")

def load_deal_info(conn, file_path):
    cur = conn.cursor()
    with open(file_path, 'r', encoding='cp1251') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute("""
                INSERT INTO rd.tmp_deal_info (
                    deal_rk, deal_num, deal_name, deal_sum, client_rk,
                    account_rk, agreement_rk, deal_start_date,
                    department_rk, product_rk, deal_type_cd,
                    effective_from_date, effective_to_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                row['deal_rk'], row['deal_num'], row['deal_name'],
                float(row['deal_sum']), int(row['client_rk']),
                int(row['account_rk']), int(row['agreement_rk']),
                row['deal_start_date'], int(row['department_rk']),
                int(row['product_rk']), row['deal_type_cd'],
                row['effective_from_date'], row['effective_to_date']
            ))
    conn.commit()