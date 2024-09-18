import json
from bs4 import BeautifulSoup
import cloudscraper
import mysql.connector
from mysql.connector import Error
from datetime import datetime

def gold_good_returns():
    def get_data(city_html):
        soup = BeautifulSoup(city_html, 'html.parser')
        rows = soup.find_all('tr')

        # Prepare the list of dictionaries
        data = []
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue  # Skip rows that don't have the expected number of columns
            
            city = cells[0].find('a').text.strip()
            rate_22k = cells[1].text.strip()
            rate_24k = cells[2].text.strip()
            rate_18k = cells[3].text.strip()
            
            data.append({
                "City": city,
                "22K Today": rate_22k,
                "24K Today": rate_24k,
                "18K Today": rate_18k
            })

        return data

    def sanitize_string(s):
        return s.encode('utf-8', 'replace').decode('utf-8')

    headers = {
        'Host': 'www.goodreturns.in',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'x-oigt-header': 'GITPL',
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-GPC': '1',
        'Connection': 'keep-alive',
        'Referer': 'https://www.goodreturns.in/gold-rates/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0',
        'TE': 'trailers'
    }
    
    scraper = cloudscraper.create_scraper()
    all_data = []

    for i in range(0, 120, 10):
        url = f'https://www.goodreturns.in/gold-rates/?gr_db_dynamic_content=metal_city_details&offset={i}'
        try:
            res = scraper.get(url, headers=headers)
            if res.status_code == 200:
                try:
                    a = res.json()
                    city_html = a.get('city_html', '')  # Ensure `city_html` key exists
                    if city_html:
                        data = get_data(city_html)
                        all_data.extend(data)
                    else:
                        print('No `city_html` key found in response')
                except Exception as e:
                    print(f'Error parsing JSON data: {e}')
            else:
                print(f'Some other error occurred with status code {res.status_code}')
        except cloudscraper.exceptions.RequestException as e:
            print(f'Request failed: {e}')

    # Database connection configuration
    config = {
        'user': 'hellodev_good_returns',
        'password': 'good420@',
        'host': '103.211.218.103',
        'database': 'hellodev_fuel_price',
    }

    # Insert data into MySQL
    if all_data:
        try:
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                cursor = connection.cursor()

                # Drop the existing table if it exists
                cursor.execute("DROP TABLE IF EXISTS gold_rates")

                # Create a new table with utf8mb4 character set
                create_table_query = '''
                CREATE TABLE gold_rates (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    City VARCHAR(255) CHARACTER SET utf8mb4,
                    `22K Today` VARCHAR(50) CHARACTER SET utf8mb4,
                    `24K Today` VARCHAR(50) CHARACTER SET utf8mb4,
                    `18K Today` VARCHAR(50) CHARACTER SET utf8mb4,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                ) CHARACTER SET utf8mb4;
                '''
                cursor.execute(create_table_query)

                # Prepare the insert statement
                insert_query = '''
                INSERT INTO gold_rates (City, `22K Today`, `24K Today`, `18K Today`, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                '''

                # Get the current timestamp
                current_timestamp = datetime.now()

                # Insert each row of data, sanitizing inputs
                for item in all_data:
                    cursor.execute(insert_query, (
                        sanitize_string(item['City']),
                        sanitize_string(item['22K Today']),
                        sanitize_string(item['24K Today']),
                        sanitize_string(item['18K Today']),
                        current_timestamp
                    ))

                # Commit the transaction
                connection.commit()
                print(f"{cursor.rowcount} records inserted successfully.")

        except Error as e:
            print(f"Error: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed.")

    else:
        print('No data was collected')
        return json.dumps([])

result = gold_good_returns()
