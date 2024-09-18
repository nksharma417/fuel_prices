import json
from bs4 import BeautifulSoup
import cloudscraper
import mysql.connector
from mysql.connector import Error
import brotli
import pytz
from datetime import datetime
# Database connection configuration
config = {
        'user': 'hellodev_good_returns',
        'password': 'good420@',
        'host': '103.211.218.103',
        'database': 'hellodev_fuel_price',
    }
# Configure Indian Timezone
IST = pytz.timezone('Asia/Kolkata')

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
                if res.headers.get('Content-Encoding') == 'br':
                    # Decompress using Brotli
                    try:
                        decompressed_data = brotli.decompress(res.content)
                        json_data = decompressed_data.decode('utf-8')
                    except Exception as e:
                        json_data = res.text
                else:
                    json_data = res.text

                try:
                    a = json.loads(json_data)
                    city_html = a.get('city_html', '')  # Ensure `city_html` key exists
                    if city_html:
                        data = get_data(city_html)
                        all_data.extend(data)
                except Exception as e:
                    pass  # Handle JSON parsing error
            else:
                pass  # Handle unexpected status code
        except cloudscraper.exceptions.RequestException as e:
            pass  # Handle request failure

    # Convert list of dictionaries to JSON
    if all_data:
        json_data = json.dumps(all_data, indent=4)
        return json_data
    else:
        return json.dumps([])

def sanitize_string(s):
    """Sanitize string to ensure it can be inserted into the database."""
    return s.encode('utf-8', 'replace').decode('utf-8')

def insert_data_to_db(data):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Check if table exists and drop it
        cursor.execute("DROP TABLE IF EXISTS gold_rates")
        
        create_table_query = """CREATE TABLE gold_rates (
                                    City VARCHAR(255) CHARACTER SET utf8mb4,
                                    `22K Today` VARCHAR(255) CHARACTER SET utf8mb4,
                                    `24K Today` VARCHAR(255) CHARACTER SET utf8mb4,
                                    `18K Today` VARCHAR(255) CHARACTER SET utf8mb4,
                                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                )"""
        cursor.execute(create_table_query)
        
        # Get the current time in IST once
        current_time = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
        
        # Insert new data
        insert_query = """INSERT INTO gold_rates (City, `22K Today`, `24K Today`, `18K Today`, timestamp)
                          VALUES (%s, %s, %s, %s, %s)"""
        
        for item in data:
            cursor.execute(insert_query, (
                sanitize_string(item['City']),
                sanitize_string(item['22K Today']),
                sanitize_string(item['24K Today']),
                sanitize_string(item['18K Today']),
                current_time  # Use the same timestamp for all entries
            ))
        
        connection.commit()

    except Error as err:
        pass  # Handle database error
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main execution
if __name__ == "__main__":
    result = gold_good_returns()
    if result:
        data = json.loads(result)
        insert_data_to_db(data)
