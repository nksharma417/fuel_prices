import json
import logging
from bs4 import BeautifulSoup
import cloudscraper
import mysql.connector
import brotli
from mysql.connector import Error

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
                  except:
                    json_data = res.text
                else:
                  json_data = res.text

                try:
                    a = json.loads(json_data)
                    city_html = a.get('city_html', '')  # Ensure `city_html` key exists
                    if city_html:
                        data = get_data(city_html)
                        all_data.extend(data)
                    else:
                        logging.warning('No `city_html` key found in response')
                except Exception as e:
                    logging.error(f'Error parsing JSON data: {e}')
            else:
                logging.error(f'Some other error occurred with status code {res.status_code}')
        except cloudscraper.exceptions.RequestException as e:
            logging.error(f'Request failed: {e}')

    # Convert list of dictionaries to JSON
    if all_data:
        json_data = json.dumps(all_data, indent=4)
        return json_data
    else:
        logging.info('No data was collected')
        return json.dumps([])

def insert_data_to_db(data):
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Check if table exists and drop it
        cursor.execute("DROP TABLE IF EXISTS gold_rates")
        
        create_table_query = """CREATE TABLE gold_rates (
                                    City VARCHAR(255),
                                    `22K Today` VARCHAR(255),
                                    `24K Today` VARCHAR(255),
                                    `18K Today` VARCHAR(255),
                                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                                )"""
        cursor.execute(create_table_query)
        logging.info("Table created successfully.")
        
        # Insert new data
        insert_query = """INSERT INTO gold_rates (City, `22K Today`, `24K Today`, `18K Today`)
                          VALUES (%s, %s, %s, %s)"""
        
        for item in data:
            cursor.execute(insert_query, (item['City'], item['22K Today'], item['24K Today'], item['18K Today']))
        
        connection.commit()
        logging.info(f"{cursor.rowcount} records inserted successfully.")

    except Error as err:
        logging.error(f"Database error: {err}")
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
    else:
        logging.info("No data returned to insert into the database.")
