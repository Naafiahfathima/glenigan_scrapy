import os
import pymysql
import logging
from scrapy.exceptions import DropItem
import configparser

class MySQLPipeline:
    """Pipeline for inserting data into MySQL database."""

    def open_spider(self, spider):
        """Connects to the database when the spider starts."""
        config = configparser.ConfigParser()
        config.read(r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\database.ini")

        self.conn = pymysql.connect(
            host=config["mysql"]["host"],
            user=config["mysql"]["user"],
            password=config["mysql"]["password"],
            database=config["mysql"]["database"],
            port=int(config["mysql"]["port"]),
        )
        self.cursor = self.conn.cursor()

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS applications (
                ref_no VARCHAR(255) PRIMARY KEY,
                Url TEXT,
                scrape_status VARCHAR(10) DEFAULT 'No'
            )
        """)

    def process_item(self, item, spider):
        """Inserts application data into the database."""
        ref_no = item["ref_no"]
        url = item["link"]

        self.cursor.execute("SELECT * FROM applications WHERE ref_no = %s", (ref_no,))
        if self.cursor.fetchone():
            raise DropItem(f"Duplicate entry: {ref_no}")

        self.cursor.execute("INSERT INTO applications (ref_no, Url) VALUES (%s, %s)", (ref_no, url))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        """Closes the database connection when the spider finishes."""
        self.cursor.close()
        self.conn.close()

# class HtmlScraperPipeline:
#     def __init__(self):
#         self.output_folder = "html_dumps"
#         self.db_config = self.load_db_config()
        
#         if not os.path.exists(self.output_folder):
#             os.makedirs(self.output_folder)

#     def load_db_config(self):
#         config = configparser.ConfigParser()
#         config.read(r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\database.ini")
#         return {
#             "host": config["mysql"]["host"],
#             "user": config["mysql"]["user"],
#             "password": config["mysql"]["password"],
#             "database": config["mysql"]["database"],
#             "port": int(config["mysql"]["port"])
#         }

#     def update_scrape_status(self, ref_no):
#         connection = pymysql.connect(**self.db_config)
#         cursor = connection.cursor()
        
#         try:
#             cursor.execute("UPDATE applications SET scrape_status = 'Yes' WHERE ref_no = %s", (ref_no,))
#             connection.commit()
#             logging.info(f"Updated scrape_status to 'Yes' for {ref_no}")
#         except Exception as e:
#             logging.error(f"Error updating scrape_status for {ref_no}: {e}")
#         finally:
#             cursor.close()
#             connection.close()

#     def process_item(self, item, spider):
#         ref_no = item['ref_no']
#         html_content = item['html_content']
        
#         sanitized_ref_no = ref_no.replace("/", "_")
#         filename = os.path.join(self.output_folder, f"{sanitized_ref_no}.html")
#         with open(filename, "w", encoding="utf-8") as file:
#             file.write(html_content)
        
#         logging.info(f"Saved: {filename}")
#         self.update_scrape_status(ref_no)
        
#         return item
