import scrapy
from scrapy.http import FormRequest
import pymysql
import json
import configparser
import logging
import re

class CouncilScraper(scrapy.Spider):
    name = "planning"

    def __init__(self, json_file="councils.json", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json_file = json_file
        self.councils = self.load_councils()
        self.db_config = self.load_db_config()

    def load_councils(self):
        """Load council configurations from JSON file."""
        with open(self.json_file, "r") as file:
            return json.load(file)

    def load_db_config(self):
        """Load database configuration from database.ini"""
        config = configparser.ConfigParser()
        config.read("database.ini")
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"]),
        }

    def start_requests(self):
        """Starts scraping for each council in the JSON file."""
        for council_name, council_info in self.councils.items():
            council_code = council_info["code"]
            url = council_info["url"]
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={"council_name": council_name, "council_code": council_code, "url": url},
            )

    def parse(self, response):
        """Extract CSRF token and submit form request."""
        csrf_token = response.xpath('//form[@id="advancedSearchForm"]//input[@name="_csrf"]/@value').get()

        if not csrf_token:
            logging.error(f"CSRF token missing for {response.meta['council_name']}, aborting!")
            return

        logging.info(f"Extracted CSRF Token: {csrf_token}")

        form_data = {
            "_csrf": csrf_token,
            "date(applicationValidatedStart)": "14/02/2025",
            "date(applicationValidatedEnd)": "15/02/2025",
            "searchType": "Application",
        }

        post_url = response.meta["url"].replace("search.do?action=advanced", "advancedSearchResults.do")

        post_url_with_params = f"{post_url}?action=firstPage"

        yield FormRequest(
            url=post_url_with_params,
            formdata=form_data,
            callback=self.parse_results,
            meta=response.meta,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": response.url,
            },
            method="POST",
        )

    def parse_results(self, response):
        """Extract application details and save them to MySQL."""
        applications = response.xpath('//li[contains(@class, "searchresult")]')

        if not applications:
            logging.info(f"No applications found for {response.meta['council_name']}.")
            return

        applications_data = []
        for app in applications:
            link_tag = app.xpath(".//a")
            link = (
                response.meta["url"].split("/online-applications")[0] + link_tag.xpath("./@href").get()
                if link_tag
                else "N/A"
            )

            description = link_tag.xpath("normalize-space(./text())").get(default="N/A")

            ref_no = app.xpath('.//p[@class="metaInfo"]/text()').re_first(r"Ref\. No:\s*([\w/.-]+)")

            sanitized_ref_no = self.sanitize_ref_no(f"{response.meta['council_code']}_{ref_no}")

            applications_data.append([sanitized_ref_no, link, "No"])

        logging.info(f"Scraped {len(applications_data)} applications for {response.meta['council_name']}.")
        
        # Save to database
        if applications_data:
            self.save_to_db(applications_data)

        # Handle pagination
        next_page_tag = response.xpath('//a[contains(@class, "next")]/@href').get()
        if next_page_tag:
            next_page_url = response.meta["url"].split("/online-applications")[0] + next_page_tag
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_results,
                meta=response.meta,
            )

    def sanitize_ref_no(self, ref_no):
        """Sanitize the reference number to ensure it is database-safe."""
        return re.sub(r'[^a-zA-Z0-9_-]', '_', ref_no)

    def save_to_db(self, data):
        """Save extracted application data to MySQL database, avoiding duplicates."""
        conn = pymysql.connect(**self.db_config)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS applications (
                ref_no VARCHAR(255) PRIMARY KEY,
                Url TEXT,
                scrape_status VARCHAR(10) DEFAULT 'No'
            )
            """
        )

        new_entries = 0
        for entry in data:
            ref_no, link, scrape_status = entry
            cursor.execute("SELECT * FROM applications WHERE ref_no = %s", (ref_no,))
            if not cursor.fetchone():
                try:
                    cursor.execute("INSERT INTO applications (ref_no, Url, scrape_status) VALUES (%s, %s, %s)", (ref_no, link, scrape_status))
                    new_entries += 1
                except pymysql.MySQLError as err:
                    logging.error(f"Insert Error for {ref_no}: {err}")

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Inserted {new_entries} new records into the database." if new_entries else "No new records inserted.")

