import scrapy
from scrapy.http import FormRequest
import json
import os
import re
import pymysql
import configparser
from glenigan.logger_config import logger
from glenigan.items import ApplicationItem, HtmlScraperItem

class CombinedSpider(scrapy.Spider):
    name = "combined_spider"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load council details from JSON
        json_path = r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\councils.json"
        logger.info(f"Loading council details from: {json_path}")

        if not os.path.exists(json_path):
            self.logger.error(f"Councils JSON file not found at: {json_path}")
            raise FileNotFoundError(f"Councils JSON file not found at: {json_path}")

        with open(json_path, "r") as file:
            self.councils = json.load(file)

        # Load database configuration
        self.db_config = self.load_db_config()
        self.tabs = [
            "summary", "details", "contacts", "dates", "makeComment", "neighbourComments",
            "consulteeComments", "constraints", "documents", "relatedCases"
        ]

    def load_db_config(self):
        """Load database configuration from database.ini"""
        config = configparser.ConfigParser()
        config.read(r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\database.ini")
        return {
            "host": config["mysql"]["host"],
            "user": config["mysql"]["user"],
            "password": config["mysql"]["password"],
            "database": config["mysql"]["database"],
            "port": int(config["mysql"]["port"])
        }

    def start_requests(self):
        """Start scraping applications and HTML pages from database."""
        # Step 1: Scrape new applications
        for council_name, council_info in self.councils.items():
            yield scrapy.Request(
                url=council_info["url"],
                callback=self.parse_council,
                meta={"council_name": council_name, "council_code": council_info["code"], "url": council_info["url"]},
            )

        # Step 2: Scrape HTML pages for applications with scrape_status = 'No'
        urls_to_scrape = self.fetch_urls_from_db()
        for ref_no, url in urls_to_scrape:
            self.logger.info(f"Scraping full HTML for application: {ref_no} -> {url}")
            yield scrapy.Request(
                url=url, 
                callback=self.parse_html, 
                meta={'ref_no': ref_no, 'base_url': url, 'all_html_content': ""},
                dont_filter=True  # Prevent duplicate filtering
            )

    def parse_council(self, response):
        """Extract CSRF token and submit form request."""
        csrf_token = response.xpath('//form[@id="advancedSearchForm"]//input[@name="_csrf"]/@value').get()
        logger.info(f"CSRF token : {csrf_token}")

        if not csrf_token:
            logger.error(f"CSRF token missing for {response.meta['council_name']}, aborting!")
            return

        form_data = {
            "_csrf": csrf_token,
            "date(applicationValidatedStart)": "18/02/2025",
            "date(applicationValidatedEnd)": "18/02/2025",
            "searchType": "Application",
        }

        post_url = response.meta["url"].replace("search.do?action=advanced", "advancedSearchResults.do")
        post_url_with_params = f"{post_url}?action=firstPage"

        yield FormRequest(
            url=post_url_with_params,
            formdata=form_data,
            callback=self.parse_results,
            meta=response.meta,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            method="POST",
        )

    def parse_results(self, response):
        """Extract application details."""
        applications = response.xpath('//li[contains(@class, "searchresult")]')

        if not applications:
            self.logger.info(f"No applications found for {response.meta['council_name']}.")
            return

        for app in applications:
            link_tag = app.xpath(".//a")
            link = response.meta["url"].split("/online-applications")[0] + link_tag.xpath("./@href").get() if link_tag else "N/A"
            ref_no = app.xpath('.//p[@class="metaInfo"]/text()').re_first(r"Ref\. No:\s*([\w/.-]+)")
            sanitized_ref_no = self.sanitize_ref_no(f"{response.meta['council_code']}_{ref_no}")

            yield ApplicationItem(ref_no=sanitized_ref_no, link=link)

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
        """Sanitize reference numbers."""
        return re.sub(r'[^a-zA-Z0-9_-]', '_', ref_no)

    def fetch_urls_from_db(self):
        """Retrieve URLs from the database where `scrape_status = 'No'`."""
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor()
        
        cursor.execute("SELECT ref_no, Url FROM applications WHERE scrape_status = 'No'")
        rows = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return rows

    def parse_html(self, response):
        """Extracts the main HTML content and starts scraping tabs."""
        ref_no = response.meta['ref_no']
        base_url = response.meta['base_url']

        logger.info(f"Scraping HTML for: {ref_no}")

        # Save main page content
        all_html_content = f"\n<!-- Main Page -->\n{response.text}"

        # Start scraping first tab
        first_tab_url = self.construct_tab_url(base_url, self.tabs[0])
        logger.info(f"Fetching first tab: {self.tabs[0]} -> {first_tab_url}")

        yield scrapy.Request(
            url=first_tab_url,
            callback=self.parse_tab,
            meta={
                "ref_no": ref_no,
                "all_html_content": all_html_content,
                "tab_index": 0,
                "base_url": base_url
            },
            dont_filter=True
        )

    def parse_tab(self, response):
        """Extracts HTML from each tab and moves to the next."""
        ref_no = response.meta["ref_no"]
        all_html_content = response.meta["all_html_content"]
        tab_index = response.meta["tab_index"]
        base_url = response.meta["base_url"]

        # Append tab content
        tab_name = self.tabs[tab_index]
        self.logger.info(f"Scraped tab: {tab_name}")
        all_html_content += f"\n<!-- Tab: {tab_name} -->\n{response.text}"

        # Move to the next tab
        next_tab_index = tab_index + 1
        if next_tab_index < len(self.tabs):
            next_tab_url = self.construct_tab_url(base_url, self.tabs[next_tab_index])
            logger.info(f"Fetching next tab: {self.tabs[next_tab_index]} -> {next_tab_url}")

            yield scrapy.Request(
                url=next_tab_url,
                callback=self.parse_tab,
                meta={
                    "ref_no": ref_no,
                    "all_html_content": all_html_content,
                    "tab_index": next_tab_index,
                    "base_url": base_url
                },
                dont_filter=True
            )
        else:
            # ✅ Yield HtmlScraperItem with full HTML
            yield HtmlScraperItem(
                ref_no=ref_no,
                url=response.url,
                html_content=all_html_content  # Ensure all tabs' HTML is included
            )

    def construct_tab_url(self, base_url, tab_name):
        """Constructs the correct tab URL."""
        if "activeTab=" in base_url:
            return base_url.split("activeTab=")[0] + f"activeTab={tab_name}"
        else:
            return base_url + f"&activeTab={tab_name}"