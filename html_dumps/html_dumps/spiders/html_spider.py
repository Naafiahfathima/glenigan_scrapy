import scrapy
from html_dumps.items import HtmlScraperItem
import pymysql
import configparser
import os

class HtmlSpider(scrapy.Spider):
    name = "html_spider"
    
    def __init__(self, *args, **kwargs):
        super(HtmlSpider, self).__init__(*args, **kwargs)
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
        """Fetch URLs from MySQL and start Scrapy requests."""
        urls_to_scrape = self.fetch_urls_from_db()
        for ref_no, url in urls_to_scrape:
            yield scrapy.Request(
                url=url, 
                callback=self.parse, 
                meta={'ref_no': ref_no, 'base_url': url, 'all_html_content': ""},
                dont_filter=True
            )

    def fetch_urls_from_db(self):
        """Retrieve URLs from the database where `scrape_status = 'No'`."""
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor()
        
        cursor.execute("SELECT ref_no, Url FROM applications WHERE scrape_status = 'No'")
        rows = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return rows

    def parse(self, response):
        """Fetch main page and initiate sequential tab scraping."""
        ref_no = response.meta['ref_no']
        base_url = response.meta['base_url']

        # Store main page content
        all_html_content = f"\n<!-- Main Page -->\n{response.text}"

        # Start scraping the first tab
        first_tab_url = self.construct_tab_url(base_url, self.tabs[0])
        self.logger.info(f"Fetching first tab: {self.tabs[0]} -> {first_tab_url}")

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
        """Process each tab sequentially and save after all tabs are scraped."""
        ref_no = response.meta["ref_no"]
        all_html_content = response.meta["all_html_content"]
        tab_index = response.meta["tab_index"]
        base_url = response.meta["base_url"]

        # Append current tab's content
        tab_name = self.tabs[tab_index]
        self.logger.info(f"Scraped tab: {tab_name}")
        all_html_content += f"\n<!-- Tab: {tab_name} -->\n{response.text}"

        # Move to the next tab
        next_tab_index = tab_index + 1
        if next_tab_index < len(self.tabs):
            next_tab_url = self.construct_tab_url(base_url, self.tabs[next_tab_index])
            self.logger.info(f"Fetching next tab: {self.tabs[next_tab_index]} -> {next_tab_url}")
            
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
            # Save the final HTML file
            sanitized_ref_no = ref_no.replace("/", "_")
            filename = os.path.join("html_dumps", f"{sanitized_ref_no}.html")

            with open(filename, "w", encoding="utf-8") as file:
                file.write(all_html_content)

            self.logger.info(f"✅ Saved: {filename}")

            # Send item to pipeline to update MySQL
            item = HtmlScraperItem()
            item['ref_no'] = ref_no
            item['url'] = response.url
            item['html_content'] = all_html_content
            yield item

    def construct_tab_url(self, base_url, tab_name):
        """Constructs the correct tab URL."""
        if "activeTab=" in base_url:
            return base_url.split("activeTab=")[0] + f"activeTab={tab_name}"
        else:
            return base_url + f"&activeTab={tab_name}"
