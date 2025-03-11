import scrapy
from scrapy.http import FormRequest
# import logging
import json
import os
import re
from glenigan.items import ApplicationItem
from glenigan.logger_config import logger

class CouncilScraper(scrapy.Spider):
    name = "plan"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load council details from JSON
        json_path = r"C:\Users\naafiah.fathima\Desktop\glenigan_scrapy\glenigan\glenigan\councils.json"
        logger.info(f"Loading council details from: {json_path}")

        if not os.path.exists(json_path):
            logger.error(f"Councils JSON file not found at: {json_path}")
            raise FileNotFoundError(f"Councils JSON file not found at: {json_path}")

        with open(json_path, "r") as file:
            self.councils = json.load(file)

    def start_requests(self):
        """Start scraping for each council from JSON file."""
        for council_name, council_info in self.councils.items():
            logger.info(f"Initiating request for council: {council_name} ({council_info['code']})")
            yield scrapy.Request(
                url=council_info["url"],
                callback=self.parse,
                meta={"council_name": council_name, "council_code": council_info["code"], "url": council_info["url"]},
            )

    def parse(self, response):
        """Extract CSRF token and submit form request."""
        csrf_token = response.xpath('//form[@id="advancedSearchForm"]//input[@name="_csrf"]/@value').get()
        logger.info(f"CSRF token : {csrf_token}")

        if not csrf_token:
            logger.error(f"CSRF token missing for {response.meta['council_name']}, aborting!")
            return

        form_data = {
            "_csrf": csrf_token,
            "date(applicationValidatedStart)": "18/02/2025",
            "date(applicationValidatedEnd)": "20/02/2025",
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
            logger.info(f"No applications found for {response.meta['council_name']}.")
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
