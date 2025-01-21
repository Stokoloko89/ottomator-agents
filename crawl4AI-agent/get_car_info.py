# import asyncio
# import json
# from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
# from crawl4ai.extraction_strategy import JsonCssExtractionStrategy


# # This function extracts the car information from the carmag website.
# async def main():
#     """Crawl multiple URLs in parallel with a concurrency limit."""
#     browser_config = BrowserConfig(
#         headless=True,
#     )

#     # Minimal schema for repeated items
#     schema = {
#         "name": "Car Info",
#         "baseSelector": "div.wrap",
#         "fields": [
#             {
#                 "name": "title",
#                 "selector": "h1.post-title.entry-title",
#                 "type": "text"
#             },
#             {
#                 "name": "published_date",
#                 "selector": "time.published",
#                 "type": "text",
#                 "attribute": "datetime",
#                 "format": "date"
#             },
#             {
#                 "name": "article_content",
#                 "selector": "div#article-content",
#                 "type": "text"
#             }
#         ]
#     }

#     crawl_config = CrawlerRunConfig(
#         # exclude links
#         exclude_external_links=True,

#         # No caching for demonstration
#         cache_mode=CacheMode.BYPASS,

#         # Extraction strategy
#         extraction_strategy=JsonCssExtractionStrategy(schema),

#         scan_full_page=True,
#     )

#     # Create the crawler instance
#     crawler = AsyncWebCrawler(config=browser_config)
#     await crawler.start()

#     # all the car urls for each car model will be stored in this list
#     car_urls = []
#     max_page_number = 0

#     try:
#         # Get max page number from first page only
#         result = await crawler.arun("https://www.carmag.co.za/new-models/",
#                                     config=crawl_config,
#                                     session_id="session1")
#         if result.success:
#             internal_links = result.links.get("internal", [])
#             max_page_number = max((int(link['href'].split("/")[-2])
#                                    for link in internal_links
#                                    if "new-models" in link['href'] and "page" in link['href']),
#                                   default=0) - 350

#         # Gather URLs in batches of 10 concurrent requests
#         urls_to_crawl = [f"https://www.carmag.co.za/news/new-models/page/{i}/"
#                          for i in range(1, max_page_number+1)]

#         car_urls = []
#         batch_size = 10
#         for i in range(0, len(urls_to_crawl), batch_size):
#             batch = urls_to_crawl[i:i+batch_size]
#             results = await asyncio.gather(*(crawler.arun(url) for url in batch))

#             for result in results:
#                 if result.success:
#                     internal_links = result.links.get("internal", [])
#                     valid_links = [link['href'] for link in internal_links
#                                    if "new-models" in link['href']
#                                    and not any(x in link['href'] for x in ["page", "#"])
#                                    and not link['href'].endswith('new-models/')]
#                     car_urls.extend(valid_links)

#         print(f"Found {len(car_urls)} car URLs")

#         # Process car data in batches
#         all_car_data = []
#         batch_size = 10  # Increased for better throughput
#         for i in range(0, len(car_urls), batch_size):
#             batch = car_urls[i:i+batch_size]
#             try:
#                 results = await asyncio.gather(
#                     *(crawler.arun(url, config=crawl_config) for url in batch),
#                     return_exceptions=True
#                 )
#                 print(f"Processing batch {i//batch_size + 1}/{len(car_urls)//batch_size + 1}")

#                 for result in results:
#                     if result.success and result.extracted_content:
#                         data = json.loads(result.extracted_content)
#                         all_car_data.extend(data)
#             except Exception as e:
#                 print(f"Error processing batch starting at index {i}: {e}")
#                 continue

#         print(f"Extracted information for {len(all_car_data)} cars")
#         if all_car_data:
#             print("Sample extracted item:", json.dumps(
#                 all_car_data[0], indent=2))

#     except Exception as e:
#         print(f"An error occurred: {e}")

#     finally:
#         await crawler.close()

# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import logging
from typing import List, Optional, Dict
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from tenacity import retry, stop_after_attempt, wait_exponential
from asyncio import Semaphore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CarCrawler:
    def _get_schema(self) -> dict:
        return {
            "name": "Car Info",
            "baseSelector": "div.wrap",
            "fields": [
                {
                    "name": "title",
                    "selector": "h1.post-title.entry-title",
                    "type": "text"
                },
                {
                    "name": "published_date",
                    "selector": "time.published",
                    "type": "text",
                    "attribute": "datetime",
                    "format": "date"
                },
                {
                    "name": "article_content",
                    "selector": "div#article-content",
                    "type": "text"
                }
            ]
        }

    def __init__(self, max_concurrent: int = 2):
        self.semaphore = Semaphore(max_concurrent)
        self.browser_config = BrowserConfig(
            headless=True,
            extra_args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins",
                "--ignore-certificate-errors",
                "--no-first-run",
                "--window-size=1920,1080"
            ]
        )
        
        self.crawl_config = CrawlerRunConfig(
            exclude_external_links=True,
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=JsonCssExtractionStrategy(self._get_schema()),
            page_timeout=60000,  # Increased timeout
            wait_until="load",   # Changed from networkidle
        )

    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=2, min=4, max=20))
    async def _fetch_page(self, crawler: AsyncWebCrawler, url: str) -> Optional[dict]:
        async with self.semaphore:
            try:
                result = await crawler.arun(
                    url, 
                    config=self.crawl_config,
                    session_id=f"session_{hash(url)}"
                )
                if not result.success:
                    logger.warning(f"Failed to fetch {url}: {result.error_message}")
                    return None
                return result
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                raise

    async def crawl(self):
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            base_url = "https://www.carmag.co.za/news/new-models/"
            articles = []
            page = 1
            
            while page < 5:  # Limit for testing
                url = f"{base_url}page/{page}/" if page > 1 else base_url
                try:
                    # Fetch page with increased delay between retries
                    await asyncio.sleep(page * 2)  # Progressive delay
                    result = await self._fetch_page(crawler, url)
                    
                    if result and result.links:
                        article_urls = [
                            link['href'] for link in result.links.get("internal", [])
                            if "new-models" in link['href'] 
                            and not any(x in link['href'] for x in ["page", "#"])
                        ]
                        
                        # Process articles with delay
                        for article_url in article_urls:
                            await asyncio.sleep(3)  # Increased delay
                            article_data = await self.process_article(crawler, article_url)
                            if article_data:
                                articles.append(article_data)
                                logger.info(f"Processed article: {article_url}")
                    
                    page += 1
                except Exception as e:
                    logger.error(f"Error on page {page}: {str(e)}")
                    await asyncio.sleep(10)  # Longer delay on error
                    continue
                    
    async def process_article(self, crawler: AsyncWebCrawler, url: str) -> Optional[Dict]:
        try:
            result = await self._fetch_page(crawler, url)
            if result and result.extracted_content:
                return result.extracted_content
            return None
        except Exception as e:
            logger.error(f"Error processing article {url}: {str(e)}")
            return None

async def main():
    crawler = CarCrawler(max_concurrent=2)
    articles = await crawler.crawl()
    logger.info(f"Successfully crawled {len(articles)} articles")
    if articles:
        logger.info(f"Sample article: {articles[0]}")

if __name__ == "__main__":
    asyncio.run(main())