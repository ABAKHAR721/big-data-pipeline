import scrapy
import re
import random
from scrapy_project.items import DynamicDataItem

class ArxivSpider(scrapy.Spider):
    name = "arxiv_spider"
    allowed_domains = ["arxiv.org"]
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'CLOSESPIDER_ITEMCOUNT': 1000,
        'DOWNLOAD_DELAY': 2.5,
        'USER_AGENT': 'Mozilla/5.0 (compatible; ResearchBot/1.0)'
    }

    LOCATIONS = [
        {"country": "USA", "city": "New York", "lat": 40.7128, "lon": -74.0060, "aff": "NYU"},
        {"country": "USA", "city": "San Francisco", "lat": 37.7749, "lon": -122.4194, "aff": "Stanford University"},
        {"country": "France", "city": "Paris", "lat": 48.8566, "lon": 2.3522, "aff": "Sorbonne Université"},
        {"country": "France", "city": "Lyon", "lat": 45.7640, "lon": 4.8357, "aff": "INSA Lyon"},
        {"country": "UK", "city": "London", "lat": 51.5074, "lon": -0.1278, "aff": "Imperial College"},
        {"country": "China", "city": "Beijing", "lat": 39.9042, "lon": 116.4074, "aff": "Tsinghua University"},
        {"country": "Morocco", "city": "Casablanca", "lat": 33.5731, "lon": -7.5898, "aff": "Hassan II University"},
        {"country": "Germany", "city": "Berlin", "lat": 52.5200, "lon": 13.4050, "aff": "TU Berlin"},
        {"country": "India", "city": "Bangalore", "lat": 12.9716, "lon": 77.5946, "aff": "IISc Bangalore"}
    ]

    def start_requests(self):
        keywords = ['Blockchain', 'Deep Learning', 'Big Data']
        base_url = "https://arxiv.org/search/?query={}&searchtype=all&source=header"
        
        for kw in keywords:
            url = base_url.format(kw)
            yield scrapy.Request(url, callback=self.parse, meta={'keyword': kw, 'count': 0})

    def parse(self, response):
        results = response.css('li.arxiv-result')
        current_count = response.meta.get('count', 0)
        keyword = response.meta['keyword']
        
        MAX_PER_KEYWORD = 350

        if current_count >= MAX_PER_KEYWORD:
            return

        for result in results:
            if current_count >= MAX_PER_KEYWORD:
                break

            item = DynamicDataItem()
            
            # Real title
            raw_title = result.css('p.title.is-5::text').get()
            item['title'] = raw_title.strip() if raw_title else None
            
            # Year filter (2015-2025)
            date_text = result.css('p.is-size-7::text').get()
            item['year'] = "Unknown"
            valid_year = False
            
            if date_text:
                year_match = re.search(r'(19|20)\d{2}', date_text)
                if year_match:
                    year_int = int(year_match.group(0))
                    if 2015 <= year_int <= 2025:
                        item['year'] = str(year_int)
                        valid_year = True
            
            if not valid_year:
                continue

            item['keyword'] = keyword
            item['source'] = 'arXiv'
            item['data_type'] = 'research_paper'
            
            # Authors
            authors_list = result.css('p.authors a::text').getall()
            item['authors'] = ', '.join(authors_list) if authors_list else 'Unknown'
            
            # Abstract
            abstract_text = result.css('span.abstract-full::text').get()
            if not abstract_text:
                abstract_text = result.css('p.abstract span::text').get()
            item['abstract'] = abstract_text.strip() if abstract_text else 'No abstract available'
            
            # Randomize location
            loc_data = random.choice(self.LOCATIONS)
            item['country'] = loc_data['country']
            item['city'] = loc_data['city']
            item['latitude'] = loc_data['lat']
            item['longitude'] = loc_data['lon']
            item['affiliation'] = loc_data['aff']

            if item['title']:
                current_count += 1
                yield item
        
        # Pagination
        if current_count < MAX_PER_KEYWORD:
            next_page = response.css('a.pagination-next::attr(href)').get()
            if next_page:
                response.meta['count'] = current_count
                yield scrapy.Request(response.urljoin(next_page), callback=self.parse, meta=response.meta)