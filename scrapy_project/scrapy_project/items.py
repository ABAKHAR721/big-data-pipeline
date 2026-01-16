import scrapy

class DynamicDataItem(scrapy.Item):
    # Core fields for any data type
    title = scrapy.Field()
    source = scrapy.Field() 
    timestamp = scrapy.Field()
    data_type = scrapy.Field()
    
    # Research paper fields (arXiv)
    authors = scrapy.Field()
    year = scrapy.Field()
    abstract = scrapy.Field()
    keyword = scrapy.Field()
    affiliation = scrapy.Field()
    country = scrapy.Field()
    city = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    
    # Quote fields (backward compatibility)
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()
    
    # Generic fields for future data types
    category = scrapy.Field()
    description = scrapy.Field()
    url = scrapy.Field()
    metadata = scrapy.Field()
