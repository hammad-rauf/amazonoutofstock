from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

import csv
import os

class AmazonSpider(CrawlSpider):

    name = "amazon"
 
    def __init__(self,pages,*args, **kwargs):                
        
        if pages == 'all':
            self.pages = 'all'
        else:
            self.pages = int(pages)

        path = os.getcwd()
        
        path = os.path.join(path,"keywords.txt")

        file = open(path,"r")
        links = []
        for link in file.readlines():
            
            link = link.replace(' ','+')
            link = link.replace("\n","")
            links.append(link)
            
            with open(f'{link}.csv', mode='w', newline='', encoding="utf-8") as write_file:
                fields = [
                    'url',
                    'brand',
                    'text',
                    'asin',
                    'ranking'
                ]
                
                writer = csv.DictWriter(write_file, fieldnames=fields)
                writer.writeheader()
                
                
        self.keywords = links
        file.close()  
          
    def start_requests(self):   

        for keyword_data in self.keywords: 

            yield Request(f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/s?k={keyword_data}",callback=self.department,meta={"keyword":keyword_data})


    def department(self,response):
        links = response.css('#departments .a-unordered-list.a-nostyle.a-vertical.a-spacing-medium li  span a:first-child::attr(href)').extract()
        
        texts = response.css('#departments .a-unordered-list.a-nostyle.a-vertical.a-spacing-medium li  span a span::text').extract()

        for link,text in zip(links,texts):
            yield response.follow(f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{link}",callback=self.outofstock,meta={'text':text,"keyword":response.meta['keyword']})

    def outofstock(self,response):
    
        links = response.css('ul[aria-labelledby="p_n_availability-title"] a::attr(href)').extract()

        for link in links:
            yield response.follow(f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{link}",callback=self.star_page,meta={'text':response.meta['text'],"keyword":response.meta['keyword']})

    def star_page(self,response):
        
        link = response.css('#reviewsRefinements a::attr(href)').extract_first()

        yield response.follow(f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{link}",callback=self.page,meta={'pages':1,'text':response.meta['text'],"keyword":response.meta['keyword']})
  
    
    def page(self,response):

        for data in response.css('div[data-component-type="s-search-result"]'):

            link = data.css("span[data-component-type='s-product-image'] a::attr(href)").extract_first()
            
            if data.css('span[aria-label="Currently unavailable."] span::text').extract_first() =='Currently unavailable.':

                yield response.follow(f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{link}",callback=self.product_page,meta={'text':response.meta['text'],"keyword":response.meta['keyword']})
        
        next_page = response.css(".a-last a::attr(href)").extract_first()

        response.meta["pages"] += 1

        if next_page and str(self.pages) == 'all':
            yield response.follow(url = f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{next_page}",callback=self.page,meta={'pages':response.meta["pages"],'text':response.meta['text'],"keyword":response.meta['keyword']}) 
        elif next_page and  (response.meta['pages'] < self.pages):
            yield response.follow(url = f"http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=https://www.amazon.co.uk/{next_page}",callback=self.page,meta={'pages':response.meta["pages"],'text':response.meta['text'],"keyword":response.meta['keyword']}) 


    def product_page(self,response):

        brand = response.css("#bylineInfo::attr(href)").extract_first()

        if not brand:
            brand = response.css(".a-link-normal.qa-byline-url::attr(href)").extract_first()

        if brand:
            brand = f'https://www.amazon.co.uk{brand}'
            
        
        link = response.url
            
        if link:
            link = link.replace("http://api.scraperapi.com/?api_key=eb2ba35b0573870cd1b4e84ea7942874&url=",'')

        
        asin = response.url

        asin = asin.split('/dp/')[1]
        asin = asin.split('/')[0]

        ranking = response.xpath("//th[contains(text(),'Best Sellers Rank')]/parent::tr/td/span/span/text()").extract_first()

        if ranking:
            ranking = ranking.replace('(','')

            BSR = ranking.split('in')[0]

            Category = ranking.split('in')[1]
        else:
            BSR = None
            Category = None
            
        yield{
            'url':link,
            'brand': brand,
            'text':response.meta['text'],
            'asin':asin,
            'BSR':BSR,
            'Category':Category
        }
        with open(f'{response.meta["keyword"]}.csv', mode='a', newline='', encoding="utf-8") as write_file:
            fields = [
                'url',
                'brand',
                'text',
                'asin',
                'BSR',
                'Category'
            ]
            writer = csv.DictWriter(write_file, fieldnames=fields)
            
            writer.writerow({
                'url':link,
                'brand': brand,
                'text':response.meta['text'],
                'asin':asin,
                'BSR':BSR,
                'Category':Category
                })



