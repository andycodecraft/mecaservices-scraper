import scrapy,os,platform,re
from crawldata.functions import *
from datetime import datetime

class CrawlerSpider(scrapy.Spider):
    name = 'rolmax'
    DATE_CRAWL=datetime.now().strftime('%Y-%m-%d')
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8','Accept-Language': 'en-GB,en;q=0.5','Connection': 'keep-alive','Upgrade-Insecure-Requests': '1','Sec-Fetch-Dest': 'document','Sec-Fetch-Mode': 'navigate','Sec-Fetch-Site': 'none','Sec-Fetch-User': '?1','Priority': 'u=0, i'}
    headers_post = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0','Accept': '*/*','Accept-Language': 'en-GB,en;q=0.5','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','X-Requested-With': 'XMLHttpRequest','Origin': 'https://www.techniekwebshop.nl','Connection': 'keep-alive','Sec-Fetch-Dest': 'empty','Sec-Fetch-Mode': 'cors','Sec-Fetch-Site': 'same-origin','Priority': 'u=0'}
    if platform.system()=='Linux':
        URL='file:////' + os.getcwd()+'/scrapy.cfg'
    else:
        URL='file:///' + os.getcwd()+'/scrapy.cfg'
    domain='https://www.rolmax-sklep.pl/'
    page_urls=[]

    def start_requests(self):
        self.get_db_data()
        yield scrapy.Request(self.domain,callback=self.parse_categories,dont_filter=True)

    def parse_categories(self,response):
        links = response.xpath('//a[@class="categories__link"]/@href').getall()

        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_subcategories, dont_filter=True)
    
    def parse_subcategories(self, response):
        # links = response.xpath('//ul[@class="side-menu__categories"]//li[@class="active"]/ul[@class="side-menu__subcategories"]/li/a/@href').getall()
        # if not links:
        link = response.url
        # self.parse_list(response)

        last_page_index = response.xpath('//ul[@class="list-nav__item pages"]/li[position()=last()-1]/a/text()').extract_first()

        if last_page_index:
            page_count = int(last_page_index)
            for i in range(page_count):
                next_link = ''
                if i > 0:
                    next_link = link.replace('.html', '')
                    next_link = f"{next_link},s{i}.html"
                else:
                    next_link = link
                
                if next_link not in self.page_urls:
                    yield scrapy.Request(next_link, callback=self.parse_list, dont_filter=True)

    def parse_list(self, response):
        links = response.xpath('//a[@class="product__name-link"]/@href').getall()
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_data, dont_filter=True)

    def parse_data(self, response):
        item={}

        item['additional_images'] = []
        item['base_image'] = ''
        item['brand'] = ''
        item['breadcrumb'] = ''
        item['shipping_time'] = ''
        item['description'] = ''
        item['manufacturer'] = ''
        item['name'] = ''
        item['part_number'] = ''
        item['price'] = 0.00
        item['price_currency'] = ''
        item['qty'] = 0
        item['sku'] = ''
        item['thumbnail_image'] = ''
        item['discount_price'] = 0.00
        item['reviews'] = []
        item['guarantee_months'] = 0
        item['review_number'] = 0

        base_image = response.xpath('//img[@class="product-cart__image" and @itemprop="image"]/@src').extract_first()
        if base_image:
            item['base_image'] = response.urljoin(base_image)
            item['thumbnail_image'] = response.urljoin(base_image)
            item['small_image'] = response.urljoin(base_image)

        additional_image_links = response.xpath('//img[@class="product-cart__gallery-image"]/@rel').getall()
        for additional_image in additional_image_links:
            item['additional_images'].append(response.urljoin(additional_image))
        
        if item['base_image'] in item['additional_images']:
            item['additional_images'].remove(item['base_image'])

        brand = response.xpath('//a[@class="product-cart__producer-link"]/span//text()').extract_first()
        if not brand:
            brand = 'unbranded'

        item['brand'] = brand
        item['manufacturer'] = brand
        
        breadcrumb_all = response.xpath('//ol[@class="breadcrumbs__list"]/li[position() > 1]/a/span/text()').getall()
        if breadcrumb_all:
            breadcrumb = "/".join(breadcrumb_all)
            item['breadcrumb'] = breadcrumb
        
        shipping_time = response.xpath('//div[@class="product-cart__item1" and text()="Czas wysyłki:"]/following-sibling::div[1]//text()').extract_first()
        if shipping_time:
            item['shipping_time'] = shipping_time
        
        shipping_cost = response.xpath('//div[@class="product-cart__item1" and text()="Koszty dostawy:"]/following-sibling::div[1]//text()').getall()
        if shipping_cost:
            item['shipping_cost'] = "".join(shipping_cost)

        description = response.xpath('//div[contains(@class, "desc-text--main")]//text()').getall()
        if description:
            item['description'] = re.sub(r'<!--.*?-->|Rozwiń opis', '', "".join(description), flags=re.DOTALL).strip()

        name = response.xpath('//h1[@class="product-cart__name"]//text()').extract_first()
        if name:
            item['name'] = name

        item['original_page_url'] = response.url
        
        part_number = response.xpath('//*[contains(text(), "Kod producenta:")]/text()').extract_first()
        if part_number:
            part_number = part_number.replace("Kod producenta: ", "")
            part_number = re.sub(r'[^A-Za-z0-9]', '', part_number)
            item['part_number'] = part_number
        
        price = response.xpath('//span[@itemprop="price"]/text()').extract_first()
        if price:
            item['price'] = float(price)

        price_currency = response.xpath('//span[@itemprop="priceCurrency"]/text()').extract_first()
        if price_currency:
            item['price_currency'] = price_currency

        qty = response.xpath('//input[@class="shopping-box__quantity-input"]/@value').extract_first()
        if qty:
            item['qty'] = int(qty)

        if part_number and brand:
            sku = f"{item['brand']}-{item['part_number']}"
            sku = sku.lower().replace(' - ', '-')
            item['sku'] = sku.replace(' ', '-')
        
        guarantee_months = response.xpath('//div[@class="product-cart__item1" and text()="Gwarancja:"]/following-sibling::div[1]//text()').extract_first()
        if guarantee_months:
            if ' miesiące' in guarantee_months:
                guarantee_months = guarantee_months.replace(' miesiące', '')
                if guarantee_months:
                    item['guarantee_months'] = int(guarantee_months)
        
        discount_price = response.xpath('//span[@class="pnetto"]//text()').extract_first()
        if discount_price:
            match = re.search(r'(\d+\.\d+)', discount_price)
            if match:
                discount_price = match.group(0)
                item['discount_price'] = discount_price
        
        review_number = response.xpath('//span[@itemprop="reviewCount"]//text()').extract_first()
        if review_number:
            item['review_number'] = review_number

        reviews_data = response.xpath('//li[@class="reviews__list-item"]')
        for review_data in reviews_data:
            it = {}
            author = review_data.xpath('.//span[@class="reviews__review-author"]//text()').extract_first()
            score = review_data.xpath('.//span[@class="rating__color-stars"]/@data-rating').extract_first()
            text = review_data.xpath('.//span[@class="reviews__review-txt"]//text()').extract_first()
            date = review_data.xpath('.//span[@class="reviews__review-date"]//text()').extract_first()
            
            it['author'] = author
            it['score'] = score
            it['text'] = text
            it['date'] = date

            item['reviews'].append(it)
        
        id = response.xpath('//meta[contains(@itemprop, "sku")]/@content').extract_first()
        item['original_id']=key_MD5(item['breadcrumb'])+'_'+str(id)

        yield item

    def get_db_data(self):
        with open('rolmax_dump.csv', 'r') as f:
            self.page_urls = {url.strip().strip('"') for url in f}
        
        print("sdfsdfsdfsdfsdf")
        

