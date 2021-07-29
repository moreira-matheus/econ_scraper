
import re
import scrapy
import unicodedata
from datetime import datetime
from locale import atof, setlocale, LC_NUMERIC

setlocale(LC_NUMERIC, 'pt_BR.UTF-8')

def remove_accents(txt):
    return unicodedata.normalize('NFKD', txt).encode('ASCII', 'ignore').decode('utf-8')

def normalize_field(txt):
    return remove_accents(txt).title().replace(' ', '')



class OlxImoveisSpider(scrapy.Spider):
    name = "olximoveis"

    def start_requests(self):
        urls = ['https://www.olx.com.br/imoveis']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.get_state_urls)

    def get_state_urls(self, response):
        states = response.css('a.dlGmlk')
        for state in states:
            state_name = state.css('::text').get()
            state_link = state.attrib['href']

            yield scrapy.Request(url=state_link,\
                                 callback=self.parse_state,\
                                 meta={'state_name': state_name})

    def parse_state(self, response):
        cities = response.css('a[data-lurker-detail="linkshelf_item"]')

        for city in cities:
            city_name = city.css('::text').get()
            city_link = city.attrib['href']
            metadata = dict(response.meta, **{'city_name': city_name})

            yield scrapy.Request(url=city_link,\
                                 callback=self.parse_page,\
                                 meta=metadata)

    def parse_page(self, response):
        ads = response.css('ul#ad-list li a')
        for ad in ads:
            yield scrapy.Request(url=ad.attrib['href'],
                                 callback=self.parse_ad,
                                 meta=response.meta)
        
        # Proxima pagina
        if response.css('a[data-lurker-detail="next_page"]'):
            url = response.css('a[data-lurker-detail="next_page"]::attr(href)').get()
            yield scrapy.Request(url=url,
                                 callback=self.parse_page,
                                 meta=response.meta)

    def parse_ad(self, response):
        item = {}
        item['Estado'] = response.meta.get('state_name', '')
        item['Regiao'] = response.meta.get('city_name', '')
        item['Titulo'] = response.css('h1::text').extract_first()
        
        publ = ''.join(response.css('div.iwtnNi span.fizSrB::text').extract())
        pat = r'(\d{1,2}\/\d{2}) às (\d{1,2}:\d{2}).+(\d{9})'
        try:
            date, time, cod = re.search(pat, publ).groups()
            item['Codigo'] = cod
            dttm = f"{date}/{datetime.now().strftime('%Y')} {time}"
            item['DataHora'] = datetime.strptime(dttm, '%d/%m/%Y %H:%M')

        except Exception as ex:
            item['DataHora'] = None
            item['Codigo'] = None

        figure = response.css('h2::text').extract_first()
        pat = r'(\S+)\s([\d*.?]+\d*)'
        try:
            currency, price = re.search(pat, figure).groups()
            item['Moeda'] = currency
            item['Preco'] = atof(price)

        except Exception as ex:
            item['Moeda'] = None
            item['Preco'] = 0.0

        for field in response.css('dt'):
            field_name = normalize_field(field.css('::text').get())
            field_value = field.xpath('./following-sibling::a/text()').extract_first()
            item[field_name] = field_value

        yield item
