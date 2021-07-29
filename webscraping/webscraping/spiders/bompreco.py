
import re
import json
import scrapy

from dateutil import parser
from datetime import datetime

class BomprecoSpider(scrapy.Spider):
    name = 'bompreco'
    start_url = 'https://www.bompreco.com.br'

    def start_requests(self):
        urls = [BomprecoSpider.start_url]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_homepage)

    def parse_homepage(self, response):
        deps = response.css('nav section ul li a::attr(href)').extract()
        for department in deps:
            url = BomprecoSpider.start_url + department
            yield scrapy.Request(url=url, callback=self.parse_dep)

    def parse_dep(self, response):
        prods_json = response.css('template[data-varname="__STATE__"] script::text').get()
        prods_dict = json.loads(prods_json)
        
        pat = r'Product:sp-\d{3,6}$'
        prods_keys = [key for key in prods_dict if re.search(pat, key)]
        
        for key in prods_keys:
            item = {}
            item['ProdutoId'] = prods_dict[key]['productId']
            item['ProdutoDescricao'] = prods_dict[key]['description']
            item['ProdutoNome'] = prods_dict[key]['productName']
            item['MarcaNome'] = prods_dict[key]['brand']
            item['MarcaId'] = prods_dict[key]['brandId']
            item['Categoria'] = prods_dict[key]['categories']['json']

            # Parsing prices
            price_range_id = prods_dict[key]['priceRange']['id']
            sell_price_id = prods_dict[price_range_id]['sellingPrice']['id']
            list_price_id = prods_dict[price_range_id]['listPrice']['id']

            item['PrecoVendaMax'] = prods_dict[sell_price_id].get('highPrice', None)
            item['PrecoVendaMin'] = prods_dict[sell_price_id].get('lowPrice', None)
            item['PrecoTabelaMax'] = prods_dict[list_price_id].get('highPrice', None)
            item['PrecoTabelaMin'] = prods_dict[list_price_id].get('lowPrice', None)

            # Parsing product info
            extended_key = key + '.items({"filter":"ALL"}).0'
            item['ProdutoCodigEan'] = prods_dict[extended_key].get('ean', None)
            item['ProdutoNomeCompleto'] = prods_dict[extended_key].get('nameComplete', None)
            item['ProdutoUnidadeMedida'] = prods_dict[extended_key].get('measurementUnit', None)
            item['ProdutoUnidadeMultiplicador'] = prods_dict[extended_key].get('unitMultiplier', None)

            image_id = prods_dict[extended_key]['images'][0]['id']
            item['ProdutoImagemUrl'] = prods_dict[image_id].get('imageUrl', None)

            # Parsing seller info
            extended_key = extended_key + '.sellers.0'
            item['VendedorId'] = prods_dict[extended_key]['sellerId']
            item['VendedorNome'] = prods_dict[extended_key]['sellerName']
            item['VendedorPadrao'] = prods_dict[extended_key]['sellerDefault']

            offer_id = prods_dict[extended_key]['commertialOffer']['id']
            item['OfertaPreco'] = prods_dict[offer_id].get('Price', None)
            item['OfertaPrecoTabela'] = prods_dict[offer_id].get('ListPrice', None)
            item['OfertaPrecoAVista'] = prods_dict[offer_id].get('spotPrice', None)
            item['OfertaPrecoSemDesconto'] = prods_dict[offer_id].get('PriceWithoutDiscount', None)
            item['OfertaImpostoQuantia'] = prods_dict[offer_id].get('Tax', 0)
            item['OfertaImpostoTaxa'] = prods_dict[offer_id].get('taxPercentage', 0.0)
            item['OfertaQuantidadeDisponivel'] = prods_dict[offer_id].get('AvailableQuantity', 0)
            item['OfertaValidade'] = parser.parse(prods_dict[offer_id].get('PriceValidUntil'),\
                                                  ignoretz=True, default=datetime.today())

            # Parsing installments info
            installments_key = 'Installments({"criteria":"MAX_WITHOUT_INTEREST"})'
            if prods_dict[offer_id].get(installments_key):
                installments_id = prods_dict[offer_id][installments_key][0]['id']
                item['ParcelaValor'] = prods_dict[installments_id].get('Value', None)
                item['ParcelaJuros'] = prods_dict[installments_id].get('InterestRate', 0.0)
                item['ParcelaMontante'] = prods_dict[installments_id].get('TotalValuePlusInsterestRate', None)
                item['ParcelaNumero'] = prods_dict[installments_id].get('NumberOfInstallments', 1)

            yield item
            