import scrapy
from scrapy.spiders import CrawlSpider, Rule, Request
from scrapy.linkextractors import LinkExtractor
import urllib
from squad_spider.items import SquadSpiderItem, strip_newlines_and_spaces
import pandas as pd
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst
from player_from_scorecard.players_from_commentary import ExtractPlayers

# class CrawlSquadSpider(CrawlSpider):

#     name = 'crawl_squad_spider'
    
#     start_urls = ['http://www.espncricinfo.com/ci/content/squad/index.html?object=1144415']
#     rules = (Rule(LinkExtractor(allow=r'ci\/content\/squad'), callback='parse_page'),)

#     def parse_teams(self, response):

#         players = response.xpath('//*/h1[contains(text(), "Squad / Players")]/../div/ul/li/div/h3/a/text()').re(r'\s*([A-Za-z \t]+)\s*')
#         country = response.xpath('//*/h1[contains(text(), "Squad / Players")]/text()').re(r'\s*([A-Za-z \t]+)\s+Squad \/ Players')[0]
#         self.logger.info('***********************************')
#         self.logger.info(f'{country} represented by {players}')
        
#     def parse_page(self, response):

#         self.logger.info(f'Received response for {response.url}')

#         participant_nations = response.xpath('//*/li/h2[contains(text(), "ICC Cricket World Cup, 2019")]/../span/a/text()').extract()
#         nation_links = response.xpath('//*/li/h2[contains(text(), "ICC Cricket World Cup, 2019")]/../span/a/@href').extract()
#         url = "http://www.espncricinfo.com"
#         for idx, each_link in enumerate(nation_links):
#             self.logger.info(f'Retriving the data for team {participant_nations[idx]} from ' + urllib.parse.urljoin(url, each_link))
#             yield Request(urllib.parse.urljoin(url, each_link), callback='parse_teams')

filename = 'player_data.txt'

class CrawlSquadSpider(scrapy.Spider):

    name = 'crawl_squad_spider'

    def start_requests(self):
        start_urls = ['http://www.espncricinfo.com/ci/content/squad/index.html?object=1144415']
        start_urls = self.get_all_player_urls() + start_urls
        for url in start_urls:
            if 'squad' in url:
                yield scrapy.Request(url=url, callback=self.parse_page)
            else:
                yield scrapy.Request(url=url, callback=self.parse_player)

    def get_all_player_urls(self):
        extract_obj = ExtractPlayers('/home/sandeep/projects/wc2019/data')
        return extract_obj.get_player_links()

    def parse_teams(self, response):

        players = response.xpath('//*/h1[contains(text(), "Squad / Players")]/../div/ul/li/div/h3/a/text()').re(r'\s*([A-Za-z \t]+)\s*')
        country = response.xpath('//*/h1[contains(text(), "Squad / Players")]/text()').re(r'\s*([A-Za-z \t]+)\s+Squad \/ Players')[0]
        player_links = response.xpath('//*/h1[contains(text(), "Squad / Players")]/../div/ul/li/div/h3/a/@href').extract()
        url = "http://www.espncricinfo.com"
        player_links = [urllib.parse.urljoin(url, each_link) for each_link in player_links]
        self.logger.info('***********************************')
        self.logger.info(f'{country} represented by {players}')
        for idx, each_player in enumerate(player_links):
            self.logger.info(f'{country}: {players[idx]} - {each_player}')
            yield scrapy.Request(url=each_player, callback=self.parse_player)
        
    def parse_page(self, response):

        self.logger.info(f'Received response for {response.url}')

        participant_nations = response.xpath('//*/li/h2[contains(text(), "ICC Cricket World Cup, 2019")]/../span/a/text()').extract()
        nation_links = response.xpath('//*/li/h2[contains(text(), "ICC Cricket World Cup, 2019")]/../span/a/@href').extract()
        url = "http://www.espncricinfo.com"
        for idx, each_link in enumerate(nation_links):
            self.logger.info(f'Retriving the data for team {participant_nations[idx]} from ' + urllib.parse.urljoin(url, each_link))
            yield Request(urllib.parse.urljoin(url, each_link), callback=self.parse_teams)

    def parse_player(self, response):

        player_dict = {}
        for each_selector in response.xpath('//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]'):
            player_dict[each_selector.xpath('.//b/text()').extract()[0]] = \
                            each_selector.xpath('.//span/text()').extract()

        player_loader = ItemLoader(item=SquadSpiderItem(), selector=response)
        player_loader.default_input_processor = MapCompose(strip_newlines_and_spaces)
        player_loader.default_output_processor = TakeFirst()
        player_loader.add_xpath('player_name', '//h1/text()')
        player_loader.add_xpath('country', '//*[@id="ciHomeContentlhs"]/div/div/div/h3/b/text()')
        player_loader.add_xpath('dob', '//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]/b[contains(text(), "Born")]/../span/text()')
        player_loader.add_xpath('role', '//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]/b[contains(text(), "Playing Role")]/../span/text()')
        player_loader.add_xpath('batting_style', '//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]/b[contains(text(), "Batting style")]/../span/text()')
        player_loader.add_xpath('bowling_style', '//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]/b[contains(text(), "Bowling style")]/../span/text()')
        player_loader.add_xpath('height', '//*[@id="ciHomeContentlhs"]/div/div/div/p[@class="ciPlayerinformationtxt"]/b[contains(text(), "Height")]/../span/text()')

        for each_table in response.xpath('//*[@class="engineTable"]'):
            header = values = []
            if len(each_table.xpath('./thead')) != 0:
                header = each_table.xpath('./thead/tr/th/text()').extract()
                header = [ '-' ] + header
                for each_row in each_table.xpath('./tbody/tr'):
                    values.append(each_row.xpath('./td/b/text()').extract() + 
                                    each_row.xpath('./td/text()').extract())
            if 'HS' in header:
                batting_avg = pd.DataFrame(values, columns=header)
                batting_avg.set_index('-', inplace=True)
                player_loader.add_value('batting_stats', batting_avg.to_dict())
            elif 'Wkts' in header:
                bowling_avg = pd.DataFrame(values, columns=header)
                bowling_avg.set_index('-', inplace=True)
                player_loader.add_value('bowling_stats', bowling_avg.to_dict())
        yield player_loader.load_item()
