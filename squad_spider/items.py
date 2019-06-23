# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import TakeFirst, MapCompose

def process_dob_string(dob_string):
    return ','.join(dob_string.strip('\n').split(',')[:2])

def strip_newlines_and_spaces(input_data):
    if isinstance(input_data, str):
        return input_data.strip('\n').strip()
    else:
        return input_data

class SquadSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    player_name = scrapy.Field()
    country = scrapy.Field()
    dob = scrapy.Field(input_processor = MapCompose(process_dob_string))
    role = scrapy.Field()
    batting_style = scrapy.Field()
    bowling_style = scrapy.Field()
    height = scrapy.Field(input_processor = MapCompose(strip_newlines_and_spaces))
    batting_stats = scrapy.Field()
    bowling_stats = scrapy.Field()
    experienced = scrapy.Field()
    player_id = scrapy.Field()