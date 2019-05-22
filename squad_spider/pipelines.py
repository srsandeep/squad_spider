# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class SquadSpiderPipeline(object):
    def process_item(self, item, spider):
        #Function to determine whether a player is experienced or not
        item['experienced'] = False
        return item
