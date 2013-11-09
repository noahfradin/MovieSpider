import urllib.request as request
from urllib.parse import quote as escape
import sys
from bs4 import BeautifulSoup as soup
from json import loads as decode_json
import csv

all_films = "http://www.imdb.com/list/p9TV548tyOw/?start%s1&view=detail&sort=listorian:asc"
poster_base = "http://www.imdb.com/title/%s/"
item_selector = '.list_item'
title_selector = '.info b a'
poster_selector = 'img'
date_selector = '.year_type'
youtube_request_base = "https://gdata.youtube.com/feeds/api/videos?alt=json&q=%s&max-results=1"
start_params = range(1, 9901, 100)
var = None

#get (title, IMBD ID, poster url, date, youtube ID) tuples
def get_chunk_data(chunk_url):
    parser = soup(get_html(chunk_url))
    return map(get_data, select_items(parser))
    
def get_data(item):
    title = get_title(item)
    date = get_date(item)
    return (title, get_id(item), get_poster_url(item), date, get_trailer_Yid(title, date))
    
def select_items(parser):
    return parser.select(item_selector)        
    
def get_title(item):
    return item.select(title_selector)[0].text
    
def get_id(item):
    return item.find('a')['href'].split('/')[-2]
    
def get_poster_url(item):
    return item.select(poster_selector)[0]['src']

def get_date(item):
    return item.select(date_selector)[0].text.strip('()')

def get_trailer_Yid(title, date):
    return json_to_Yid(decode_json(youtube_request(title, date)))
    
def json_to_Yid(api_json):
    return api_json['feed']['entry'][0]['id']['$t'].split('/')[-1]
    
def youtube_request(title, date):
    return request.urlopen(make_query(title, date)).read().decode('utf8')
    
def make_query(title, date):
    return youtube_request_base%escape("%s official trailer %s"%(title, date))
    
    
    
def get_html(chunk_url):
    return request.urlopen(chunk_url)
    
def url_gen():
    for start in start_params:
        yield all_films%start
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        out = sys.stdout
    else:
        out = open(sys.argv[1],'w')
    writer = csv.writer(out)
    for url in url_gen():
        for record in get_chunk_data(url):
            writer.writerow(record)