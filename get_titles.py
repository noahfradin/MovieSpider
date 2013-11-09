#Spider to aggregate information on 10,000 movie titles
#author: Anson Rosenthal (anrosent)
#
#Crawls IMDB list "All U.S. Released Movies: 1970-2014", aggregating following data for each title:
#   -title
#   -IMDB unique ID
#   -URL of poster for movie, hosted on IMDB
#   -Year of release
#   -Youtube unique ID

import urllib.request as request
from urllib.parse import quote as escape
import sys
from bs4 import BeautifulSoup as soup
from json import loads as decode_json
import csv

all_films = "http://www.imdb.com/list/p9TV548tyOw/?start=%s&view=detail&sort=listorian:asc"
poster_base = "http://www.imdb.com/title/%s/"
item_selector = '.list_item'
title_selector = '.info b a'
poster_selector = 'img'
date_selector = '.year_type'
youtube_request_base = "https://gdata.youtube.com/feeds/api/videos?alt=json&q=%s&max-results=1"
start_params = range(1, 9901, 100)

#get (title, IMBD ID, poster url, date, youtube ID) tuples
def get_chunk_data(chunk_url):
    parser = soup(get_request(chunk_url))
    return map(get_data, select_items(parser))
    
#get all relevant data for each item in the list on IMDB    
def get_data(item):
    title = get_title(item)
    date = get_date(item)
    return (title, get_id(item), get_poster_url(item), date, get_trailer_Yid(title, date))
    
#get the relevant HTML nodes for the list items    
def select_items(parser):
    return parser.select(item_selector)        
    
#get title from node    
def get_title(item):
    return item.select(title_selector)[0].text
    
#get IMDB unique ID for list item    
def get_id(item):
    return item.find('a')['href'].split('/')[-2]
    
#get url for the movie poster    
def get_poster_url(item):
    return item.select(poster_selector)[0]['src']

#get year of release    
def get_date(item):
    return item.select(date_selector)[0].text.strip('()')

#get Youtube unique ID using request to Youtube API    
def get_trailer_Yid(title, date):
    return json_to_Yid(decode_json(youtube_request(title, date)))
    
#extract Youtube unique ID from JSON Youtube API response    
def json_to_Yid(api_json):
    return api_json['feed']['entry'][0]['id']['$t'].split('/')[-1]
    
#send request to Youtube API using movie title and year of release    
def youtube_request(title, date):
    return request.urlopen(make_query(title, date)).read().decode('utf8')

#formulate Youtube API query parameters:
#   -Search for <title> official trailer <year of release>
#   -limit results to just top (ordered by relevance)
#   -return response in JSON format
def make_query(title, date):
    return youtube_request_base%escape("%s official trailer %s"%(title, date))
    
############################################################

#make http GET request
def get_request(chunk_url):
    return request.urlopen(chunk_url)

#crawl each page of the list by varying the start query parameter to the IMDB site    
def url_gen():
    for start in start_params:
        yield all_films%start
        
#run the spider, writing records in CSV format to standard out or provided file        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        out = sys.stdout
    else:
        out = open(sys.argv[1],'w')
    writer = csv.writer(out)
    for url in url_gen():
        for record in get_chunk_data(url):
            writer.writerow(record)