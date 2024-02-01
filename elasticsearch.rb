# aggs examples
# resource: https://logz.io/blog/elasticsearch-aggregations/#keys

# returns a doc count in the entertainment category
client.search(index:'news_headlines', body: { 
    aggs: {
        "entertainment_filter": {
            "filter": {
                "term": {
                    "category": "ENTERTAINMENT"
                }
            }
        }
    }
}
)

client.search(index:'news_headlines', body: {
    query: {
        "range": {
            "date": {
                "gte": "2020-06-20"
                "lte": "2021-06-20"
            }
        }
    }
}
)

# query to find non-fiction items and aggregate into common terms found in "Name" into buckets.
client.search(index:"a_b_v2", body: {
    query: {
        "match": { 
            "Name": {
                query: "habits"
            }     
        }
    },
    aggs: {
            "habits": {
                "terms": {
                    "field": "Name.keyword"
                }
            }

        }

    }
)



client.search(index:'amazon_book_bestsellers', body: {
    query: {
        "match": {
            "Name": {
                query: "The American",
                # operator: "and"
            }
        }
    }
})

# stored in genre_aggs variable
client.search(index:'amazon_book_bestsellers', body: {
    aggs: {
        "by_genre": {
            "terms": {
                "field": "Genre"
            }
        }
    }
})



mapping_settings = {
    properties: {
        "Author": { type: "text",
            fields: {
                keyword: {
                    type: "keyword"
                }
            }
        },                                          
        "Genre": {type: "keyword"},                                        
        "Name": {type: "text",
            fields: {
                keyword: {
                    type: "keyword"
                }
            }},                                            
        "Price": {type: "long"},                                           
        "Reviews": {type: "long"},                                         
        "User Rating": {type: "double"},                                   
        "Year": {type: "long"}

}
}                                                  

client.indices.create(index:"a_b_v2", body: {mappings: mapping_settings})

client.indices.get_mapping(index:"a_b_v2")

# updates items index
client.update(index:"books",id: "9jZOLY0BG7D_cr39Yg7P", body: {doc: { "author": "ASHTON"}})

# delete items index
client.delete(index: "books", id: "9jZOLY0BG7D_cr39Yg7P")

# create items in index
client.index(index: "books", body: {"title": "TEST BOOK"})

reindex_query = {
    source: {
        index: "amazon_book_bestsellers",
    },
    dest: {
        index: "a_b_v2"
    }
}

# searching for taylor swift in both the headline and short description fields
# stored in taylor_swift variable
# we want to emphasize the weight in headline
client.search(index:'news_headlines', body: {
    size: 5,
    query: {
        "multi_match": {
            "query": "Taylor Swift",
            "fields": ["headline^2", "short_description"]
        }
    }
})

client.search(index:"a_b_v2", body: {
    size: 0,
    aggs: {
        "name_key": {
            "terms": {
                "field": "Name.keyword"
            }
        }

    },
    query: {
        "match": { 
            "Name": {
                query: "habits"
            }     
        }
    }
}
)