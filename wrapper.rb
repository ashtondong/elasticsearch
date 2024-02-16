require "elasticsearch"
require "csv"
require "json"

module Wrapper
  
class Wrapper 
    def initialize 
        puts "helloworld"
    end


end

class Loader

    INDEX = "new_bestsellers" # "new_bestsellers" is the full length data set

    def self.convert_csv
        file_path = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.csv"

        # csv and json file paths
        csv_file = file_path
        json_file = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.json"

        # read csv file, including headers
        csv_data = CSV.read(csv_file, headers: true)

        # convert to hash
        csv_hash = csv_data.map(&:to_h)

        # pretty json format
        json_data = JSON.pretty_generate(csv_hash)

        # write onto json file
        File.write(json_file, json_data)
    
    end


    def self.import

        client = Elasticsearch::Client.new

        # json file path
        json_file = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/shortbestsellers.json"

        read_json = File.read(json_file)

        json_hashes = JSON.parse(read_json)

        json_hashes.each do |item|
            client.index(index: INDEX, body: item)
        end


    end

    def self.delete id=nil
        client = Elasticsearch::Client.new

        if id == nil
            client.indices.delete(index: INDEX)
        else 
            client.delete(index: INDEX, id: id)
        end
        
    end

    def self.search term=nil, genre=nil, year=nil
        client = Elasticsearch::Client.new
        # agg on genre and year, look into generating relevant top results
        # return query
        if year != nil
            client.search(index: INDEX, body: { 
                query: {
                    bool: {
                        must: [ # demo with "1984", "Fiction", "1970"
                            { "match": { "Year": year } },
                            { "match": { "Genre.keyword": genre } },
                            { "match": { "Name": term } },
                            ]
                        }
                    }
                }
            )

        elsif term != nil # option to search genre, name, and author using one argument
            client.search(index: INDEX, body: { 
                query: {
                    "multi_match": { 
                        "query": term,
                        "type": "cross_fields",
                        "fields": ["Name^2", "Genre", "Author"], # demo with "George", "Math"
                        "minimum_should_match": 2
                        }
                    }
                }
            )

        else # option to produce all available genres without requiring input
            client.search(index: INDEX, body: {
                "size": 0,
                aggs: {
                    "genre_aggs": {
                        "terms": {
                            "field": "Genre.keyword"
                            }
                        }
                    } 
                     
                }
             )
        end

    end

    def self.update id, body
        client = Elasticsearch::Client.new
        client.update(index: INDEX, id: id, body: {doc: body})
    end

    def self.insert name, author, genre, year
        client = Elasticsearch::Client.new
        client.index(index: INDEX, body: {"Name": name, "Author": author, "Genre": genre, "Year": year})
    end

end

end
