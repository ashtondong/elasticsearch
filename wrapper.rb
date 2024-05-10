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

    INDEX = "new_bestsellers" # "new_bestsellers" is the full length data set, "short_bestsellers is a redacted version"

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


    def self.multi_match(term)
        {
            "multi_match": { 
                "query": term,
                # cross_field allows us to look for the terms and criteria across all specified fields
                # especially when using "operator" and "minimum_should_match" because the best_fields default is field-centric looking for all requirments in each field in order to return a matching doc.
                "type": "cross_fields",
                "fields": ["Name^2", "Genre", "Author", "Year"], # demo Name^2 field boosting with "George" and "best_fields" vs "cross_fields" with "math colleen dragons"
                "minimum_should_match": 2
            }
        }
    end


    def self.genre_aggregation
        {
            "genres": {
                "terms": { "field": "Genre.keyword"}
                }

        }   
    end


    def self.search term=nil
    # user input can include an argument for term or no argument for term (default)

        client = Elasticsearch::Client.new
        
        queries = []
        # this allows for an array of hashes that is dynamically added if the arguments of "search" are given
        queries << multi_match(term) unless term.nil? 

            return client.search(index: INDEX, body: { 
                query: {
                    bool: {
                        must: queries # searches across the multi_match fields
                    }
                },
                aggs: genre_aggregation # aggregates query results by genre
            }
        )
    
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