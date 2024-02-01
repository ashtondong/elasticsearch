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


    def self.import file_path = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.csv"
        client = Elasticsearch::Client.new

        # csv and json file paths
        csv_file = file_path
        json_file = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.json"

        read_json = File.read(json_file)

        json_hashes = JSON.parse(read_json)

        json_hashes.each do |item|
            client.index(index: "new_bestsellers", body: item)
        end


    end

    def self.delete index, id=nil
        client = Elasticsearch::Client.new

        if id == nil
            client.indices.delete(index: index)
        else 
            client.delete(index: index, id: id)
        end
        
    end

    # tested using params "new_bestsellers", "Non Fiction", "Lies"
    def self.search index, genre, term
        client = Elasticsearch::Client.new
        # agg on genre and year, look into generating relevant top results
        # return query

        client.search(index: index, body: { 
            aggs: {
                "genre_aggs":{
                    "filter": {
                        "term": {
                            "Genre.keyword": genre
                            }
                        }
                    
                    }
                },
            query: {
                "match":{
                    "Name": {
                        query: term
                    } 
                }
            }
        }
    )
    end


end

end
