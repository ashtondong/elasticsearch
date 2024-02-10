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

    INDEX = "short_bestsellers" # "new_bestsellers" is the full length data set

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

    # tested using params "new_bestsellers", "Non Fiction", "Lies"
    def self.search genre, term, year=nil
        client = Elasticsearch::Client.new
        # agg on genre and year, look into generating relevant top results
        # return query
        if year != nil
            client.search(index: INDEX, body: { 
            # aggs: {
            #     "year": {
            #         "filter": {
            #             #date
            #         }
            #     }
            # }
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
        client.search(index: INDEX, body: { 
            # aggs: {
            #     "year": {
            #         "filter": {
            #             #date
            #         }
            #     }
            # }
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

    def self.update id, body
        client = Elasticsearch::Client.new
        client.update(index: INDEX, id: id, body: {doc: body})
    end

end

end
