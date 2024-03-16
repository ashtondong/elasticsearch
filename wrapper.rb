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
                "fields": ["Name^2", "Genre", "Author"], # demo Name^2 field boosting with "George" and "best_fields" vs "cross_fields" with "math colleen dragons"
                "minimum_should_match": 2
            }
        }
    end


    def self.search term:nil, genre:nil, year:nil, author:nil
        client = Elasticsearch::Client.new
        
        # this allows for an array of hashes that is dynamically added if the parameters of "search" are given
        # user input must now include term:"term", genre:"genre", or year:"year"
        queries = []
        queries << multi_match(term) unless term.nil?
        queries << { "match": { "Genre.keyword": genre } } unless genre.nil?
        queries << { "match": { "Year": year } } unless year.nil?
        queries << { "match": { "Author": author } } unless author.nil?

            return client.search(index: INDEX, body: { 
                query: {
                    bool: {
                        must: queries # queries will only run whichever queries are present and allows the user to enter whatever parameters they wish
                        }
                    }
                }
            )

         # TODO: look into filters, aggregates (buckets)
         # we want to create a feature that if a user selected a filter for multiple genres, that it will return multiple genres aggregate
    
    end
    
    def self.filter filter_view
        client = Elasticsearch::Client.new

        fields = { "filters": { "terms": { "field": "Author.keyword" } } } if filter_view == "Author"
        fields = { "filters": { "terms": { "field": "Genre.keyword" } } } if filter_view == "Genre"
        fields = { "filters": { "terms": { "field": "Year.keyword" } } } if filter_view == "Year"

        client.search(index: INDEX, body: {
                "size": 20,
                aggs: fields
                
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
