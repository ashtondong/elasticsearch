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
    def self.import file_path = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.csv"
        client = Elasticsearch::Client.new

        # csv and json file paths
        csv_file = file_path
        json_file = "/Users/Ashton/Desktop/studysoup/elasticsearch/datafiles/bestsellers.json"

        # # read csv file, including headers
        # csv_data = CSV.read(csv_file, headers: true)

        # # convert to hash
        # csv_hash = csv_data.map(&:to_h)

        # # pretty json format
        # json_data = JSON.pretty_generate(csv_hash)

        # # write onto json file
        # File.write(json_file, json_data)

        # puts "success"

        read_json = File.read(json_file)

        json_hashes = JSON.parse(read_json)

        json_hashes.each do |item|
            client.index(index: "new_bestsellers", body: item)
        end


    end

end

end

Wrapper::Loader.import