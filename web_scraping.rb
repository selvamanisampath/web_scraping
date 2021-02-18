# dependencies
require 'httparty' # fetches raw html from a given url
require 'nokogiri' # parses the raw html and finds out required values
require 'byebug' # debugging purposes
require 'csv' # csv file module
require 'active_record' #table ORM model conversion
require 'bulk_insert' # bulk insert in single shot mysql query

#DB Connection
ActiveRecord::Base.establish_connection(
  { :adapter => 'mysql2',
   :database => '<your_db>',
   :host => 'localhost',
   :username => '<mysql_username>',
   :password => "mysql_password" }
)

class WebScraping < ActiveRecord::Base
  self.table_name = "web_scraping"
end

#Table schema 
=begin
CREATE TABLE `web_scraping` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `website` varchar(50) NOT NULL DEFAULT 'gartner',
  `content_title` varchar(255) DEFAULT NULL,
  `content_url` mediumtext,
  `is_processed` tinyint(4) DEFAULT '0',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content_descriptions` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2684 DEFAULT CHARSET=utf8
=end

# URL to scrape
BASE_URL = "<WEBSITE BASE URL>"
SCRAPE_URL = "#{BASE_URL}/absolure/url/path"

# Getting URLs list
def fetch(url)
  response = HTTParty.get(url)
  if response.body.empty?
    print "No response from #{url}"
    return
  end
  response
end

def scrape_glossary_links(doc)
  url_indexes = []
  glossary_lists_elements = doc.css("div.search-result-container")
  anchor_elements = glossary_lists_elements.css("a.result-heading")
  anchor_elements.each do |element|
    url_indexes << { content_title: "#{element.attributes["data-val"].value}", content_url: "#{BASE_URL}#{element.attributes["href"].value}" }
  end
  url_indexes
end

def scrape_glossary_description(doc)
  begin
    div_element = doc.css("section.grid-norm div.col-md-12")
    p_element = div_element.css("p").text
    ol_element = div_element.css("ol").text if div_element
    return p_element +"\n"+ ol_element.to_s
  rescue => e
    print "exception : #{e} element : #{div_element}"
  end
end

def fetch_contents(scrape_type,url)
  result = fetch(url)
  doc = Nokogiri::HTML(result) if !result.nil?
  case scrape_type
  when "index"
    return scrape_glossary_links(doc)
  when "glossary_descriptions"
    return scrape_glossary_description(doc)
  end
end

def write_to_db(web_scraping_attrs)
  WebScraping.bulk_insert do |worker|
    web_scraping_attrs.each do |attrs|
      worker.add(attrs)
    end
  end
end

def write_to_csv(filename)
  glossary_term_data = WebScraping.where(is_processed: 1)
    .select("content_title as glossary_term, content_descriptions as glossary_description")
  CSV.open( filename, 'wb' ) do |writer|
    writer << glossary_term_data.first.attributes.map { |row,val| row }
    glossary_term_data.each do |gt|
      writer << gt.attributes.map { |a,v| v }
    end
  end
end

#Collect Glossary Terms and its Link
url_indexes_result = fetch_contents('index',SCRAPE_URL)

#Write into a Table
write_to_db(url_indexes_result)

loop do
  #Read Pending Data to Scrape
  WebScraping.where(is_processed: 0).select(:id, :content_url).find_in_batches(batch_size: 10) do |urls|
    urls.each do |url|
      begin
        description = fetch_contents("glossary_descriptions",url.content_url)
        url.update_columns(content_descriptions: description, is_processed: 1)
      rescue SocketError => sock_err
        print "TCP Connection error... Going to sleep for three seconds.. "
        sleep(3)
      rescue => e
        print "exception : #{e}, url: #{url}"        
      end
    end
    sleep(2)
  end
  break if WebScraping.where(is_processed: 0).count == 0
end

#Write to CSV File
write_to_csv("glossary_descriptions.csv")