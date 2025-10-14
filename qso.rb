#!/usr/bin/env ruby

# maintain and update a database of ham radio qso's and callsigns
require 'optparse'
require 'sequel'
require 'nokogiri'
require 'net/http'
require 'tty-prompt'

# manage creation and updates of the database
class QSODb
    attr_accessor :db_file, :db
  
    def initialize(db_file='qso.db')
      @db_file = db_file
    end

    def open_db()
      @db = Sequel.sqlite(@db_file)
    end

    # create a new database in @db_file
    # if there is already a database in that file it will be overwritten and lost
    def create()
      if File.exist?(@db_file)
        puts "File #{@db_file} already exists.  Any data will be lost.  Proceed?"
        response = gets.chomp.downcase

        if response == 'y' || response == 'yes'
          puts "Overwriting database file #{@db_file}"
        else
          puts "Database creation cancelled"
          return 
        end
      end # file existance check

      open_db()

      # create the callsign table
      puts "Creating callsign table"
      @db.create_table? :callsign do
        primary_key :callsign_id
        String :call, unique: true
        String :fname
        String :name
        String :addr1
        String :addr2
        String :state
        String :zip
        String :country
        String :lat
        String :lon
        String :grid
        String :email
        String :license_class
        DateTime :looked_up_at
      end

      # create the qso table
      puts "creating qso table"
      @db.create_table? :qso do
        primary_key :qso_id
        String :date, null: false
        String :time, null: false
        String :band
        Float :frequency
        String :mode
        String :comment
        String :qso
        String :rst_sent
        String :rst_rcvd

        foreign_key :callsign_id, :callsign, key: :callsign_id
        unique [:callsign_id, :date, :time]
      end

    end # create

    # add a record to the callsign table specified by a hash
    # returns the primary key for the new record
    def add_callsign_record(record_hash)
      if !@db
        open_db()
      end
      # will error on duplicates
      @db[:callsign].insert(record_hash)
    end

    # return a primary key for a given callsign
    # or nil if it doesn't exist
    def get_callsign_id(callsign)
      if !@db
        open_db()
      end

      return @db[:callsign].where(call: callsign).get(:id)
    end

end # class QSODb

# handle queries to qrz.com
class QrzClient
 BASE_URL = 'https://xmldata.qrz.com/xml/current/'

  attr_accessor :session_key, :username, :password
  
  def initialize()
    prompt = TTY::Prompt.new

    puts "Please provide login credentials for qrz.com"
    username = prompt.ask('Username:', required: true)
    password = prompt.mask('Password:', required: true)

    @username = username.upcase
    @password = password
    @session_key = nil
  end
  
  def login
    params = "username=#{@username}&password=#{@password}"
    xml = fetch_xml(params)
    doc = Nokogiri::XML(xml)
    doc.remove_namespaces!
    
    @session_key = doc.at_xpath('//Session/Key')&.text
    
    if error = doc.at_xpath('//Session/Error')
      raise "QRZ Login Error: #{error.text}"
    end
    
    @session_key
  end
  
  def get_callsign_record(callsign)
    login unless @session_key
    
    params = "s=#{@session_key}&callsign=#{callsign}"
    xml = fetch_xml(params)
    doc = Nokogiri::XML(xml)
    doc.remove_namespaces!
    
    if error = doc.at_xpath('//Session/Error')
      raise "QRZ Lookup Error: #{error.text}"
    end
    
    data = {
      call: doc.at_xpath('//Callsign/call')&.text,
      fname: doc.at_xpath('//Callsign/fname')&.text,
      name: doc.at_xpath('//Callsign/name')&.text,
      addr1: doc.at_xpath('//Callsign/addr1')&.text,
      addr2: doc.at_xpath('//Callsign/addr2')&.text,
      state: doc.at_xpath('//Callsign/state')&.text,
      zip: doc.at_xpath('//Callsign/zip')&.text,
      country: doc.at_xpath('//Callsign/country')&.text,
      lat: doc.at_xpath('//Callsign/lat')&.text,
      lon: doc.at_xpath('//Callsign/lon')&.text,
      grid: doc.at_xpath('//Callsign/grid')&.text,
      email: doc.at_xpath('//Callsign/email')&.text,
      license_class: doc.at_xpath('//Callsign/class')&.text,
    }
    
    data
  end
  
  private
  
  def fetch_xml(params)
    uri = URI("#{BASE_URL}?#{params}")
    Net::HTTP.get(uri)
  end
end # class QrzClient

# interactive prompting and updates
class QSOPrompt
end # class QSOPrompt

# top level driver and argument handling
def run

  options = {}
  options[:db_path] = 'qso.db'
  options[:create] = false
  options[:add_callsign] = nil

  opts = OptionParser.new do |parser|

    parser.separator ""
    parser.separator "QSO and Callsign database management"
    parser.separator ""

    parser.on("-dFILE", "--db_file FILE", "Specify a database file (defaults to qso.db)", String) do |value|
      options[:db_path] = value
    end

    parser.on("-c", "--[no-]create", "Create a new empty database file.") do |c|
      options[:create] = c
    end

    parser.on("-a CALLSIGN", "--add-callsign CALLSIGN", "Add a new entry to the callsign table") do |ac|
      options[:add_callsign] = ac
    end

   parser.on_tail("-h", "--help", "--usage", "Show this usage message and quit.") do
      puts parser.help
      exit
    end
  end 

  opts.parse!

  puts "Using database file #{options[:db_path]}"
  # handle qrz api requests
  qrz = QrzClient.new()

  # initialize the database object with the specified or defaulted path
  database = QSODb.new(options[:db_path])

  # create a new database if requested
  if options[:create]
    database.create
  end

  # add a record for the specified callsign
  if options[:add_callsign]
    puts("adding callsign info for #{options[:add_callsign]}")
    database.add_callsign_record(qrz.get_callsign_record(options[:add_callsign]))
  end

end # of run

if __FILE__ ==  $PROGRAM_NAME
  run
end
