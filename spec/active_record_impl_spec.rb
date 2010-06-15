require File.expand_path('../spec_helper', __FILE__)
require 'sqlite3'
require File.expand_path('../entry_impl_examples', __FILE__)

describe Sunspot::IndexQueue::Entry::ActiveRecordImpl do

  before :all do
    db_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    Dir.mkdir(db_dir) unless File.exist?(db_dir)
    db = File.join(db_dir, 'sunspot_index_queue_test.sqlite3')
    ActiveRecord::Base.establish_connection("adapter" => "sqlite3", "database" => db)
    Sunspot::IndexQueue::Entry.implementation = :active_record
    Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
  end
  
  after :all do
    db_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    db = File.join(db_dir, 'sunspot_index_queue_test.sqlite3')
    ActiveRecord::Base.connection.disconnect!
    File.delete(db) if File.exist?(db)
    Dir.delete(db_dir) if File.exist?(db_dir) and Dir.entries(db_dir).reject{|f| f.match(/^\.+$/)}.empty?
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  let(:factory) do
    factory = Object.new
    def factory.create (attributes)
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(attributes)
    end
    
    def factory.delete_all
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.delete_all
    end
    
    def factory.find (id)
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_id(id)
    end
    
    factory
  end
    
  it_should_behave_like "Entry implementation"

end
