require 'spec_helper'
require 'active_record'

describe "Sunspot::IndexQueue integration tests" do
  before :all do
    db_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    Dir.mkdir(db_dir) unless File.exist?(db_dir)
    Dir.chdir(db_dir) do
      FileUtils.rm_rf('data') if File.exist?('data')
      Dir.mkdir('data')
      `sunspot-solr start --port=18983 --data-directory=data --pid-dir=data --log-file=data/solr.log --max-memory=64m`
      raise "Failed to start Solr on port 18983" unless $? == 0
      # Wait until the server is responding
      ping_uri = URI.parse("http://127.0.0.1:18983/solr/ping")
      solr_started = false
      100.times do
        begin
          Net::HTTP.get(ping_uri)
          solr_started = true
          break
        rescue
          sleep(0.1)
        end
      end
      raise "Solr failed to start after 10 seconds" unless solr_started
    end
    
    db = File.join(db_dir, 'sunspot_index_queue_test.sqlite3')
    File.delete(db) if File.exist?(db)
    ActiveRecord::Base.establish_connection("adapter" => "sqlite3", "database" => db)
    Sunspot::IndexQueue::Entry.implementation = :active_record
    Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
    
    @solr_session = Sunspot::Session.new do |config|
      config.solr.url = 'http://127.0.0.1:18983/solr'
    end
  end
  
  after :all do
    db_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    Dir.chdir(db_dir) do
      `sunspot-solr stop --pid-dir=data`
    end
    db = File.join(db_dir, 'sunspot_index_queue_test.sqlite3')
    data = File.join(db_dir, 'data')
    FileUtils.rm_rf(data) if File.exist?(data)
    ActiveRecord::Base.connection.disconnect!
    File.delete(db) if File.exist?(db)
    Dir.delete(db_dir) if File.exist?(db_dir) and Dir.entries(db_dir).reject{|f| f.match(/^\.+$/)}.empty?
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  let(:session) { Sunspot::IndexQueue::SessionProxy.new(queue) }
  let(:queue) { Sunspot::IndexQueue.new(:session => @solr_session, :batch_size => 2) }
  
  it "should actually work" do
    Sunspot::IndexQueue::Test::Searchable.mock_db do
      record_1 = Sunspot::IndexQueue::Test::Searchable.new("1", "one")
      record_2 = Sunspot::IndexQueue::Test::Searchable.new("2", "two")
      record_3 = Sunspot::IndexQueue::Test::Searchable::Subclass.new("3", "three")
      Sunspot::IndexQueue::Test::Searchable.save(record_1, record_2, record_3)
    
      # Enqueue records
      queue.index(record_1)
      queue.index(record_2)
      queue.index(record_3)
      queue.index(record_2)
      queue.total_count.should == 3
      queue.ready_count.should == 3
      queue.error_count.should == 0
      queue.errors.should == []
    
      # Should not be found
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "three"}.results.should == []
    
      # Process queue
      queue.process
      queue.total_count.should == 0
      queue.ready_count.should == 0
      queue.error_count.should == 0
      queue.errors.should == []
    
      # Should be found
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "two"}.results.should == [record_2]
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "three"}.results.should == [record_3]
    
      # Subclass should be found
      session.search(Sunspot::IndexQueue::Test::Searchable::Subclass){with :value, "two"}.results.should == []
      session.search(Sunspot::IndexQueue::Test::Searchable::Subclass){with :value, "three"}.results.should == [record_3]
    
      # Update record
      record_3.value = "four"
      
      queue.index(record_3)
      Sunspot::IndexQueue::Test::Searchable.save(record_3)
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "three"}.results.should == [record_3]
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "four"}.results.should == []
      queue.process
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "three"}.results.should == []
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "four"}.results.should == [record_3]
    
      # Remove record
      queue.remove(record_3)
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "four"}.results.should == [record_3]
      queue.process
      session.search(Sunspot::IndexQueue::Test::Searchable){with :value, "four"}.results.should == []
    end
  end
  
end
