require 'spec_helper'
require 'active_record'

describe "Sunspot::IndexQueue integration tests" do
  before :all do
    ActiveRecord::Base.establish_connection("adapter" => "sqlite3", "database" => ":memory:")
    Sunspot::IndexQueue::Entry.implementation = :active_record
    Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
    
    data_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    FileUtils.rm_rf(data_dir) if File.exist?(data_dir)
    Dir.mkdir(data_dir)
    Dir.chdir(data_dir) do
      `sunspot-solr start --port=18983 --data-directory=. --pid-dir=. --log-file=./solr.log --max-memory=64m`
      raise "Failed to start Solr on port 18983" unless $? == 0
      # Wait until the server is responding
      ping_uri = URI.parse("http://127.0.0.1:18983/solr/admin/ping")
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
    @solr_session = Sunspot::Session.new do |config|
      config.solr.url = 'http://127.0.0.1:18983/solr'
    end
  end
  
  after :all do
    data_dir = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp'))
    if File.exist?(data_dir)
      Dir.chdir(data_dir) do
        `sunspot-solr stop --pid-dir=.`
      end
      FileUtils.rm_rf(data_dir)
    end
    Sunspot::IndexQueue::Entry::ActiveRecordImpl.connection.drop_table(Sunspot::IndexQueue::Entry::ActiveRecordImpl.table_name) if Sunspot::IndexQueue::Entry::ActiveRecordImpl.table_exists?
    ActiveRecord::Base.connection.disconnect!
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  let(:session) { Sunspot::IndexQueue::SessionProxy.new(queue) }
  let(:queue) { Sunspot::IndexQueue.new(:session => @solr_session, :batch_size => 2) }
  
  it "should actually work" do
    Sunspot::IndexQueue::Test::Searchable.mock_db do
      record_1 = Sunspot::IndexQueue::Test::Searchable.new(1, "one")
      record_2 = Sunspot::IndexQueue::Test::Searchable.new(2, "two")
      record_3 = Sunspot::IndexQueue::Test::Searchable::Subclass.new(3, "three")
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
