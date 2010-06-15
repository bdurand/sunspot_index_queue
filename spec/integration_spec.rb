require 'spec_helper'
require 'active_record'

describe "Sunspot::IndexQueue integration tests" do
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
  
  it "should actually work" do
    queue = Sunspot::IndexQueue.new
    record_1 = Sunspot::IndexQueue::Test::Searchable.new("1")
    record_2 = Sunspot::IndexQueue::Test::Searchable.new("2")
    record_3 = Sunspot::IndexQueue::Test::Searchable.new("3")
    queue.index(record_1)
    queue.index(record_2)
    queue.index(record_3)
    queue.remove(record_2)
    queue.total_count.should == 3
    queue.ready_count.should == 3
    queue.error_count.should == 0
    queue.errors.should == []
    queue.session.should_receive(:index).with(record_1)
    queue.session.should_not_receive(:index).with(record_2)
    queue.session.should_receive(:remove_by_id).with("Sunspot::IndexQueue::Test::Searchable", "2")
    queue.session.should_receive(:index).with(record_3)
    queue.process
    queue.total_count.should == 0
    queue.ready_count.should == 0
    queue.error_count.should == 0
    queue.errors.should == []
  end
  
end
