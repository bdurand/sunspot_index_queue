require File.expand_path('../spec_helper', __FILE__)
require 'sqlite3'

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
  
  context "class methods" do
    before :each do
      test_class = "Sunspot::IndexQueue::Test::Searchable"
      @entry_1 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 1, :operation => 'u', :priority => 0, :index_at => Time.now)
      @entry_2 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 2, :operation => 'u', :priority => 10, :index_at => Time.now, :error => "boom!", :attempts => 1)
      @entry_3 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => "Object", :record_id => 3, :operation => 'u', :priority => 0, :index_at => Time.now, :error => "boom!", :attempts => 1)
      @entry_4 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 4, :operation => 'd', :priority => 0, :index_at => 1.minute.from_now, :lock => 100)
      @entry_5 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 5, :operation => 'u', :priority => -10, :index_at => 1.minute.ago)
      @entry_6 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 6, :operation => 'u', :priority => 0, :index_at => 1.hour.ago)
      @entry_7 = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => test_class, :record_id => 7, :operation => 'd', :priority => 10, :index_at => 1.minute.ago)
    end
  
    after :each do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.delete_all
    end
  
    let(:queue) { Sunspot::IndexQueue.new(:batch_size => 3, :retry_interval => 5, :class_names => "Sunspot::IndexQueue::Test::Searchable")}
  
    it "should get the total_count" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.total_count(queue).should == 6
    end
    
    it "should get the ready_count" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.ready_count(queue).should == 5
    end
    
    it "should get the error_count" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.error_count(queue).should == 1
    end
    
    it "should get the errors" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.errors(queue).should == [@entry_2]
    end
      
    it "should reset all entries" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.reset!(queue)
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.error_count(queue).should == 0
      @entry_2.reload
      @entry_2.error.should == nil
      @entry_2.attempts.should == 0
      
      @entry_3.reload
      @entry_3.error.should_not == nil
      @entry_3.attempts.should == 1
      
      @entry_4.reload
      @entry_4.lock.should == nil
    end
    
    it "should get the next_batch! by index time and priority" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.next_batch!(queue).should == [@entry_2, @entry_7, @entry_1]
    end
    
    it "should add an entry" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.add(Sunspot::IndexQueue::Test::Searchable, 10, :update, 2)
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_record_id(10)
      entry.record_class_name.should == "Sunspot::IndexQueue::Test::Searchable"
      entry.record_id.should == "10"
      entry.operation.should == "u"
      entry.priority.should == 2
      entry.index_at.should <= Time.now
    end
    
    it "should delete a list of entry ids" do
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.delete_entries([@entry_1.id, @entry_2.id])
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_id(@entry_1.id).should == nil
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_id(@entry_2.id).should == nil
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_id(@entry_4.id).should == @entry_4
    end
  end
  
  context "instance methods" do
    
    it "should get the record_class_name" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.new(:record_class_name => "Test")
      entry.record_class_name.should == "Test"
    end
    
    it "should get the record_id" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.new(:record_id => "1")
      entry.record_id.should == "1"
    end
    
    it "should determine if the entry is an update" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.new(:operation => 'u')
      entry.update?.should == true
      entry.delete?.should == false
    end
    
    it "should determine if the entry is a delete" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.new(:operation => 'd')
      entry.delete?.should == true
      entry.update?.should == false
    end
    
    it "should reset an entry to be indexed immediately" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => "Test", :record_id => 1, :operation => 'u', :priority => 10, :index_at => 10.minutes.from_now, :error => "boom!", :attempts => 2, :lock => 100)
      entry.reset!
      entry.reload
      entry.index_at.should <= Time.now
      entry.error.should == nil
      entry.attempts.should == 0
      entry.lock.should == nil
    end
    
    it "should set the error on an entry" do
      entry = Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(:record_class_name => "Test", :record_id => 1, :operation => 'u', :priority => 10, :index_at => 10.minutes.from_now, :attempts => 1, :lock => 100)
      error = ArgumentError.new("boom")
      error.stub!(:backtrace).and_return(["line 1", "line 2"])
      entry.set_error!(error)
      entry.reload
      entry.index_at.should > Time.now
      entry.error.should include("ArgumentError")
      entry.error.should include("boom")
      entry.error.should include("line 1")
      entry.error.should include("line 2")
      entry.attempts.should == 2
      entry.lock.should == nil
    end
    
  end

end
