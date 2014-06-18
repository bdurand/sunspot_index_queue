require 'spec_helper'

# Shared examples for Entry implementations. In order to use these examples, the example group should define
# a block for :factory that will create an entry when yielded to with a hash of attributes..
shared_examples_for "Entry implementation" do

  after :each do
    factory.delete_all
  end
  
  context "class methods" do
    before :each do
      test_class = "Sunspot::IndexQueue::Test::Searchable"
      @entry_1 = factory.create('record_class_name' => test_class, 'record_id' => 1, 'is_delete' => false, 'priority' => 0, 'run_at' => Time.now.utc)
      @entry_2 = factory.create('record_class_name' => test_class, 'record_id' => 2, 'is_delete' => false, 'priority' => 10, 'run_at' => Time.now.utc, 'error' => "boom!", 'attempts' => 1)
      @entry_3 = factory.create('record_class_name' => "Object", 'record_id' => 3, 'is_delete' => false, 'priority' => 5, 'run_at' => Time.now.utc, 'error' => "boom!", 'attempts' => 1)
      @entry_4 = factory.create('record_class_name' => test_class, 'record_id' => 4, 'is_delete' => true, 'priority' => 0, 'run_at' => Time.now.utc + 60)
      @entry_5 = factory.create('record_class_name' => test_class, 'record_id' => 5, 'is_delete' => false, 'priority' => -10, 'run_at' => Time.now.utc - 60)
      @entry_6 = factory.create('record_class_name' => test_class, 'record_id' => 6, 'is_delete' => false, 'priority' => 0, 'run_at' => Time.now.utc - 3600)
      @entry_7 = factory.create('record_class_name' => test_class, 'record_id' => 7, 'is_delete' => true, 'priority' => 10, 'run_at' => Time.now.utc - 60)
    end
    
    context "without class_names filter" do
      let(:queue) { Sunspot::IndexQueue.new(:batch_size => 3, :retry_interval => 5)}

      it "should get the total_count" do
        Sunspot::IndexQueue::Entry.implementation.total_count(queue).should == 7
      end

      it "should get the ready_count" do
        Sunspot::IndexQueue::Entry.implementation.ready_count(queue).should == 6
      end

      it "should get the error_count" do
        Sunspot::IndexQueue::Entry.implementation.error_count(queue).should == 2
      end

      it "should get the errors" do
        errors = Sunspot::IndexQueue::Entry.implementation.errors(queue, 2, 0)
        errors.collect{|e| e.record_id}.sort.should == [@entry_2.record_id, @entry_3.record_id]

        errors = Sunspot::IndexQueue::Entry.implementation.errors(queue, 1, 1)
        ([@entry_2.record_id, @entry_3.record_id] - errors.collect{|e| e.record_id}).size.should == 1
      end

      it "should reset all entries" do
        Sunspot::IndexQueue::Entry.implementation.reset!(queue)
        Sunspot::IndexQueue::Entry.implementation.error_count(queue).should == 0
        @entry_2 = factory.find(@entry_2.id)
        @entry_2.error.should == nil
        @entry_2.attempts.should == 0

        @entry_3 = factory.find(@entry_3.id)
        @entry_3.error.should == nil
        @entry_3.attempts.should == 0
      end

      it "should get the next_batch! by index time and priority" do
        batch = Sunspot::IndexQueue::Entry.implementation.next_batch!(queue)
        batch.collect{|e| e.record_id}.sort.should == [@entry_2.record_id, @entry_3.record_id, @entry_7.record_id]
        batch = Sunspot::IndexQueue::Entry.implementation.next_batch!(queue)
        batch.collect{|e| e.record_id}.sort.should == [@entry_1.record_id, @entry_5.record_id, @entry_6.record_id]
      end
    end
    
    context "with class_names filter" do
      let(:queue) { Sunspot::IndexQueue.new(:batch_size => 3, :retry_interval => 5, :class_names => "Sunspot::IndexQueue::Test::Searchable")}
  
      it "should get the total_count" do
        Sunspot::IndexQueue::Entry.implementation.total_count(queue).should == 6
      end
  
      it "should get the ready_count" do
        Sunspot::IndexQueue::Entry.implementation.ready_count(queue).should == 5
      end
  
      it "should get the error_count" do
        Sunspot::IndexQueue::Entry.implementation.error_count(queue).should == 1
      end
  
      it "should get the errors" do
        errors = Sunspot::IndexQueue::Entry.implementation.errors(queue, 2, 0)
        errors.collect{|e| e.record_id}.sort.should == [@entry_2.record_id]
        errors = Sunspot::IndexQueue::Entry.implementation.errors(queue, 1, 1)
        errors.collect{|e| e.record_id}.sort.should == []
      end
    
      it "should reset all entries" do
        Sunspot::IndexQueue::Entry.implementation.reset!(queue)
        Sunspot::IndexQueue::Entry.implementation.error_count(queue).should == 0
        @entry_2 = factory.find(@entry_2.id)
        @entry_2.error.should == nil
        @entry_2.attempts.should == 0
    
        @entry_3 = factory.find(@entry_3.id)
        @entry_3.error.should_not == nil
        @entry_3.attempts.should == 1
      end
  
      it "should get the next_batch! by index time and priority" do
        batch = Sunspot::IndexQueue::Entry.implementation.next_batch!(queue)
        batch.collect{|e| e.record_id}.sort.should == [@entry_2.record_id, @entry_6.record_id, @entry_7.record_id]
        batch = Sunspot::IndexQueue::Entry.implementation.next_batch!(queue)
        batch.collect{|e| e.record_id}.sort.should == [@entry_1.record_id, @entry_5.record_id]
      end
    end
    
    context "add and remove" do
      it "should add an entry" do
        Sunspot::IndexQueue::Entry.implementation.add(Sunspot::IndexQueue::Test::Searchable, 10, false, 100)
        entry = Sunspot::IndexQueue::Entry.implementation.next_batch!(Sunspot::IndexQueue.new).detect{|e| e.priority == 100}
        entry.record_class_name.should == "Sunspot::IndexQueue::Test::Searchable"
        entry.record_id.should == 10
        entry.is_delete?.should == false
        entry.priority.should == 100
      end
  
      it "should delete a list of entry ids" do
        Sunspot::IndexQueue::Entry.implementation.delete_entries([@entry_1, @entry_2])
        factory.find(@entry_1.id).should == nil
        factory.find(@entry_2.id).should == nil
        factory.find(@entry_4.id).id.should == @entry_4.id
      end
      
      it "should not add multiple entries unless a row is being processed" do
        Sunspot::IndexQueue::Entry.implementation.add(Sunspot::IndexQueue::Test::Searchable, 10, false, 80)
        Sunspot::IndexQueue::Entry.implementation.next_batch!(Sunspot::IndexQueue.new)
        Sunspot::IndexQueue::Entry.implementation.add(Sunspot::IndexQueue::Test::Searchable, 10, false, 100)
        Sunspot::IndexQueue::Entry.implementation.add(Sunspot::IndexQueue::Test::Searchable, 10, false, 110)
        Sunspot::IndexQueue::Entry.implementation.add(Sunspot::IndexQueue::Test::Searchable, 10, true, 90)
        Sunspot::IndexQueue::Entry.implementation.reset!(Sunspot::IndexQueue.new)
        entries = Sunspot::IndexQueue::Entry.implementation.next_batch!(Sunspot::IndexQueue.new)
        entries.detect{|e| e.priority == 80}.record_id.should == 10
        entries.detect{|e| e.priority == 100}.should == nil
        entries.detect{|e| e.priority == 90}.should == nil
        entry = entries.detect{|e| e.priority == 110}
        entry.is_delete?.should == true
      end
    end
  end

  context "instance methods" do
  
    it "should get the record_class_name" do
      entry = Sunspot::IndexQueue::Entry.implementation.new('record_class_name' => "Test")
      entry.record_class_name.should == "Test"
    end
  
    it "should get the record_id" do
      entry = Sunspot::IndexQueue::Entry.implementation.new('record_id' => 1)
      entry.record_id.should == 1
    end
  
    it "should determine if the entry is an delete" do
      entry = Sunspot::IndexQueue::Entry.implementation.new('is_delete' => false)
      entry.is_delete?.should == false
      entry = Sunspot::IndexQueue::Entry.implementation.new('is_delete' => true)
      entry.is_delete?.should == true
    end
    
    it "should reset an entry to be indexed immediately" do
      entry = factory.create('record_class_name' => "Test", 'record_id' => 1, 'is_delete' => false, 'priority' => 10, 'run_at' => Time.now.utc + 600, 'error' => "boom!", 'attempts' => 2)
      queue = Sunspot::IndexQueue.new
      queue.error_count.should == 1
      queue.ready_count.should == 0
      entry.reset!
      queue.error_count.should == 0
      queue.ready_count.should == 1
      factory.find(entry.id).attempts.should == 0
    end
  
    it "should set the error on an entry" do
      entry = factory.create('record_class_name' => "Test", 'record_id' => 1, 'is_delete' => false, 'priority' => 10, 'run_at' => Time.now.utc + 600, 'attempts' => 1)
      error = ArgumentError.new("boom")
      error.stub(:backtrace).and_return(["line 1", "line 2"])
      entry.set_error!(error)
      entry = factory.find(entry.id)
      entry.error.should include("ArgumentError")
      entry.error.should include("boom")
      entry.error.should include("line 1")
      entry.error.should include("line 2")
    end
  end
end
