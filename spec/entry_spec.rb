require File.expand_path('../spec_helper', __FILE__)

describe Sunspot::IndexQueue::Entry do
  
  context "implementation" do
    after :each do
      Sunspot::IndexQueue::Entry.implementation = nil
    end
    
    it "should use the active record implementation by default" do
      Sunspot::IndexQueue::Entry.implementation = nil
      Sunspot::IndexQueue::Entry.implementation.should == Sunspot::IndexQueue::Entry::ActiveRecordImpl
    end
    
    it "should be able to set the implementation with a class" do
      Sunspot::IndexQueue::Entry.implementation = Sunspot::IndexQueue::Entry::MockImpl
      Sunspot::IndexQueue::Entry.implementation.should == Sunspot::IndexQueue::Entry::MockImpl
    end
  
    it "should be able to set the implementation with a class name" do
      Sunspot::IndexQueue::Entry.implementation = "Sunspot::IndexQueue::Entry::MockImpl"
      Sunspot::IndexQueue::Entry.implementation.should == Sunspot::IndexQueue::Entry::MockImpl
    end
  
    it "should be able to set the implementation with a symbol" do
      Sunspot::IndexQueue::Entry.implementation = :mock
      Sunspot::IndexQueue::Entry.implementation.should == Sunspot::IndexQueue::Entry::MockImpl
    end
  end
  
  context "proxy class methods" do
    
    before :all do
      Sunspot::IndexQueue::Entry.implementation = :mock
    end
    
    after :all do
      Sunspot::IndexQueue::Entry.implementation = nil
    end
    
    let(:implementation) { Sunspot::IndexQueue::Entry.implementation }
    let(:queue) { Sunspot::IndexQueue.new }
    let(:entry) { Sunspot::IndexQueue::Entry.implementation.new }
    
    it "should proxy the total_count method to the implementation" do
      implementation.should_receive(:total_count).with(queue).and_return(100)
      Sunspot::IndexQueue::Entry.total_count(queue).should == 100
    end
    
    it "should proxy the ready_count method to the implementation" do
      implementation.should_receive(:ready_count).with(queue).and_return(100)
      Sunspot::IndexQueue::Entry.ready_count(queue).should == 100
    end
    
    it "should proxy the error_count method to the implementation" do
      implementation.should_receive(:error_count).with(queue).and_return(100)
      Sunspot::IndexQueue::Entry.error_count(queue).should == 100
    end
    
    it "should proxy the errors method to the implementation" do
      implementation.should_receive(:errors).with(queue, 2, 1).and_return([entry])
      Sunspot::IndexQueue::Entry.errors(queue, 2, 1).should == [entry]
    end
    
    it "should proxy the reset! method to the implementation" do
      implementation.should_receive(:reset!).with(queue)
      Sunspot::IndexQueue::Entry.reset!(queue)
    end
    
    it "should proxy the next_batch! method to the implementation" do
      implementation.should_receive(:next_batch!).with(queue).and_return([entry])
      Sunspot::IndexQueue::Entry.next_batch!(queue).should == [entry]
    end
  end
  
  context "class methods" do
    before :all do
      Sunspot::IndexQueue::Entry.implementation = :mock
    end
    
    after :all do
      Sunspot::IndexQueue::Entry.implementation = nil
    end
    
    let(:implementation) { Sunspot::IndexQueue::Entry.implementation }
    let(:queue) { Sunspot::IndexQueue.new }
    
    it "should enqueue an entry to the implementation" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, false, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, 1, false, 2)
    end
    
    it "should enqueue an entry to the implementation given a class name" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, false, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, "Sunspot::IndexQueue::Test::Searchable", 1, false, 2)
    end
        
    it "should enqueue multiple entries to the implementation" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, false, 2)
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, false, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, [1, 2], false, 2)
    end
    
    it "should not enqueue an entry for an object to the implementation" do
      queue.class_names << "Sunspot::IndexQueue::Test::Searchable"
      queue.class_names << "Sunspot::IndexQueue::Test::Searchable::Subclass"
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, true, 0)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, 1, true, 0)
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable::Subclass, 2, true, 0)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable::Subclass, 2, true, 0)
      lambda{ Sunspot::IndexQueue::Entry.enqueue(queue, Object, 1, false) }.should raise_error(ArgumentError)
    end
    
    context "load all records" do
      it "should load all records for an array of entries at once" do
        entry_1 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 1)
        entry_2 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 2)
        entry_3 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable::Subclass", :record_id => 3)
        Sunspot::IndexQueue::Entry.load_all_records([entry_1, entry_2, entry_3])
        record_1 = entry_1.instance_variable_get(:@record)
        record_1.should == Sunspot::IndexQueue::Test::Searchable.new(1)
        entry_1.record.object_id.should == record_1.object_id
        record_2 = entry_2.instance_variable_get(:@record)
        record_2.should == Sunspot::IndexQueue::Test::Searchable.new(2)
        entry_2.record.object_id.should == record_2.object_id
        record_3 = entry_3.instance_variable_get(:@record)
        record_3.should == Sunspot::IndexQueue::Test::Searchable::Subclass.new(3)
        entry_3.record.object_id.should == record_3.object_id
      end
    
      it "should load all records for an array of entries at once even if ids clash between record class names" do
        entry_1 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 1)
        entry_2 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 2)
        entry_3 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable::Subclass", :record_id => 1)
        entry_4 = Sunspot::IndexQueue::Entry.implementation.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable::Subclass", :record_id => 2)
        Sunspot::IndexQueue::Entry.load_all_records([entry_1, entry_2, entry_3, entry_4])
        record_1 = entry_1.instance_variable_get(:@record)
        record_1.should == Sunspot::IndexQueue::Test::Searchable.new(1)
        entry_1.record.object_id.should == record_1.object_id
        record_2 = entry_2.instance_variable_get(:@record)
        record_2.should == Sunspot::IndexQueue::Test::Searchable.new(2)
        entry_2.record.object_id.should == record_2.object_id
        record_3 = entry_3.instance_variable_get(:@record)
        record_3.should == Sunspot::IndexQueue::Test::Searchable::Subclass.new(1)
        entry_3.record.object_id.should == record_3.object_id
        record_4 = entry_4.instance_variable_get(:@record)
        record_4.should == Sunspot::IndexQueue::Test::Searchable::Subclass.new(2)
        entry_4.record.object_id.should == record_4.object_id
      end
    end
    
  end
  
  context "instance methods" do
    it "should get a record for the entry using the Sunspot DataAccessor" do
      entry = Sunspot::IndexQueue::Entry::MockImpl.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 1)
      entry.record.class.should == Sunspot::IndexQueue::Test::Searchable
      entry.record.id.should == 1
    end
    
    it "should set if an entry has been processed" do
      entry = Sunspot::IndexQueue::Entry::MockImpl.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => 1)
      entry.processed?.should == false
      entry.processed = true
      entry.processed?.should == true
      entry.processed = false
      entry.processed?.should == false
    end
  end
  
end
