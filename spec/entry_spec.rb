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
    
    it "should add an entry to the implementation" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, 1, :update, 2)
    end
    
    it "should add an entry to the implementation given a class name" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, "Sunspot::IndexQueue::Test::Searchable", 1, :update, 2)
    end
    
    it "should add an entry to the implementation for a base class if it exists" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable::Subclass, 1, :update, 2)
    end
        
    it "should add multiple entries to the implementation" do
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 2)
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :update, 2)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, [1, 2], :update, 2)
    end
    
    it "should not an entry for an object to the implementation" do
      queue.class_names << "Sunspot::IndexQueue::Test::Searchable"
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :delete, 0)
      Sunspot::IndexQueue::Entry.enqueue(queue, Sunspot::IndexQueue::Test::Searchable::Subclass, 2, :delete, 0)
      lambda{ Sunspot::IndexQueue::Entry.enqueue(queue, Object, 1, :delete) }.should raise_error(ArgumentError)
    end
    
  end
  
  context "instance methods" do
    it "should get a record for the entry using the Sunspot DataAccessor" do
      entry = Sunspot::IndexQueue::Entry::MockImpl.new(:record_class_name => "Sunspot::IndexQueue::Test::Searchable", :record_id => "1")
      entry.record.class.should == Sunspot::IndexQueue::Test::Searchable
      entry.record.id.should == "1"
    end
  end
  
end
