require File.expand_path('../spec_helper', __FILE__)

describe Sunspot::IndexQueue do
  
  before :all do
    Sunspot::IndexQueue::Entry.implementation = :mock
  end
  
  after :all do
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  context "enqueing entries" do
    let(:queue) { Sunspot::IndexQueue.new }
    
    it "should be able to index a record" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :update, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :update, 1)
      queue.index(Sunspot::IndexQueue::Test::Searchable.new(1))
      queue.index(Sunspot::IndexQueue::Test::Searchable.new(2), :priority => 1)
    end
  
    it "should be able to index a class and id" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :update, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :update, 1)
      queue.index(:class => Sunspot::IndexQueue::Test::Searchable, :id => 1)
      queue.index({:class => Sunspot::IndexQueue::Test::Searchable, :id => 2}, :priority => 1)
    end
  
    it "should be able to index multiple records" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [1, 2], :update, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [3, 4], :update, 1)
      queue.index_all(Sunspot::IndexQueue::Test::Searchable, [1, 2])
      queue.index_all(Sunspot::IndexQueue::Test::Searchable, [3, 4], :priority => 1)
    end
  
    it "should be able to remove a record" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :delete, 1)
      queue.remove(Sunspot::IndexQueue::Test::Searchable.new(1))
      queue.remove(Sunspot::IndexQueue::Test::Searchable.new(2), :priority => 1)
    end
  
    it "should be able to remove a class and id" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :delete, 1)
      queue.remove(:class => Sunspot::IndexQueue::Test::Searchable, :id => 1)
      queue.remove({:class => Sunspot::IndexQueue::Test::Searchable, :id => 2}, :priority => 1)
    end
  
    it "should be able to remove multiple records" do
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [1, 2], :delete, 0)
      Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [3, 4], :delete, 1)
      queue.remove_all(Sunspot::IndexQueue::Test::Searchable, [1, 2])
      queue.remove_all(Sunspot::IndexQueue::Test::Searchable, [3, 4], :priority => 1)
    end
  
    it "should be able to set the priority for indexing or removing records in a block" do
      Sunspot::IndexQueue.default_priority.should == 0
      
      Sunspot::IndexQueue.set_priority(1) do
        Sunspot::IndexQueue.default_priority.should == 1
        
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :update, 1)
        queue.index(Sunspot::IndexQueue::Test::Searchable.new(1))
      
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :update, 1)
        queue.index(:class => Sunspot::IndexQueue::Test::Searchable, :id => 2)
      
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [3], :update, 1)
        queue.index_all(Sunspot::IndexQueue::Test::Searchable, [3])
      
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 1, :delete, 1)
        queue.remove(Sunspot::IndexQueue::Test::Searchable.new(1))
      
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, 2, :delete, 1)
        queue.remove(:class => Sunspot::IndexQueue::Test::Searchable, :id => 2)
      
        Sunspot::IndexQueue::Entry.should_receive(:enqueue).with(queue, Sunspot::IndexQueue::Test::Searchable, [3], :delete, 1)
        queue.remove_all(Sunspot::IndexQueue::Test::Searchable, [3])
      end
      
      Sunspot::IndexQueue.default_priority.should == 0
    end
  end
  
  context "processing" do
    let(:queue) { Sunspot::IndexQueue.new(:batch_size => 2, :session => mock(:session)) }
    let(:entry_1) { Sunspot::IndexQueue::Entry::MockImpl.new(:record => record_1, :operation => :delete) }
    let(:entry_2) { Sunspot::IndexQueue::Entry::MockImpl.new(:record => record_2, :operation => :delete) }
    let(:entry_3) { Sunspot::IndexQueue::Entry::MockImpl.new(:record => record_3, :operation => :delete) }
    let(:record_1) { Sunspot::IndexQueue::Test::Searchable.new(1) }
    let(:record_2) { Sunspot::IndexQueue::Test::Searchable.new(2) }
    let(:record_3) { Sunspot::IndexQueue::Test::Searchable.new(3) }
    
    it "should process all entries in the queue in batch of batch_size" do
      Sunspot::IndexQueue::Entry.should_receive(:next_batch!).with(queue).and_return([entry_1, entry_2], [entry_3], [])
      Sunspot::IndexQueue::Entry::MockImpl.should_receive(:ready_count).with(queue).and_return(0)
      queue.session.should_receive(:batch).twice.and_yield
      queue.session.should_receive(:remove_by_id).with("Sunspot::IndexQueue::Test::Searchable", "1")
      queue.session.should_receive(:remove_by_id).with("Sunspot::IndexQueue::Test::Searchable", "2")
      queue.session.should_receive(:remove_by_id).with("Sunspot::IndexQueue::Test::Searchable", "3")
      queue.session.should_receive(:commit).twice
      Sunspot::IndexQueue::Entry::MockImpl.should_receive(:delete_entries).with([entry_1, entry_2])
      Sunspot::IndexQueue::Entry::MockImpl.should_receive(:delete_entries).with([entry_3])
      queue.process
    end
  end
  
  context "maintenance" do
    let(:queue) { Sunspot::IndexQueue.new }
    
    it "should be able to reset all entries to clear errors and set them to be processed immediately" do
      Sunspot::IndexQueue::Entry.should_receive(:reset!).with(queue)
      queue.reset!
    end
    
    it "should get the total number of entries in the queue" do
      Sunspot::IndexQueue::Entry.should_receive(:total_count).with(queue).and_return(10)
      queue.total_count.should == 10
    end
    
    it "should get the number of entries in the queue ready to be processed" do
      Sunspot::IndexQueue::Entry.should_receive(:ready_count).with(queue).and_return(10)
      queue.ready_count.should == 10
    end
    
    it "should get the number of entries with errors in the queue" do
      Sunspot::IndexQueue::Entry.should_receive(:error_count).with(queue).and_return(10)
      queue.error_count.should == 10
    end
  end
  
end
