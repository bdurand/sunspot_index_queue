require File.expand_path('../spec_helper', __FILE__)

describe Sunspot::IndexQueue::SessionProxy do
  
  before :all do
    Sunspot::IndexQueue::Entry.implementation = :mock
  end
  
  after :all do
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  context "initialization" do
    let(:queue) { Sunspot::IndexQueue.new }
    
    it "should use the default queue by default" do
      proxy = Sunspot::IndexQueue::SessionProxy.new
      proxy.queue.session == proxy.session
    end
    
    it "should use the queue's session by default" do
      proxy = Sunspot::IndexQueue::SessionProxy.new(queue)
      proxy.session.should == queue.session
      proxy.queue.should == queue
    end
    
    it "should be able to specify the underlying session" do
      session = Sunspot::Session.new
      proxy = Sunspot::IndexQueue::SessionProxy.new(queue, session)
      proxy.session.should == session
      proxy.queue.session.should_not == proxy.session
    end
  end
  
  context "delgated methods" do
    
    subject { Sunspot::IndexQueue::SessionProxy.new(queue, session) }
    let(:session) { Sunspot::Session.new }
    let(:queue) { Sunspot::IndexQueue.new }
    
    it "should delegate new_search" do
      session.should_receive(:new_search).with(String, Symbol)
      subject.new_search(String, Symbol)
    end
    
    it "should delegate search" do
      session.should_receive(:search).with(String, Symbol)
      subject.search(String, Symbol)
    end
    
    it "should delegate new_more_like_this" do
      session.should_receive(:new_more_like_this).with(:test, String, Symbol)
      subject.new_more_like_this(:test, String, Symbol)
    end
    
    it "should delegate more_like_this" do
      session.should_receive(:more_like_this).with(:test, String, Symbol)
      subject.more_like_this(:test, String, Symbol)
    end
    
    it "should delegate config" do
      subject.config.should == session.config
    end
  end
  
  context "indexing methods" do
    
    subject { Sunspot::IndexQueue::SessionProxy.new(queue, session) }
    let(:session) { mock(:session) }
    let(:queue) { Sunspot::IndexQueue.new(:session => mock(:queue_session)) }
    
    it "should yield the block to batch" do
      executed = false
      subject.batch do
        executed = true
      end
      executed.should == true
    end
    
    it "should not do anything on commit" do
      subject.commit
    end
    
    it "should not do anything on commit_if_delete_dirty" do
      subject.commit_if_delete_dirty
    end
    
    it "should not do anything on commit_if_dirty" do
      subject.commit_if_dirty
    end
    
    it "should not mark deletes as dirty" do
      Sunspot::IndexQueue::Entry.implementation.stub!(:add)
      subject.remove(Sunspot::IndexQueue::Test::Searchable.new(1))
      subject.delete_dirty?.should == false
    end
    
    it "should not mark the session as dirty" do
      Sunspot::IndexQueue::Entry.implementation.stub!(:add)
      subject.index(Sunspot::IndexQueue::Test::Searchable.new(1))
      subject.delete_dirty?.should == false
    end
    
    it "should queue up objects being indexed" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 0)
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :update, 0)
      subject.index(Sunspot::IndexQueue::Test::Searchable.new(1), [Sunspot::IndexQueue::Test::Searchable.new(2)])
    end
    
    it "should queue up objects being indexed and committed" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :update, 0)
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :update, 0)
      subject.index!(Sunspot::IndexQueue::Test::Searchable.new(1), [Sunspot::IndexQueue::Test::Searchable.new(2)])
    end
    
    it "should queue up objects being removed" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :delete, 0)
      subject.remove(Sunspot::IndexQueue::Test::Searchable.new(1), [Sunspot::IndexQueue::Test::Searchable.new(2)])
    end
    
    it "should queue up objects being removed and committed" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 2, :delete, 0)
      subject.remove!(Sunspot::IndexQueue::Test::Searchable.new(1), [Sunspot::IndexQueue::Test::Searchable.new(2)])
    end
    
    it "should queue up objects being removed by id" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      subject.remove_by_id(Sunspot::IndexQueue::Test::Searchable, 1)
    end
    
    it "should queue up objects being removed by id and committed" do
      Sunspot::IndexQueue::Entry.implementation.should_receive(:add).with(Sunspot::IndexQueue::Test::Searchable, 1, :delete, 0)
      subject.remove_by_id(Sunspot::IndexQueue::Test::Searchable, 1)
    end
    
    context "not queueable" do
      
      subject { Sunspot::IndexQueue::SessionProxy.new(queue, session) }
      let(:session) { Sunspot::Session.new }
      let(:queue) { Sunspot::IndexQueue.new }
    
      it "should immediately remove objects using the queue session if the method takes a block" do
        executed = false
        queue.session.should_receive(:remove).with(:test).and_yield
        subject.remove(:test) do
          executed = true
        end
        executed.should == true
      end
      
      it "should immediately remove objects and commit using the queue session if the method takes a block" do
        executed = false
        queue.session.should_receive(:remove!).with(:test).and_yield
        subject.remove!(:test) do
          executed = true
        end
        executed.should == true
      end
      
      it "should immediately remove all classes using the queue session" do
        queue.session.should_receive(:remove_all).with(Sunspot::IndexQueue::Test::Searchable)
        subject.remove_all(Sunspot::IndexQueue::Test::Searchable)
      end
      
      it "should immediately remove all classes and and commit using the queue session" do
        queue.session.should_receive(:remove_all!).with(Sunspot::IndexQueue::Test::Searchable)
        subject.remove_all!(Sunspot::IndexQueue::Test::Searchable)
      end
    end    
  end
  
end
