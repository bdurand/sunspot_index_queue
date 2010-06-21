require File.expand_path('../spec_helper', __FILE__)

describe Sunspot::IndexQueue::Batch do
  
  before :all do
    Sunspot::IndexQueue::Entry.implementation = :mock
  end
  
  after :all do
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  subject { Sunspot::IndexQueue::Batch.new(queue, [entry_1, entry_2]) }
  let(:entry_1) { Sunspot::IndexQueue::Entry::MockImpl.new(:record => record_1, :delete => false) }
  let(:entry_2) { Sunspot::IndexQueue::Entry::MockImpl.new(:record => record_2, :delete => true) }
  let(:record_1) { Sunspot::IndexQueue::Test::Searchable.new(1) }
  let(:record_2) { Sunspot::IndexQueue::Test::Searchable.new(2) }
  let(:queue) { Sunspot::IndexQueue.new(:session => session) }
  let(:session) { Sunspot::Session.new }
  
  it "should submit all entries in a batch to Solr and commit them" do
    entry_1.stub!(:record).and_return(record_1)
    session.should_receive(:batch).and_yield
    session.should_receive(:index).with(record_1)
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id)
    session.should_receive(:commit)
    Sunspot::IndexQueue::Entry.implementation.should_receive(:delete_entries).with([entry_1, entry_2])
    subject.submit!
    entry_1.processed?.should == true
    entry_2.processed?.should == true
  end
  
  it "should submit all entries individually and commit them if the batch errors out" do
    entry_1.stub!(:record).and_return(record_1)
    session.should_receive(:batch).and_yield
    session.should_receive(:index).with(record_1).twice
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id).twice
    session.should_receive(:commit).and_raise("boom")
    session.should_receive(:commit)
    Sunspot::IndexQueue::Entry.implementation.should_receive(:delete_entries).with([entry_1, entry_2])
    subject.submit!
    entry_1.processed?.should == true
    entry_2.processed?.should == true
  end
  
  
  it "should add error messages to each entry that errors out" do
    entry_1.stub!(:record).and_return(record_1)
    error = StandardError.new("boom")
    session.should_receive(:batch).and_yield
    session.should_receive(:index).and_raise(error)
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id)
    session.should_receive(:commit)
    entry_1.should_receive(:set_error!).with(error, queue.retry_interval)
    Sunspot::IndexQueue::Entry.implementation.should_receive(:delete_entries).with([entry_2])
    subject.submit!
    entry_1.processed?.should == false
    entry_2.processed?.should == true
  end
  
  it "should add error messages to all entries when a commit fails" do
    entry_1.stub!(:record).and_return(record_1)
    error = StandardError.new("boom")
    session.should_receive(:batch).and_yield
    session.should_receive(:index).with(record_1).twice
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id).twice
    session.should_receive(:commit).twice.and_raise(error)
    Sunspot::IndexQueue::Entry.implementation.should_not_receive(:delete_entries)
    entry_1.should_receive(:set_error!).with(error, queue.retry_interval)
    entry_2.should_receive(:set_error!).with(error, queue.retry_interval)
    subject.submit!
    entry_1.processed?.should == false
    entry_2.processed?.should == false
  end
  
  it "should silently ignore entries that no longer have a record" do
    entry_1.stub!(:record).and_return(nil)
    session.should_receive(:batch).and_yield
    session.should_not_receive(:index)
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id)
    session.should_receive(:commit)
    Sunspot::IndexQueue::Entry.implementation.should_receive(:delete_entries).with([entry_1, entry_2])
    subject.submit!
    entry_1.processed?.should == true
    entry_2.processed?.should == true
  end
  
  it "should raise an error and reset entries if the Solr server is not responding for an entry" do
    entry_1.stub!(:record).and_return(record_1)
    error = Errno::ECONNREFUSED.new
    session.should_receive(:batch).and_yield
    session.should_receive(:index).with(record_1)
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id).and_raise(error)
    session.should_not_receive(:commit)
    Sunspot::IndexQueue::Entry.implementation.should_not_receive(:delete_entries)
    entry_1.should_receive(:reset!)
    entry_2.should_receive(:reset!)
    lambda{subject.submit!}.should raise_error(Sunspot::IndexQueue::SolrNotResponding)
    entry_1.processed?.should == false
    entry_2.processed?.should == false
  end
  
  it "should raise an error and reset entries if the Solr server is not responding on a commit" do
    entry_1.stub!(:record).and_return(record_1)
    error = Errno::ECONNREFUSED.new
    session.should_receive(:batch).and_yield
    session.should_receive(:index).with(record_1)
    session.should_receive(:remove_by_id).with(entry_2.record_class_name, entry_2.record_id)
    session.should_receive(:commit).and_raise(error)
    Sunspot::IndexQueue::Entry.implementation.should_not_receive(:delete_entries)
    entry_1.should_receive(:reset!)
    entry_2.should_receive(:reset!)
    lambda{subject.submit!}.should raise_error(Sunspot::IndexQueue::SolrNotResponding)
    entry_1.processed?.should == false
    entry_2.processed?.should == false
  end
  
end
