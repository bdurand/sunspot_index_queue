require File.expand_path('../spec_helper', __FILE__)
require 'sqlite3'
require File.expand_path('../entry_impl_examples', __FILE__)

describe Sunspot::IndexQueue::Entry::ActiveRecordImpl do

  before :all do
    ActiveRecord::Base.establish_connection("adapter" => "sqlite3", "database" => ":memory:")
    Sunspot::IndexQueue::Entry.implementation = :active_record
    Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
  end
  
  after :all do
    ActiveRecord::Base.connection.disconnect!
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  let(:factory) do
    factory = Object.new
    def factory.create (attributes)
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.create!(attributes)
    end
    
    def factory.delete_all
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.delete_all
    end
    
    def factory.find (id)
      Sunspot::IndexQueue::Entry::ActiveRecordImpl.find_by_id(id)
    end
    
    factory
  end
    
  it_should_behave_like "Entry implementation"

end
