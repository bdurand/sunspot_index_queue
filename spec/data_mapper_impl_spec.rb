require File.expand_path('../spec_helper', __FILE__)
require 'sqlite3'
require  'dm-migrations'
require File.expand_path('../entry_impl_examples', __FILE__)

describe Sunspot::IndexQueue::Entry::DataMapperImpl do

  before :all do
    DataMapper.setup(:default, 'sqlite::memory:')
    Sunspot::IndexQueue::Entry.implementation = :data_mapper
    Sunspot::IndexQueue::Entry::DataMapperImpl.auto_migrate!
  end
  
  after :all do
    Sunspot::IndexQueue::Entry.implementation = nil
  end
  
  let(:factory) do
    factory = Object.new
    def factory.create (attributes)
      Sunspot::IndexQueue::Entry::DataMapperImpl.create!(attributes)
    end
    
    def factory.delete_all
      Sunspot::IndexQueue::Entry::DataMapperImpl.all.destroy!
    end
    
    def factory.find (id)
      Sunspot::IndexQueue::Entry::DataMapperImpl.get(id)
    end
    
    factory
  end
    
  it_should_behave_like "Entry implementation"

end
