require File.expand_path('../spec_helper', __FILE__)
require File.expand_path('../entry_impl_examples', __FILE__)

describe Sunspot::IndexQueue::Entry::RedisImpl do

  before :all do
    Sunspot::IndexQueue::Entry.implementation = :redis
    Sunspot::IndexQueue::Entry::RedisImpl.connection = 'localhost'
    Sunspot::IndexQueue::Entry::RedisImpl.datastore_name = "sunspot_index_queue_test"
  end

  after :all do
    Sunspot::IndexQueue::Entry.implementation = nil
  end

  let(:factory) do
    factory = Object.new
    def factory.create (attributes)
      Sunspot::IndexQueue::Entry::RedisImpl.create(attributes)
    end

    def factory.delete_all
      collection = Sunspot::IndexQueue::Entry::RedisImpl.collection
      Sunspot::IndexQueue::Entry::RedisImpl.delete_entries(collection)
    end

    def factory.find (id)
      Sunspot::IndexQueue::Entry::RedisImpl.find_entry(id)
    end

    factory
  end

  it_should_behave_like "Entry implementation"

end
