require 'rubygems'

require 'uri'
require 'fileutils'
require 'net/http'

if ENV["ACTIVE_RECORD_VERSION"]
  gem 'activerecord', ENV["ACTIVE_RECORD_VERSION"]
else
  gem 'activerecord'
end

if ENV["DATA_MAPPER_VERSON"]
  gem 'dm-core', ENV["DATA_MAPPER_VERSON"]
else
  gem 'dm-core'
end

if ENV["SUNSPOT_VERSION"]
  gem 'sunspot', ENV["SUNSPOT_VERSION"]
else
  gem 'sunspot'
end

require File.expand_path('../../lib/sunspot_index_queue', __FILE__)

module Sunspot
  class IndexQueue
    module Test
      # Test class for searching against. 
      class Searchable
        class << self
          # Create a mock database in a block that will reload copies of saved objects.
          def mock_db
            save = Thread.current[:mock_db]
            Thread.current[:mock_db] = {}
            begin
              yield
            ensure
              Thread.current[:mock_db] = save
            end
          end
          
          def db
            Thread.current[:mock_db]
          end
          
          def save(*objects)
            objects.each do |obj|
              db[obj.id.to_s] = obj.dup
            end
          end
        end
        
        attr_reader :id
        attr_accessor :value
        def initialize(id, value=nil)
          @id = id
          @value = value
        end
        
        def ==(value)
          value.is_a?(self.class) && @id == value.id
        end
        
        class DataAccessor < Sunspot::Adapters::DataAccessor
          def load(id)
            Searchable.db ? Searchable.db[id.to_s] : Searchable.new(id)
          end
          
          def load_all(ids)
            ids.collect{|id| load(id)}.compact
          end
        end

        class InstanceAdapter < Sunspot::Adapters::InstanceAdapter
          def id
            @instance.id
          end
        end
        
        class Subclass < Searchable
        end
        
        # This class mocks out the behavior of ActiveRecord DataAccessor where an include can be attached for eager loading.
        class IncludeClass < Searchable
          def self.sunspot_options
            {:include => :test}
          end
          
          class IncludeDataAccessor < DataAccessor
            attr_accessor :include
          end
        end
      end

      Sunspot::Adapters::InstanceAdapter.register(Searchable::InstanceAdapter, Searchable)
      Sunspot::Adapters::DataAccessor.register(Searchable::DataAccessor, Searchable)
      Sunspot::Adapters::DataAccessor.register(Searchable::IncludeClass::IncludeDataAccessor, Searchable::IncludeClass)
      Sunspot.setup(Searchable) do
        string :value
      end
    end
    
    module Entry
      class MockImpl
        include Entry
        
        attr_reader :record_class_name, :record_id, :error, :attempts
        
        def initialize(options = {})
          if options[:record]
            @record_class_name = options[:record].class.name
            @record_id = options[:record].id
          else
            @record_class_name = options[:record_class_name]
            @record_id = options[:record_id]
          end
          @is_delete = !!options[:delete]
        end
        
        def is_delete?
          @is_delete
        end
        
        def id
          object_id
        end
        
        def set_error!(message, retry_interval = nil)
          @error = message
        end
      end
    end
  end
end

