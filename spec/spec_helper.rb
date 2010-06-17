require 'rubygems'
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
          
          def save (*objects)
            objects.each do |obj|
              db[obj.id] = obj.dup
            end
          end
        end
        
        attr_reader :id
        attr_accessor :value
        def initialize (id, value=nil)
          @id = id
          @value = value
        end
        
        def == (value)
          value.is_a?(self.class) && @id == value.id
        end
        
        class DataAccessor < Sunspot::Adapters::DataAccessor
          def load (id)
            Searchable.db ? Searchable.db[id] : Searchable.new(id)
          end
        end

        class InstanceAdapter < Sunspot::Adapters::InstanceAdapter
          def id
            @instance.id
          end
        end
        
        class Subclass < Searchable
        end
      end
      
      Sunspot::Adapters::InstanceAdapter.register(Searchable::InstanceAdapter, Searchable)
      Sunspot::Adapters::DataAccessor.register(Searchable::DataAccessor, Searchable)
      Sunspot.setup(Searchable) do
        string :value
      end
    end
    
    module Entry
      class MockImpl
        include Entry
        
        attr_reader :record_class_name, :record_id
        
        def initialize (options = {})
          if options[:record]
            @record_class_name = options[:record].class.name
            @record_id = options[:record].id.to_s
          else
            @record_class_name = options[:record_class_name]
            @record_id = options[:record_id].to_s
          end
          @is_delete = !!options[:delete]
        end
        
        def is_delete?
          @is_delete
        end
        
        def id
          object_id
        end
      end
    end
  end
end

