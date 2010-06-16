require 'rubygems'
require File.expand_path('../../lib/sunspot_index_queue', __FILE__)

module Sunspot
  class IndexQueue
    module Test
      class DataAccessor < Sunspot::Adapters::DataAccessor
        def load (id)
          Searchable.new(id)
        end
      end
      
      class InstanceAdapter < Sunspot::Adapters::InstanceAdapter
        def id
          @instance.id
        end
      end
      
      class Searchable
        attr_reader :id
        def initialize (id)
          @id = id
        end
        
        def == (value)
          value.is_a?(self.class) && @id == value.id
        end
        
        class Subclass < Searchable
        end
      end
      
      Sunspot::Adapters::InstanceAdapter.register(InstanceAdapter, Searchable)
      Sunspot::Adapters::DataAccessor.register(DataAccessor, Searchable)
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

