module Sunspot
  class IndexQueue
    # Abstract queue entry interface. All the gory details on actually handling the queue are handled by a
    # specific implementation class. The default implementation will use ActiveRecord as the backing queue.
    module Entry
      autoload :ActiveRecordImpl, File.expand_path('../entry/active_record_impl', __FILE__)
      autoload :MongoImpl, File.expand_path('../entry/mongo_impl', __FILE__)
      
      class << self
        # Set the implementation class to use for the queue. This can be set as either a class object,
        # full class name, or a symbol representing one of the default implementations.
        #
        #   # These all set the implementation to use the default ActiveRecord queue.
        #   Sunspot::IndexQueue::Entry.implementation = :active_record
        #   Sunspot::IndexQueue::Entry.implementation = "Sunspot::IndexQueue::Entry::ActiveRecordImpl"
        #   Sunspot::IndexQueue::Entry.implementation = Sunspot::IndexQueue::Entry::ActiveRecordImpl
        #
        # Implementations should support pulling entries in batches by a priority where higher priority
        # entries are processed first. Errors should be automatically retried after an interval specified
        # by the IndexQueue. The batch size set by the IndexQueue should also be honored.
        def implementation= (klass)
          unless klass.is_a?(Class) || klass.nil?
            class_name = klass.to_s
            class_name = Sunspot::Util.camel_case(class_name).gsub('/', '::') unless class_name.include?('::')
            if class_name.include?('::') || !const_defined?("#{class_name}Impl")
              klass = Sunspot::Util.full_const_get(class_name)
            else
              klass = const_get("#{class_name}Impl")
            end
          end
          @implementation = klass
        end
        
        # The implementation class used for the queue.
        def implementation
          @implementation ||= ActiveRecordImpl
        end
        
        # Get a count of the queue entries for an IndexQueue. Implementations must implement this method.
        def total_count (queue)
          implementation.total_count(queue)
        end
        
        # Get a count of the entries ready to be processed for an IndexQueue. Implementations must implement this method.
        def ready_count (queue)
          implementation.ready_count(queue)
        end
        
        # Get a count of the error entries for an IndexQueue. Implementations must implement this method.
        def error_count (queue)
          implementation.error_count(queue)
        end
        
        # Get the error entries for an IndexQueue. Implementations must implement this method.
        def errors (queue)
          implementation.errors(queue)
        end
        
        # Get the next batch of entries to process for IndexQueue. Implementations must implement this method.
        def next_batch! (queue)
          implementation.next_batch!(queue)
        end
        
        # Reset the entries in the queue to be excuted again immediately and clear any errors.
        def reset! (queue)
          implementation.reset!(queue)
        end
        
        # Add an entry the queue. The operation will be either +:update+ or +:delete+. Implementations must implement this method.
        def add (klass, id, operation, options = {})
          raise NotImplementedError.new
        end
        
        # Add multiple entries to the queue. The operation will be either +:update+ or +:delete+.
        def enqueue (queue, klass, ids, operation, priority)
          klass = Sunspot::Util.full_const_get(klass.to_s) unless klass.is_a?(Class)
          klass = klass.base_class if klass.respond_to?(:base_class)
          unless queue.class_names.empty? || queue.class_names.include?(klass.name)
            raise ArgumentError.new("Class #{klass.name} is not in the class names allowed for the queue")
          end
          priority = priority.to_i
          if ids.is_a?(Array)
            ids.each do |id|
              implementation.add(klass, id, operation, priority)
            end
          else
            implementation.add(klass, ids, operation, priority)
          end
        end
        
        # Delete entries from the queue. Implementations must implement this method.
        def delete_entries (entries)
          implementation.delete_entries(entries)
        end
      end
      
      # Get the record represented by this entry.
      def record
        @record ||= Sunspot::Adapters::DataAccessor.create(Sunspot::Util.full_const_get(record_class_name)).load_all([record_id]).first
      end
      
      # Get the record class name. Implementations must implement this method.
      def record_class_name
        raise NotImplementedError.new
      end
      
      # Get the record id. Implementations must implement this method.
      def record_id
        raise NotImplementedError.new
      end
      
      # True if this entry represents an update operation. Implementations must implement this method.
      def update?
        raise NotImplementedError.new
      end

      # True if this entry represents a delete operation. Implementations must implement this method.
      def delete?
        raise NotImplementedError.new
      end

      # Set the error message on an entry. Implementations must implement this method.
      def set_error! (error, retry_interval = nil)
        raise NotImplementedError.new
      end

      # Reset an entry to be executed again immediatel and clear any errors. Implementations must implement this method.
      def reset!
        raise NotImplementedError.new
      end
    end
  end
end
