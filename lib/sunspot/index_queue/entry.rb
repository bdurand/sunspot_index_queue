module Sunspot
  class IndexQueue
    # Abstract queue entry interface. All the gory details on actually handling the queue are handled by a
    # specific implementation class. The default implementation will use ActiveRecord as the backing queue.
    #
    # Implementing classes must define attribute readers for +id+, +record_class_name+, +record_id+, +error+,
    # +attempts+, and +is_delete?+.
    module Entry
      autoload :ActiveRecordImpl, File.expand_path('../entry/active_record_impl', __FILE__)
      autoload :DataMapperImpl, File.expand_path('../entry/data_mapper_impl', __FILE__)
      autoload :MongoImpl, File.expand_path('../entry/mongo_impl', __FILE__)
      
      attr_writer :processed

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
        def implementation=(klass)
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
        def total_count(queue)
          implementation.total_count(queue)
        end
        
        # Get a count of the entries ready to be processed for an IndexQueue. Implementations must implement this method.
        def ready_count(queue)
          implementation.ready_count(queue)
        end
        
        # Get a count of the error entries for an IndexQueue. Implementations must implement this method.
        def error_count(queue)
          implementation.error_count(queue)
        end
        
        # Get the specified number of error entries for an IndexQueue. Implementations must implement this method.
        def errors(queue, limit, offset)
          implementation.errors(queue, limit, offset)
        end
        
        # Get the next batch of entries to process for IndexQueue. Implementations must implement this method.
        def next_batch!(queue)
          implementation.next_batch!(queue)
        end
        
        # Reset the entries in the queue to be excuted again immediately and clear any errors.
        def reset!(queue)
          implementation.reset!(queue)
        end
        
        # Add an entry the queue. +is_delete+ will be true if the entry is a delete. Implementations must implement this method.
        def add(klass, id, delete, options = {})
          raise NotImplementedError.new("add")
        end
        
        # Add multiple entries to the queue. +delete+ will be true if the entry is a delete.
        def enqueue(queue, klass, ids, delete, priority)
          klass = Sunspot::Util.full_const_get(klass.to_s) unless klass.is_a?(Class)
          unless queue.class_names.empty? || queue.class_names.include?(klass.name)
            raise ArgumentError.new("Class #{klass.name} is not in the class names allowed for the queue")
          end
          priority = priority.to_i
          if ids.is_a?(Array)
            ids.each do |id|
              implementation.add(klass, id, delete, priority)
            end
          else
            implementation.add(klass, ids, delete, priority)
          end
        end
        
        # Delete entries from the queue. Implementations must implement this method.
        def delete_entries(entries)
          implementation.delete_entries(entries)
        end
        
        # Load all records in an array of entries. This can be faster than calling load on each DataAccessor
        # depending on the implementation
        def load_all_records(entries)
          classes = entries.collect{|entry| entry.record_class_name}.uniq.collect{|name| Sunspot::Util.full_const_get(name) rescue nil}.compact
          map = entries.inject({}){|hash, entry| hash[entry.record_id.to_s] = entry; hash}
          classes.each do |klass|
            ids = entries.collect{|entry| entry.record_id}
            adapter = Sunspot::Adapters::DataAccessor.create(klass)
            if klass.respond_to?(:sunspot_options) && klass.sunspot_options && klass.sunspot_options[:include] && adapter.respond_to?(:include=)
              adapter.include = klass.sunspot_options[:include]
            end
            adapter.load_all(ids).each do |record|
              entry = map[Sunspot::Adapters::InstanceAdapter.adapt(record).id.to_s]
              entry.instance_variable_set(:@record, record) if entry
            end
          end
        end
      end
      
      def processed?
        @processed = false unless defined?(@processed)
        @processed
      end
      
      # Get the record represented by this entry.
      def record
        @record ||= Sunspot::Adapters::DataAccessor.create(Sunspot::Util.full_const_get(record_class_name)).load_all([record_id]).first
      end

      # Set the error message on an entry. Implementations must implement this method.
      def set_error!(error, retry_interval = nil)
        raise NotImplementedError.new("set_error!")
      end

      # Reset an entry to be executed again immediatel and clear any errors. Implementations must implement this method.
      def reset!
        raise NotImplementedError.new("reset!")
      end
    end
  end
end
