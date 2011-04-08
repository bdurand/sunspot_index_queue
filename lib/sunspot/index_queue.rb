module Sunspot
  # Implementation of an asynchronous queue for indexing records with Solr. Entries are added to the queue
  # defining which records should be indexed or removed. The queue will then process those entries and
  # send them to the Solr server in batches. This has two advantages over just updating in place. First,
  # problems with Solr will not cause your application to stop functioning. Second, batching the commits
  # to Solr is more efficient and it should be able to handle more throughput when you have a lot of records
  # to index.
  class IndexQueue
    autoload :Batch, File.expand_path('../index_queue/batch', __FILE__)
    autoload :Entry, File.expand_path('../index_queue/entry', __FILE__)
    autoload :SessionProxy, File.expand_path('../index_queue/session_proxy', __FILE__)
    
    # This exception will be thrown if Solr is not responding.
    class SolrNotResponding < StandardError
    end
    
    attr_accessor :retry_interval, :batch_size
    attr_reader :session, :class_names
    
    class << self
      # Set the default priority for indexing items within a block. Higher priority items will be processed first.
      def set_priority(priority, &block)
        save_val = Thread.current[:sunspot_index_queue_priority]
        begin
          Thread.current[:sunspot_index_queue_priority] = priority.to_i
          yield
        ensure
          Thread.current[:sunspot_index_queue_priority] = save_val
        end
      end
      
      # Get the default indexing priority. Defaults to zero.
      def default_priority
        Thread.current[:sunspot_index_queue_priority] || 0
      end
    end
    
    # Create a new IndexQueue. Available options:
    #
    # +:retry_interval+ - The number of seconds to wait between to retry indexing when an attempt fails
    # (defaults to 1 minute). If an entry fails multiple times, it will be delayed for the interval times
    # the number of failures. For example, if the interval is 1 minute and it has failed twice, the record
    # won't be attempted again for 2 minutes.
    #
    # +:batch_size+ - The maximum number of records to try submitting to solr at one time (defaults to 100).
    #
    # +:class_names+ - A list of class names that the queue will process. This can be used to have different
    # queues process different classes of records when they need to different configurations.
    #
    # +:session+ - The Sunspot::Session object to use for communicating with Solr (defaults to a session with the default config).
    def initialize(options = {})
      @retry_interval = options[:retry_interval] || 60
      @batch_size = options[:batch_size] || 100
      @batch_handler = nil
      @class_names = []
      if options[:class_names].is_a?(Array)
        @class_names.concat(options[:class_names].collect{|name| name.to_s})
      elsif options[:class_names]
        @class_names << options[:class_names].to_s
      end
      @session = options[:session] || Sunspot::Session.new
    end
    
    # Provide a block that will handle submitting batches of records. The block will take a Batch object and must call
    # +submit!+ on it. This can be useful for doing things such as providing an identity map for records in the batch.
    # Example:
    #
    #   # Use the ActiveRecord identity map for each batch submitted to reduce database activity.
    #   queue.batch_handler do
    #     ActiveRecord::Base.cache do |batch|
    #       batch.submit!
    #     end
    #   end
    def batch_handler(&block)
      @batch_handler = block
    end
    
    # Add a record to be indexed to the queue. The record can be specified as either an indexable object or as
    # as hash with :class and :id keys. The priority to be indexed can be passed in the options as +:priority+
    # (defaults to 0).
    def index(record_or_hash, options = {})
      klass, id = class_and_id(record_or_hash)
      Entry.enqueue(self, klass, id, false, options[:priority] || self.class.default_priority)
    end
    
    # Add a record to be removed to the queue. The record can be specified as either an indexable object or as
    # as hash with :class and :id keys. The priority to be indexed can be passed in the options as +:priority+
    # (defaults to 0).
    def remove(record_or_hash, options = {})
      klass, id = class_and_id(record_or_hash)
      Entry.enqueue(self, klass, id, true, options[:priority] || self.class.default_priority)
    end

    # Add a list of records to be indexed to the queue. The priority to be indexed can be passed in the
    # options as +:priority+ (defaults to 0).
    def index_all(klass, ids, options = {})
      Entry.enqueue(self, klass, ids, false, options[:priority] || self.class.default_priority)
    end

    # Add a list of records to be removed to the queue. The priority to be indexed can be passed in the
    # options as +:priority+ (defaults to 0).
    def remove_all(klass, ids, options = {})
      Entry.enqueue(self, klass, ids, true, options[:priority] || self.class.default_priority)
    end
    
    # Get the number of entries to be processed in the queue.
    def total_count
      Entry.total_count(self)
    end
    
    # Get the number of entries in the queue that are ready to be processed.
    def ready_count
      Entry.ready_count(self)
    end
    
    # Get the number of entries that have errors in the queue.
    def error_count
      Entry.error_count(self)
    end
    
    # Get the entries in the queue that have errors. Supported options are +:limit+ (default 50) and +:offset+ (default 0).
    def errors(options = {})
      limit = options[:limit] ? options[:limit].to_i : 50
      Entry.errors(self, limit, options[:offset].to_i)
    end
    
    # Reset all entries in the queue to clear errors and set them to be indexed immediately.
    def reset!
      Entry.reset!(self)
    end
    
    # Process the queue. Exits when there are no more entries to process at the current time.
    # Returns the number of entries processed.
    #
    # If any errors are encountered while processing the queue, they will be logged with the errors so they can
    # be fixed and tried again later. However, if Solr is refusing connections, the processing is stopped right
    # away and a Sunspot::IndexQueue::SolrNotResponding exception is raised.
    def process
      count = 0
      loop do
        entries = Entry.next_batch!(self)
        if entries.nil? || entries.empty?
          break if Entry.ready_count(self) == 0
        else
          batch = Batch.new(self, entries)
          if defined?(@batch_handler) && @batch_handler
            @batch_handler.call(batch)
          else
            batch.submit!
          end
          count += entries.select{|e| e.processed? }.size
        end
      end
      count
    end
    
    private
    
    # Get the class and id for either a record or a hash containing +:class+ and +:id+ options
    def class_and_id(record_or_hash)
      if record_or_hash.is_a?(Hash)
        [record_or_hash[:class], record_or_hash[:id]]
      else
        [record_or_hash.class, Sunspot::Adapters::InstanceAdapter.adapt(record_or_hash).id]
      end
    end
  end
end
