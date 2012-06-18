require 'mongo'

module Sunspot
  class IndexQueue
    module Entry
      # Implementation of an indexing queue backed by MongoDB (http://mongodb.org/). This implementation
      # uses the mongo gem directly and so is independent of any ORM you may be using.
      #
      # To set it up, you need to set the connection and database that it will use.
      #
      #   Sunspot::IndexQueue::Entry::MongoImpl.connection = 'localhost'
      #   Sunspot::IndexQueue::Entry::MongoImpl.database_name = 'my_database'
      #   # or
      #   Sunspot::IndexQueue::Entry::MongoImpl.connection = Mongo::Connection.new('localhost', 27017)
      #   Sunspot::IndexQueue::Entry::MongoImpl.database_name = 'my_database'
      class MongoImpl
        include Entry

        class << self
          # Set the connection to MongoDB. The args can either be a Mongo::Connection object, or the args
          # that can be used to create a new Mongo::Connection.
          def connection=(*args)
            @connection = args.first.is_a?(Mongo::Connection) ? args.first : Mongo::Connection.new(*args)
          end

          # Get the connection currently in use.
          def connection
            @connection
          end

          # Set the name of the database which will contain the queue collection.
          def database_name=(name)
            @collection = nil
            @database_name = name
          end

          # Get the collection used to store the queue.
          def collection
            unless @collection
              @collection = connection.db(@database_name)["sunspot_index_queue_entries"]
              @collection.create_index([[:record_class_name, Mongo::ASCENDING], [:record_id, Mongo::ASCENDING]])
              @collection.create_index([[:run_at, Mongo::ASCENDING], [:record_class_name, Mongo::ASCENDING], [:priority, Mongo::DESCENDING]])
            end
            @collection
          end

          # Create a new entry.
          def create(attributes)
            entry = new(attributes)
            entry.save
            entry
          end

          # Find one entry given a selector or object id.
          def find_one(spec_or_object_id=nil, opts={})
            doc = collection.find_one(spec_or_object_id, opts)
            doc ? new(doc) : nil
          end

          # Find an array of entries given a selector.
          def find(selector={}, opts={})
            collection.find(selector, opts).collect{|doc| new(doc)}
          end

          # Logger used to log errors.
          def logger
            @logger
          end

          # Set the logger used to log errors.
          def logger=(logger)
            @logger = logger
          end

          # Implementation of the total_count method.
          def total_count(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => {'$in' => queue.class_names}}
            collection.find(conditions).count
          end

          # Implementation of the ready_count method.
          def ready_count(queue)
            conditions = {:run_at => {'$lte' => Time.now.utc}}
            unless queue.class_names.empty?
              conditions[:record_class_name] = {'$in' => queue.class_names}
            end
            collection.find(conditions).count
          end

          # Implementation of the error_count method.
          def error_count(queue)
            conditions = {:error => {'$ne' => nil}}
            unless queue.class_names.empty?
              conditions[:record_class_name] = {'$in' => queue.class_names}
            end
            collection.find(conditions).count
          end

          # Implementation of the errors method.
          def errors(queue, limit, offset)
            conditions = {:error => {'$ne' => nil}}
            unless queue.class_names.empty?
              conditions[:record_class_name] = {'$in' => queue.class_names}
            end
            find(conditions, :limit => limit, :skip => offset, :sort => :id)
          end

          # Implementation of the reset! method.
          def reset!(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => {'$in' => queue.class_names}}
            collection.update(conditions, {"$set" => {:run_at => Time.now.utc, :attempts => 0, :error => nil}}, :multi => true)
          end

          # Implementation of the next_batch! method.
          def next_batch!(queue)
            conditions = {:run_at => {'$lte' => Time.now.utc}}
            unless queue.class_names.empty?
              conditions[:record_class_name] = {'$in' => queue.class_names}
            end
            entries = []
            while entries.size < queue.batch_size
              begin
                lock = rand(0x7FFFFFFF)
                doc = collection.find_and_modify(:update => {"$set" => {:run_at => Time.now.utc + queue.retry_interval, :error => nil, :lock => lock}}, :query => conditions, :limit => queue.batch_size, :sort => [[:priority, Mongo::DESCENDING], [:run_at, Mongo::ASCENDING]])
                break unless doc
                entries << new(doc)
              rescue Mongo::OperationFailure
                break
              end
            end
            entries
          end

          # Implementation of the add method.
          def add(klass, id, delete, priority)
            queue_entry_key = {:record_id => id, :record_class_name => klass.name, :lock => nil}
            queue_entry = find_one(queue_entry_key) || new(queue_entry_key.merge(:priority => priority))
            queue_entry.is_delete = delete
            queue_entry.priority = priority if priority > queue_entry.priority
            queue_entry.run_at = Time.now.utc
            queue_entry.save
          end

          # Implementation of the delete_entries method.
          def delete_entries(entries)
            collection.remove(:_id => {'$in' => entries.map(&:id)})
          end
        end

        attr_reader :doc

        # Create a new entry from a document hash.
        def initialize(attributes = {})
          @doc = {}
          attributes.each do |key, value|
            @doc[key.to_s] = value
          end
          @doc['priority'] = 0 unless doc['priority']
          @doc['attempts'] = 0 unless doc['attempts']
        end

        # Get the entry id.
        def id
          doc['_id']
        end

        # Get the entry id.
        def record_class_name
          doc['record_class_name']
        end

        # Set the entry record_class_name.
        def record_class_name=(value)
          doc['record_class_name'] =  value.nil? ? nil : value.to_s
        end

        # Get the entry id.
        def record_id
          doc['record_id']
        end

        # Set the entry record_id.
        def record_id=(value)
          doc['record_id'] =  value
        end

        # Get the entry run_at time.
        def run_at
          doc['run_at']
        end

        # Set the entry run_at time.
        def run_at=(value)
          value = Time.parse(value.to_s) unless value.nil? || value.is_a?(Time)
          doc['run_at'] =  value.nil? ? nil : value.utc
        end

        # Get the entry priority.
        def priority
          doc['priority']
        end

        # Set the entry priority.
        def priority=(value)
          doc['priority'] =  value.to_i
        end

        # Get the entry attempts.
        def attempts
          doc['attempts'] || 0
        end

        # Set the entry attempts.
        def attempts=(value)
          doc['attempts'] =  value.to_i
        end

        # Get the entry error.
        def error
          doc['error']
        end

        # Set the entry error.
        def error=(value)
          doc['error'] =  value.nil? ? nil : value.to_s
        end

        # Get the entry delete entry flag.
        def is_delete?
          doc['is_delete']
        end

        # Set the entry delete entry flag.
        def is_delete=(value)
          doc['is_delete'] =  !!value
        end

        # Save the entry to the database.
        def save
          id = self.class.collection.save(doc)
          doc['_id'] = id if id
        end

        # Implementation of the set_error! method.
        def set_error!(error, retry_interval = nil)
          self.attempts += 1
          self.run_at = (retry_interval * attempts).from_now.utc if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          begin
            save
          rescue => e
            if self.class.logger
              self.class.logger.warn(error)
              self.class.logger.warn(e)
            end
          end
        end

        # Implementation of the reset! method.
        def reset!
          begin
            self.error = nil
            self.attempts = 0
            self.run_at = Time.now.utc
            self.save
          rescue => e
            self.class.logger.warn(e) if self.class.logger
          end
        end

        def == (value)
          value.is_a?(self.class) && ((id && id == value.id) || (doc == value.doc))
        end
      end
    end
  end
end
