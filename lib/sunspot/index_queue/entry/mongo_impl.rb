require 'mongo'

module Sunspot
  class IndexQueue
    module Entry
      # Implementation of an indexing queue backed by MongoDB (http://mongodb.org/).
      #
      # To set it up, you need to set the connection and database that it will use.
      #
      #   TODO example include instructions on Passenger
      class MongoImpl
        include Entry

        UPDATE = 'u'
        DELETE = 'd'

        class << self
          def connection= (*args)
            @connection = args.first.is_a?(Mongo::Connection) ? args.first : Mongo::Connection.new(*args)
          end
          
          def connection
            @connection
          end
          
          def database_name= (name)
            @collection = nil
            @database_name = name
          end
          
          def collection
            @collection ||= connection.db(@database_name)["sunspot_index_queue_entries"]
          end
          
          def total_count (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            find(conditions).count
          end
          
          def ready_count (queue)
            conditions = ["index_at <= ?", Time.now.utc]
            unless queue.class_names.empty?
              conditions.first << " AND record_class_name IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          def error_count (queue)
            conditions = ["error IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND record_class_name IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          def errors (queue)
            conditions = ["error IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND record_class_name IN (?)"
              conditions << queue.class_names
            end
            all(:conditions => conditions)
          end

          def reset! (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            update_all({:index_at => Time.now.utc, :attempts => 0, :error => nil, :lock => nil}, conditions)
          end
          
          def next_batch! (queue)
            conditions = ["index_at <= ?", Time.now.utc]
            unless queue.class_names.empty?
              conditions.first << " AND record_class_name IN (?)"
              conditions << queue.class_names
            end
            batch_entries = all(:select => "id", :conditions => ["index_at <= ?", Time.now.utc], :limit => queue.batch_size, :order => 'priority DESC, id')
            queue_entry_ids = batch_entries.collect{|entry| entry.id}
            lock = rand(0x7FFFFFFF)
            update_all({:index_at => queue.retry_interval.from_now.utc, :lock => lock, :error => nil}, :id => queue_entry_ids) unless queue_entry_ids.empty?
            entries = all(:conditions => {:id => queue_entry_ids, :lock => lock})
            entries.sort! do |a, b|
              cmp = b.priority <=> a.priority
              cmp = a.id <=> b.id if cmp == 0
              cmp
            end
            entries
          end

          def add (klass, id, operation, priority)
            operation = operation.to_s.downcase[0, 1]
            queue_entry_key = {:record_id => id, :record_class_name => klass.name, :lock => nil}
            queue_entry = first(:conditions => queue_entry_key) || new(queue_entry_key.merge(:priority => priority))
            queue_entry.operation = operation
            queue_entry.priority = priority if priority < queue_entry.priority
            queue_entry.index_at = Time.now.utc
            queue_entry.save!
          end
          
          def delete_entries (ids)
            delete_all(:id => ids)
          end
        end
      
        def record_class_name
          self[:record_class_name]
        end
      
        def record_id
          self[:record_id]
        end
        
        def update?
          self.operation == UPDATE
        end

        def delete?
          self.operation == DELETE
        end

        def set_error! (error, retry_interval = nil)
          self.attempts += 1
          self.index_at = (retry_interval * attempts).from_now.utc if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          self.lock = nil
          begin
            save!
          rescue => e
            logger.warn(error)
            logger.warn(e)
          end
        end

        def reset!
          begin
            update_attributes!(:attempts => 0, :error => nil, :lock => nil, :index_at => Time.now)
          rescue => e
            logger.warn(e)
          end
        end
      end
    end
  end
end
