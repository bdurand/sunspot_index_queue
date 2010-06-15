require 'dm-core'
require 'dm-aggregates'

module Sunspot
  class IndexQueue
    module Entry
      # Implementation of an indexing queue backed by ActiveRecord.
      #
      # To create the table, you should have a migration containing the following:
      #
      #   self.up
      #     Sunspot::IndexQueue::Entry::ActiveRecordImpl.create_table
      #   end
      #
      #   self.down
      #     drop_table Sunspot::IndexQueue::Entry::ActiveRecordImpl.table_name
      #   end
      class DataMapperImpl
        include DataMapper::Resource
        include Entry
        
        storage_names[:default] = "sunspot_index_queue_entries"
        property :id, Serial
        property :index_at, Time, :index => :index_at
        property :record_class_name, String, :index => [:record, :index_at]
        property :record_id, String, :index => [:record]
        property :priority, Integer, :default => 0, :index => :index_at
        property :operation, String
        property :lock, Integer
        property :error, String
        property :attempts, Integer, :default => 0
        
        class << self
          # Implementation of the total_count method.
          def total_count (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            count(conditions)
          end
          
          # Implementation of the ready_count method.
          def ready_count (queue)
            conditions = {:index_at.lte => Time.now.utc}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            count(conditions)
          end

          # Implementation of the error_count method.
          def error_count (queue)
            conditions = {:error.not => nil}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            count(conditions)
          end

          # Implementation of the errors method.
          def errors (queue, limit, offset)
            conditions = {:error.not => nil}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            all(conditions.merge(:limit => limit, :offset => offset, :order => :id))
          end

          # Implementation of the reset! method.
          def reset! (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            all(conditions).update!(:index_at => Time.now.utc, :attempts => 0, :error => nil, :lock => nil)
          end
          
          # Implementation of the next_batch! method.
          def next_batch! (queue)
            conditions = {:index_at.lte => Time.now.utc}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            batch_entries = all(conditions.merge(:fields => [:id], :limit => queue.batch_size, :order => [:priority.desc, :index_at]))
            queue_entry_ids = batch_entries.collect{|entry| entry.id}
            return [] if queue_entry_ids.empty?
            lock = rand(0x7FFFFFFF)
            all(:id => queue_entry_ids).update!(:index_at => Time.now.utc + queue.retry_interval, :lock => lock, :error => nil)
            all(:id => queue_entry_ids, :lock => lock)
          end

          # Implementation of the add method.
          def add (klass, id, operation, priority)
            operation = operation.to_s.downcase[0, 1]
            queue_entry_key = {:record_id => id, :record_class_name => klass.name, :lock => nil}
            queue_entry = first(:conditions => queue_entry_key) || new(queue_entry_key.merge(:priority => priority))
            queue_entry.operation = operation
            queue_entry.priority = priority if priority < queue_entry.priority
            queue_entry.index_at = Time.now.utc
            queue_entry.save!
          end
          
          # Implementation of the delete_entries method.
          def delete_entries (ids)
            all(:id => ids).destroy!
          end
        end

        # Implementation of the set_error! method.
        def set_error! (error, retry_interval = nil)
          self.attempts += 1
          self.index_at = Time.now.utc + (retry_interval * attempts) if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          self.lock = nil
          begin
            save!
          rescue => e
            DataMapper.logger.warn(error)
            DataMapper.logger.warn(e)
          end
        end

        # Implementation of the reset! method.
        def reset!
          begin
            update!(:attempts => 0, :error => nil, :lock => nil, :index_at => Time.now.utc)
          rescue => e
            DataMapper.logger.warn(e)
          end
        end
      end
    end
  end
end
