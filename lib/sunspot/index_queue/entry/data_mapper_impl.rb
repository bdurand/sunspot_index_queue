require 'dm-core'
require 'dm-aggregates'

module Sunspot
  class IndexQueue
    module Entry
      # Implementation of an indexing queue backed by Datamapper.
      #
      # To create the table, you can use +dm-migrations+ and run +auto_migrate!+ on this class.
      #
      # This implementation assumes the primary key of the records being indexed in an integer
      # since that works with most data models and is very efficient. If this is not the case,
      # you can subclass this class and change the data type of the +record_id+ property.
      class DataMapperImpl
        include DataMapper::Resource
        include Entry
        
        storage_names[:default] = "sunspot_index_queue_entries"
        property :id, Serial
        property :run_at, Time, :index => :run_at
        property :record_class_name, String, :index => :run_at
        property :record_id, Integer, :index => :record_id
        property :priority, Integer, :default => 0, :index => :run_at
        property :is_delete, Boolean
        property :lock, Integer
        property :error, String
        property :attempts, Integer, :default => 0
        
        class << self
          # Implementation of the total_count method.
          def total_count(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            count(conditions)
          end
          
          # Implementation of the ready_count method.
          def ready_count(queue)
            conditions = {:run_at.lte => Time.now.utc}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            count(conditions)
          end

          # Implementation of the error_count method.
          def error_count(queue)
            conditions = {:error.not => nil}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            count(conditions)
          end

          # Implementation of the errors method.
          def errors(queue, limit, offset)
            conditions = {:error.not => nil}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            all(conditions.merge(:limit => limit, :offset => offset, :order => :id))
          end

          # Implementation of the reset! method.
          def reset!(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            all(conditions).update!(:run_at => Time.now.utc, :attempts => 0, :error => nil, :lock => nil)
          end
          
          # Implementation of the next_batch! method.
          def next_batch!(queue)
            conditions = {:run_at.lte => Time.now.utc}
            conditions[:record_class_name] = queue.class_names unless queue.class_names.empty?
            batch_entries = all(conditions.merge(:fields => [:id], :limit => queue.batch_size, :order => [:priority.desc, :run_at]))
            queue_entry_ids = batch_entries.collect{|entry| entry.id}
            return [] if queue_entry_ids.empty?
            lock = rand(0x7FFFFFFF)
            all(:id => queue_entry_ids).update!(:run_at => Time.now.utc + queue.retry_interval, :lock => lock, :error => nil)
            all(:id => queue_entry_ids, :lock => lock)
          end

          # Implementation of the add method.
          def add(klass, id, delete, priority)
            queue_entry_key = {:record_id => id, :record_class_name => klass.name, :lock => nil}
            queue_entry = first(:conditions => queue_entry_key) || new(queue_entry_key.merge(:priority => priority))
            queue_entry.is_delete = delete
            queue_entry.priority = priority if priority > queue_entry.priority
            queue_entry.run_at = Time.now.utc
            queue_entry.save!
          end
          
          # Implementation of the delete_entries method.
          def delete_entries(entries)
            all(:id => entries.map(&:id)).destroy!
          end
        end

        # Implementation of the set_error! method.
        def set_error!(error, retry_interval = nil)
          self.attempts += 1
          self.run_at = Time.now.utc + (retry_interval * attempts) if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          self.lock = nil
          begin
            save!
          rescue => e
            if DataMapper.logger
              DataMapper.logger.warn(error)
              DataMapper.logger.warn(e)
            end
          end
        end

        # Implementation of the reset! method.
        def reset!
          begin
            update!(:attempts => 0, :error => nil, :lock => nil, :run_at => Time.now.utc)
          rescue => e
            DataMapper.logger.warn(e)
          end
        end
      end
    end
  end
end
