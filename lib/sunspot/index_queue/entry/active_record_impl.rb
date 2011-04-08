require 'active_record'

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
      #
      # The default set up is to use an integer for the +record_id+ column type since it
      # is the most efficient and works with most data models. If you need to use a string
      # as the primary key, you can add additional statements to the migration to do so.
      class ActiveRecordImpl < ActiveRecord::Base
        include Entry
        
        set_table_name :sunspot_index_queue_entries

        class << self
          # Implementation of the total_count method.
          def total_count(queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            count(:conditions => conditions)
          end
          
          # Implementation of the ready_count method.
          def ready_count(queue)
            conditions = ["#{connection.quote_column_name('run_at')} <= ?", Time.now.utc]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          # Implementation of the error_count method.
          def error_count(queue)
            conditions = ["#{connection.quote_column_name('error')} IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            count(:conditions => conditions)
          end

          # Implementation of the errors method.
          def errors(queue, limit, offset)
            conditions = ["#{connection.quote_column_name('error')} IS NOT NULL"]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            all(:conditions => conditions, :limit => limit, :offset => offset, :order => :id)
          end

          # Implementation of the reset! method.
          def reset! (queue)
            conditions = queue.class_names.empty? ? {} : {:record_class_name => queue.class_names}
            update_all({:run_at => Time.now.utc, :attempts => 0, :error => nil, :lock => nil}, conditions)
          end
          
          # Implementation of the next_batch! method.
          def next_batch!(queue)
            conditions = ["#{connection.quote_column_name('run_at')} <= ?", Time.now.utc]
            unless queue.class_names.empty?
              conditions.first << " AND #{connection.quote_column_name('record_class_name')} IN (?)"
              conditions << queue.class_names
            end
            batch_entries = all(:select => "id", :conditions => conditions, :limit => queue.batch_size, :order => 'priority DESC, run_at')
            queue_entry_ids = batch_entries.collect{|entry| entry.id}
            return [] if queue_entry_ids.empty?
            lock = rand(0x7FFFFFFF)
            update_all({:run_at => queue.retry_interval.from_now.utc, :lock => lock, :error => nil}, :id => queue_entry_ids)
            all(:conditions => {:id => queue_entry_ids, :lock => lock})
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
            delete_all(:id => entries)
          end
          
          # Create the table used to store the queue in the database.
          def create_table
            connection.create_table table_name do |t|
              t.string :record_class_name, :null => false
              t.integer :record_id, :null => false
              t.boolean :is_delete, :null => false, :default => false
              t.datetime :run_at, :null => false
              t.integer :priority, :null => false, :default => 0
              t.integer :lock, :null => true
              t.string :error, :null => true, :limit => 4000
              t.integer :attempts, :null => false, :default => 0
            end

            connection.add_index table_name, :record_id
            connection.add_index table_name, [:run_at, :record_class_name, :priority], :name => "#{table_name}_run_at"
          end
        end

        # Implementation of the set_error! method.
        def set_error!(error, retry_interval = nil)
          self.attempts += 1
          self.run_at = (retry_interval * attempts).from_now.utc if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          self.lock = nil
          begin
            save!
          rescue => e
            if logger
              logger.warn(error)
              logger.warn(e)
            end
          end
        end

        # Implementation of the reset! method.
        def reset!
          begin
            update_attributes!(:attempts => 0, :error => nil, :lock => nil, :run_at => Time.now.utc)
          rescue => e
            logger.warn(e)
          end
        end
      end
    end
  end
end
