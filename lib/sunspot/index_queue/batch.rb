module Sunspot
  class IndexQueue
    # Batch of entries to be indexed with Solr.
    class Batch
      attr_reader :entries
    
      def initialize (queue, entries = nil)
        @queue = queue
        @entries = []
        @entries.concat(entries) if entries
        @delete_entries = []
      end
      
      # Submit the entries to solr. If they are successfully committed, the entries will be deleted.
      # Otherwise, any entries that generated errors will be updated with the error messages and
      # set to be processed again in the future.
      def submit!
        Entry.load_all_records(entries)
        begin
          # First try submitting the entries in a batch since that's the most efficient.
          # If there are errors, try each entry individually in case there's a bad document.
          session.batch do
            entries.each do |entry|
              submit_entry(entry)
            end
          end
          commit!
        rescue StandardError => e
          submit_each_entry
        rescue TimeoutError => e
          submit_each_entry
        end
      end

      private
      
      def session
        @queue.session
      end
      
      # Send the Solr commit command and delete the entries if it succeeds.
      def commit!
        session.commit
        Entry.delete_entries(@delete_entries) unless @delete_entries.empty?
      rescue Exception => e
        @delete_entries.clear
        raise e
      end
      
      # Submit all entries to Solr individually and then commit.
      def submit_each_entry
        return unless solr_up?
        
        entries.each do |entry|
          submit_entry(entry)
        end
        
        begin
          commit!
        rescue StandardError => e
          entries.each do |entry|
            entry.set_error!(e, @queue.retry_interval)
          end
        rescue TimeoutError => e
          entries.each do |entry|
            entry.set_error!(e, @queue.retry_interval)
          end
        end
      end
      
      # Send an entry to Solr doing an update or delete as necessary.
      def submit_entry (entry)
        log_entry_error(entry) do
          if entry.is_delete?
            session.remove_by_id(entry.record_class_name, entry.record_id)
          else
            record = entry.record
            session.index(record) if record
          end
        end
      end
      
      # Update an entry with an error message if a block fails.
      def log_entry_error (entry)
        begin
          yield
          @delete_entries << entry
        rescue StandardError => e
          solr_up? ? entry.set_error!(e, @queue.retry_interval) : entry.reset!
        rescue TimeoutError => e
          solr_up? ? entry.set_error!(e, @queue.retry_interval) : entry.reset!
        end
      end
      
      def solr_up?
        # TODO this is a placeholder in case Sunspot should expose the ping command.
        true
      end
    end
  end
end
