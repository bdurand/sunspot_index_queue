module Sunspot
  class IndexQueue
    # Batch of entries to be indexed with Solr.
    class Batch
      attr_reader :entries
      
      # Errors that cause batch processing to stop and are immediately passed on to the caller. All other
      # are logged on the entry on the assumption that they can be fixed later while other entries can still
      # be processed.
      PASS_THROUGH_EXCEPTIONS = [SystemExit, NoMemoryError, Interrupt, SignalException, Errno::ECONNREFUSED]
      
      def initialize(queue, entries = nil)
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
        clear_processed(entries)
        begin
          # First try submitting the entries in a batch since that's the most efficient.
          # If there are errors, try each entry individually in case there's a bad document.
          session.batch do
            entries.each do |entry|
              submit_entry(entry)
            end
          end
          commit!
        rescue Exception => e
          @delete_entries.clear
          entries.each{|entry| entry.processed = false}
          if PASS_THROUGH_EXCEPTIONS.include?(e.class)
            raise e
          else
            submit_each_entry
          end
        end
      rescue Exception => e
        begin
          clear_processed(entries)
          entries.each{|entry| entry.reset!} if PASS_THROUGH_EXCEPTIONS.include?(e.class)
        ensure
          # Use a more specific error to indicate Solr is down.
          e = SolrNotResponding.new(e.message) if e.is_a?(Errno::ECONNREFUSED)
          raise e
        end
      end

      private
      
      def session
        @queue.session
      end
      
      # Clear the processed flag on all entries.
      def clear_processed(entries)
        entries.each{|entry| entry.processed = false}
      end
      
      # Send the Solr commit command and delete the entries if it succeeds.
      def commit!
        session.commit
        Entry.delete_entries(@delete_entries) unless @delete_entries.empty?
      rescue Exception => e
        clear_processed(entries)
        raise e
      ensure
        @delete_entries.clear
      end
      
      # Submit all entries to Solr individually and then commit.
      def submit_each_entry
        entries.each do |entry|
          submit_entry(entry)
        end
        
        begin
          commit!
        rescue Exception => e
          if PASS_THROUGH_EXCEPTIONS.include?(e.class)
            raise e
          else
            entries.each do |entry|
              entry.set_error!(e, @queue.retry_interval)
            end
          end
        end
      end
      
      # Send an entry to Solr doing an update or delete as necessary.
      def submit_entry(entry)
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
      def log_entry_error(entry)
        begin
          yield
          entry.processed = true
          @delete_entries << entry
        rescue Exception => e
          if PASS_THROUGH_EXCEPTIONS.include?(e.class)
            raise e
          else
            entry.set_error!(e, @queue.retry_interval)
          end
        end
      end
    end
  end
end
