require 'sunspot/session_proxy/abstract_session_proxy'

module Sunspot
  class IndexQueue
    # This is a Sunspot::SessionProxy that works with the IndexQueue class. Most update requests will
    # be added to the queue and be processed asynchronously. The exceptions are the +remove+ method with
    # a block and the +remove_all+ method. These will send their commands directly to Solr since the queue
    # cannot handle delete by query. You should avoid calling these methods 
    class SessionProxy < Sunspot::SessionProxy::AbstractSessionProxy
      attr_reader :queue, :session
      
      delegate :new_search, :search, :new_more_like_this, :more_like_this, :config, :to => :session
      
      def initialize (queue = nil, session = nil)
        @queue = queue || IndexQueue.new
        @session = session || @queue.session
      end
      
      def batch
        yield if block_given?
      end
      
      def commit
        # no op
      end
      
      def commit_if_delete_dirty
        # no op
      end

      def commit_if_dirty
        # no op
      end
      
      def delete_dirty?
        false
      end
      
      def dirty?
        false
      end
      
      def index (*objects)
        objects.flatten.each do |object|
          queue.index(object)
        end
      end
      
      def index! (*objects)
        index(*objects)
      end
      
      def remove (*objects, &block)
        if block
          # Delete by query not supported by queue, so send to server
          queue.session.remove(*objects, &block)
        else
          objects.flatten.each do |object|
            queue.remove(object)
          end
        end
      end
      
      def remove! (*objects, &block)
        if block
          # Delete by query not supported by queue, so send to server
          queue.session.remove!(*objects, &block)
        else
          remove(*objects)
        end
      end
      
      def remove_all (*classes)
        # Delete by query not supported by queue, so send to server
        queue.session.remove_all(*classes)
      end
      
      def remove_all! (*classes)
        # Delete by query not supported by queue, so send to server
        queue.session.remove_all!(*classes)
      end
      
      def remove_by_id (clazz, id)
        queue.remove(:class => clazz, :id => id)
      end
      
      def remove_by_id! (clazz, id)
        remove_by_id(clazz, id)
      end
    end
  end
end
