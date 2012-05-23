require 'redis'
begin
  require 'yajl/json_gem'
rescue LoadError
  require 'json'
end

module Sunspot
  class IndexQueue
    module Entry
      class RedisImpl
        include Entry

        attr_accessor :record_id, :record_class_name, :is_delete, :run_at, :priority, :error, :attempts, :is_locked, :duplicate

        class << self
          def connection= (*args)
            host, port = *args
            host ||= 'localhost'
            port ||= 6379
            @connection = Redis.new(:host => host, :port => port)
          end

          def connection
            @connection
          end

          def logger
            @logger
          end

          def logger= (logger)
            @logger = logger
          end

          def datastore_name= (name)
            @datastore_name = name
          end

          def datastore_name
            @datastore_name
          end

          def collection
            object_array = []
            @connection.hvals(@datastore_name).each {|value| object_array << self.new(JSON.parse(value))}
            object_array.sort
          end

          def total_count(queue)
            if queue.class_names.empty?
              @connection.hlen @datastore_name
            else
              collection.select{|object| queue.class_names.include?(object.record_class_name)}.size
            end
          end

          def ready_count(queue)
            present_time = Time.now.utc
            if queue.class_names.empty?
              collection.select{|object| Time.parse(object.run_at) < present_time}.size
            else
              collection.select{|object| (Time.parse(object.run_at) < present_time) && queue.class_names.include?(object.record_class_name)}.size
            end
          end

          def error_count(queue)
            if queue.class_names.empty?
              collection.select{|object| object.error != nil}.size
            else
              collection.select{|object| (object.error != nil) && queue.class_names.include?(object.record_class_name)}.size
            end
          end

          def errors(queue, limit, offset)
            if queue.class_names.empty?
              collection.select{|object| object.error != nil}.slice(offset..limit)
            else
              collection.select{|object| (object.error != nil) && queue.class_names.include?(object.record_class_name)}.slice(offset..limit)
            end
          end

          def reset!(queue)
            collection_to_reset =
            if queue.class_names.empty?
              collection
            else
              collection.select{|object| queue.class_names.include?(object.record_class_name)}
            end
            collection_to_reset.each do |object|
              object.run_at = Time.now.utc
              object.attempts = 0
              object.error = nil
              object.is_locked = false
              @connection.hset(@datastore_name, "#{object.record_class_name}_#{object.record_id}", object.json_formatted)
            end
          end

          def next_batch! (queue)
            object_array = []
            collection_to_process =
            if queue.class_names.empty?
              collection
            else
              collection.select{|object| queue.class_names.include?(object.record_class_name)}
            end
            collection_to_process.each do |object|
              object_array << object if (Time.parse(object.run_at) <= Time.now.utc) && !object.is_locked
            end
            sliced_object_array = object_array.slice!(0..(queue.batch_size - 1))
            sliced_object_array = sliced_object_array.nil? ? [] : sliced_object_array
            sliced_object_array.each do |object|
              object.is_locked = true
              @connection.hset @datastore_name, "#{object.record_class_name}_#{object.record_id}", object.json_formatted
            end
            sliced_object_array
          end

          def add(klass, id, delete, priority)
            redis_object = if @connection.hexists(@datastore_name, "#{klass.name}_#{id}") && !find_entry("#{klass.name}_#{id}").is_locked
                             find_entry("#{klass.name}_#{id}")
                           else
                             if @connection.hexists(@datastore_name, "#{klass.name}_#{id}_dup") && !find_entry("#{klass.name}_#{id}_dup").is_locked
                               find_entry("#{klass.name}_#{id}_dup")
                             else
                               new(:priority => priority, :record_class_name => klass.name, :record_id => id)
                             end
                           end
            redis_object.is_delete = delete
            redis_object.priority = priority if priority > redis_object.priority
            redis_object.run_at = Time.now.utc
            redis_key = (@connection.hexists(@datastore_name, "#{klass.name}_#{id}") && find_entry("#{klass.name}_#{id}").is_locked) ?
                        "#{klass.name}_#{id}_dup" :
                        "#{klass.name}_#{id}"
            redis_object.duplicate = @connection.hexists(@datastore_name, "#{klass.name}_#{id}") && find_entry("#{klass.name}_#{id}").is_locked
            @connection.hset(@datastore_name, redis_key, redis_object.json_formatted)
          end

          def create(attributes)
            redis_object = new(attributes)
            @connection.hset(@datastore_name, "#{redis_object.record_class_name}_#{redis_object.record_id}", redis_object.json_formatted)
            redis_object
          end

          def delete_entries (records)
            records.each do |record|
              redis_key = "#{record.record_class_name}_#{record.record_id}"
              redis_key << "_dup" if record.duplicate
              @connection.hdel @datastore_name, redis_key
            end
          end

          def find_entry(id)
            @connection.hexists(@datastore_name, id) ?
            new(JSON.parse(@connection.hget(@datastore_name, id))) : nil
          end
        end

        def initialize(options = {})
          [:record_id, :record_class_name, :is_delete, :run_at, :priority, :error, :attempts, :is_locked, :duplicate].each do |attribute|
            instance_variable_set("@#{attribute.to_s}", options[attribute] || options[attribute.to_s])
            @attempts ||= 0
            @priority ||= 0
            @is_delete ||= false
            @is_locked ||= false
            @duplicate ||= false
          end
        end

        def json_formatted
          JSON.dump("record_id" => self.record_id, "record_class_name" => self.record_class_name, "is_delete" => self.is_delete, "duplicate" => self.duplicate,
           "run_at" => self.run_at, "priority" => self.priority, "error" => self.error, "attempts" => self.attempts, "is_locked" => self.is_locked)
        end

        def set_error! (error, retry_interval = nil)
          self.attempts += 1
          self.run_at = Time.now.utc + (retry_interval * attempts) if retry_interval
          self.error = "#{error.class.name}: #{error.message}\n#{error.backtrace.join("\n")[0, 4000]}"
          begin
            self.class.connection.hset(self.class.datastore_name, "#{self.record_class_name}_#{self.record_id}", self.json_formatted)
          rescue => e
            if logger = self.class.logger
              logger.warn(error)
              logger.warn(e)
            end
          end
        end

        def reset!
          begin
            self.run_at = Time.now.utc
            self.attempts = 0
            self.error = nil
            self.class.connection.hset(self.class.datastore_name, "#{self.record_class_name}_#{self.record_id}", self.json_formatted)
          rescue => e
            if logger = self.class.logger
              logger.warn(e)
            end
          end
        end

        def id
          "#{record_class_name}_#{record_id}"
        end

        def is_delete?
          is_delete
        end

        def <=> (redis_object)
          priority.nil? ? (redis_object.run_at <=> self.run_at) : (redis_object.priority <=> self.priority)
        end
      end
    end
  end
end
