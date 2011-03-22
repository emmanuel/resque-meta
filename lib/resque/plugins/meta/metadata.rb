require 'time'

module Resque
  module Plugins
    module Meta
      class Metadata
        extend Resque::Helpers

        attr_reader :job_id, :job_class, :data, :expire_in

        def self.store(job_id, data, expire_at)
          key = "meta:#{job_id}"
          redis.set(key, encode(data))
          redis.expireat("resque:#{key}", expire_at) if expire_at > 0
        end

        # Retrieve the metadata for a given job.  If you call this
        # from a class that extends Meta, then the metadata will
        # only be returned if the metadata for that id is for the
        # same class.  Explicitly calling Metadata.get(some_id)
        # will return the metadata for a job of any type.
        def self.get(job_id, job_class = nil)
          if data = load(job_id, job_class)
            Metadata.new(job_id, data["job_class"], data)
          end
        end

        def self.load(job_id, job_class = nil)
          key = "meta:#{job_id}"
          if json = redis.get(key)
            data = decode(json)
            if !job_class || Meta == job_class || job_class.to_s == data['job_class']
              data
            end
          end
        end

        def initialize(job_id, job_class, data = nil)
          @job_id = job_id
          @job_class = job_class.is_a?(String) ? self.class.constantize(job_class) : job_class
          @expire_in = @job_class.expire_meta_in || 0

          @data = data || {}
          @data["job_class"] = @job_class.to_s
        end

        # Reload the metadata from the store
        def reload!
          if data = self.class.load(job_id, job_class)
            @data = data
          end
          self
        end

        # Save the metadata. returns self
        def save
          self.class.store(job_id, data, expire_at)
          self
        end

        def [](key)
          data[key]
        end

        def []=(key, val)
          data[key.to_s] = val
        end

        # methods in modules can be easily overridden or extended later (with more modules)
        include module InstanceMethods
          def expire_at
            Time.now.to_i + expire_in
          end
        end

      protected

        def from_time_format_str(key)
          (t = self[key]) && Time.parse(t)
        end

        def to_time_format_str(time)
          time.utc.iso8601(6)
        end

      end
    end
  end
end
