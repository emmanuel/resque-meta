require 'time'

module Resque
  module Plugins
    module Meta
      class Metadata
        attr_reader :job_class, :meta_id, :data, :enqueued_at, :expire_in

        def self.store(meta_id, data, expire_at)
          key = "meta:#{meta_id}"
          Resque.redis.set(key, Resque.encode(data))
          Resque.redis.expireat("resque:#{key}", expire_at) if expire_at > 0
        end

        # Retrieve the metadata for a given job.  If you call this
        # from a class that extends Meta, then the metadata will
        # only be returned if the metadata for that id is for the
        # same class.  Explicitly, calling Metadata.get(some_id)
        # will return the metadata for a job of any type.
        def self.get(meta_id, job_class = nil)
          key = "meta:#{meta_id}"
          if json = Resque.redis.get(key)
            data = Resque.decode(json)
            if !job_class || Meta == job_class || job_class.to_s == data['job_class']
              Metadata.new(data)
            end
          end
        end

        def initialize(data_hash)
          data_hash['enqueued_at'] ||= to_time_format_str(Time.now)
          @data = data_hash
          @meta_id = data_hash['meta_id'].dup
          @enqueued_at = from_time_format_str('enqueued_at')
          @job_class = data_hash['job_class']
          if @job_class.is_a?(String)
            @job_class = Resque.constantize(data_hash['job_class'])
          else
            data_hash['job_class'] = @job_class.to_s
          end
          @expire_in = @job_class.expire_meta_in || 0
        end

        # Reload the metadata from the store
        def reload!
          if new_meta = self.class.get(meta_id, job_class)
            @data = new_meta.data
          end
          self
        end

        # Save the metadata. returns self
        def save
          self.class.store(meta_id, data, expire_at)
          self
        end

        def [](key)
          data[key]
        end

        def []=(key, val)
          data[key.to_s] = val
        end

        def start!
          @started_at = Time.now
          self['started_at'] = to_time_format_str(@started_at)
          save
        end

        def started_at
          @started_at ||= from_time_format_str('started_at')
        end

        def finish!
          data['succeeded'] = true unless data.has_key?('succeeded')
          @finished_at = Time.now
          self['finished_at'] = to_time_format_str(@finished_at)
          save
        end

        def finished_at
          @finished_at ||= from_time_format_str('finished_at')
        end

        def expire_at
          if finished? && expire_in > 0
            finished_at.to_i + expire_in
          else
            0
          end
        end

        def enqueued?
          !started?
        end

        def working?
          started? && !finished?
        end

        def started?
          !!started_at
        end

        def finished?
          !!finished_at
        end

        def fail!
          self['succeeded'] = false
          finish!
        end

        def succeeded?
          finished? ? self['succeeded'] : nil
        end

        def failed?
          finished? ? !self['succeeded'] : nil
        end

        def seconds_enqueued
          (started_at || Time.now).to_f - enqueued_at.to_f
        end

        def seconds_processing
          if started?
            (finished_at || Time.now).to_f - started_at.to_f
          else
            0
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
