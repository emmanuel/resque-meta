require 'resque/plugins/meta'

module Resque
  module Plugins
    # extend Resque::Plugins::Timestamps to get start/finish/fail timestamps
    # and lifecycle query methods on Resque::Plugins::Meta::Metadata
    #
    # For example:
    #
    #     require 'resque-meta'
    #
    #     class MyJob
    #       extend Resque::Plugins::Timestamps
    #
    #       def self.perform(job_id, *args)
    #         heavy_lifting
    #       end
    #     end
    #
    #     job_id = MyJob.enqueue('stuff')
    #     meta0 = MyJob.get_meta(job_id)
    #     meta0.job_id        # => '03c9e1a045ad012dd20500264a19273c'
    #     meta0.enqueued_at   # => 'Wed May 19 13:42:41 -0600 2010'
    #     meta0.started_at    # => nil
    #
    #     # later
    #     meta1 = MyJob.get_meta('03c9e1a045ad012dd20500264a19273c')
    #     meta1.enqueued_at   # => 'Wed May 19 13:42:41 -0600 2010'
    #     meta1.started_at    # => 'Wed May 19 13:42:51 -0600 2010'
    #
    #     # later still
    #     meta2 = MyJob.get_meta('03c9e1a045ad012dd20500264a19273c')
    #     meta2.started_at    # => 'Wed May 19 13:42:51 -0600 2010'
    #     meta2.finished_at   # => 'Wed May 19 13:43:01 -0600 2010'
    module Timestamps
      include Meta

      def self.extended(base)
        Meta::Metadata.send(:include, Timestamps::MetadataExtensions)
      end

      def after_enqueue_timestamp(job_id, *args)
        meta = get_meta(job_id) and meta.enqueued!
      end

      def before_perform_timestamp(job_id, *args)
        meta = get_meta(job_id) and meta.started!
      end

      def after_perform_timestamp(job_id, *args)
        meta = get_meta(job_id) and meta.finished!
      end

      def on_failure_timestamp(e, job_id, *args)
        meta = get_meta(job_id) and meta.failed!
      end

      module MetadataExtensions
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

        def succeeded?
          finished? ? self[:succeeded] : nil
        end

        def failed?
          finished? ? !self[:succeeded] : nil
        end

        def enqueued!
          self.enqueued_at = Time.now
          save
        end

        def started!
          self.started_at = Time.now
          save
        end

        def finished!
          self[:succeeded] = true unless include?(:succeeded)
          self.finished_at = Time.now
          save
        end

        def failed!
          self[:succeeded] = false
          finished!
        end

        def enqueued_at
          @enqueued_at ||= get_timestamp(:enqueued_at)
        end

        def started_at
          @started_at ||= get_timestamp(:started_at)
        end

        def finished_at
          @finished_at ||= get_timestamp(:finished_at)
        end

        def enqueued_at=(time)
          set_timestamp(:enqueued_at, time)
        end

        def started_at=(time)
          set_timestamp(:started_at, time)
        end

        def finished_at=(time)
          set_timestamp(:finished_at, time)
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

        # override default implementation to base expiry on finish time
        def expire_at
          if finished? && @expire_in > 0
            finished_at.to_i + @expire_in
          else
            super
          end
        end

      private

        def get_timestamp(name)
          time_string = self[name] and Time.parse(time_string)
        end

        def set_timestamp(name, time)
          self.instance_variable_set("@#{name}", time)
          self[name] = time.utc.iso8601(6)
        end

      end # module MetadataExtensions
    end # module Timestamps
  end # module Plugins
end # module Resque
