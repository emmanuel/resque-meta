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
        Resque::Plugins::Meta::Metadata.send(:include, Resque::Plugins::Timestamps::Metadata)
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

      module Metadata
        def enqueued!
          @enqueued_at = Time.now
          self['enqueued_at'] = to_time_format_str(@enqueued_at)
          save
        end

        def enqueued_at
          @enqueued_at ||= from_time_format_str('enqueued_at')
        end

        def started!
          @started_at = Time.now
          self['started_at'] = to_time_format_str(@started_at)
          save
        end

        def started_at
          @started_at ||= from_time_format_str('started_at')
        end

        def finished!
          data['succeeded'] = true unless data.has_key?('succeeded')
          @finished_at = Time.now
          self['finished_at'] = to_time_format_str(@finished_at)
          save
        end

        def finished_at
          @finished_at ||= from_time_format_str('finished_at')
        end

        def failed!
          self['succeeded'] = false
          finished!
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

        def succeeded?
          finished? ? self['succeeded'] : nil
        end

        def failed?
          finished? ? !self['succeeded'] : nil
        end

        def expire_at
          if finished? && @expire_in > 0
            finished_at.to_i + @expire_in
          else
            super
          end
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
      end # module Metadata

    end # module Timestamps
  end # module Plugins
end # module Resque
