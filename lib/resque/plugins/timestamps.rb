# After extending Resque::Plugins::Meta,
# extend Resque::Plugins::Timestamps to get start/finish/fail timestamps
# and lifecycle query methods on Resque::Plugins::Meta::Metadata
module Resque
  module Plugins
    module Timestamps

      def before_perform_meta(meta_id, *args)
        if meta = Metadata.get(meta_id, self)
          meta.start!
        end
      end

      def after_perform_meta(meta_id, *args)
        if meta = Metadata.get(meta_id, self)
          meta.finish!
        end
      end

      def on_failure_meta(e, meta_id, *args)
        if meta = Metadata.get(meta_id, self)
          meta.fail!
        end
      end

      module Metadata
        module InstanceMethods
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

        end
      end

    end

    Meta::Metadata.send(:include, Timestamps::Metadata::InstanceMethods)
  end
end