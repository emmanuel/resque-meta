# After extending Resque::Plugins::Meta,
# extend Resque::Plugins::Timestamps to get start/finish/fail timestamps
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

    end
  end
end