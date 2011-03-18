require 'digest/sha1'
require 'resque'
require 'resque/plugins/meta/version'
require 'resque/plugins/meta/metadata'

module Resque
  module Plugins
    # If you want to be able to add metadata for a job
    # to track anything you want, extend it with this module.
    #
    # For example:
    #
    #     require 'resque-meta'
    #
    #     class MyJob
    #       extend Resque::Plugins::Meta
    #
    #       def self.perform(meta_id, *args)
    #         heavy_lifting
    #       end
    #     end
    #
    #     meta0 = MyJob.enqueue('stuff')
    #     meta0.enqueued_at # => 'Wed May 19 13:42:41 -0600 2010'
    #     meta0.meta_id # => '03c9e1a045ad012dd20500264a19273c'
    #     meta0['foo'] = 'bar' # => 'bar'
    #     meta0.save
    #
    #     # later
    #     meta1 = MyJob.get_meta('03c9e1a045ad012dd20500264a19273c')
    #     meta1.job_class # => MyJob
    #     meta1.enqueued_at # => 'Wed May 19 13:42:41 -0600 2010'
    #     meta1['foo'] # => 'bar'
    module Meta

      # Override in your job to control the metadata id. It is
      # passed the same arguments as `perform`, that is, your job's
      # payload.
      def meta_id(*args)
        Digest::SHA1.hexdigest([ Time.now.to_f, rand, self, args ].join)
      end

      # Override in your job to control the how many seconds a job's
      # metadata will live after it finishes.  Defaults to 24 hours.
      # Return nil or 0 to set them to never expire.
      def expire_meta_in
        24 * 60 * 60
      end

      # Enqueues a job in Resque and return the association metadata.
      # The meta_id in the returned object can be used to fetch the
      # metadata again in the future.
      def enqueue(*args)
        meta = Metadata.new('meta_id' => meta_id(args), 'job_class' => self.to_s)
        meta.save
        Resque.enqueue(self, meta.meta_id, *args)
        meta
      end

      def get_meta(meta_id)
        Metadata.get(meta_id, self)
      end
      module_function :get_meta
      public :get_meta

      def before_perform_meta(meta_id, *args)
        if meta = get_meta(meta_id)
          meta.start!
        end
      end

      def after_perform_meta(meta_id, *args)
        if meta = get_meta(meta_id)
          meta.finish!
        end
      end

      def on_failure_meta(e, meta_id, *args)
        if meta = get_meta(meta_id)
          meta.fail!
        end
      end
    end
  end
end
