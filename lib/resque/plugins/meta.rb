require 'resque'
require 'resque/plugins/job_identity'
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
    #       def self.perform(job_id, *args)
    #         heavy_lifting
    #       end
    #     end
    #
    #     job_id = MyJob.enqueue('stuff') # => '03c9e1a045ad012dd20500264a19273c'
    #     meta0 = MyJob.get_meta(job_id)
    #     meta0.job_id          # => '03c9e1a045ad012dd20500264a19273c'
    #     meta0['foo'] = 'bar'  # => 'bar'
    #     meta0.save            # => meta0
    #
    #     # later
    #     meta1 = MyJob.get_meta('03c9e1a045ad012dd20500264a19273c')
    #     meta1.job_class       # => MyJob
    #     meta1['foo']          # => 'bar'
    module Meta
      include JobIdentity

      # Enqueues a job in Resque and return the associated metadata.
      # The job_id in the returned object can be used to fetch the
      # metadata again in the future.
      def before_enqueue_create_metadata(job_id, *args)
        Metadata.new(job_id, self).save
      end

    module_function

      # Override in your job to control the many seconds a job's
      # metadata will live after it finishes.  Defaults to 24 hours.
      # Return nil or 0 to set them to never expire.
      def expire_meta_in
        24 * 60 * 60
      end
      public :expire_meta_in

      def get_meta(job_id)
        Metadata.get(job_id, self)
      end
      public :get_meta

    end
  end
end
