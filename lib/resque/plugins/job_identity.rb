require 'digest/sha1'

module Resque
  module Plugins
    module JobIdentity
      # Enqueue a job in Resque and return the associated job_id.
      # The returned job_id can be used to refer to the job in the future.
      def enqueue(*args)
        job_id = job_id(args)
        yield(job_id) if block_given?
        Resque.enqueue(self, job_id, *args)
        job_id
      end

      # Override in your job to control the job id. It is passed the same
      # arguments as `perform`, that is, your job's payload.
      def job_id(*args)
        Digest::SHA1.hexdigest([ Time.now.to_f, rand, self, args ].join)
      end

    end
  end
end