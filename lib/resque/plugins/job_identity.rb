require 'digest/sha1'

module Resque
  module Plugin
    def before_enqueue_hooks(job)
      job.methods.grep(/^before_enqueue/).sort
    end
  end

  module Plugins
    module JobIdentity
      # Enqueue a job in Resque and return the associated job_id.
      # The returned job_id can be used to refer to the job in the future.
      # prepends enqueued job_id to args
      def enqueue(*args)
        job_id = job_identity(args)

        before_enqueue_hooks = Resque::Plugin.before_enqueue_hooks(self)
        before_enqueue_hooks.each do |hook|
          send(hook, job_id, *args)
        end

        Resque.enqueue(self, job_id, *args)

        job_id
      end

      # Override in your job to control the job id. It is passed the same
      # arguments as `perform`, that is, your job's payload.
      # NOTE: expects a single args array, not a list of args!
      def job_identity(args)
        Digest::SHA1.hexdigest([ Time.now.to_f, rand, self, args ].join)
      end

    end
  end
end