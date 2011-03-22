require File.expand_path('../test_helper', __FILE__)
require 'resque-timestamps'

class TimestampsTest < Test::Unit::TestCase
  class TimedJob
    extend Resque::Plugins::Timestamps
    @queue = :timed

    def self.perform(job_id, key, val)
      meta = get_meta(job_id)
      meta[key] = val
      meta.save
    end
  end

  class AnotherTimedJob
    extend Resque::Plugins::Timestamps
    @queue = :timed

    def self.perform(job_id)
    end
  end

  class SlowTimedJob
    extend Resque::Plugins::Timestamps
    @queue = :timed

    def self.expire_meta_in
      1
    end

    def self.perform(job_id, key, val)
      meta = get_meta(job_id)
      meta[key] = val
      meta.save
      sleep 0.5
    end
  end

  class FailingTimedJob
    extend Resque::Plugins::Timestamps
    @queue = :timed

    def self.perform(*args)
      raise 'boom'
    end
  end


  def setup
    Resque.redis.flushall
  end

  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::Timestamps)
    end
  end

  def test_enqueued_metadata
    now = Time.now
    job_id = TimedJob.enqueue('foo', 'bar')
    meta = TimedJob.get_meta(job_id)
    assert meta.enqueued_at.to_f > now.to_f, "#{meta.enqueued_at} should be after #{now}"
    assert meta.seconds_enqueued > 0.0, "seconds_enqueued should be greater than zero"
    assert meta.enqueued?
    assert !meta.started?
    assert_equal 0, meta.seconds_processing
    assert !meta.finished?
    assert_equal Resque::Plugins::Meta::Metadata, meta.class
    assert_equal TimedJob, meta.job_class
  end

  def test_processed_job
    job_id = TimedJob.enqueue('foo', 'bar')
    meta = TimedJob.get_meta(job_id)
    assert_nil meta['foo']
    worker = Resque::Worker.new(:timed)
    worker.work(0)

    meta = TimedJob.get_meta(job_id)
    assert_equal TimedJob, meta.job_class
    assert meta.started?, 'Job should have started'
    assert meta.finished?, 'Job should be finished'
    assert meta.succeeded?, 'Job should have succeeded'
    assert !meta.enqueued?, 'Job should have been removed from the queue'
    assert meta.seconds_enqueued > 0.0, "seconds_enqueued should be greater than zero"
    assert meta.seconds_processing > 0.0, "seconds_processing should be greater than zero"
  end

  def test_slow_job
    job_id = SlowTimedJob.enqueue('foo', 'bar')
    worker = Resque::Worker.new(:timed)
    thread = Thread.new { worker.work(0) }

    sleep 0.2
    meta = SlowTimedJob.get_meta(job_id)
    assert !meta.enqueued?
    assert meta.started?
    assert meta.working?
    assert !meta.finished?

    thread.join # job should be done
    meta.reload!
    assert !meta.enqueued?
    assert meta.started?
    assert !meta.working?
    assert meta.finished?
    assert meta.succeeded?
    assert !meta.failed?

    sleep 2
    assert_nil Resque::Plugins::Meta.get_meta(job_id)
  end

  def test_failing_job
    job_id = FailingTimedJob.enqueue()
    meta = FailingTimedJob.get_meta(job_id)
    assert_nil meta.failed?
    worker = Resque::Worker.new(:timed)
    worker.work(0)

    sleep 0.5
    meta.reload!
    assert meta.finished?
    assert meta.failed?
    assert !meta.succeeded?
  end

end
