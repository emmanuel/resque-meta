require File.expand_path('../test_helper', __FILE__)
require 'resque-meta'

class MetaJob
  extend Resque::Plugins::Meta
  @queue = :test

  def self.expire_meta_in
    1
  end

  def self.perform(job_id, key, val)
    meta = get_meta(job_id)
    meta[key] = val
    meta.save
  end
end

class AnotherJob
  extend Resque::Plugins::Meta
  @queue = :test

  def self.perform(job_id)
  end
end

class SlowJob
  extend Resque::Plugins::Meta
  @queue = :test

  def self.expire_meta_in
    1
  end

  def self.perform(job_id, key, val)
    meta = get_meta(job_id)
    meta[key] = val
    meta.save
    sleep 1
  end
end

class MetaTest < Test::Unit::TestCase
  def setup
    Resque.redis.flushall
  end

  def test_meta_version
    assert_equal '1.0.3', Resque::Plugins::Meta::Version
  end

  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::Meta)
    end
  end

  def test_resque_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 8
  end

  def test_enqueued_metadata
    now = Time.now
    meta = MetaJob.enqueue('foo', 'bar')
    assert_not_nil meta
    assert_not_nil meta.job_id
    assert_nil meta['foo']
    assert_equal Resque::Plugins::Meta::Metadata, meta.class
    assert_equal MetaJob, meta.job_class
  end

  def test_processed_job
    meta = MetaJob.enqueue('foo', 'bar')
    assert_nil meta['foo']
    worker = Resque::Worker.new(:test)
    worker.work(0)

    meta = MetaJob.get_meta(meta.job_id)
    assert_equal MetaJob, meta.job_class
    assert_equal 'bar', meta['foo'], "'foo' not found in #{meta.inspect}"
  end

  def test_wrong_id_for_class
    meta = MetaJob.enqueue('foo', 'bar')

    assert_nil AnotherJob.get_meta(meta.job_id)
    assert_not_nil Resque::Plugins::Meta.get_meta(meta.job_id)
    assert_not_nil Resque::Plugins::Meta::Metadata.get(meta.job_id)
  end

  def test_expired_metadata
    meta = MetaJob.enqueue('foo', 'bar')
    worker = Resque::Worker.new(:test)
    worker.work(0)

    sleep 2
    reloaded = MetaJob.get_meta(meta.job_id)
    assert_nil reloaded
  end

  def test_slow_job
    meta = SlowJob.enqueue('foo', 'bar')
    worker = Resque::Worker.new(:test)
    thread = Thread.new { worker.work(0) }

    sleep 0.1
    meta = SlowJob.get_meta(meta.job_id)

    thread.join # job should be done
    meta.reload!

    sleep 2 # metadata should be expired
    assert_nil Resque::Plugins::Meta.get_meta(meta.job_id)
  end

  def test_saving_additional_metadata
    meta = MetaJob.enqueue('stuff')
    meta['foo'] = 'bar'
    meta.save

    # later
    meta = MetaJob.get_meta(meta.job_id)
    assert_equal 'bar', meta['foo']
  end
end
