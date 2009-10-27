require 'redis/namespace'

begin
  require 'yajl'
rescue LoadError
  require 'json'
end

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/stat'
require 'resque/job'
require 'resque/worker'

module Resque
  extend self

  #
  # We need a Redis server!
  #

  def redis=(server)
    case server
    when String
      host, port = server.split(':')
      redis = Redis.new(:host => host, :port => port, :thread_safe => true)
      @redis = Redis::Namespace.new(:resque, :redis => redis)
    when Redis
      @redis = Redis::Namespace.new(:resque, :redis => server)
    else
      raise "I don't know what to do with #{server.inspect}"
    end
  end

  def redis
    return @redis if @redis
    self.redis = 'localhost:6379'
    self.redis
  end

  def to_s
    "Resque Client connected to #{redis.server}"
  end


  #
  # queue manipulation
  #

  def push(queue, item)
    watch_queue(queue)
    redis.rpush "queue:#{queue}", encode(item)
  end

  def pop(queue)
    decode redis.lpop("queue:#{queue}")
  end

  def size(queue)
    redis.llen("queue:#{queue}").to_i
  end

  def peek(queue, start = 0, count = 1)
    list_range("queue:#{queue}", start, count)
  end

  # Also used by Resque::Job for access to the `failed` faux-queue
  # (for now)
  def list_range(key, start = 0, count = 1)
    if count == 1
      decode redis.lindex(key, start)
    else
      Array(redis.lrange(key, start, start+count-1)).map do |item|
        decode item
      end
    end
  end

  def queues
    redis.smembers(:queues)
  end

  # Used internally to keep track of which queues
  # we've created.
  def watch_queue(queue)
    @watched_queues ||= {}
    return if @watched_queues[queue]
    redis.sadd(:queues, queue.to_s)
  end


  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will return a non-true value.
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    Job.create(determine_queue(klass), klass, *args)
  end

  # Just like 'enqueue' above, but this will skip enqueuing the job if
  # the job already exists in the queue.
  #
  # If no workers are running and this is called N times with the same
  # arguments, your job will be enqueued once.
  #
  # Note that this will only detect collisions with jobs created
  # with this method. Calling enqueue then enqueue_once will still
  # result in 2 jobs in the queue.
  def enqueue_once(klass, *args)
    Job.create_once(determine_queue(klass), klass, *args)
  end

  def determine_queue(klass)
    queue = klass.instance_variable_get(:@queue)
    queue ||= klass.queue if klass.respond_to?(:queue)

    if !queue || queue.to_s.empty?
      raise NoQueueError.new("Jobs must be placed onto a queue.")
    end
    queue
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end


  #
  # worker shortcuts
  #

  def workers
    Worker.all
  end

  def working
    Worker.working
  end


  #
  # stats
  #

  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.size,
      :failed    => Stat[:failed],
      :servers   => [redis.server]
    }
  end

  def keys
    redis.keys("*").map do |key|
      key.sub('resque:', '')
    end
  end


  #
  # encoding / decoding
  #

  def encode(object)
    if defined? Yajl
      Yajl::Encoder.encode(object)
    else
      JSON(object)
    end
  end

  def decode(object)
    return unless object

    if defined? Yajl
      Yajl::Parser.parse(object)
    else
      JSON(object)
    end
  end
end
