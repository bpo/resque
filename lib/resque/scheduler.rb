module Resque
  module Scheduler
    extend Resque::Helpers

    # Jobs will be enqueued within FUZZINESS seconds of their target.
    FUZZINESS = 5

    #
    # redis access
    #
    def self.redis
      return @redis if @redis
      self.redis = Resque.redis
      self.redis
    end

    def self.redis=(server)
      @redis = Redis::Namespace.new(:scheduler, :redis => server)
    end

    # Resque::Scheduler.enqueue_at(Time.now, :critical, MyJob)
    # Resque::Scheduler.enqueue_at(Time.now + 600, :critical, MyJob)
    def self.enqueue_at(time, queue, klass, *args)
      delayed = encode "time"  => time,
                       "queue" => queue,
                       "class" => klass,
                       "args"  => args

      id = hash_id(delayed)
      redis.sadd "tasks", "#{time.to_i}:#{id}"
      redis.set "task_#{id}", delayed
    end

    # Drops any pending jobs into the appropriate Resque queue.
    # Returns false until every job needing scheduled has been
    # scheduled correctly.
    def self.schedule_all_jobs
      time = Time.now.to_i + FUZZINESS
      $stdout.puts "Checking for jobs to be run before #{time}"
      return unless redis.type("resque:scheduler:tasks") == "set"

      while redis.sort("tasks", :limit => [0,1]).first =~ /(.*):(.*)/
        run_at, id = $1, $2
        break if run_at.to_i > time # next scheduled is in the future.
        run_job(run_at, id)
      end
    end

    def self.run_job(run_at, id)
      $stdout.puts "Running job #{id}"
      job = decode redis.get("task_#{id}")
      clean(run_at, id)
      Resque.push(job["queue"], job, true) if job
    end

    def self.clean(run_at, id)
      redis.del "task_#{id}"
      redis.srem "tasks", "#{run_at}:#{id}"
    end

    def self.run
      loop do
        schedule_all_jobs
        sleep 2 * FUZZINESS
      end
    end
  end
end
