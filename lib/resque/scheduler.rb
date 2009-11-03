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
      delayed = encode "queue" => queue,
                       "class" => klass,
                       "args"  => args

      id = hash_id(delayed)
      redis.sadd "tasks", id
      redis.set "time_#{id}", time
      redis.set "task_#{id}", delayed
    end

    # Drops any pending jobs into the appropriate Resque queue.
    # Returns false until every job needing scheduled has been
    # scheduled correctly.
    def self.schedule_all_jobs
      time = Time.now.to_i + FUZZINESS
      $stdout.puts "Checking for jobs to be run before #{time}"
      return unless redis.type("resque:scheduler:tasks") == "set"

      while id = redis.sort("tasks", :by => "time_*", :limit => [0,1]).first
        run_at = redis.get "time_#{id}"
        break if run_at.to_i > time # next scheduled is in the future.
        run_job(id)
      end
    end

    def self.run_job(id)
      $stdout.puts "Running job #{id}"
      job = decode redis.get("task_#{id}")
      clean(id)
      Resque.push(job["queue"], job, true) if job
    end

    def self.clean(id)
      redis.del "task_#{id}"
      redis.del "time_#{id}"
      redis.srem "tasks", id
    end

    def self.run
      loop do
        schedule_all_jobs
        sleep 2 * FUZZINESS
      end
    end
  end
end
