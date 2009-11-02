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
      run_at = time.to_i
      delayed = encode( { "time"  => time,
                          "queue" => queue,
                          "class" => klass,
                          "args"  => args } )
      id = hash_id(delayed)

      redis.sadd "tasks", id
      redis.set "task_#{id}", delayed
      redis.set "time_#{id}", run_at
    end

    # Drops any pending jobs into the appropriate Resque queue.
    # Returns false until every job needing scheduled has been
    # scheduled correctly.
    def self.jobs_scheduled
      time = Time.now.to_i + FUZZINESS
      $stdout.puts "Checking for jobs to be run before #{time}"
      tasks = redis.sort "tasks", :by => "resque:scheduler:time_*",
                                  :limit => [0, 10]
      return true if tasks.empty?

      while (id = tasks.shift) && (run_at = redis.get("time_#{id}")) && run_at.to_i <= time
        job = decode redis.get("task_#{id}")
        redis.del "time_#{id}"
        redis.del "task_#{id}"
        $stdout.puts "Enqueuing #{job.inspect}"
        Resque.push job["queue"], job, true
        redis.srem "tasks", id
      end

      # remaining tasks aren't until later?
      !tasks.empty?
    end

    # In the off chance the scheduler dies unexpectedly, this cleans
    # orphaned data.
    def self.clean
      redis.smembers("tasks").each do |id|
        if !redis.get("time_#{id}") || !redis.get("task_#{id}")
          redis.del("time_#{id}")
          redis.del("task_#{id}")
          redis.srem("tasks", id)
        end
      end
    end

    def self.run
      clean
      loop do
        while jobs_scheduled
          sleep 2*FUZZINESS
        end
      end
    end
  end
end
