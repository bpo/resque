require 'resque/scheduler'

module Resque
  module Failure
    ##
    # Support for automatically retrying jobs after the fashion of
    # Delayed::Job.
    class AutoRetry < Base
      extend Helpers

      ##
      # require 'resque/failure/auto_retry'
      #
      # Resque::Failure.backend = Resque::Failure::AutoRetry
      # Resque::Failure::AutoRetry.backend = Resque::Failure::Hoptoad
      #
      # AutoRetry's backend support is used after a job fails repeated
      # retries.
      extend BackendSupport

      attr_accessor :attempts, :id

      def self.max=(max)
        @max = max
      end

      def self.max
        @max ||= 10 # keep this low until the scheduler is persistent
      end

      #
      # resque execution of delayed payloads
      #
      def self.perform(attempts, payload)
        Resque.constantize(payload['class']).perform(*payload['args'])
      end

      #
      # failure handling
      #
      def save
        unwrap_payload
        attempts < AutoRetry.max ? try_again : give_up
      end

      def try_again
        delay = (attempts ** 4) + 5

        log "Scheduling #{payload.inspect} for execution in #{delay}s after #{attempts} attempts."
        Scheduler.enqueue_at(Time.now + delay, queue, self.class, attempts, payload)
      end

      def give_up
        log "Giving up on #{payload.inspect} after #{attempts} attempts."
        AutoRetry.backend.new(exception, worker, queue, payload).save
      end

      def unwrap_payload
        if payload["class"] == self.class.to_s
          self.attempts = self.payload["args"].shift.to_i + 1
          self.payload  = self.payload["args"].shift
        else
          self.attempts = 1
        end
      end
    end
  end
end
