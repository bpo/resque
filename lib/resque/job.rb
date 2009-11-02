module Resque
  class Job
    attr_accessor :worker
    attr_reader   :queue, :payload
    include Helpers

    def initialize(queue, payload)
      @queue = queue
      @payload = payload
    end

    def self.create(queue, klass, *args)
      Resque.push(queue, :class => klass.to_s, :args => args)
    end

    def self.create_once(queue, klass, *args)
      Resque.push(queue, {:class => klass.to_s, :args => args}, true)
    end

    def self.reserve(queue)
      return unless payload = Resque.pop(queue)
      new(queue, payload)
    end

    def perform
      return unless object && object.respond_to?(:perform)
      args ? object.perform(*args) : object.perform
    end

    def object
      @object ||= objectify(@payload)
    end

    def args
      @payload['args']
    end

    def objectify(payload)
      if payload.is_a?(Hash) && payload['class']
        constantize(payload['class'])
      end
    end

    def fail(exception)
      Failure.create \
        :payload   => payload,
        :exception => exception,
        :worker    => worker,
        :queue     => queue
    end

    def inspect
      obj = @payload
      "(Job{%s} | %s | %s)" % [ @queue, obj['class'], obj['args'].inspect ]
    end
  end
end
