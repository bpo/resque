module Resque
  module Failure

    ##
    # Pass common class-level methods from the failure system through
    # to a specified Failure backend.
    #
    module BackendSupport
      def backend=(backend)
        @backend = backend
      end

      def backend
        return @backend if @backend
        require 'resque/failure/redis'
        @backend = Failure::Redis
      end

      def count
        backend.count
      end

      def all(start = 0, count = 1)
        backend.all(start, count)
      end

      def url
        backend.url
      end
    end
  end
end
