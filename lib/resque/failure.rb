require 'resque/failure/backend_support'

module Resque
  module Failure

    ##
    # require 'resque/failure/hoptoad'
    # Resque::Failure.backend = Resque::Failure::Hoptoad
    extend BackendSupport

    def self.create(options = {})
      backend.new(*options.values_at(:exception, :worker, :queue, :payload)).save
    end
  end
end
