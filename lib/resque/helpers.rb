require 'digest/sha1'

module Resque
  module Helpers
    def hash_id(hash)
      Digest::SHA1.hexdigest hash.to_a.sort_by {|k,v| k.to_s}.to_s
    end

    def redis
      Resque.redis
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
end
