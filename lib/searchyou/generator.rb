require 'searchyou/indexer'
require 'searchyou/configuration'

module Searchyou

  class Generator < Jekyll::Generator

    safe true
    priority :lowest

    # Public: Invoked by Jekyll during the generation phase.
    def generate(site)

      # Gather the configuration options
      configuration = Configuration.new(site)

      # Prepare the indexer
      indexer = Searchyou::Indexer.new(configuration)
      indexer.start

      # Iterate through the site contents and send to indexer
      # TODO: what are we indexing?
      site.posts.each do |doc|
        indexer << doc.data.merge({
          id: doc.id,
          content: doc.content
        })
      end

      # Signal to the indexer that we're done adding content
      indexer.finish

    # Handle any exceptions gracefully
    rescue => e
      $stderr.puts "Searchyll: #{e.class.name} - #{e.message}"
      $stderr.puts "Backtrace: #{e.backtrace.each{|l| puts l};nil}"
      raise(e)
    end

  end

end
