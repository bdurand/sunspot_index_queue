require 'rubygems'
require 'rake'

desc 'Default: run unit tests.'
task :default => :test

begin
  require 'rspec'
  require 'rspec/core/rake_task'
  desc 'Run the unit tests'
  RSpec::Core::RakeTask.new(:test)
rescue LoadError
  task :test do
    STDERR.puts "You must have rspec 2.0 installed to run the tests"
  end
end

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sunspot_index_queue"
    gem.summary = %Q{Asynchronous Solr indexing support for the sunspot gem with an emphasis on reliablity and throughput.}
    gem.description = %Q(This gem provides asynchronous indexing to Solr for the sunspot gem. It uses a pluggable model for the backing queue and provides support for ActiveRecord, DataMapper, and MongoDB out of the box.)
    gem.email = "brian@embellishedvisions.com"
    gem.homepage = "http://github.com/bdurand/sunspot_index_queue"
    gem.authors = ["Brian Durand"]
    gem.rdoc_options = ["--charset=UTF-8", "--main", "README.rdoc", "MIT_LICENSE"]
    
    gem.add_dependency('sunspot', '>= 1.1.0')
    gem.add_development_dependency('sqlite3')
    gem.add_development_dependency('activerecord', '>= 2.2')
    gem.add_development_dependency('dm-core', '>= 1.0.0')
    gem.add_development_dependency('dm-aggregates', '>=1.0.0')
    gem.add_development_dependency('dm-migrations', '>=1.0.0')
    gem.add_development_dependency('dm-sqlite-adapter', '>=1.0.0')
    gem.add_development_dependency('mongo')
    gem.add_development_dependency('rspec', '>= 2.0.0')
    gem.add_development_dependency('jeweler')
  end

  Jeweler::GemcutterTasks.new
rescue LoadError
end
