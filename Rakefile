require 'rubygems'
require 'rake'
require 'rake/rdoctask'

desc 'Default: run unit tests.'
task :default => :test

begin
  require 'spec/rake/spectask'
  desc 'Test the gem.'
  Spec::Rake::SpecTask.new(:test) do |t|
    t.spec_files = FileList.new('spec/**/*_spec.rb')
  end
rescue LoadError
  task :test do
    STDERR.puts "You must have rspec >= 1.3.0 to run the tests"
  end
end

desc 'Generate documentation for sunspot_index_queue.'
Rake::RDocTask.new(:rdoc) do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.options << '--title' << 'Sunspot Index Queue' << '--line-numbers' << '--inline-source' << '--main' << 'README.rdoc'
  rdoc.rdoc_files.include('README.rdoc')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sunspot_index_queue"
    gem.summary = %Q{Asynchronous Solr indexing support for the sunspot gem with an emphasis on reliablity and throughput.}
    gem.description = %Q(This gem provides asynchronous indexing to Solr for the sunspot gem. It uses a pluggable model for the backing queue and provides support for ActiveRecord and MongoDB out of the box.)
    gem.email = "brian@embellishedvisions.com"
    gem.homepage = "http://github.com/bdurand/sunspot_index_queue"
    gem.authors = ["Brian Durand"]
    gem.rdoc_options = ["--charset=UTF-8", "--main", "README.rdoc"]
    
    gem.add_dependency('sunspot', '>= 1.1.0')
    gem.add_development_dependency('sqlite3')
    gem.add_development_dependency('activerecord', '>= 2.2')
    gem.add_development_dependency('dm-core', '>= 1.0.0')
    gem.add_development_dependency('dm-aggregates', '>=1.0.0')
    gem.add_development_dependency('dm-migrations', '>=1.0.0')
    gem.add_development_dependency('mongo')
    gem.add_development_dependency('rspec', '>= 1.3.0')
    gem.add_development_dependency('jeweler')
  end

  Jeweler::GemcutterTasks.new
rescue LoadError
end
