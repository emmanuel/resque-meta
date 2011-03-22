test_dir = File.dirname(__FILE__)
$LOAD_PATH << File.expand_path('../lib', test_dir)
require 'test/unit'
require 'rubygems'
require 'resque'

#
# make sure we can run redis
#

if !system("which redis-server")
  puts '', "** can't find `redis-server` in your path"
  puts "** try running `sudo rake install`"
  abort ''
end


#
# start our own redis when the tests start,
# kill it when they end
#

at_exit do
  next if $!

  exit_code = Test::Unit::AutoRunner.run

  pid = `ps -A -o pid,command | grep [r]edis-test`.split(" ")[0]
  puts "Killing test redis server[#{pid}]..."
  `rm -f #{test_dir}/dump.rdb`
  Process.kill("KILL", pid.to_i)
  exit exit_code
end

puts "Starting redis for testing at localhost:9736..."
`redis-server #{test_dir}/redis-test.conf`
Resque.redis = 'localhost:9736'
