class Fluent::RdsPgsqlLogInput < Fluent::Input
  Fluent::Plugin.register_input('rds_pgsql_log', self)

  LOG_REGEXP = /^(?<time>\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2} .+?):(?<host>.*?):(?<user>.*?)@(?<database>.*?):\[(?<pid>.*?)\]:(?<message_level>.*?):(?<message>.*)$/

  config_param :access_key_id, :string, :default => nil
  config_param :secret_access_key, :string, :default => nil
  config_param :region, :string, :default => nil
  config_param :db_instance_identifier, :string, :default => nil
  config_param :pos_file, :string, :default => "fluent-plugin-rds-pgsql-log-pos.dat"
  config_param :refresh_interval, :integer, :default => 30
  config_param :tag, :string, :default => "rds-pgsql.log"

  def configure(conf)
    super
    require 'aws-sdk'

    if @access_key_id.nil? && has_not_iam_role?
      raise Fluent::ConfigError.new("access_key_id is required")
    end
    if @secret_access_key.nil? && has_not_iam_role?
      raise Fluent::ConfigError.new("secret_access_key is required")
    end

    raise Fluent::ConfigError.new("region is required") unless @region
    raise Fluent::ConfigError.new("db_instance_identifier is required") unless @db_instance_identifier
    raise Fluent::ConfigError.new("pos_file is required") unless @pos_file
    raise Fluent::ConfigError.new("refresh_interval is required") unless @refresh_interval
    raise Fluent::ConfigError.new("tag is required") unless @tag
  end

  def start
    super

    # pos file touch
    File.open(@pos_file, File::RDWR|File::CREAT).close

    begin
      param = {
        :region => @region,
        :access_key_id => @access_key_id,
        :secret_access_key => @secret_access_key,
      }
      @rds = Aws::RDS::Client.new(param)
    rescue => e
      $log.warn "RDS Client error occurred: #{e.message}"
    end
 
    @loop = Coolio::Loop.new
    timer_trigger = TimerWatcher.new(@refresh_interval, true, &method(:input))
    timer_trigger.attach(@loop)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    super
    @loop.stop
    @thread.join
    @timestamp_file.close
  end

  private

  def run
    @loop.run
  end

  def input
    begin
      # get & parse pos file
      $log.debug "pos file get start"
      pos_last_written_timestamp = 0
      pos_info = {}
      File.open(@pos_file, File::RDONLY) do |file|
        file.each_line do |line|
          $log.debug "pos: #{line}"
          
          pos_match = /^(\d+)$/.match(line)
          if pos_match
            pos_last_written_timestamp = pos_match[1].to_i
          end
  
          pos_match = /^(.+)\t(.+)$/.match(line)
          $log.debug "2: #{pos_match}"
          if pos_match
          #  pos_info[pos.match[1]] = pos_match[2]
          end
          $log.debug "3: #{pos_match}"
        end
      end
    rescue => e
      $og.warn "pos file get and parse error occurred: #{e.message}"
    end

    # get log file list
    begin
      $log.debug "get log file-list from rds #{@db_instance_identifier}, #{pos_last_written_timestamp}"
      log_files = @rds.describe_db_log_files(
        db_instance_identifier: @db_instance_identifier,
        file_last_written: pos_last_written_timestamp,
        max_records: 10,
      )
    rescue => e
      $log.warn "RDS Client describe_db_log_files error occurred: #{e.message}"
    end
    
    begin
      $log.debug "get log from rds"
      log_files.each do |log_file|
        log_file.describe_db_log_files.each do |item|
          # save maximum written timestamp value
          pos_last_written_timestamp = item[:last_written] if pos_last_written_timestamp < item[:last_written]
  
          # log file download
          log_file_name = item[:log_file_name]
          marker = pos_info[log_file_name]
  
          $log.debug "download log from rds: #{log_file_name}"
          logs = @rds.download_db_log_file_portion(
            db_instance_identifier: @db_instance_identifier,
            log_file_name: log_file_name,
            number_of_lines: 1,
            marker: marker,
          )
          logs.each do |log|
            # save got line's marker
            pos_info[log_file_name] = log.marker
  
            # 
            line_match = LOG_REGEXP.match(log.log_file_data)
            next unless line_match
  
            record = {
              "time" => line_match[:time],
              "host" => line_match[:host],
              "user" => line_match[:user],
              "database" => line_match[:database],
              "pid" => line_match[:pid],
              "message_level" => line_match[:message_level],
              "message" => line_match[:message],
            }
            Fluent::Engine.emit(@tag, Fluent::Engine.now, record)
          end
        end
      end
    rescue => e
      $log.warn "RDS Client describe_db_log_files error occurred: #{e.message}"
    end

    # pos file write
    begin
      $log.debug "pos file write"
      File.open(@pos_file, File::WRONLY|File::TRUNC) do |file|
        file.puts pos_last_written_timestamp.to_s

        pos_info.each do |log_file_name, marker|
          file.puts "#{log_file_name}\t#{marker}"
        end
      end
    rescue => e
      $log.warn "pos file write error occurred: #{e.message}"
    end

    $log.debug "input method end"
  end

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, &callback)
      @callback = callback
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    end
  end
end
