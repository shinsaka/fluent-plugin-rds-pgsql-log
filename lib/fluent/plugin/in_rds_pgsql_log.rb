class Fluent::RdsPgsqlLogInput < Fluent::Input
  Fluent::Plugin.register_input('rds_pgsql_log', self)

  LOG_REGEXP = /^(?<time>\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2} .+?):(?<host>.*?):(?<user>.*?)@(?<database>.*?):\[(?<pid>.*?)\]:(?<message_level>.*?):(?<message>.*)$/

  config_param :access_key_id, :string, :default => nil
  config_param :secret_access_key, :string, :default => nil
  config_param :region, :string, :default => nil
  config_param :db_instance_identifier, :string, :default => nil
  config_param :pos_file, :string, :default => "fluentd-plugin-rds-pgsql-log-pos.dat"
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
    FileUtils.touch(@marker_pos_file)
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
      $log.warn "fluent-plugin-rds-pgsql-log: #{e.message}"
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
    $log.info "fluent-plugin-rds-pgsql-log: input start"

    # get & parse pos file
    pos_last_written_timestamp = 0
    pos_info = {}
    File.open(@pos_file, File::RDONLY) do |file|
      file.each_line do |line|
        pos_match = /^(\d+)$/.match(line)
        pos_last_written_timestamp = pos.match.values_at(1).to_i if pos_match.size == 2

        pos_match = /^(.+)\t(.+)$/.match(line)
        pos_info[pos.match.values_at(1)] = pos.match.values_at(2) if pos_match.size == 3
      end
    end

    # get log file list
    log_files = @rds.describe_db_log_files(
      db_instance_identifier: @db_instance_identifier,
      file_last_written: pos_last_written_timestamp,
      max_records: 10,
    )
    
    log_files.each do |log_file|
      log_file.describe_db_log_files.each do |item|
        # save maximum written timestamp value
        pos_last_written_timestamp = item[:last_written] if pos_last_written_timestamp < item[:last_written]

        # log file download
        log_file_name = item[:log_file_name]
        marker = pos_info[log_file_name]

        logs = @rds.download_db_log_file_portion(
          db_instance_identifier: @db_instance_identifier,
          log_file_name: item[:log_file_name],
          number_of_lines: 1,
          marker: marker,
        )
        logs.each do |log|
          # save got line's marker
          pos_info[log_file_name] log.marker

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

    # pos file write
    File.open(@pos_file, File::WRONLY) do |file|
      file.write pos_last_written_timestamp.to_s

      pos_info.each do |log_file_name, marker|
        file.write "#{log_file_name}\t#{marker}"
      end
    end
    $log.info "fluent-plugin-rds-pgsql-log: input end"
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
