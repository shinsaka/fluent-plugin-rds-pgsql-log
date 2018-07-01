require 'fluent/input'
require 'aws-sdk'

class Fluent::Plugin::RdsPgsqlLogInput < Fluent::Plugin::Input
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

    raise Fluent::ConfigError.new("region is required") unless @region
    if !has_iam_role?
      raise Fluent::ConfigError.new("access_key_id is required") if @access_key_id.nil?
      raise Fluent::ConfigError.new("secret_access_key is required") if @secret_access_key.nil?
    end
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
      options = {
        :region => @region,
      }
      if @access_key_id && @secret_access_key
        options[:access_key_id] = @access_key_id
        options[:secret_access_key] = @secret_access_key
      end
      @rds = Aws::RDS::Client.new(options)
    rescue => e
      log.warn "RDS Client error occurred: #{e.message}"
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
  end

  private

  def run
    @loop.run
  end

  def input
    get_and_parse_posfile
    log_files = get_logfile_list
    get_logfile(log_files)
    put_posfile
  end

  def has_iam_role?
    begin
      ec2 = Aws::EC2::Client.new(region: @region)
      !ec2.config.credentials.nil?
    rescue => e
      log.warn "EC2 Client error occurred: #{e.message}"
    end
  end

  def get_and_parse_posfile
    begin
      # get & parse pos file
      log.debug "pos file get start"

      pos_last_written_timestamp = 0
      pos_info = {}
      File.open(@pos_file, File::RDONLY) do |file|
        file.each_line do |line|

          pos_match = /^(\d+)$/.match(line)
          if pos_match
            pos_last_written_timestamp = pos_match[1].to_i
            log.debug "pos_last_written_timestamp: #{pos_last_written_timestamp}"
          end

          pos_match = /^(.+)\t(.+)$/.match(line)
          if pos_match
            pos_info[pos_match[1]] = pos_match[2]
            log.debug "log_file: #{pos_match[1]}, marker: #{pos_match[2]}"
          end
        end
        @pos_last_written_timestamp = pos_last_written_timestamp
        @pos_info = pos_info
      end
    rescue => e
      log.warn "pos file get and parse error occurred: #{e.message}"
    end
  end

  def put_posfile
    # pos file write
    begin
      log.debug "pos file write"
      File.open(@pos_file, File::WRONLY|File::TRUNC) do |file|
        file.puts @pos_last_written_timestamp.to_s

        @pos_info.each do |log_file_name, marker|
          file.puts "#{log_file_name}\t#{marker}"
        end
      end
    rescue => e
      log.warn "pos file write error occurred: #{e.message}"
    end
  end

  def get_logfile_list
    begin
      log.debug "get logfile-list from rds: db_instance_identifier=#{@db_instance_identifier}, pos_last_written_timestamp=#{@pos_last_written_timestamp}"
      @rds.describe_db_log_files(
        db_instance_identifier: @db_instance_identifier,
        file_last_written: @pos_last_written_timestamp,
        max_records: 10,
      )
    rescue => e
      log.warn "RDS Client describe_db_log_files error occurred: #{e.message}"
    end
  end

  def get_logfile(log_files)
    begin
      log_files.each do |log_file|
        log_file.describe_db_log_files.each do |item|
          # save maximum written timestamp value
          @pos_last_written_timestamp = item[:last_written] if @pos_last_written_timestamp < item[:last_written]

          # log file download
          log_file_name = item[:log_file_name]
          marker = @pos_info.has_key?(log_file_name) ? @pos_info[log_file_name] : "0"

          log.debug "download log from rds: log_file_name=#{log_file_name}, marker=#{marker}"
          logs = @rds.download_db_log_file_portion(
            db_instance_identifier: @db_instance_identifier,
            log_file_name: log_file_name,
            marker: marker,
          )
          raw_records = get_logdata(logs)

          #emit
          parse_and_emit(raw_records, log_file_name) unless raw_records.nil?
        end
      end
    rescue => e
      log.warn e.message
    end
  end

  def get_logdata(logs)
    log_file_name = logs.context.params[:log_file_name]
    raw_records = []
    begin
      logs.each do |log|
        # save got line's marker
        @pos_info[log_file_name] = log.marker

        raw_records += log.log_file_data.split("\n")
      end
    rescue => e
      log.warn e.message
    end
    return raw_records
  end

  def parse_and_emit(raw_records, log_file_name)
    begin
      log.debug "raw_records.count: #{raw_records.count}"
      record = nil
      raw_records.each do |raw_record|
        log.debug "raw_record=#{raw_record}"
        line_match = LOG_REGEXP.match(raw_record)

        unless line_match
          # combine chain of log
          record["message"] << "\n" + raw_record unless record.nil?
        else
          # emit before record
          router.emit(@tag, Fluent::Engine.now, record) unless record.nil?

          # set a record
          record = {
            "time" => line_match[:time],
            "host" => line_match[:host],
            "user" => line_match[:user],
            "database" => line_match[:database],
            "pid" => line_match[:pid],
            "message_level" => line_match[:message_level],
            "message" => line_match[:message],
            "log_file_name" => log_file_name,
          }
        end
      end
      # emit last record
      router.emit(@tag, Fluent::Engine.now, record) unless record.nil?
    rescue => e
      log.warn e.message
    end
  end

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, &callback)
      @callback = callback
      on_timer # first call
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    end
  end
end
