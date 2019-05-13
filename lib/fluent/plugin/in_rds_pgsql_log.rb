require 'fluent/input'
require 'aws-sdk-ec2'
require 'aws-sdk-rds'

class Fluent::Plugin::RdsPgsqlLogInput < Fluent::Plugin::Input
  Fluent::Plugin.register_input('rds_pgsql_log', self)

  helpers :timer

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
  end

  def start
    super

    # pos file touch
    File.open(@pos_file, File::RDWR|File::CREAT).close

    schedule_next
  end

  def shutdown
    super
  end

  private

  def input
    get_and_parse_posfile

    log_files = get_first_unseen_log_file
    return schedule_next if log_files.empty?

    additional_data_pending = read_and_forward(log_files[0])

    put_posfile

    # Directly schedule next fetch if additional data is pending
    return schedule_next(1) if additional_data_pending

    schedule_next
  rescue => e
    log.warn "input fetching error occurred: #{e.message}"
    schedule_next
  end

  def schedule_next(interval = @refresh_interval)
    timer_execute(:poll_logs, interval, repeat: false, &method(:input))
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

  def get_first_unseen_log_file
    begin
      log.debug "get logfile-list from rds: db_instance_identifier=#{@db_instance_identifier}, pos_last_written_timestamp=#{@pos_last_written_timestamp}"
      response = @rds.describe_db_log_files(
        db_instance_identifier: @db_instance_identifier,
        file_last_written: @pos_last_written_timestamp,
        max_records: 1,
      )

      response[:describe_db_log_files]
    rescue => e
      log.warn "RDS Client describe_db_log_files error occurred: #{e.message}"
    end
  end

  def read_and_forward(log_file)
    begin
      # log file download
      log_file_name = log_file[:log_file_name]
      marker = @pos_info.has_key?(log_file_name) ? @pos_info[log_file_name] : "0"

      log.debug "download log from rds: log_file_name=#{log_file_name}, marker=#{marker}"
      log_file_portion = @rds.download_db_log_file_portion(
        db_instance_identifier: @db_instance_identifier,
        log_file_name: log_file_name,
        marker: marker,
      )
      raw_records = get_logdata(log_file_portion, log_file_name)

      unless raw_records.nil?
        # save maximum written timestamp value
        last_seen_record_time = parse_and_emit(raw_records, log_file_name)
        unless last_seen_record_time.nil?
          @pos_last_written_timestamp = timestamp_with_ms(last_seen_record_time)
        else
          @pos_last_written_timestamp += 1
        end
      else
        @pos_last_written_timestamp += 1
      end

      additional_data_pending = log_file_portion.additional_data_pending
    rescue => e
      log.warn e.message
    end
  end

  def timestamp_with_ms(time)
    (Time.parse(time).to_f * 1000).to_i
  end

  def get_logdata(log_file_portion, log_file_name)
    # save got line's marker
    @pos_info[log_file_name] = log_file_portion.marker

    log_file_portion.log_file_data.split("\n")
  rescue => e
    log.warn e.message
  end

  def event_time_of_row(record)
    time = Time.parse(record["time"])
    return Fluent::EventTime.from_time(time)
  end

  def parse_and_emit(raw_records, log_file_name)
    last_seen_record_time = nil

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
          router.emit(@tag, event_time_of_row(record), record) unless record.nil?

          # set a record
          last_seen_record_time = line_match[:time]
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
      router.emit(@tag, event_time_of_row(record), record) unless record.nil?
    rescue => e
      log.warn e.message
    end

    last_seen_record_time
  end
end
