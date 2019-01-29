require_relative '../helper'

class RdsPgsqlLogInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  DEFAULT_CONFIG = {
    access_key_id: 'dummy_access_key_id',
    secret_access_key: 'dummy_secret_access_key',
    region: 'ap-northeast-1',
    db_instance_identifier: 'test-postgres-id',
    refresh_interval: 2,
    pos_file: 'pgsql-log-pos.dat',
  }

  def parse_config(conf = {})
    ''.tap{|s| conf.each { |k, v| s << "#{k} #{v}\n" } }
  end

  def create_driver(conf = DEFAULT_CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::RdsPgsqlLogInput).configure(parse_config conf)
  end

  def iam_info_url
    'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
  end

  def use_iam_role
    stub_request(:get, iam_info_url)
      .to_return(status: [200, 'OK'], body: "hostname")
    stub_request(:get, "#{iam_info_url}hostname")
      .to_return(status: [200, 'OK'],
                 body: {
                   "AccessKeyId" => "dummy",
                   "SecretAccessKey" => "secret",
                   "Token" => "token"
                 }.to_json)
  end

  def test_configure
    use_iam_role
    d = create_driver
    assert_equal 'dummy_access_key_id', d.instance.access_key_id
    assert_equal 'dummy_secret_access_key', d.instance.secret_access_key
    assert_equal 'ap-northeast-1', d.instance.region
    assert_equal 'test-postgres-id', d.instance.db_instance_identifier
    assert_equal 'pgsql-log-pos.dat', d.instance.pos_file
    assert_equal 2, d.instance.refresh_interval
  end

  def test_get_log_files
    use_iam_role
    d = create_driver

    aws_client_stub = Aws::RDS::Client.new(stub_responses: {
      describe_db_log_files: {
        describe_db_log_files: [
          {
            log_file_name: 'db.log',
            last_written: 123,
            size: 123
          }
        ],
        marker: 'marker'
      },
      download_db_log_file_portion: {
        log_file_data: "2019-01-26 22:10:20 UTC::@:[129155]:LOG:some db log",
        marker: 'marker',
        additional_data_pending: false
      }
    })

    d.instance.instance_variable_set(:@rds, aws_client_stub)

    d.run(timeout: 3, expect_emits: 1)

    events = d.events
    assert_equal(events[0][1].iso8601, "2019-01-26T22:10:20Z")
    assert_equal(events[0][2]["pid"], '129155')
    assert_equal(events[0][2]["message_level"], 'LOG')
    assert_equal(events[0][2]["message"], 'some db log')
    assert_equal(events[0][2]["log_file_name"], 'db.log')
  end
end
