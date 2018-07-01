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
    refresh_interval: 30,
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
    assert_equal 30, d.instance.refresh_interval
  end
end
