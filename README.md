# Amazon RDS for PostgreSQL log input plugin for fluentd

## Overview
- Amazon Web Services RDS log input plugin for fluentd

## Installation

    $ fluentd-gem fluent-plugin-rds-pgsql-log

## AWS ELB Settings
- settings see: [PostgreSQL Database Log Files](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_LogAccess.Concepts.PostgreSQL.html)

## Configuration

```config
<source>
  type rds_pgsql_log
  access_key_id          <access_key>
  secret_access_key      <secret_access_key>
  region                 <region name>
  db_instance_identifier <instance identifier>
  refresh_interval       <interval number by second(default value is 30 if omitted)>
  tag                    <tag name(default value is rds-pgsql.log>
  pos_file               <log getting position file(optional)>
</source>
```

### Example setting
```config
<source>
  type rds_pgsql_log
  access_key_id     XXXXXXXXXXXXXXXXXXXX
  secret_access_key xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  region ap-northeast-1
  db_instance_identifier test-postgres
  refresh_interval  30
  tag pgsql.log
  pos_file /tmp/pgsql-log-pos.dat
</source>

<match pgsql.log>
  type stdout
</match>
```

### json output example
```
```

