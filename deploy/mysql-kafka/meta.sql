drop table if exists `maxwell`.`bootstrap`;
create table bootstrap
(
    id              bigint auto_increment primary key,
    database_name   varchar(255) charset utf8                     not null,
    table_name      varchar(255) charset utf8                     not null,
    where_clause    text                                          null,
    is_complete     tinyint unsigned            default '0'       not null,
    inserted_rows   bigint unsigned             default '0'       not null,
    total_rows      bigint unsigned             default '0'       not null,
    created_at      datetime                                      null,
    started_at      datetime                                      null,
    completed_at    datetime                                      null,
    binlog_file     varchar(255)                                  null,
    binlog_position int unsigned                default '0'       null,
    client_id       varchar(255) charset latin1 default 'maxwell' not null,
    comment         varchar(255) charset utf8                     null
);



drop table if exists `maxwell`.`columns`;
create table columns
(
    id            bigint auto_increment primary key,
    schema_id     bigint                    null,
    table_id      bigint                    null,
    name          varchar(255) charset utf8 null,
    charset       varchar(255)              null,
    coltype       varchar(255)              null,
    is_signed     tinyint unsigned          null,
    enum_values   text charset utf8         null,
    column_length tinyint unsigned          null
);

create index schema_id on columns (schema_id);
create index table_id on columns (table_id);


drop table if exists `maxwell`.`databases`;
create table `databases`
(
    id        bigint auto_increment primary key,
    schema_id bigint                    null,
    name      varchar(255) charset utf8 null,
    charset   varchar(255)              null
);

create index schema_id on `databases` (schema_id);


drop table if exists `maxwell`.`heartbeats`;
create table heartbeats
(
    server_id int unsigned                                  not null,
    client_id varchar(255) charset latin1 default 'maxwell' not null,
    heartbeat bigint                                        not null,
    primary key (server_id, client_id)
);


drop table if exists `maxwell`.`positions`;
create table positions
(
    server_id           int unsigned                                  not null,
    binlog_file         varchar(255)                                  null,
    binlog_position     int unsigned                                  null,
    gtid_set            varchar(4096)                                 null,
    client_id           varchar(255) charset latin1 default 'maxwell' not null,
    heartbeat_at        bigint                                        null,
    last_heartbeat_read bigint                                        null,
    primary key (server_id, client_id)
);

drop table if exists `maxwell`.`schemas`;
create table `schemas`
(
    id                  bigint auto_increment primary key,
    binlog_file         varchar(255)                  null,
    binlog_position     int unsigned                  null,
    last_heartbeat_read bigint            default 0   null,
    gtid_set            varchar(4096)                 null,
    base_schema_id      bigint                        null,
    deltas              mediumtext charset utf8       null,
    server_id           int unsigned                  null,
    position_sha        char(40) charset latin1       null,
    charset             varchar(255)                  null,
    version             smallint unsigned default '0' not null,
    deleted             tinyint(1)        default 0   not null,
    constraint position_sha unique (position_sha)
);


drop table if exists `maxwell`.`tables`;
create table tables
(
    id          bigint auto_increment primary key,
    schema_id   bigint                     null,
    database_id bigint                     null,
    name        varchar(255) charset utf8  null,
    charset     varchar(255)               null,
    pk          varchar(1024) charset utf8 null
);

create index database_id on tables (database_id);
create index schema_id on tables (schema_id);
