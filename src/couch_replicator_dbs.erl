-module(couch_replicator_dbs).

-behaviour(couch_define_db).

-export([databases/0, validate_name/1, options/1, name/1]).

databases() ->
    [name("replicator_db")].

validate_name(DbName) ->
    name("replicator_db") == couch_db:normalize_dbname(DbName).

options(_Name) ->
    [
        {before_doc_update, fun couch_replicator_manager:before_doc_update/2},
        {after_doc_read, fun couch_replicator_manager:after_doc_read/2},
        sys_db
    ].

name("replicator_db") ->
    config:get("replicator", "db", "_replicator").
