% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_doc_processor).
-behaviour(couch_multidb_changes).

% multidb changes callback
-export([db_created/2, db_deleted/2, db_found/2, db_change/3]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3,
    pp_rep_id/1
]).


db_created(DbName, Server) ->
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_deleted(DbName, Server) ->
    clean_up_replications(DbName),
    Server.

db_found(DbName, Server) ->
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_change(DbName, {ChangeProps} = Change, Server) ->
    try
        ok = process_update(DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_doc_process_error(DbName, DocId, Error)
    end,
    Server.


-spec process_update(binary(), {[_]}) -> ok.
process_update(DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    Owner = couch_replicator_clustering:owner(DbName, DocId),
    case {Owner, get_json_value(deleted, Change, false)} of
    {_, true} ->
        remove_jobs(DbName, DocId);
    {unstable, false} ->
	couch_log:notice("Not starting '~s' as cluster is unstable", [DocId]);
    {ThisNode, false} when ThisNode =:= node() ->
        couch_log:notice("Maybe starting '~s' as I'm the owner", [DocId]),
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"triggered">> ->
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"completed">> ->
            couch_log:notice("Replication '~s' marked as completed", [DocId]);
        <<"error">> ->
            % Handle replications started from older versions of replicator
            % which wrote transient errors to replication docs
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"failed">> ->
            Reason = get_json_value(<<"_replication_state_reason">>, RepProps),
            Msg = "Replication '~s' marked as failed with reason '~s'",
            couch_log:warning(Msg, [DocId, Reason])
        end;
    {Owner, false} ->
        couch_log:notice("Not starting '~s' as owner is ~s.", [DocId, Owner])
    end,
    ok.


-spec maybe_start_replication(binary(), binary(), {[_]}) -> ok.
maybe_start_replication(DbName, DocId, RepDoc) ->
    Rep0 = couch_replicator_docs:parse_rep_doc(RepDoc),
    #rep{id = {BaseId, _} = RepId} = Rep0,
    Rep = Rep0#rep{db_name = DbName},
    case couch_replicator:rep_state(RepId) of
    nil ->
        couch_log:notice("Attempting to start replication `~s` (document `~s`).",
            [pp_rep_id(RepId), DocId]),
        case couch_replicator_scheduler:add_job(Rep) of
        ok ->
            ok;
        {error, already_added} ->
            couch_log:warning("replicator scheduler: ~p was already added", [Rep])
        end,
        ok;
    #rep{doc_id = DocId} ->
        ok;
    #rep{doc_id = null} ->
        couch_log:warning("Replication `~s` specified by document `~s`"
            " already running as a transient replication, started via"
            " `_replicate` API endpoint", [pp_rep_id(RepId), DocId]);
    #rep{db_name = OtherDbName, doc_id = OtherDocId} ->
        case mem3:dbname(OtherDbName) =:= mem3:dbname(DbName) of
        true ->
            couch_log:notice("Replication `~s` specified by document `~s`"
                " already started, triggered by document `~s` from the same"
                " database", [pp_rep_id(RepId), DocId, OtherDocId]);
        false ->
            couch_log:warning("Replication `~s` specified by document `~s`"
                " already started triggered by document `~s` from a different"
                " database", [pp_rep_id(RepId), DocId, OtherDocId])
        end,
        maybe_tag_rep_doc(DbName, DocId, RepDoc, ?l2b(BaseId))
    end,
    ok.


-spec maybe_tag_rep_doc(binary(), binary(), {[_]}, binary()) -> ok.
maybe_tag_rep_doc(DbName, DocId, {RepProps}, RepId) ->
    case get_json_value(<<"_replication_id">>, RepProps) of
    RepId ->
        ok;
    _ ->
        couch_replicator_docs:update_doc_replication_id(DbName, DocId, RepId)
    end.


-spec remove_jobs(binary(), binary()) -> ok.
remove_jobs(DbName, DocId) ->
    LogMsg = "Stopped replication `~s` , replication document `~s`",
    [
        begin
            couch_replicator_scheduler:remove_job(RepId),
            couch_log:notice(LogMsg, [pp_rep_id(RepId), DocId])
        end || RepId <- find_jobs_by_doc(DbName, DocId)
    ],
    ok.


% TODO: make this a function in couch_replicator_scheduler API
-spec clean_up_replications(binary()) -> ok.
clean_up_replications(DbName) ->
    RepIds = find_jobs_by_dbname(DbName),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


% TODO: make this a function in couch_replicator_scheduler API
-spec find_jobs_by_dbname(binary()) -> list(#rep{}).
find_jobs_by_dbname(DbName) ->
    RepSpec = #rep{db_name = DbName, _ = '_'},
    MatchSpec = {job, '$1', RepSpec, '_', '_'},
    [RepId || [RepId] <- ets:match(couch_replicator_scheduler, MatchSpec)].


% TODO: make this a function in couch_replicator_scheduler API
-spec find_jobs_by_doc(binary(), binary()) -> list(#rep{}).
find_jobs_by_doc(DbName, DocId) ->
    RepSpec =  #rep{db_name = DbName, doc_id = DocId, _ = '_'},
    MatchSpec = {job, '$1', RepSpec, '_', '_'},
    [RepId || [RepId] <- ets:match(couch_replicator_scheduler, MatchSpec)].



