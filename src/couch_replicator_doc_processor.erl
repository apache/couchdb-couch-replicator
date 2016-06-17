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
    couch_stats:increment_counter([couch_replicator, docs, dbs_created]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_deleted(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_deleted]),
    clean_up_replications(DbName),
    Server.

db_found(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_found]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_change(DbName, {ChangeProps} = Change, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, db_changes]),
    try
        ok = process_update(DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_failed(DbName, DocId, Error)
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
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"triggered">> ->
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"completed">> ->
            couch_log:notice("Ignoring completed replication '~s'", [DocId]);
        <<"error">> ->
            % Handle replications started from older versions of replicator
            % which wrote transient errors to replication docs
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"failed">> ->
            couch_log:warning("Ignoring failed replication '~s'", [DocId])
        end;
    {Owner, false} ->
        ok
    end,
    ok.


-spec maybe_start_replication(binary(), binary(), {[_]}) -> ok.
maybe_start_replication(DbName, DocId, RepDoc) ->
    #rep{id = RepId} = Rep0 = couch_replicator_docs:parse_rep_doc(RepDoc),
    Rep = Rep0#rep{db_name = DbName},
    case couch_replicator_scheduler:rep_state(RepId) of
    nil ->
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
            Msg = io_lib:format("Replication `~s` specified by document `~s`"
                " already started, triggered by document `~s` from the same"
                " database", [pp_rep_id(RepId), DocId, OtherDocId]),
            couch_log:notice(Msg, []),
            couch_replicator_docs:update_failed(DbName, DocId, Msg);
        false ->
            Msg = io_lib:format("Replication `~s` specified by document `~s`"
                " already started triggered by document `~s` from a different"
                " database", [pp_rep_id(RepId), DocId, OtherDocId]),
            couch_log:warning(Msg, []),
            couch_replicator_docs:update_failed(DbName, DocId, Msg)
        end
    end,
    ok.


-spec remove_jobs(binary(), binary()) -> ok.
remove_jobs(DbName, DocId) ->
    RepIds = couch_replicator_scheduler:find_jobs_by_doc(DbName, DocId),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


-spec clean_up_replications(binary()) -> ok.
clean_up_replications(DbName) ->
    RepIds = couch_replicator_scheduler:find_jobs_by_dbname(DbName),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(DOC2, <<"doc2">>).
-define(R1, {"1", ""}).
-define(R2, {"2", ""}).


doc_processor_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_bad_change(),
            t_regular_change(),
            t_deleted_change(),
            t_triggered_change(),
            t_completed_change(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_already_running_same_docid(),
            t_already_running_transient(),
            t_already_running_other_db_other_doc(),
            t_already_running_other_doc_same_db()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        meck:expect(couch_replicator_docs, parse_rep_doc,
            fun(_) -> throw({bad_rep_doc, <<"bad">>}) end),
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change())),
        ?assert(added_job())
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
            fun(?DB, ?DOC1) -> [#rep{id = ?R2}] end),
        ?assertEqual(ok, process_update(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(added_job())
    end).

% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assert(did_not_add_job())
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(added_job())
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"failed">>))),
        ?assert(did_not_add_job())
    end).


% Normal change, but according to cluster ownership algorithm, replication belongs to
% a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_update(?DB, change())),
        ?assert(did_not_add_job())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% Replication is already running, with same doc id. Ignore change.
t_already_running_same_docid() ->
   ?_test(begin
       mock_already_running(?DB, ?DOC1),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% There is a transient replication with same replication id running. Ignore change.
t_already_running_transient() ->
   ?_test(begin
       mock_already_running(null, null),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% There is a duplicate replication potentially from a different db and doc.
% Write permanent failure to doc.
t_already_running_other_db_other_doc() ->
   ?_test(begin
       mock_already_running(<<"otherdb">>, ?DOC2),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job()),
       ?assert(updated_doc_with_failed_state())
   end).

% There is a duplicate replication potentially from same db and different doc.
% Write permanent failure to doc.
t_already_running_other_doc_same_db() ->
   ?_test(begin
       mock_already_running(?DB, ?DOC2),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job()),
       ?assert(updated_doc_with_failed_state())
   end).


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_scheduler, add_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    meck:expect(couch_replicator_docs, parse_rep_doc,
        fun({DocProps}) ->
            #rep{id = ?R1, doc_id = get_json_value(<<"_id">>, DocProps)}
        end).


teardown(_) ->
    meck:unload().


mock_already_running(DbName, DocId) ->
    meck:expect(couch_replicator_scheduler, rep_state,
         fun(RepId) -> #rep{id = RepId, doc_id = DocId, db_name = DbName} end).


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


added_job() ->
    meck:called(couch_replicator_scheduler, add_job, [
        #rep{id = ?R1, db_name = ?DB, doc_id = ?DOC1}]).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [#rep{id = Id}]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_add_job() ->
    0 == meck:num_calls(couch_replicator_scheduler, add_job, '_').


updated_doc_with_failed_state() ->
    1 == meck:num_calls(couch_replicator_docs, update_failed, '_').


change() ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


change(State) ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>},
            {<<"_replication_state">>, State}
        ]}}
    ]}.


deleted_change() ->
    {[
        {<<"id">>, ?DOC1},
        {<<"deleted">>, true},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


bad_change() ->
    {[
        {<<"id">>, ?DOC2},
        {doc, {[
            {<<"_id">>, ?DOC2},
            {<<"source">>, <<"src">>}
        ]}}
    ]}.




-endif.
