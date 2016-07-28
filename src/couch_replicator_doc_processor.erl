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

-export([start_link/0]).
-export([docs/0]).

% multidb changes callback
-export([db_created/2, db_deleted/2, db_found/2, db_change/3]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2,
         code_change/3, terminate/2]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-define(ERROR_MAX_BACKOFF_EXPONENT, 17).  % ~ 1 day on average
-define(TS_DAY_SEC, 86400).

-type filter_type() ::  nil | view | user | docids | mango.
-type repstate() :: unscheduled | error | scheduled.


-record(rdoc, {
    id :: db_doc_id() | '_' | {any(), '_'},
    state :: repstate() | '_',
    rep :: #rep{} | nil | '_',
    rid :: rep_id() | nil | '_',
    filter :: filter_type() | '_',
    info :: binary() | nil | '_',
    errcnt :: non_neg_integer() | '_',
    worker :: reference() | nil | '_'
}).



% couch_multidb_changes API callbacks

db_created(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_created]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.


db_deleted(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_deleted]),
    ok = gen_server:call(?MODULE, {clean_up_replications, DbName}, infinity),
    Server.


db_found(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_found]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.


db_change(DbName, {ChangeProps} = Change, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, db_changes]),
    try
        ok = process_change(DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_failed(DbName, DocId, Error)
    end,
    Server.


% Private helpers for multidb changes API, these updates into the doc
% processor gen_server

process_change(DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    Owner = couch_replicator_clustering:owner(DbName, DocId),
    Id = {DbName, DocId},
    case {Owner, get_json_value(deleted, Change, false)} of
    {_, true} ->
        ok = gen_server:call(?MODULE, {removed, Id}, infinity);
    {unstable, false} ->
        couch_log:notice("Not starting '~s' as cluster is unstable", [DocId]);
    {ThisNode, false} when ThisNode =:= node() ->
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            ok = process_updated(Id, JsonRepDoc);
        <<"triggered">> ->
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            ok = process_updated(Id, JsonRepDoc);
        <<"completed">> ->
            ok = gen_server:call(?MODULE, {completed, Id}, infinity);
        <<"error">> ->
            % Handle replications started from older versions of replicator
            % which wrote transient errors to replication docs
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            ok = process_updated(Id, JsonRepDoc);
        <<"failed">> ->
            ok
        end;
    {Owner, false} ->
        ok
    end,
    ok.


process_updated({DbName, _DocId} = Id, JsonRepDoc) ->
    % Parsing replication doc (but not calculating the id) could throw an
    % exception which would indicate this document is malformed. This exception
    % should propagate to db_change function and will be recorded as permanent
    % failure in the document. User will have to delete and re-create the document
    % to fix the problem.
    Rep0 = couch_replicator_docs:parse_rep_doc_without_id(JsonRepDoc),
    Rep = Rep0#rep{db_name = DbName},
    Filter = case couch_replicator_filters:parse(Rep#rep.options) of
    {ok, nil} ->
        nil;
    {ok, {user, _FName, _QP}} ->
        user;
    {ok, {view, _FName, _QP}} ->
        view;
    {ok, {docids, _DocIds}} ->
        docids;
    {ok, {mango, _Selector}} ->
        mango;
    {error, FilterError} ->
        throw(FilterError)
    end,
    gen_server:call(?MODULE, {updated, Id, Rep, Filter}, infinity).


% Doc processor gen_server API and callbacks

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [],  []).


init([]) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #rdoc.id}]),
    {ok, nil}.


terminate(_Reason, _State) ->
    ok.


handle_call({updated, Id, Rep, Filter}, _From, State) ->
    ok = updated_doc(Id, Rep, Filter),
    {reply, ok, State};

handle_call({removed, Id}, _From, State) ->
    ok = removed_doc(Id),
    {reply, ok, State};

handle_call({completed, Id}, _From, State) ->
    true = ets:delete(?MODULE, Id),
    {reply, ok, State};

handle_call({clean_up_replications, DbName}, _From, State) ->
    ok = removed_db(DbName),
    {reply, ok, State}.


handle_cast(Msg, State) ->
    {stop, {error, unexpected_message, Msg}, State}.


handle_info({'DOWN', Ref, _, _, #doc_worker_result{id = Id, result = Res}},
        State) ->
    ok = worker_returned(Ref, Id, Res),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Doc processor gen_server private helper functions

% Handle doc update -- add to ets, then start a worker to try to turn it into
% a replication job. In most cases it will succeed quickly but for filtered
% replicationss or if there are duplicates, it could take longer
% (theoretically indefinitely) until a replication could be started.
-spec updated_doc(db_doc_id(), #rep{}, filter_type()) -> ok.
updated_doc(Id, Rep, Filter) ->
    Row = #rdoc{
        id = Id,
        state = unscheduled,
        rep = Rep,
        rid = nil,
        filter = Filter,
        info = nil,
        errcnt = 0,
        worker = nil
    },
    true = ets:insert(?MODULE, Row),
    ok = maybe_start_worker(Id),
    ok.


-spec worker_returned(reference(), db_doc_id(), rep_start_result()) -> ok.
worker_returned(Ref, Id, {ok, RepId}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref} = Row] ->
        true = ets:insert(?MODULE, update_docs_row(Row, RepId)),
        ok = maybe_start_worker(Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok;

worker_returned(Ref, Id, {temporary_error, Reason}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref, errcnt = ErrCnt} = Row] ->
        NewRow = Row#rdoc{
            rid = nil,
            state = error,
            info = Reason,
            errcnt = ErrCnt + 1,
            worker = nil
        },
        true = ets:insert(?MODULE, NewRow),
        ok = maybe_start_worker(Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok;

worker_returned(Ref, Id, {permanent_failure, _Reason}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref}] ->
        true = ets:delete(?MODULE, Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok.


% Filtered replication id didn't change.
-spec update_docs_row(#rdoc{}, rep_id()) -> #rdoc{}.
update_docs_row(#rdoc{rid = RepId, filter = user} = Row, RepId) ->
    Row#rdoc{state = scheduled, errcnt = 0, worker = nil};

% Calculated new replication id for a filtered replication. Make sure
% to schedule another check as filter code could change. Replications starts
% could have been failing, so also clear error count.
update_docs_row(#rdoc{rid = nil, filter = user} = Row, RepId) ->
    Row#rdoc{rid = RepId, state = scheduled, errcnt = 0, worker = nil};

% Replication id of existing replication job with filter has changed.
% Remove old replication job from scheduler and schedule check to check for
% future changes.
update_docs_row(#rdoc{rid = OldRepId, filter = user} = Row, RepId) ->
    ok = couch_replicator_scheduler:remove_job(OldRepId),
    Msg = io_lib:format("Replication id changed: ~p -> ~p", [OldRepId, RepId]),
    Row#rdoc{
        rid = RepId,
        state = scheduled,
        info = couch_util:to_binary(Msg),
        errcnt = 0,
        worker = nil
     };

% Calculated new replication id for non-filtered replication.
 update_docs_row(#rdoc{rid = nil} = Row, RepId) ->
    Row#rdoc{
        rep = nil, % remove replication doc body, after this we won't needed any more
        rid = RepId,
        state = scheduled,
        info = nil,
        errcnt = 0,
        worker = nil
     }.


-spec error_backoff(non_neg_integer()) -> seconds().
error_backoff(ErrCnt) ->
    Exp = min(ErrCnt, ?ERROR_MAX_BACKOFF_EXPONENT),
    5 + random:uniform(1 bsl Exp).


-spec filter_backoff() -> seconds().
filter_backoff() ->
    Total = ets:info(?MODULE, size),
    Range = 1 + min(2 * (Total / 10), ?TS_DAY_SEC),
    60 + random:uniform(round(Range)).


% Document removed from db -- clear ets table and remove all scheduled jobs
-spec removed_doc(db_doc_id()) -> ok.
removed_doc({DbName, DocId} = Id) ->
    ets:delete(?MODULE, Id),
    RepIds = couch_replicator_scheduler:find_jobs_by_doc(DbName, DocId),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


% Whole db shard is gone -- remove all its ets rows and stop jobs
-spec removed_db(binary()) -> ok.
removed_db(DbName) ->
    EtsPat = #rdoc{id = {DbName, '_'}, _ = '_'},
    ets:match_delete(?MODULE, EtsPat),
    RepIds = couch_replicator_scheduler:find_jobs_by_dbname(DbName),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


% Spawn a worker process which will attempt to calculate a replication id, then
% start a replication. Returns a process monitor reference. The worker is
% guaranteed to exit with rep_start_result() type only.
-spec maybe_start_worker(db_doc_id()) -> ok.
maybe_start_worker(Id) ->
    case ets:lookup(?MODULE, Id) of
    [] ->
        ok;
    [#rdoc{state = scheduled, filter = Filter}] when Filter =/= user ->
        ok;
    [#rdoc{rep = Rep} = Doc] ->
        Wait = get_worker_wait(Doc),
        WRef = couch_replicator_doc_processor_worker:spawn_worker(Id, Rep, Wait),
        true = ets:insert(?MODULE, Doc#rdoc{worker = WRef}),
        ok
    end.


-spec get_worker_wait(#rdoc{}) -> seconds().
get_worker_wait(#rdoc{state = scheduled, filter = user}) ->
    filter_backoff();

get_worker_wait(#rdoc{state = error, errcnt = ErrCnt}) ->
    error_backoff(ErrCnt);

get_worker_wait(#rdoc{state = unscheduled}) ->
    0.


% _scheduler/docs HTTP endpoint helpers

-spec docs() -> [{[_]}] | [].
docs() ->
    ets:foldl(fun(RDoc, Acc) -> [ejson_doc(RDoc) | Acc]  end, [], ?MODULE).


-spec ejson_state_info(binary() | nil) -> binary() | null.
ejson_state_info(nil) ->
    null;
ejson_state_info(Info) when is_binary(Info) ->
    Info.


-spec ejson_rep_id(rep_id() | nil) -> binary() | null.
ejson_rep_id(nil) ->
    null;
ejson_rep_id({BaseId, Ext}) ->
    iolist_to_binary([BaseId, Ext]).


-spec ejson_doc(#rdoc{}) -> {[_]}.
ejson_doc(RDoc) ->
    #rdoc{
       id = {DbName, DocId},
       state = RepState,
       info = StateInfo,
       rid = RepId,
       errcnt = ErrorCount
    } = RDoc,
    {[
        {doc_id, DocId},
        {database, mem3:dbname(DbName)},
        {id, ejson_rep_id(RepId)},
        {state, RepState},
        {info, ejson_state_info(StateInfo)},
        {error_count, ErrorCount},
        {node, node()}
    ]}.



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
            t_active_replication_completed(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_ejson_docs()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
            fun(?DB, ?DOC1) -> [#rep{id = ?R2}] end),
        ?assertEqual(ok, process_change(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Completed change comes for what used to be an active job. In this case
% remove entry from doc_processor's ets (because there is no linkage or
% callback mechanism for scheduler to tell doc_processsor a replication just
% completed).
t_active_replication_completed() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1}))
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"failed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Normal change, but according to cluster ownership algorithm, replication belongs to
% a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(did_not_spawn_worker())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_change(?DB, change())),
       ?assert(did_not_spawn_worker())
   end).


% Check if docs/0 function produces expected ejson after adding a job
t_ejson_docs() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        EJsonDocs = docs(),
        ?assertMatch([{[_|_]}], EJsonDocs),
        [{DocProps}] = EJsonDocs,
        ExpectedProps = [
            {database, ?DB},
            {doc_id, ?DOC1},
            {error_count, 0},
            {id, null},
            {info, null},
            {node, node()},
            {state, unscheduled}
        ],
        ?assertEqual(ExpectedProps, lists:usort(DocProps))
    end).


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(config, get, fun(_, _, Default) -> Default end),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_doc_processor_worker, spawn_worker, 3, wref),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    {ok, Pid} = start_link(),
    Pid.


teardown(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    meck:unload().


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


started_worker(Id) ->
    meck:called(couch_replicator_doc_processor_worker, spawn_worker,
        [Id, '_', '_']).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [#rep{id = Id}]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_spawn_worker() ->
    0 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker,
        '_').

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
