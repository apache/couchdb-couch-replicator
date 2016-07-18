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

-module(couch_replicator_doc_processor_worker).

-export([spawn_worker/3]).

-include("couch_replicator.hrl").

-import(couch_replicator_utils, [
    pp_rep_id/1
]).

-define(WORKER_TIMEOUT_MSEC, 61000).

% Spawn a worker which attempts to calculate replication id then add a
% replication job to scheduler. This function create a monitor to the worker
% a worker will then exit with the #doc_worker_result{} record within
% ?WORKER_TIMEOUT_MSEC timeout period.A timeout is considered a `temporary_error`.
% Result will be sent as the `Reason` in the {'DOWN',...} message.
-spec spawn_worker(db_doc_id(), #rep{}, seconds()) -> reference().
spawn_worker(Id, Rep, WaitSec) ->
    {_Pid, WRef} = spawn_monitor(fun() -> worker_fun(Id, Rep, WaitSec) end),
    WRef.


% Private functions

-spec worker_fun(db_doc_id(), #rep{}, seconds()) -> no_return().
worker_fun(Id, Rep, WaitSec) ->
    timer:sleep(WaitSec * 1000),
    Fun = fun() ->
        try maybe_start_replication(Id, Rep) of
            Res ->
                exit(Res)
        catch
            throw:{filter_fetch_error, Reason} ->
                exit({temporary_error, Reason});
            _Tag:Reason ->
                exit({temporary_error, Reason})
        end
    end,
    {Pid, Ref} = spawn_monitor(Fun),
    receive
        {'DOWN', Ref, _, Pid, Result} ->
            exit(#doc_worker_result{id = Id, result = Result})
    after ?WORKER_TIMEOUT_MSEC ->
        erlang:demonitor(Ref, [flush]),
        exit(Pid, kill),
        {DbName, DocId} = Id,
        Msg = io_lib:format("Replication for db ~p doc ~p failed to start due "
            "to timeout after ~B seconds", [DbName, DocId, ?WORKER_TIMEOUT_MSEC/1000]),
        Result = {temporary_error, couch_util:to_binary(Msg)},
        exit(#doc_worker_result{id = Id, result = Result})
    end.


% Try to start a replication. Used by a worker. This function should return
% rep_start_result(), also throws {filter_fetch_error, Reason} if cannot fetch filter.
% It can also block for an indeterminate amount of time while fetching the
% filter.
maybe_start_replication(Id, RepWithoutId) ->
    Rep = couch_replicator_docs:update_rep_id(RepWithoutId),
    case maybe_add_job_to_scheduler(Id, Rep) of
    {ok, RepId} ->
        {ok, RepId};
    {temporary_error, Reason} ->
        {temporary_error, Reason};
    {permanent_failure, Reason} ->
        {DbName, DocId} = Id,
        couch_replicator_docs:update_failed(DbName, DocId, Reason),
        {permanent_failure, Reason}
    end.


-spec maybe_add_job_to_scheduler(db_doc_id(), #rep{}) -> rep_start_result().
maybe_add_job_to_scheduler({_DbName, DocId}, Rep) ->
    RepId = Rep#rep.id,
    case couch_replicator_scheduler:rep_state(RepId) of
    nil ->
        case couch_replicator_scheduler:add_job(Rep) of
        ok ->
           ok;
        {error, already_added} ->
            couch_log:warning("replicator scheduler: ~p was already added", [Rep])
        end,
        {ok, RepId};
    #rep{doc_id = DocId} ->
        {ok, RepId};
    #rep{doc_id = null} ->
        Msg = io_lib:format("Replication `~s` specified by document `~s`"
            " already running as a transient replication, started via"
            " `_replicate` API endpoint", [pp_rep_id(RepId), DocId]),
        {temporary_error, couch_util:to_binary(Msg)};
    #rep{db_name = OtherDb, doc_id = OtherDocId} ->
        Msg = io_lib:format("Replication `~s` specified by document `~s`"
            " already started, triggered by document `~s` from db `~s`",
            [pp_rep_id(RepId), DocId, OtherDocId, mem3:dbname(OtherDb)]),
        {permanent_failure, couch_util:to_binary(Msg)}
    end.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(R1, {"0b7831e9a41f9322a8600ccfa02245f2", ""}).


doc_processor_worker_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_should_add_job(),
            t_already_running_same_docid(),
            t_already_running_transient(),
            t_already_running_other_db_other_doc(),
            t_spawn_worker()
        ]
    }.


% Replication is already running, with same doc id. Ignore change.
t_should_add_job() ->
   ?_test(begin
       Id = {?DB, ?DOC1},
       Rep = couch_replicator_docs:parse_rep_doc_without_id(change()),
       ?assertEqual({ok, ?R1}, maybe_start_replication(Id, Rep)),
       ?assert(added_job())
   end).


% Replication is already running, with same doc id. Ignore change.
t_already_running_same_docid() ->
   ?_test(begin
       Id = {?DB, ?DOC1},
       mock_already_running(?DB, ?DOC1),
       Rep = couch_replicator_docs:parse_rep_doc_without_id(change()),
       ?assertEqual({ok, ?R1}, maybe_start_replication(Id, Rep)),
       ?assert(did_not_add_job())
   end).


% There is a transient replication with same replication id running. Ignore change.
t_already_running_transient() ->
   ?_test(begin
       Id = {?DB, ?DOC1},
       mock_already_running(null, null),
       Rep = couch_replicator_docs:parse_rep_doc_without_id(change()),
       ?assertMatch({temporary_error, _}, maybe_start_replication(Id, Rep)),
       ?assert(did_not_add_job())
   end).


% There is a duplicate replication potentially from a different db and doc.
% Write permanent failure to doc.
t_already_running_other_db_other_doc() ->
   ?_test(begin
       Id = {?DB, ?DOC1},
       mock_already_running(<<"otherdb">>, <<"otherdoc">>),
       Rep = couch_replicator_docs:parse_rep_doc_without_id(change()),
       ?assertMatch({permanent_failure, _}, maybe_start_replication(Id, Rep)),
       ?assert(did_not_add_job()),
       1 == meck:num_calls(couch_replicator_docs, update_failed, '_')
   end).


% Should spawn worker
t_spawn_worker() ->
   ?_test(begin
       Id = {?DB, ?DOC1},
       Rep = couch_replicator_docs:parse_rep_doc_without_id(change()),
       Ref = spawn_worker(Id, Rep, 0),
       Res = receive  {'DOWN', Ref, _, _, Reason} -> Reason
           after 1000 -> timeout end,
       Expect = #doc_worker_result{id = Id, result = {ok, ?R1}},
       ?assertEqual(Expect, Res),
       ?assert(added_job())
   end).


% Test helper functions

setup() ->
    meck:expect(couch_replicator_scheduler, add_job, 1, ok),
    meck:expect(config, get, fun(_, _, Default) -> Default end),
    meck:expect(couch_server, get_uuid, 0, this_is_snek),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    meck:expect(couch_replicator_scheduler, rep_state, 1, nil),
    ok.


teardown(_) ->
    meck:unload().


mock_already_running(DbName, DocId) ->
    meck:expect(couch_replicator_scheduler, rep_state,
         fun(RepId) -> #rep{id = RepId, doc_id = DocId, db_name = DbName} end).


added_job() ->
    1 == meck:num_calls(couch_replicator_scheduler, add_job, '_').


did_not_add_job() ->
    0 == meck:num_calls(couch_replicator_scheduler, add_job, '_').


change() ->
    {[
         {<<"_id">>, ?DOC1},
         {<<"source">>, <<"src">>},
         {<<"target">>, <<"tgt">>}
     ]}.

-endif.
