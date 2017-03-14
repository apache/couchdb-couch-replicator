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

-module(couch_replicator).

-export([replicate/2, ensure_rep_db_exists/0]).
-export([stream_active_docs_info/3, stream_terminal_docs_info/4]).
-export([replication_states/0]).
-export([job/1, doc/3]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").
-include("couch_replicator_api_wrap.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").

-define(REPLICATION_STATES, [
    initializing,  % Just added to scheduler
    error,         % Could not be turned into a replication job
    running,       % Scheduled and running
    pending,       % Scheduled and waiting to run
    crashing,      % Scheduled but crashing, possibly backed off by the scheduler
    completed,     % Non-continuous (normal) completed replication
    failed         % Terminal failure, will not be retried anymore
]).

-import(couch_util, [
    get_value/2,
    get_value/3
]).


-type user_doc_cb() :: fun(({[_]}, any()) -> any()).
-type query_acc() :: {binary(), user_doc_cb(), any()}.


-spec replicate({[_]}, #user_ctx{}) ->
    {ok, {continuous, binary()}} |
    {ok, {[_]}} |
    {ok, {cancelled, binary()}} |
    {error, any()}.
replicate(PostBody, Ctx) ->
    {ok, Rep0} = couch_replicator_utils:parse_rep_doc(PostBody, Ctx),
    Rep = Rep0#rep{start_time = os:timestamp()},
    #rep{id = RepId, options = Options, user_ctx = UserCtx} = Rep,
    case get_value(cancel, Options, false) of
    true ->
        CancelRepId = case get_value(id, Options, nil) of
        nil ->
            RepId;
        RepId2 ->
            RepId2
        end,
        case check_authorization(CancelRepId, UserCtx) of
        ok ->
            cancel_replication(CancelRepId);
        not_found ->
            {error, not_found}
        end;
    false ->
        check_authorization(RepId, UserCtx),
        {ok, Listener} = rep_result_listener(RepId),
        Result = do_replication_loop(Rep),
        couch_replicator_notifier:stop(Listener),
        Result
    end.


% This is called from supervisor. Must respect supervisor protocol so
% it returns `ignore`.
-spec ensure_rep_db_exists() -> ignore.
ensure_rep_db_exists() ->
    {ok, _Db} = couch_replicator_docs:ensure_rep_db_exists(),
    couch_log:notice("~p : created local _replicator database", [?MODULE]),
    ignore.


-spec do_replication_loop(#rep{}) ->
    {ok, {continuous, binary()}} | {ok, tuple()} | {error, any()}.
do_replication_loop(#rep{id = {BaseId, Ext} = Id, options = Options} = Rep) ->
    case couch_replicator_scheduler:add_job(Rep) of
    ok ->
        ok;
    {error, already_added} ->
        couch_log:notice("Replication '~s' already running", [BaseId ++ Ext]),
        ok
    end,
    case get_value(continuous, Options, false) of
    true ->
        {ok, {continuous, ?l2b(BaseId ++ Ext)}};
    false ->
        wait_for_result(Id)
    end.


-spec rep_result_listener(rep_id()) -> {ok, pid()}.
rep_result_listener(RepId) ->
    ReplyTo = self(),
    {ok, _Listener} = couch_replicator_notifier:start_link(
        fun({_, RepId2, _} = Ev) when RepId2 =:= RepId ->
                ReplyTo ! Ev;
            (_) ->
                ok
        end).


-spec wait_for_result(rep_id()) ->
    {ok, any()} | {error, any()}.
wait_for_result(RepId) ->
    receive
    {finished, RepId, RepResult} ->
        {ok, RepResult};
    {error, RepId, Reason} ->
        {error, Reason}
    end.


-spec cancel_replication(rep_id()) ->
    {ok, {cancelled, binary()}} | {error, not_found}.
cancel_replication({BasedId, Extension} = RepId) ->
    FullRepId = BasedId ++ Extension,
    couch_log:notice("Canceling replication '~s' ...", [FullRepId]),
    case couch_replicator_scheduler:rep_state(RepId) of
    #rep{} ->
        ok = couch_replicator_scheduler:remove_job(RepId),
        couch_log:notice("Replication '~s' cancelled", [FullRepId]),
        {ok, {cancelled, ?l2b(FullRepId)}};
    nil ->
        couch_log:notice("Replication '~s' not found", [FullRepId]),
        {error, not_found}
    end.


-spec replication_states() -> [atom()].
replication_states() ->
    ?REPLICATION_STATES.


-spec stream_terminal_docs_info(binary(), user_doc_cb(), any(), [atom()]) -> any().
stream_terminal_docs_info(Db, Cb, UserAcc, States) ->
    DDoc = <<"_replicator">>,
    View = <<"terminal_states">>,
    QueryCb = fun handle_replicator_doc_query/2,
    Args = #mrargs{view_type = map, reduce = false},
    Acc = {Db, Cb, UserAcc, States},
    try fabric:query_view(Db, DDoc, View, QueryCb, Acc, Args) of
    {ok, {Db, Cb, UserAcc1, States}} ->
        UserAcc1
    catch
        error:database_does_not_exist ->
            UserAcc;
        error:{badmatch, {not_found, Reason}} ->
            Msg = "Could not find _design/~s ~s view in replicator db ~s : ~p",
            couch_log:error(Msg, [DDoc, View, Db, Reason]),
            couch_replicator_docs:ensure_cluster_rep_ddoc_exists(Db),
            timer:sleep(1000),
            stream_terminal_docs_info(Db, Cb, UserAcc, States)
    end.


-spec stream_active_docs_info(user_doc_cb(), any(), [atom()]) -> any().
stream_active_docs_info(Cb, UserAcc, States) ->
    Nodes = lists:sort([node() | nodes()]),
    stream_active_docs_info(Nodes, Cb, UserAcc, States).


-spec stream_active_docs_info([node()], user_doc_cb(), any(), [atom()]) -> any().
stream_active_docs_info([], _Cb, UserAcc, _States) ->
    UserAcc;

stream_active_docs_info([Node | Nodes], Cb, UserAcc, States) ->
    case rpc:call(Node, couch_replicator_doc_processor, docs, [States]) of
        {badrpc, Reason} ->
            ErrMsg = "Could not get replicator docs from ~p. Error: ~p",
            couch_log:error(ErrMsg, [Node, Reason]),
            stream_active_docs_info(Nodes, Cb, UserAcc, States);
        Results ->
            UserAcc1 = lists:foldl(Cb, UserAcc, Results),
            stream_active_docs_info(Nodes, Cb, UserAcc1, States)
    end.


-spec handle_replicator_doc_query
    ({row, [_]} , query_acc()) -> {ok, query_acc()};
    ({error, any()}, query_acc()) -> {error, any()};
    ({meta, any()}, query_acc()) -> {ok,  query_acc()};
    (complete, query_acc()) -> {ok, query_acc()}.
handle_replicator_doc_query({row, Props}, {Db, Cb, UserAcc, States}) ->
    DocId = couch_util:get_value(id, Props),
    DocStateBin = couch_util:get_value(key, Props),
    DocState = erlang:binary_to_existing_atom(DocStateBin, utf8),
    MapValue = couch_util:get_value(value, Props),
    {Source, Target, StartTime, StateTime, StateInfo} = case MapValue of
        [Src, Tgt, StartT, StateT, Info] ->
            {Src, Tgt, StartT, StateT, Info};
        _Other ->
            % Handle the case where the view code was upgraded but new view code
            % wasn't updated yet (before a _scheduler/docs request was made)
            {null, null, null, null, null}
    end,
    % Set the error_count to 1 if failed. This is mainly for consistency as
    % jobs from doc_processor and scheduler will have that value set
    ErrorCount = case DocState of failed -> 1; _ -> 0 end,
    case filter_replicator_doc_query(DocState, States) of
        true ->
            EjsonInfo = {[
                {doc_id, DocId},
                {database, Db},
                {id, null},
                {source, strip_url_creds(Source)},
                {target, strip_url_creds(Target)},
                {state, DocState},
                {error_count, ErrorCount},
                {info, StateInfo},
                {last_updated, StateTime},
                {start_time, StartTime}
            ]},
            {ok, {Db, Cb, Cb(EjsonInfo, UserAcc), States}};
        false ->
            {ok, {Db, Cb, UserAcc, States}}
    end;
handle_replicator_doc_query({error, Reason}, _Acc) ->
    {error, Reason};
handle_replicator_doc_query({meta, _Meta}, Acc) ->
    {ok, Acc};
handle_replicator_doc_query(complete, Acc) ->
    {stop, Acc}.


-spec strip_url_creds(binary() | {[_]}) -> binary().
strip_url_creds(Endpoint) ->
    case couch_replicator_docs:parse_rep_db(Endpoint, [], []) of
        #httpdb{url=Url} ->
            iolist_to_binary(couch_util:url_strip_password(Url));
        LocalDb when is_binary(LocalDb) ->
            LocalDb
    end.


-spec filter_replicator_doc_query(atom(), [atom()]) -> boolean().
filter_replicator_doc_query(_DocState, []) ->
    true;
filter_replicator_doc_query(State, States) when is_list(States) ->
    lists:member(State, States).


-spec job(binary()) -> {ok, {[_]}} | {error, not_found}.
job(JobId0) when is_binary(JobId0) ->
    JobId = couch_replicator_ids:convert(JobId0),
    {Res, _Bad} = rpc:multicall(couch_replicator_scheduler, job, [JobId]),
    case [JobInfo || {ok, JobInfo} <- Res] of
        [JobInfo| _] ->
            {ok, JobInfo};
        [] ->
            {error, not_found}
    end.


-spec doc(binary(), binary(), [_]) -> {ok, {[_]}} | {error, not_found}.
doc(RepDb, DocId, UserCtx) ->
    {Res, _Bad} = rpc:multicall(couch_replicator_doc_processor, doc, [RepDb, DocId]),
    case [DocInfo || {ok, DocInfo} <- Res] of
        [DocInfo| _] ->
            {ok, DocInfo};
        [] ->
            doc_from_db(RepDb, DocId, UserCtx)
    end.


-spec doc_from_db(binary(), binary(), [_]) -> {ok, {[_]}} | {error, not_found}.
doc_from_db(RepDb, DocId, UserCtx) ->
    case fabric:open_doc(RepDb, DocId, [UserCtx, ejson_body]) of
        {ok, Doc} ->
            {Props} = couch_doc:to_json_obj(Doc, []),
            Source = get_value(<<"source">>, Props),
            Target = get_value(<<"target">>, Props),
            State = get_value(<<"_replication_state">>, Props, null),
            StartTime = get_value(<<"_replication_start_time">>, Props, null),
            StateTime = get_value(<<"_replication_state_time">>, Props, null),
            {StateInfo, ErrorCount} = case State of
                <<"completed">> ->
                    Info = get_value(<<"_replication_stats">>, Props, null),
                    {Info, 0};
                <<"failed">> ->
                    Info = get_value(<<"_replication_state_reason">>, Props, null),
                    {Info, 1};
                _OtherState ->
                    {null, 0}
            end,
            {ok, {[
                {doc_id, DocId},
                {database, RepDb},
                {id, null},
                {source, strip_url_creds(Source)},
                {target, strip_url_creds(Target)},
                {state, State},
                {error_count, ErrorCount},
                {info, StateInfo},
                {start_time, StartTime},
                {last_updated, StateTime}
            ]}};
         {not_found, _Reason} ->
            {error, not_found}
    end.


-spec check_authorization(rep_id(), #user_ctx{}) -> ok | not_found.
check_authorization(RepId, #user_ctx{name = Name} = Ctx) ->
    case couch_replicator_scheduler:rep_state(RepId) of
    #rep{user_ctx = #user_ctx{name = Name}} ->
        ok;
    #rep{} ->
        couch_httpd:verify_is_server_admin(Ctx);
    nil ->
        not_found
    end.


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

authorization_test_() ->
    {
        foreach,
        fun () -> ok end,
        fun (_) -> meck:unload() end,
        [
            t_admin_is_always_authorized(),
            t_username_must_match(),
            t_replication_not_found()
        ]
    }.


t_admin_is_always_authorized() ->
    ?_test(begin
        expect_rep_user_ctx(<<"someuser">>, <<"_admin">>),
        UserCtx = #user_ctx{name = <<"adm">>, roles = [<<"_admin">>]},
        ?assertEqual(ok, check_authorization(<<"RepId">>, UserCtx))
    end).


t_username_must_match() ->
     ?_test(begin
        expect_rep_user_ctx(<<"user">>, <<"somerole">>),
        UserCtx1 = #user_ctx{name = <<"user">>, roles = [<<"somerole">>]},
        ?assertEqual(ok, check_authorization(<<"RepId">>, UserCtx1)),
        UserCtx2 = #user_ctx{name = <<"other">>, roles = [<<"somerole">>]},
        ?assertThrow({unauthorized, _}, check_authorization(<<"RepId">>, UserCtx2))
    end).


t_replication_not_found() ->
     ?_test(begin
        meck:expect(couch_replicator_scheduler, rep_state, 1, nil),
        UserCtx1 = #user_ctx{name = <<"user">>, roles = [<<"somerole">>]},
        ?assertEqual(not_found, check_authorization(<<"RepId">>, UserCtx1)),
        UserCtx2 = #user_ctx{name = <<"adm">>, roles = [<<"_admin">>]},
        ?assertEqual(not_found, check_authorization(<<"RepId">>, UserCtx2))
    end).


expect_rep_user_ctx(Name, Role) ->
    meck:expect(couch_replicator_scheduler, rep_state,
        fun(_Id) ->
            UserCtx = #user_ctx{name = Name, roles = [Role]},
            #rep{user_ctx = UserCtx}
        end).


strip_url_creds_test_() ->
     {
        foreach,
        fun () -> meck:expect(config, get, fun(_, _, Default) -> Default end) end,
        fun (_) -> meck:unload() end,
        [
            t_strip_local_db_creds(),
            t_strip_http_basic_creds(),
            t_strip_http_props_creds()
        ]
    }.


t_strip_local_db_creds() ->
    ?_test(?assertEqual(<<"localdb">>, strip_url_creds(<<"localdb">>))).


t_strip_http_basic_creds() ->
    ?_test(begin
        Url1 = <<"http://adm:pass@host/db">>,
        ?assertEqual(<<"http://adm:*****@host/db/">>, strip_url_creds(Url1)),
        Url2 = <<"https://adm:pass@host/db">>,
        ?assertEqual(<<"https://adm:*****@host/db/">>, strip_url_creds(Url2))
    end).


t_strip_http_props_creds() ->
    ?_test(begin
        Props1 = {[{<<"url">>, <<"http://adm:pass@host/db">>}]},
        ?assertEqual(<<"http://adm:*****@host/db/">>, strip_url_creds(Props1)),
        Props2 = {[ {<<"url">>, <<"http://host/db">>},
            {<<"headers">>, {[{<<"Authorization">>, <<"Basic pa55">>}]}}
        ]},
        ?assertEqual(<<"http://host/db/">>, strip_url_creds(Props2))
    end).

-endif.
