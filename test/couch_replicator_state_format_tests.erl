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

-module(couch_replicator_state_format_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

%%-define(TIMEOUT_EUNIT, 30).
-define(TIMEOUT, 1000).

-define(USER, "user_foo").
-define(PASS, "top_secret").

setup() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    ok = couch_db:close(Db),
    DbName.

setup(local) ->
    setup();
setup(remote) ->
    {remote, setup()};
setup({Service, {A, B}}) ->
    Ctx = test_util:start_couch([couch_replicator]),
    ok = config:set("admins", ?USER, ?PASS, _Persist=false),
    Source = setup(A),
    Target = setup(B),
    {ok, Pid} = start(Service, {Source, Target}),
    {Ctx, Pid, {Source, Target}}.

teardown({remote, DbName}) ->
    teardown(DbName);
teardown(DbName) ->
    ok = couch_server:delete(DbName, [?ADMIN_CTX]),
    ok.

teardown({_, _}, {Ctx, _Pid, {Source, Target}}) ->
    teardown(Source),
    teardown(Target),
    %%exit(Pid, kill),
    ok = test_util:stop_couch(Ctx).

start(replicator, {Source, Target}) ->
    RepObject = {[
        {<<"source">>, db(Source)},
        {<<"target">>, db(Target)},
        {<<"use_checkpoints">>, false}
    ]},
    {ok, Rep} = couch_replicator_utils:parse_rep_doc(RepObject, ?ADMIN_USER),
    {ok, Pid} = gen_server:start_link(couch_replicator, Rep, []),
    {ok, Pid};
start(worker, {Source, Target}) ->
    {ok, Pid} = couch_replicator_worker:start_link(
        self(), db(Source), db(Target), self(), 1),
    receive
        {get_changes, _} ->
            ok
    after ?TIMEOUT ->
            throw(timeout)
    end,
    {ok, Pid};
start(httpc_pool, {Source, _Target}) ->
    couch_replicator_httpc_pool:start_link(db(Source), []);
start(manager, {_Source, _Target}) ->
    {ok, whereis(couch_replicator_manager)}.


state_format_test_() ->
    Pairs = [{local, local}, {local, remote},
             {remote, local}, {remote, remote}],
    Tests = [
        fun should_format_state/2,
        fun should_strip_credentials/2
    ],
    Targets = [replicator, worker, httpc_pool, manager],
    {
        "Make sure we format state of the process",
        {
            foreachx,
            fun setup/1, fun teardown/2,
            [{{Target, Pair}, Test}
                ||
                    Pair <- Pairs,
                    Test <- Tests,
                    Target <- Targets]
        }
    }.

should_format_state({replicator, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        ExpectedStateKeys = [
            start_seq,
            committed_seq,
            current_through_seq,
            highest_seq_done,
            rep_starttime,
            src_starttime,
            tgt_starttime,
            timer,
            session_id,
            source_seq,
            use_checkpoints,
            checkpoint_interval,
            rep_details
        ],
        ?assertEqualLists(ExpectedStateKeys, proplists:get_keys(State)),
        RepState = proplists:get_value(rep_details, State),
        ExpectedRepKeys = [
            id,
            options,
            view,
            doc_id,
            db_name,
            source,
            target
        ],
        ?assertEqualLists(ExpectedRepKeys, proplists:get_keys(RepState)),
        ok
    end)};
should_format_state({worker, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        ExpectedStateKeys = [
            cp,
            loop,
            max_parallel_conns,
            writer,
            pending_fetch,
            flush_waiter,
            source_db_compaction_notifier,
            target_db_compaction_notifier,
            batch,
            source,
            target
        ],
        ?assertEqualLists(ExpectedStateKeys, proplists:get_keys(State)),
        ok
    end)};
should_format_state({httpc_pool, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        ExpectedStateKeys = [
            url,
            limit
        ],
        ?assertEqualLists(ExpectedStateKeys, proplists:get_keys(State)),
        ok
    end)};
should_format_state({manager, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        ExpectedStateKeys = [
            event_listener,
            scan_pid,
            max_retries,
            epoch
        ],
        ?assertEqualLists(ExpectedStateKeys, proplists:get_keys(State)),
        ok
    end)}.


should_strip_credentials({replicator, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        RepState = proplists:get_value(rep_details, State),
        Source = proplists:get_value(source, RepState),
        Target = proplists:get_value(target, RepState),
        ?assertNot(contains_password(Source)),
        ?assertNot(contains_password(Target)),
        ok
    end)};
should_strip_credentials({worker, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        Source = proplists:get_value(source, State),
        Target = proplists:get_value(target, State),
        ?assertNot(contains_password(Source)),
        ?assertNot(contains_password(Target)),
        ok
    end)};
should_strip_credentials({httpc_pool, _} = Args, {_Ctx, Pid, _}) ->
    {test_id(Args), ?_test(begin
        State = get_state(Pid),
        URL = proplists:get_value(url, State),
        ?assertNot(contains_password(URL)),
        ok
    end)};
should_strip_credentials({manager, _} = Args, {_Ctx, _Pid, _}) ->
    {test_id(Args), ?_test(ok)}.

test_id({Target, {From, To}}) ->
    lists:flatten(io_lib:format("~p: ~p -> ~p", [Target, From, To])).

db({remote, DbName}) ->
    iolist_to_binary([
        "http://", ?USER, ":", ?PASS, "@",
        config:get("httpd", "bind_address", "127.0.0.1"),
        ":", integer_to_list(mochiweb_socket_server:get(couch_httpd, port)),
        "/", DbName
    ]);
db(DbName) ->
    DbName.

contains_password(Bin) when is_binary(Bin) ->
    contains_password(?b2l(Bin));
contains_password(String) ->
    string:str(String, ?PASS) =/= 0.

get_state(Pid) ->
    {status, Pid, {module, _}, SItems} = sys:get_status(Pid),
    Filtered = [Props || Props <- SItems, is_list(Props)],
    Misc = lists:flatten(lists:foldl(fun(SItem, _) ->
        proplists:get_all_values(data, SItem)
    end, undefined, Filtered)),
    proplists:get_value("State", Misc).
