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

-module(couch_replicator_connection_tests).

-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

-define(TIMEOUT, 1000).


setup() ->
    Host = config:get("httpd", "bind_address", "127.0.0.1"),
    Port = config:get("httpd", "port", "5984"),
    {Host, Port}.

teardown(_) ->
    ok.


httpc_pool_test_() ->
    {
        "replicator connection sharing tests",
        {
            setup,
            fun() -> test_util:start_couch([couch_replicator]) end, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun connections_shared_after_relinquish/1,
                    fun connections_not_shared_after_owner_death/1,
                    fun idle_connections_closed/1,
                    fun test_owner_monitors/1
                ]
            }
        }
    }.


connections_shared_after_relinquish({Host, Port}) ->
    ?_test(begin
        URL = "http://" ++ Host ++ ":" ++ Port,
        Self = self(),
        {ok, Pid} = couch_replicator_connection:acquire(URL),
        couch_replicator_connection:relinquish(Pid),
        spawn(fun() ->
            Self ! couch_replicator_connection:acquire(URL)
        end),
        receive
            {ok, Pid2} ->
                ?assertEqual(Pid, Pid2)
        end
    end).


connections_not_shared_after_owner_death({Host, Port}) ->
    ?_test(begin
        URL = "http://" ++ Host ++ ":" ++ Port,
        Self = self(),
        spawn(fun() ->
            Self ! couch_replicator_connection:acquire(URL),
            1/0
        end),
        receive
            {ok, Pid} ->
                {ok, Pid2} = couch_replicator_connection:acquire(URL),
                ?assertNotEqual(Pid, Pid2),
                MRef = monitor(process, Pid),
                receive {'DOWN', MRef, process, Pid, _Reason} ->
                    ?assert(not is_process_alive(Pid));
                    Other -> throw(Other)
                end
        end
    end).


idle_connections_closed({Host, Port}) ->
    ?_test(begin
        URL = "http://" ++ Host ++ ":" ++ Port,
        {ok, Pid} = couch_replicator_connection:acquire(URL),
        couch_replicator_connection ! close_idle_connections,
        ?assert(ets:member(couch_replicator_connection, Pid)),
        % block until idle connections have closed
        sys:get_status(couch_replicator_connection),
        couch_replicator_connection:relinquish(Pid),
        couch_replicator_connection ! close_idle_connections,
        % block until idle connections have closed
        sys:get_status(couch_replicator_connection),
        ?assert(not ets:member(couch_replicator_connection, Pid))
    end).


test_owner_monitors({Host, Port}) ->
    ?_test(begin
        URL = "http://" ++ Host ++ ":" ++ Port,
        {ok, Worker0} = couch_replicator_connection:acquire(URL),
        assert_monitors_equal([{process, self()}]),
        couch_replicator_connection:relinquish(Worker0),
        assert_monitors_equal([]),
        {Workers, Monitors}  = lists:foldl(fun(_, {WAcc, MAcc}) ->
            {ok, Worker1} = couch_replicator_connection:acquire(URL),
            MAcc1 = [{process, self()} | MAcc],
            assert_monitors_equal(MAcc1),
            {[Worker1 | WAcc], MAcc1}
        end, {[], []}, lists:seq(1,5)),
        lists:foldl(fun(Worker2, Acc) ->
            [_ | NewAcc] = Acc,
            couch_replicator_connection:relinquish(Worker2),
            assert_monitors_equal(NewAcc),
            NewAcc
        end, Monitors, Workers)
    end).


assert_monitors_equal(ShouldBe) ->
    sys:get_status(couch_replicator_connection),
    {monitors, Monitors} = process_info(whereis(couch_replicator_connection), monitors),
    ?assertEqual(Monitors, ShouldBe).
