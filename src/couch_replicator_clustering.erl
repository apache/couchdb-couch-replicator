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

-module(couch_replicator_clustering).
-behaviour(gen_server).
-behaviour(config_listener).

% public API
-export([start_link/0, owner/2, owner/1]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2,
         code_change/3, terminate/2]).

% config_listener callbacks
-export([handle_config_change/5, handle_config_terminate/3]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").

-define(DEFAULT_QUIET_PERIOD, 60). % seconds

-record(state, {
    last_change :: erlang:timestamp(),
    quiet_period = ?DEFAULT_QUIET_PERIOD :: non_neg_integer(),
    timer :: timer:tref()
}).


-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


% owner/2 function computes ownership for a {DbName, DocId} tuple
% Returns {ok no_owner} in case no DocId is null. That case
% would happen in the old replicator_manager if replication was
% posted from _replicate endpoint and not via a document in 
% *_replicator db.
%
% {error, unstable} value is returned if cluster membership has
% been changing recently. Recency is a configuration parameter.
%
-spec owner(Dbname :: binary(), DocId :: binary() | null) ->
    {ok, node()} | {ok, no_owner} | {error, unstable}.
owner(_DbName, null) ->
    {ok, no_owner};
owner(<<"shards/", _/binary>> = DbName, DocId) ->
    IsStable = gen_server:call(?MODULE, is_stable, infinity),
    case IsStable of
        false ->
            {error, unstable};
        true ->
            {ok, owner_int(DbName, DocId)}
    end;
owner(_DbName, _DocId) ->
    {ok, node()}.



% owner/1 function computes ownership based on the single
% input Key parameter. It will uniformly distribute this Key
% across the list of current live nodes in the cluster without
% regard to shard ownership.
%
% Originally this function was used in chttpd for replications
% coming from _replicate endpoint. It was called choose_node
% and was called like this:
%  choose_node([couch_util:get_value(<<"source">>, Props),
%               couch_util:get_value(<<"target">>, Props)])
%
-spec owner(term()) -> node().
owner(Key) when is_binary(Key) ->
    Checksum = erlang:crc32(Key),
    Nodes = lists:sort([node() | nodes()]),
    lists:nth(1 + Checksum rem length(Nodes), Nodes);
owner(Key) ->
    owner(term_to_binary(Key)).


% gen_server callbacks


init([]) ->
    net_kernel:monitor_nodes(true),
    ok = config:listen_for_changes(?MODULE, self()),
    Interval = config:get_integer("replicator", "cluster_quiet_period", 
        ?DEFAULT_QUIET_PERIOD),
    couch_log:debug("Initialized clustering gen_server ~w", [self()]),
    {ok, #state{
        last_change = os:timestamp(),
        quiet_period = Interval,
        timer = new_timer(Interval)
    }}.


handle_call(is_stable, _From, State) ->
    {reply, is_stable(State), State}.


handle_cast({set_quiet_period, QuietPeriod}, State) when
    is_integer(QuietPeriod), QuietPeriod > 0 ->
    {noreply, State#state{quiet_period = QuietPeriod}}.


handle_info({nodeup, _Node}, State) ->
    Ts = os:timestamp(),
    Timer = new_timer(State#state.quiet_period),
    {noreply, State#state{last_change = Ts, timer = Timer}};

handle_info({nodedown, _Node}, State) ->
    Ts = os:timestamp(),
    Timer = new_timer(State#state.quiet_period),
    {noreply, State#state{last_change = Ts, timer = Timer}};

handle_info(rescan_check, State) ->
   timer:cancel(State#state.timer),
   case is_stable(State) of
       true ->
	   trigger_rescan(),
           {noreply, State};
       false ->
	   Timer = new_timer(State#state.quiet_period),
	   {noreply, State#state{timer = Timer}}
   end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


%% Internal functions

new_timer(Interval) ->
    {ok, Timer} = timer:send_after(Interval, rescan_check),
    Timer.

trigger_rescan() ->
    couch_log:notice("Triggering replicator rescan from ~p", [?MODULE]),
    couch_replicator_manager_sup:restart_mdb_listener(),
    ok.

is_stable(State) ->
    % os:timestamp() results are not guaranteed to be monotonic
    Sec = case timer:now_diff(os:timestamp(), State#state.last_change) of
        USec when USec < 0 ->
            0;
        USec when USec >= 0 ->
             USec / 1000000
    end,
    Sec > State#state.quiet_period.


handle_config_change("replicator", "cluster_quiet_period", V, _, S) ->
    ok = gen_server:cast(S, {set_quiet_period, list_to_integer(V)}),
    {ok, S};
handle_config_change(_, _, _, _, S) ->
    {ok, S}.


handle_config_terminate(_, stop, _) -> ok;
handle_config_terminate(Self, _, _) ->
    spawn(fun() ->
        timer:sleep(5000),
        config:listen_for_changes(?MODULE, Self)
    end).


owner_int(DbName, DocId) ->
    Live = [node() | nodes()],
    Nodes = [N || #shard{node=N} <- mem3:shards(mem3:dbname(DbName), DocId),
                  lists:member(N, Live)],
    hd(mem3_util:rotate_list({DbName, DocId}, lists:sort(Nodes))).


