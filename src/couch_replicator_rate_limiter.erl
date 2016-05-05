-module(couch_replicator_rate_limiter).
-behaviour(gen_server).


-include_lib("couch/include/couch_db.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").
-include_lib("couch_replicator_api_wrap.hrl").


-export([
    start_link/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export ([
    maybe_start_rate_limiter/3,
    maybe_decrement_rep_count/1,
    maybe_delay_request/1
    ]).

-record(rep_limiter, {
    requestCounter = 0,
    lastUpdateTimestamp = 0,
    pid,
    replications = 0,
    limit, % max requests
    period % interval is in seconds
}).


%% For each unique host, an entry is a rep_limiter record
-define(HOSTS, couch_replicator_limit_hosts).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


update_host(Host, Limit, Period) ->
    gen_server:call(?MODULE, {update_host, Host, Limit, Period}, infinity).


decrement_replications(Host) ->
    gen_server:call(?MODULE, {decrement_replications, Host}, infinity).


% gen_server functions.
init([]) ->
    process_flag(trap_exit, true),
    ets:new(?HOSTS, [set, public, named_table]),
    {ok, nil}.


handle_call({update_host, Host, Limit, Period}, _From, State) ->
    Reps = case ets:insert_new(?HOSTS, {Host, #rep_limiter{}}) of
        true ->
            % Brand new host for replication, need to spawn
            % a new resetter process and initialize
            LoopPid = spawn_link(fun() ->
                reset_requests_loop(Host, Period)
            end),
            modify_host(Host, [{4, LoopPid}, {5, 1}, {6, Limit},
                {7, Period}]),
            1;
        false ->
            % this allows the rate limit to be changed during replication
            modify_host(Host, [{6, Limit}, {7, Period}]),
            update_rep_count(Host, 1)
    end,
    {ok, Reps, State};


handle_call({decrement_replications, Host}, _From, State) ->
    Replications = case update_rep_count(Host, -1) of
        Rep0 when Rep0 =< 0 ->
            ResetPid = ets:lookup_element(?HOSTS, Host, 4),
            % no more replications at this moment, stop resetter
            % process and remove the host from the ets table
            exit(ResetPid, stop_resetter),
            remove_host(Host);
        _ ->
            ok % this shoulld be some error
    end,
    {ok, Replications, State}.


handle_cast(_, State) ->
    {noreply, State}.


handle_info({'EXIT', _FromPid, stop_resetter}, State) ->
    {noreply, State};

handle_info({'EXIT', _FromPid, Reason}, State) ->
    couch_log:error("couch_replicator_rate_limiter reset process
            died abnormally due to: ~p", [Reason]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


modify_host(Host, Elements) ->
    ets:update_element(?HOSTS, Host, Elements).


remove_host(Host) ->
    ets:delete(?HOSTS, Host).


update_rep_count(Host, Value) ->
    ets:update_counter(?HOSTS, Host, {5, Value}).


increment_req_count(Host) ->
    ets:update_counter(?HOSTS, Host, {2, 1}).


code_change(_OldVsn, nil, _Extra) ->
    {ok, nil}.

reset_requests_loop(Host, Period) ->
    modify_host(Host, [{2, 0}, {3, os:timestamp()}]),
    timer:sleep(Period),
    reset_requests_loop(Host, Period).

% Returns sleep time in milliseconds
calculate_sleep_time(Host, RequestCounter) ->
    {Limit, Period} = get_host_rate(Host),
    Interval = (Period / Limit) * 1000,
    LastUpdateTimestamp = ets:lookup_element(?HOSTS, Host, 3),
    ReqTimestamp = os:timestamp(),
    TimeDiff0 = timer:now_diff(ReqTimestamp, LastUpdateTimestamp),
    TimeDiff1 = TimeDiff0 div 1000 rem 1000,
    CountDiff = RequestCounter - Limit,
    NextReset = ts_to_millisec(LastUpdateTimestamp) + (Period * 1000),
    case {CountDiff, TimeDiff1} of
        {C0, _} when C0 =< 0 ->
            0;
        {C0, T0} when T0 > 0 ->
            (NextReset - T0) + (C0 * Interval);
        {C0, T0} when T0 =< 0 ->
             % This can occur if the request comes in really close to
             % to the reset time and we read the reset value too late.
             abs(T0) + (C0 * Interval)
    end.


ts_to_millisec({Mega, Sec, Micro}) ->
    (Mega * 1000000) + (Sec * 1000) +  (Micro div 1000 rem 1000).


get_host_rate(Host) ->
    Limit =  try ets:lookup_element(?HOSTS, Host, 6) of
        L ->
            L
        catch error:badarg ->
            -1
    end,
    Period = try ets:lookup_element(?HOSTS, Host, 7) of
        P ->
            P
        catch error:badarg ->
            -1
    end,
    {Limit, Period}.



maybe_start_rate_limiter(#httpdb{url = Url}, Limit, Period) ->
    Host = ibrowse_lib:parse_url(Url),
    case {Limit, Period} of
        {-1, _} ->
            false;
        {_, -1} ->
            false;
        {L0, P0} ->
            update_host(Host, L0, P0)
    end;
% disabled for local
maybe_start_rate_limiter(_, _ , _) ->
    false.


maybe_decrement_rep_count(Url) ->
    Host = ibrowse_lib:parse_url(Url),
    {Limit, Period} = get_host_rate(Host),
    case {Limit, Period} of
        {-1, _} ->
            false;
        {_, -1} ->
            false;
        {_, _} ->
            decrement_replications(Host)
    end.


maybe_delay_request(Url) ->
    Host = ibrowse_lib:parse_url(Url),
    {Limit, Period} = get_host_rate(Host),
    case {Limit, Period} of
        {-1, _} ->
            false;
        {_, -1} ->
            false;
        {_, _} ->
            Counter = increment_req_count(Host),
            Sleep = calculate_sleep_time(Host, Counter),
            timer:sleep(Sleep)
    end.
