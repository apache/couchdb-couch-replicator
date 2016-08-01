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

-module(couch_replicator_scheduler).
-behaviour(gen_server).
-behaviour(config_listener).
-vsn(1).

-include("couch_replicator_scheduler.hrl").
-include("couch_replicator.hrl").
-include("couch_replicator_api_wrap.hrl").
-include_lib("couch/include/couch_db.hrl").

%% public api
-export([start_link/0, add_job/1, remove_job/1, reschedule/0]).
-export([rep_state/1, find_jobs_by_dbname/1, find_jobs_by_doc/2]).
-export([jobs/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([format_status/2]).

%% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

%% types
-type event_type() :: added | started | stopped | {crashed, any()}.
-type event() :: {Type:: event_type(), When :: erlang:timestamp()}.
-type history() :: nonempty_list(event()).

%% definitions
-define(MAX_BACKOFF_EXPONENT, 10).
-define(BACKOFF_INTERVAL_MICROS, 30 * 1000 * 1000).

-define(DEFAULT_MAX_JOBS, 100).
-define(DEFAULT_MAX_CHURN, 20).
-define(DEFAULT_MAX_HISTORY, 20).
-define(DEFAULT_SCHEDULER_INTERVAL, 60000).
-record(state, {interval, timer, max_jobs, max_churn, max_history}).
-record(job, {
          id :: job_id() | '$1' | '_',
          rep :: #rep{} | '_',
          pid :: undefined | pid() | '$1' | '_',
          monitor :: undefined | reference() | '_',
          history :: history() | '_'}).

-record(stats_acc, {
          now  :: erlang:timestamp(),
          pending_t = 0 :: non_neg_integer(),
          running_t = 0 :: non_neg_integer(),
          crashed_t = 0 :: non_neg_integer(),
          pending_n = 0 :: non_neg_integer(),
          running_n = 0 :: non_neg_integer(),
          crashed_n = 0 :: non_neg_integer()}).


%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(#rep{}) -> ok | {error, already_added}.
add_job(#rep{} = Rep) when Rep#rep.id /= undefined ->
    Job = #job{
        id = Rep#rep.id,
        rep = Rep,
        history = [{added, os:timestamp()}]},
    gen_server:call(?MODULE, {add_job, Job}).


-spec remove_job(job_id()) -> ok.
remove_job(Id) ->
    gen_server:call(?MODULE, {remove_job, Id}).


-spec reschedule() -> ok.
% Trigger a manual reschedule. Used for testing and/or ops.
reschedule() ->
    gen_server:call(?MODULE, reschedule).


-spec rep_state(rep_id()) -> #rep{} | nil.
rep_state(RepId) ->
    case (catch ets:lookup_element(?MODULE, RepId, #job.rep)) of
        {'EXIT',{badarg, _}} ->
            nil;
        Rep ->
            Rep
    end.

-spec find_jobs_by_dbname(binary()) -> list(#rep{}).
find_jobs_by_dbname(DbName) ->
    Rep = #rep{db_name = DbName, _ = '_'},
    MatchSpec = #job{id = '$1', rep = Rep, _ = '_'},
    [RepId || [RepId] <- ets:match(?MODULE, MatchSpec)].


-spec find_jobs_by_doc(binary(), binary()) -> list(#rep{}).
find_jobs_by_doc(DbName, DocId) ->
    Rep =  #rep{db_name = DbName, doc_id = DocId, _ = '_'},
    MatchSpec = #job{id = '$1', rep = Rep, _ = '_'},
    [RepId || [RepId] <- ets:match(?MODULE, MatchSpec)].




%% gen_server functions

init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ok = config:listen_for_changes(?MODULE, self()),
    Interval = config:get_integer("replicator", "interval", ?DEFAULT_SCHEDULER_INTERVAL),
    MaxJobs = config:get_integer("replicator", "max_jobs", ?DEFAULT_MAX_JOBS),
    MaxChurn = config:get_integer("replicator", "max_churn", ?DEFAULT_MAX_CHURN),
    MaxHistory = config:get_integer("replicator", "max_history", ?DEFAULT_MAX_HISTORY),
    {ok, Timer} = timer:send_after(Interval, reschedule),
    State = #state{
        interval = Interval,
        max_jobs = MaxJobs,
        max_churn = MaxChurn,
        max_history = MaxHistory,
        timer = Timer
    },
    {ok, State}.


handle_call({add_job, Job}, _From, State) ->
    case add_job_int(Job) of
        true ->
            case running_job_count() of
                RunningJobs when RunningJobs < State#state.max_jobs ->
                    start_job_int(Job, State),
                    update_running_jobs_stats();
                _ ->
                    ok
                end,
            couch_stats:increment_counter([couch_replicator, jobs, adds]),
            TotalJobs = ets:info(?MODULE, size),
            couch_stats:update_gauge([couch_replicator, jobs, total], TotalJobs),
            {reply, ok, State};
        false ->
            couch_stats:increment_counter([couch_replicator, jobs, duplicate_adds]),
            {reply, {error, already_added}, State}
    end;

handle_call({remove_job, Id}, _From, State) ->
    case job_by_id(Id) of
        {ok, Job} ->
            ok = stop_job_int(Job, State),
            true = remove_job_int(Job),
            couch_stats:increment_counter([couch_replicator, jobs, removes]),
            TotalJobs = ets:info(?MODULE, size),
            couch_stats:update_gauge([couch_replicator, jobs, total], TotalJobs),
            update_running_jobs_stats(),
            {reply, ok, State};
        {error, not_found} ->
            {reply, ok, State}
    end;

handle_call(reschedule, _From, State) ->
    ok = reschedule(State),
    {reply, ok, State};

handle_call(_, _From, State) ->
    {noreply, State}.


handle_cast({set_max_jobs, MaxJobs}, State) when is_integer(MaxJobs), MaxJobs >= 0 ->
    couch_log:notice("~p: max_jobs set to ~B", [?MODULE, MaxJobs]),
    {noreply, State#state{max_jobs = MaxJobs}};

handle_cast({set_max_churn, MaxChurn}, State) when is_integer(MaxChurn), MaxChurn > 0 ->
    couch_log:notice("~p: max_churn set to ~B", [?MODULE, MaxChurn]),
    {noreply, State#state{max_churn = MaxChurn}};

handle_cast({set_max_history, MaxHistory}, State) when is_integer(MaxHistory), MaxHistory > 0 ->
    couch_log:notice("~p: max_history set to ~B", [?MODULE, MaxHistory]),
    {noreply, State#state{max_history = MaxHistory}};

handle_cast({set_interval, Interval}, State) when is_integer(Interval), Interval > 0 ->
    couch_log:notice("~p: interval set to ~B", [?MODULE, Interval]),
    {noreply, State#state{interval = Interval}};

handle_cast(_, State) ->
    {noreply, State}.


handle_info(reschedule, State) ->
    ok = reschedule(State),
    {ok, cancel} = timer:cancel(State#state.timer),
    {ok, Timer} = timer:send_after(State#state.interval, reschedule),
    {noreply, State#state{timer = Timer}};

handle_info({'DOWN', _Ref, process, Pid, normal}, State) ->
    {ok, Job} = job_by_pid(Pid),
    couch_log:notice("~p: Job ~p completed normally", [?MODULE, Job#job.id]),
    remove_job_int(Job),
    update_running_jobs_stats(),
    {noreply, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    {ok, Job} = job_by_pid(Pid),
    couch_log:notice("~p: Job ~p died with reason: ~p",
        [?MODULE, Job#job.id, Reason]),
    ok = update_state_crashed(Job, Reason, State),
    start_pending_jobs(State),
    update_running_jobs_stats(),
    {noreply, State};

handle_info(_, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


format_status(_Opt, [_PDict, State]) ->
    [{max_jobs, State#state.max_jobs},
     {running_jobs, running_job_count()},
     {pending_jobs, pending_job_count()}].


%% config listener functions

handle_config_change("replicator", "max_jobs", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_jobs, list_to_integer(V)}),
    {ok, Pid};

handle_config_change("replicator", "max_churn", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_churn, list_to_integer(V)}),
    {ok, Pid};

handle_config_change("replicator", "interval", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_interval, list_to_integer(V)}),
    {ok, Pid};

handle_config_change("replicator", "max_history", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_history, list_to_integer(V)}),
    {ok, Pid};

handle_config_change(_, _, _, _, Pid) ->
    {ok, Pid}.


handle_config_terminate(_, stop, _) ->
    ok;

handle_config_terminate(Self, _, _) ->
    spawn(fun() ->
        timer:sleep(5000),
        config:listen_for_changes(?MODULE, Self)
    end).


%% private functions

% Return up to a given number of oldest, not recently crashed jobs. Try to be
% memory efficient and use ets:foldl to accumulate jobs.
-spec pending_jobs(non_neg_integer()) -> [#job{}].
pending_jobs(0) ->
    % Handle this case as user could set max_churn to 0. If this is passed to
    % other function clause it will crash as gb_sets:largest assumes set is not
    % empty.
    [];

pending_jobs(Count) when is_integer(Count), Count > 0 ->
    Set0 = gb_sets:new(),  % [{LastStart, Job},...]
    Now = os:timestamp(),
    {Set1, _, _} = ets:foldl(fun pending_fold/2, {Set0, Now, Count}, ?MODULE),
    [Job || {_Started, Job} <- gb_sets:to_list(Set1)].


pending_fold(Job, {Set, Now, Count}) ->
    Set1 = case {not_recently_crashed(Job, Now), gb_sets:size(Set) >= Count} of
        {true, true} ->
             % Job is healthy but already reached accumulated limit, so might
             % have to replace one of the accumulated jobs
             pending_maybe_replace(Job, Set);
        {true, false} ->
             % Job is healthy and we haven't reached the limit, so add job
             % to accumulator
             gb_sets:add_element({last_started(Job), Job}, Set);
        {false, _} ->
             % This jobs is not healthy (has crashed too recently), so skip it.
             Set
    end,
    {Set1, Now, Count}.


% Replace Job in the accumulator if it is older than youngest job there.
pending_maybe_replace(Job, Set) ->
    Started = last_started(Job),
    {Youngest, YoungestJob} = gb_sets:largest(Set),
    case Started < Youngest of
        true ->
            Set1 = gb_sets:delete({Youngest, YoungestJob}, Set),
            gb_sets:add_element({Started, Job}, Set1);
        false ->
            Set
    end.


start_jobs(Count, State) ->
    [start_job_int(Job, State) || Job <- pending_jobs(Count)],
    ok.


-spec stop_jobs(non_neg_integer(), boolean(), #state{}) -> non_neg_integer().
stop_jobs(Count, IsContinuous, State) ->
    Running0 = running_jobs(),
    ContinuousPred = fun(Job) -> is_continuous(Job) =:= IsContinuous end,
    Running1 = lists:filter(ContinuousPred, Running0),
    Running2 = lists:sort(fun oldest_job_first/2, Running1),
    Running3 = lists:sublist(Running2, Count),
    length([stop_job_int(Job, State) || Job <- Running3]).


oldest_job_first(#job{} = A, #job{} = B) ->
    last_started(A) =< last_started(B).


not_recently_crashed(#job{} = Job, Now) ->
    case Job#job.history of
        [{added, _When}] ->
            true;
        [{stopped, _When} | _] ->
            true;
        _ ->
            case Crashes = crashes_before_stop(Job) of
                [] ->
                    true;
                [{{crashed, _Reason}, When} | _] ->
                    timer:now_diff(Now, When) >= backoff_micros(length(Crashes))
            end
    end.

-spec crashes_before_stop(#job{}) -> list().
crashes_before_stop(#job{history = History}) ->
    EventsBeforeStop = lists:takewhile(
        fun({Event, _}) -> Event =/= stopped end, History),
    [Crash || {{crashed, _Reason}, _When} = Crash <- EventsBeforeStop].


-spec backoff_micros(non_neg_integer()) -> non_neg_integer().
backoff_micros(CrashCount) ->
    BackoffExp = erlang:min(CrashCount - 1, ?MAX_BACKOFF_EXPONENT),
    (1 bsl BackoffExp) * ?BACKOFF_INTERVAL_MICROS.


-spec add_job_int(#job{}) -> boolean().
add_job_int(#job{} = Job) ->
    ets:insert_new(?MODULE, Job).


start_job_int(#job{pid = Pid}, _State) when Pid /= undefined ->
    ok;

start_job_int(#job{} = Job, State) ->
    case couch_replicator_scheduler_sup:start_child(Job#job.rep) of
        {ok, Child} ->
            Ref = monitor(process, Child),
            ok = update_state_started(Job, Child, Ref, State),
            couch_log:notice("~p: Job ~p started as ~p",
                [?MODULE, Job#job.id, Job#job.pid]);
        {error, {already_started, OtherPid}} ->
            couch_log:notice("~p: Job ~p already running as ~p. Most likely"
                " because a duplicate replication is running on another node",
                [?MODULE, Job#job.id, OtherPid]),
            ok = update_state_crashed(Job, "Duplicate replication running", State);
        {error, Reason} ->
            couch_log:notice("~p: Job ~p failed to start for reason ~p",
                [?MODULE, Job, Reason]),
            ok = update_state_crashed(Job, Reason, State)
    end.


-spec stop_job_int(#job{}, #state{}) -> ok | {error, term()}.
stop_job_int(#job{pid = undefined}, _State) ->
    ok;

stop_job_int(#job{} = Job, State) ->
    ok = couch_replicator_scheduler_sup:terminate_child(Job#job.pid),
    demonitor(Job#job.monitor, [flush]),
    ok = update_state_stopped(Job, State),
    couch_log:notice("~p: Job ~p stopped as ~p",
        [?MODULE, Job#job.id, Job#job.pid]).


-spec remove_job_int(#job{}) -> true.
remove_job_int(#job{} = Job) ->
    ets:delete(?MODULE, Job#job.id).


-spec running_job_count() -> non_neg_integer().
running_job_count() ->
    ets:info(?MODULE, size) - pending_job_count().


-spec running_jobs() -> [#job{}].
running_jobs() ->
    ets:select(?MODULE, [{#job{pid = '$1', _='_'}, [{is_pid, '$1'}], ['$_']}]).


-spec pending_job_count() -> non_neg_integer().
pending_job_count() ->
    ets:select_count(?MODULE, [{#job{pid=undefined, _='_'}, [], [true]}]).





-spec job_by_pid(pid()) -> {ok, #job{}} | {error, not_found}.
job_by_pid(Pid) when is_pid(Pid) ->
    case ets:match_object(?MODULE, #job{pid=Pid, _='_'}) of
        [] ->
            {error, not_found};
        [#job{}=Job] ->
            {ok, Job}
    end.

-spec job_by_id(job_id()) -> {ok, #job{}} | {error, not_found}.
job_by_id(Id) ->
    case ets:lookup(?MODULE, Id) of
        [] ->
            {error, not_found};
        [#job{}=Job] ->
            {ok, Job}
    end.


-spec update_state_stopped(#job{}, #state{}) -> ok.
update_state_stopped(Job, State) ->
    Job1 = reset_job_process(Job),
    Job2 = update_history(Job1, stopped, os:timestamp(), State),
    true = ets:insert(?MODULE, Job2),
    couch_stats:increment_counter([couch_replicator, jobs, stops]),
    ok.


-spec update_state_started(#job{}, pid(), reference(), #state{}) -> ok.
update_state_started(Job, Pid, Ref, State) ->
    Job1 = set_job_process(Job, Pid, Ref),
    Job2 = update_history(Job1, started, os:timestamp(), State),
    true = ets:insert(?MODULE, Job2),
    couch_stats:increment_counter([couch_replicator, jobs, starts]),
    ok.


-spec update_state_crashed(#job{}, any(), #state{}) -> ok.
update_state_crashed(Job, Reason, State) ->
    Job1 = reset_job_process(Job),
    Job2 = update_history(Job1, {crashed, Reason}, os:timestamp(), State),
    true = ets:insert(?MODULE, Job2),
    couch_stats:increment_counter([couch_replicator, jobs, crashes]),
    ok.


-spec set_job_process(#job{}, pid(), reference()) -> #job{}.
set_job_process(#job{} = Job, Pid, Ref) when is_pid(Pid), is_reference(Ref) ->
    Job#job{pid = Pid, monitor = Ref}.


-spec reset_job_process(#job{}) -> #job{}.
reset_job_process(#job{} = Job) ->
    Job#job{pid = undefined, monitor = undefined}.


-spec reschedule(#state{}) -> ok.
reschedule(State) ->
    Running = running_job_count(),
    Pending = pending_job_count(),
    stop_excess_jobs(State, Running),
    start_pending_jobs(State, Running, Pending),
    rotate_jobs(State, Running, Pending),
    update_running_jobs_stats(),
    ok.


-spec stop_excess_jobs(#state{}, non_neg_integer()) -> ok.
stop_excess_jobs(State, Running) ->
    #state{max_jobs=MaxJobs} = State,
    StopCount = Running - MaxJobs,
    if StopCount > 0 ->
        Stopped = stop_jobs(StopCount, true, State),
        OneshotLeft = StopCount - Stopped,
        if OneshotLeft > 0 ->
            stop_jobs(OneshotLeft, false, State),
            ok;
        true ->
            ok
        end;
    true ->
        ok
    end.


start_pending_jobs(State) ->
    start_pending_jobs(State, running_job_count(), pending_job_count()).


start_pending_jobs(State, Running, Pending) ->
    #state{max_jobs=MaxJobs} = State,
    if Running < MaxJobs, Pending > 0 ->
        start_jobs(MaxJobs - Running, State);
    true ->
        ok
    end.

-spec rotate_jobs(#state{}, non_neg_integer(), non_neg_integer()) -> ok.
rotate_jobs(State, Running, Pending) ->
    #state{max_jobs=MaxJobs, max_churn=MaxChurn} = State,
    if Running == MaxJobs, Pending > 0 ->
        RotateCount = lists:min([Pending, Running, MaxChurn]),
        StopCount = stop_jobs(RotateCount, true, State),
        start_jobs(StopCount, State);
    true ->
        ok
    end.


-spec last_started(#job{}) -> erlang:timestamp().
last_started(#job{} = Job) ->
    case lists:keyfind(started, 1, Job#job.history) of
        false ->
            {0, 0, 0};
        {started, When} ->
            When
    end.


-spec update_history(#job{}, event_type(), erlang:timestamp(), #state{}) -> #job{}.
update_history(Job, Type, When, State) ->
    History0 = [{Type, When} | Job#job.history],
    History1 = lists:sublist(History0, State#state.max_history),
    Job#job{history = History1}.


-spec update_running_jobs_stats() -> ok.
update_running_jobs_stats() ->
    Acc0 = #stats_acc{now = os:timestamp()},
    AccR = ets:foldl(fun stats_fold/2, Acc0, ?MODULE),
    #stats_acc{
        pending_t = PendingSum,
        running_t = RunningSum,
        crashed_t = CrashedSum,
        pending_n = PendingN,
        running_n = RunningN,
        crashed_n = CrashedN
    } = AccR,
    PendingAvg = avg(PendingSum, PendingN),
    RunningAvg = avg(RunningSum, RunningN),
    CrashedAvg = avg(CrashedSum, CrashedN),
    couch_stats:update_gauge([couch_replicator, jobs, pending], PendingN),
    couch_stats:update_gauge([couch_replicator, jobs, running], RunningN),
    couch_stats:update_gauge([couch_replicator, jobs, crashed], CrashedN),
    couch_stats:update_gauge([couch_replicator, jobs, avg_pending], PendingAvg),
    couch_stats:update_gauge([couch_replicator, jobs, avg_running], RunningAvg),
    couch_stats:update_gauge([couch_replicator, jobs, avg_crashed], CrashedAvg),
    ok.


-spec stats_fold(#job{}, #stats_acc{}) -> #stats_acc{}.
stats_fold(#job{pid = undefined, history = [{added, T}]}, Acc) ->
    #stats_acc{now = Now, pending_t = SumT, pending_n = Cnt} = Acc,
    Dt = round(timer:now_diff(Now, T) / 1000000),
    Acc#stats_acc{pending_t = SumT + Dt, pending_n = Cnt + 1};

stats_fold(#job{pid = undefined, history = [{stopped, T} | _]}, Acc) ->
    #stats_acc{now = Now, pending_t = SumT, pending_n = Cnt} = Acc,
    Dt = round(timer:now_diff(Now, T) / 1000000),
    Acc#stats_acc{pending_t = SumT + Dt, pending_n = Cnt + 1};

stats_fold(#job{pid = undefined, history = [{{crashed, _}, T} | _]}, Acc) ->
    #stats_acc{now = Now, crashed_t = SumT, crashed_n = Cnt} = Acc,
    Dt = round(timer:now_diff(Now, T) / 1000000),
    Acc#stats_acc{crashed_t = SumT + Dt, crashed_n = Cnt + 1};

stats_fold(#job{pid = P, history = [{started, T} | _]}, Acc) when is_pid(P) ->
    #stats_acc{now = Now, running_t = SumT, running_n = Cnt} = Acc,
    Dt = round(timer:now_diff(Now, T) / 1000000),
    Acc#stats_acc{running_t = SumT + Dt, running_n = Cnt + 1}.



-spec avg(Sum :: non_neg_integer(), N :: non_neg_integer())  -> non_neg_integer().
avg(_Sum, 0) ->
    0;

avg(Sum, N) when N > 0 ->
    round(Sum / N).


-spec ejson_url(#httpdb{} | binary()) -> binary().
ejson_url(#httpdb{}=Httpdb) ->
    couch_util:url_strip_password(Httpdb#httpdb.url);
ejson_url(DbName) when is_binary(DbName) ->
    DbName.


-spec jobs() -> [[tuple()]].
jobs() ->
    ets:foldl(fun(Job, Acc) ->
        Rep = Job#job.rep,
        Source = ejson_url(Rep#rep.source),
        Target = ejson_url(Rep#rep.target),
        History = lists:map(fun(Event) ->
            EventProps  = case Event of
                {{crashed, Reason}, _When} ->
                    [{type, crashed}, {reason, crash_reason_json(Reason)}];
                {Type, _When} ->
                    [{type, Type}]
            end,
            {_Type, {_Mega, _Sec, Micros}=When} = Event,
            {{Y, Mon, D}, {H, Min, S}} = calendar:now_to_universal_time(When),
            ISO8601 = iolist_to_binary(io_lib:format(
                "~B-~2..0B-~2..0BT~2..0B-~2..0B-~2..0B.~BZ",
                [Y,Mon,D,H,Min,S,Micros]
            )),
            {[{timestamp, ISO8601} | EventProps]}
        end, Job#job.history),
        {BaseID, Ext} = Job#job.id,
        Pid = case Job#job.pid of
            undefined ->
                null;
            P when is_pid(P) ->
                ?l2b(pid_to_list(P))
        end,
        [{[
            {id, iolist_to_binary([BaseID, Ext])},
            {pid, Pid},
            {source, iolist_to_binary(Source)},
            {target, iolist_to_binary(Target)},
            {database, Rep#rep.db_name},
            {user, (Rep#rep.user_ctx)#user_ctx.name},
            {doc_id, Rep#rep.doc_id},
            {history, History},
            {node, node()}
        ]} | Acc]
    end, [], couch_replicator_scheduler).


crash_reason_json({_CrashType, Info}) when is_binary(Info) ->
    Info;
crash_reason_json(Reason) when is_binary(Reason) ->
    Reason;
crash_reason_json(_) ->
    <<"unknown">>.


-spec is_continuous(#job{}) -> boolean().
is_continuous(#job{rep = Rep}) ->
    couch_util:get_value(continuous, Rep#rep.options, false).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


backoff_micros_test_() ->
    BaseInterval = ?BACKOFF_INTERVAL_MICROS,
    [?_assertEqual(R * BaseInterval, backoff_micros(N)) || {R, N} <- [
        {1, 1}, {2, 2}, {4, 3}, {8, 4}, {16, 5}, {32, 6}, {64, 7}, {128, 8},
        {256, 9}, {512, 10}, {1024, 11}, {1024, 12}
    ]].


crashes_before_stop_test_() ->
    [?_assertEqual(R, crashes_before_stop(job(H))) || {R, H} <- [
        {[], [added()]},
        {[], [stopped()]},
        {[crashed()], [crashed()]},
        {[crashed()], [started(), crashed()]},
        {[crashed()], [added(), started(), crashed()]},
        {[crashed(3), crashed(1)], [crashed(3), started(2), crashed(1)]},
        {[], [stopped(), crashed()]},
        {[crashed(3)], [crashed(3), stopped(2), crashed(1)]}
    ]].


last_started_test_() ->
    [?_assertEqual({0, R, 0}, last_started(job(H))) || {R, H} <- [
         {0, [added()]},
         {0, [crashed(1)]},
         {1, [started(1)]},
         {1, [added(), started(1)]},
         {2, [started(2), started(1)]},
         {2, [crashed(3), started(2), started(1)]}
    ]].


oldest_job_first_test() ->
    J0 = job([crashed()]),
    J1 = job([started(1)]),
    J2 = job([started(2)]),
    Sort = fun(Jobs) -> lists:sort(fun oldest_job_first/2, Jobs) end,
    ?assertEqual([], Sort([])),
    ?assertEqual([J1], Sort([J1])),
    ?assertEqual([J1, J2], Sort([J2, J1])),
    ?assertEqual([J0, J1, J2], Sort([J2, J1, J0])).


scheduler_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_pending_jobs_simple(),
            t_pending_jobs_skip_crashed(),
            t_one_job_starts(),
            t_no_jobs_start_if_max_is_0(),
            t_one_job_starts_if_max_is_1(),
            t_max_churn_does_not_throttle_initial_start(),
            t_excess_oneshot_only_jobs(),
            t_excess_continuous_only_jobs(),
            t_excess_prefer_continuous_first(),
            t_stop_oldest_first(),
            t_start_oldest_first(),
            t_dont_stop_if_nothing_pending(),
            t_max_churn_limits_number_of_rotated_jobs(),
            t_if_pending_less_than_running_start_all_pending(),
            t_running_less_than_pending_swap_all_running(),
            t_oneshot_dont_get_rotated(),
            t_rotate_continuous_only_if_mixed(),
            t_oneshot_dont_get_starting_priority(),
            t_oneshot_will_hog_the_scheduler(),
            t_if_excess_is_trimmed_rotation_doesnt_happen()
         ]
    }.


t_pending_jobs_simple() ->
   ?_test(begin
        Job1 = oneshot(1),
        Job2 = oneshot(2),
        setup_jobs([Job2, Job1]),
        ?assertEqual([], pending_jobs(0)),
        ?assertEqual([Job1], pending_jobs(1)),
        ?assertEqual([Job1, Job2], pending_jobs(2)),
        ?assertEqual([Job1, Job2], pending_jobs(3))
    end).


t_pending_jobs_skip_crashed() ->
   ?_test(begin
        Job = oneshot(1),
        History = [crashed(os:timestamp()) | Job#job.history],
        Job1 = Job#job{history = History},
        Job2 = oneshot(2),
        Job3 = oneshot(3),
        setup_jobs([Job2, Job1, Job3]),
        ?assertEqual([Job2], pending_jobs(1)),
        ?assertEqual([Job2, Job3], pending_jobs(2)),
        ?assertEqual([Job2, Job3], pending_jobs(3))
    end).


t_one_job_starts() ->
    ?_test(begin
        setup_jobs([oneshot(1)]),
        ?assertEqual({0, 1}, run_stop_count()),
        reschedule(mock_state(?DEFAULT_MAX_JOBS)),
        ?assertEqual({1, 0}, run_stop_count())
    end).


t_no_jobs_start_if_max_is_0() ->
    ?_test(begin
        setup_jobs([oneshot(1)]),
        reschedule(mock_state(0)),
        ?assertEqual({0, 1}, run_stop_count())
    end).


t_one_job_starts_if_max_is_1() ->
    ?_test(begin
        setup_jobs([oneshot(1), oneshot(2)]),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count())
    end).


t_max_churn_does_not_throttle_initial_start() ->
    ?_test(begin
        setup_jobs([oneshot(1), oneshot(2)]),
        reschedule(mock_state(?DEFAULT_MAX_JOBS, 0)),
        ?assertEqual({2, 0}, run_stop_count())
    end).


t_excess_oneshot_only_jobs() ->
    ?_test(begin
        setup_jobs([oneshot_running(1), oneshot_running(2)]),
        ?assertEqual({2, 0}, run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 2}, run_stop_count())
    end).


t_excess_continuous_only_jobs() ->
    ?_test(begin
        setup_jobs([continuous_running(1), continuous_running(2)]),
        ?assertEqual({2, 0}, run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 1}, run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 2}, run_stop_count())
    end).


t_excess_prefer_continuous_first() ->
    ?_test(begin
        Jobs = [
            continuous_running(1),
            oneshot_running(2),
            continuous_running(3)
        ],
        setup_jobs(Jobs),
        ?assertEqual({3, 0}, run_stop_count()),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(2)),
        ?assertEqual({2, 1}, run_stop_count()),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(1)),
        ?assertEqual({1, 0}, oneshot_run_stop_count()),
        reschedule(mock_state(0)),
        ?assertEqual({0, 1}, oneshot_run_stop_count())
    end).


t_stop_oldest_first() ->
    ?_test(begin
        Jobs = [
            continuous_running(7),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2)),
        ?assertEqual({2, 1}, run_stop_count()),
        ?assertEqual([4], jobs_stopped()),
        reschedule(mock_state(1)),
        ?assertEqual([7], jobs_running())
    end).


t_start_oldest_first() ->
    ?_test(begin
        setup_jobs([continuous(7), continuous(2), continuous(5)]),
        reschedule(mock_state(1)),
        ?assertEqual({1, 2}, run_stop_count()),
        ?assertEqual([2], jobs_running()),
        reschedule(mock_state(2)),
        ?assertEqual([7], jobs_stopped())
    end).


t_dont_stop_if_nothing_pending() ->
    ?_test(begin
        setup_jobs([continuous_running(1), continuous_running(2)]),
        reschedule(mock_state(2)),
        ?assertEqual({2, 0}, run_stop_count())
    end).


t_max_churn_limits_number_of_rotated_jobs() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous(3),
            continuous_running(4)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2, 1)),
        ?assertEqual([2, 3], jobs_stopped())
    end).


t_if_pending_less_than_running_start_all_pending() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous(3),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(3)),
        ?assertEqual([1, 2, 5], jobs_running())
    end).


t_running_less_than_pending_swap_all_running() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous(2),
            continuous(3),
            continuous_running(4),
            continuous_running(5)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2)),
        ?assertEqual([3, 4, 5], jobs_stopped())
    end).


t_oneshot_dont_get_rotated() ->
    ?_test(begin
        setup_jobs([oneshot_running(1), continuous(2)]),
        reschedule(mock_state(1)),
        ?assertEqual([1], jobs_running())
    end).


t_rotate_continuous_only_if_mixed() ->
    ?_test(begin
        setup_jobs([continuous(1), oneshot_running(2), continuous_running(3)]),
        reschedule(mock_state(2)),
        ?assertEqual([1, 2], jobs_running())
    end).


t_oneshot_dont_get_starting_priority() ->
    ?_test(begin
        setup_jobs([continuous(1), oneshot(2), continuous_running(3)]),
        reschedule(mock_state(1)),
        ?assertEqual([1], jobs_running())
    end).


% This tested in other test cases, it is here to mainly make explicit a property
% of one-shot replications -- they can starve other jobs if they "take control" of
% all the available scheduler slots.
t_oneshot_will_hog_the_scheduler() ->
    ?_test(begin
        Jobs = [
            oneshot_running(1),
            oneshot_running(2),
            oneshot(3),
            continuous(4)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(2)),
        ?assertEqual([1, 2], jobs_running())
    end).


t_if_excess_is_trimmed_rotation_doesnt_happen() ->
    ?_test(begin
        Jobs = [
            continuous(1),
            continuous_running(2),
            continuous_running(3)
        ],
        setup_jobs(Jobs),
        reschedule(mock_state(1)),
        ?assertEqual([3], jobs_running())
    end).


% Test helper functions

setup() ->
    catch ets:delete(?MODULE),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_replicator_scheduler_sup, terminate_child, 1, ok),
    meck:expect(couch_stats, increment_counter, 1, ok),
    meck:expect(couch_stats, update_gauge, 2, ok),
    Pid = mock_pid(),
    meck:expect(couch_replicator_scheduler_sup, start_child, 1, {ok, Pid}).


teardown(_) ->
    catch ets:delete(?MODULE),
    meck:unload().


setup_jobs(Jobs) when is_list(Jobs) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ets:insert(?MODULE, Jobs).


all_jobs() ->
    lists:usort(ets:tab2list(?MODULE)).


jobs_stopped() ->
    [Job#job.id || Job <- all_jobs(), Job#job.pid =:= undefined].


jobs_running() ->
    [Job#job.id || Job <- all_jobs(), Job#job.pid =/= undefined].


run_stop_count() ->
    {length(jobs_running()), length(jobs_stopped())}.


oneshot_run_stop_count() ->
    Running = [Job#job.id || Job <- all_jobs(), Job#job.pid =/= undefined,
        not is_continuous(Job)],
    Stopped = [Job#job.id || Job <- all_jobs(), Job#job.pid =:= undefined,
        not is_continuous(Job)],
    {length(Running), length(Stopped)}.


mock_state(MaxJobs) ->
    #state{
        max_jobs = MaxJobs,
        max_churn = ?DEFAULT_MAX_CHURN,
        max_history = ?DEFAULT_MAX_HISTORY
    }.

mock_state(MaxJobs, MaxChurn) ->
    #state{
        max_jobs = MaxJobs,
        max_churn = MaxChurn,
        max_history = ?DEFAULT_MAX_HISTORY
    }.


continuous(Id) when is_integer(Id) ->
    Started = Id,
    Hist = [stopped(Started+1), started(Started), added()],
    #job{
        id = Id,
        history = Hist,
        rep = #rep{options = [{continuous, true}]}
    }.


continuous_running(Id) when is_integer(Id) ->
    Started = Id,
    Pid = mock_pid(),
    #job{
        id = Id,
        history = [started(Started), added()],
        rep = #rep{options = [{continuous, true}]},
        pid = Pid,
        monitor = monitor(process, Pid)
    }.


oneshot(Id) when is_integer(Id) ->
    Started = Id,
    Hist = [stopped(Started + 1), started(Started), added()],
    #job{id = Id, history = Hist, rep = #rep{options = []}}.


oneshot_running(Id) when is_integer(Id) ->
    Started = Id,
    Pid = mock_pid(),
    #job{
        id = Id,
        history = [started(Started), added()],
        rep = #rep{options = []},
        pid = Pid,
        monitor = monitor(process, Pid)
    }.


job(Hist) when is_list(Hist) ->
    #job{history = Hist}.


mock_pid() ->
   list_to_pid("<0.999.999>").

crashed() ->
    crashed(0).


crashed(WhenSec) when is_integer(WhenSec)->
    {{crashed, some_reason}, {0, WhenSec, 0}};
crashed({MSec, Sec, USec}) ->
    {{crashed, some_reason}, {MSec, Sec, USec}}.


started() ->
    started(0).


started(WhenSec) ->
    {started, {0, WhenSec, 0}}.


stopped() ->
    stopped(0).


stopped(WhenSec) ->
    {stopped, {0, WhenSec, 0}}.

added() ->
    {added, {0, 0, 0}}.

-endif.

