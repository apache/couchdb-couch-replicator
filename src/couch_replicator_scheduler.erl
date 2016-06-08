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

%% public api
-export([start_link/0, add_job/1, remove_job/1, reschedule/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([format_status/2]).

%% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

%% types
-type event_type() :: started | stopped | crashed.
-type event() :: {Type:: event_type(), When :: erlang:timestamp()}.
-type history() :: [Events :: event()].

%% definitions
-define(MAX_HISTORY, 20).
-define(MINIMUM_CRASH_INTERVAL, 60 * 1000000).

-define(DEFAULT_MAX_JOBS, 100).
-define(DEFAULT_MAX_CHURN, 20).
-define(DEFAULT_SCHEDULER_INTERVAL, 60000).
-record(state, {interval, timer, max_jobs, max_churn}).
-record(job, {
          id :: job_id(),
          rep :: #rep{},
          pid :: undefined | pid(),
          monitor :: undefined | reference(),
          history :: history()}).

%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(#rep{}) -> ok | {error, already_added}.
add_job(#rep{} = Rep) when Rep#rep.id /= undefined ->
    Job = #job{
        id = Rep#rep.id,
        rep = Rep,
        history = []},
    gen_server:call(?MODULE, {add_job, Job}).


-spec remove_job(job_id()) -> ok.
remove_job(Id) ->
    gen_server:call(?MODULE, {remove_job, Id}).


-spec reschedule() -> ok.
% Trigger a manual reschedule. Used for testing and/or ops.
reschedule() ->
    gen_server:call(?MODULE, reschedule).

%% gen_server functions

init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ok = config:listen_for_changes(?MODULE, self()),
    Interval = config:get_integer("replicator", "interval", ?DEFAULT_SCHEDULER_INTERVAL),
    MaxJobs = config:get_integer("replicator", "max_jobs", ?DEFAULT_MAX_JOBS),
    MaxChurn = config:get_integer("replicator", "max_churn", ?DEFAULT_MAX_CHURN),
    {ok, Timer} = timer:send_after(Interval, reschedule),
    {ok, #state{interval = Interval, max_jobs = MaxJobs, max_churn = MaxChurn, timer = Timer}}.


handle_call({add_job, Job}, _From, State) ->
    case add_job_int(Job) of
        true ->
            case running_job_count() of
                RunningJobs when RunningJobs < State#state.max_jobs ->
                    start_job_int(Job);
                _ ->
                    ok
                end,
            {reply, ok, State};
        false ->
            {reply, {error, already_added}, State}
    end;

handle_call({remove_job, Id}, _From, State) ->
    case job_by_id(Id) of
        {ok, Job} ->
            ok = stop_job_int(Job),
            true = remove_job_int(Job),
            {reply, ok, State};
        {error, not_found} ->
            {reply, ok, State}
    end;

handle_call(reschedule, _From, State) ->
    ok = reschedule(State#state.max_jobs, State#state.max_churn),
    {reply, ok, State};

handle_call(_, _From, State) ->
    {noreply, State}.


handle_cast({set_max_jobs, MaxJobs}, State) when is_integer(MaxJobs), MaxJobs >= 0 ->
    couch_log:notice("~p: max_jobs set to ~B", [?MODULE, MaxJobs]),
    {noreply, State#state{max_jobs = MaxJobs}};

handle_cast({set_max_churn, MaxChurn}, State) when is_integer(MaxChurn), MaxChurn > 0 ->
    couch_log:notice("~p: max_churn set to ~B", [?MODULE, MaxChurn]),
    {noreply, State#state{max_churn = MaxChurn}};

handle_cast({set_interval, Interval}, State) when is_integer(Interval), Interval > 0 ->
    couch_log:notice("~p: interval set to ~B", [?MODULE, Interval]),
    {noreply, State#state{interval = Interval}};

handle_cast(_, State) ->
    {noreply, State}.


handle_info(reschedule, State) ->
    ok = reschedule(State#state.max_jobs, State#state.max_churn),
    {ok, cancel} = timer:cancel(State#state.timer),
    {ok, Timer} = timer:send_after(State#state.interval, reschedule),
    {noreply, State#state{timer = Timer}};

handle_info({'DOWN', _Ref, process, Pid, normal}, State) ->
    {ok, Job} = job_by_pid(Pid),
    couch_log:notice("~p: Job ~p completed normally", [?MODULE, Job#job.id]),
    remove_job_int(Job),
    {noreply, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    {ok, Job0} = job_by_pid(Pid),
    couch_log:notice("~p: Job ~p died with reason: ~p",
        [?MODULE, Job0#job.id, Reason]),
    Job1 = update_history(Job0#job{pid = undefined, monitor = undefined},
        crashed, os:timestamp()),
    true = ets:insert(?MODULE, Job1),
    start_pending_jobs(State#state.max_jobs),
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

start_jobs(Count) ->
    Runnable0 = pending_jobs(),
    Runnable1 = lists:sort(fun oldest_job_first/2, Runnable0),
    Runnable2 = lists:filter(fun not_recently_crashed/1, Runnable1),
    Runnable3 = lists:sublist(Runnable2, Count),
    lists:foreach(fun start_job_int/1, Runnable3).


stop_jobs(Count) ->
    Running0 = running_jobs(),
    Running1 = lists:sort(fun oldest_job_first/2, Running0),
    Running2 = lists:sublist(Running1, Count),
    lists:foreach(fun stop_job_int/1, Running2).


oldest_job_first(#job{} = A, #job{} = B) ->
    last_started(A) =< last_started(B).


not_recently_crashed(#job{} = Job) ->
    case crash_history(Job) of
        [] ->
            true;
        [{crashed, When} | _] ->
            timer:now_diff(os:timestamp(), When)
                >= ?MINIMUM_CRASH_INTERVAL
    end.


crash_history(#job{} = Job) ->
    [Crash || {crashed, _When} = Crash <- Job#job.history].


-spec add_job_int(#job{}) -> boolean().
add_job_int(#job{} = Job) ->
    ets:insert_new(?MODULE, Job).


start_job_int(#job{pid = Pid}) when Pid /= undefined ->
    ok;

start_job_int(#job{} = Job0) ->
    case couch_replicator_scheduler_sup:start_child(Job0#job.rep) of
        {ok, Child} ->
            Ref = monitor(process, Child),
            Job1 = update_history(Job0#job{pid = Child, monitor = Ref},
                started, os:timestamp()),
            true = ets:insert(?MODULE, Job1),
            couch_log:notice("~p: Job ~p started as ~p",
                [?MODULE, Job1#job.id, Job1#job.pid]);
        {error, Reason} ->
            couch_log:notice("~p: Job ~p failed to start for reason ~p",
                [?MODULE, Job0, Reason])
    end.


-spec stop_job_int(#job{}) -> ok | {error, term()}.
stop_job_int(#job{pid = undefined}) ->
    ok;

stop_job_int(#job{} = Job0) ->
    ok = couch_replicator_scheduler_sup:terminate_child(Job0#job.pid),
    demonitor(Job0#job.monitor, [flush]),
    Job1 = update_history(Job0#job{pid = undefined, monitor = undefined},
        stopped, os:timestamp()),
    true = ets:insert(?MODULE, Job1),
    couch_log:notice("~p: Job ~p stopped as ~p",
        [?MODULE, Job0#job.id, Job0#job.pid]).


-spec remove_job_int(#job{}) -> true.
remove_job_int(#job{} = Job) ->
    ets:delete(?MODULE, Job#job.id).


-spec running_job_count() -> non_neg_integer().
running_job_count() ->
    ets:info(?MODULE, size) - pending_job_count().


-spec running_jobs() -> [#job{}].
running_jobs() ->
    ets:tab2list(?MODULE) -- pending_jobs().


-spec pending_job_count() -> non_neg_integer().
pending_job_count() ->
    MatchSpec = [{#job{pid='$1', _='_'}, [{'not', {'is_pid', '$1'}}], [true]}],
    ets:select_count(?MODULE, MatchSpec).


-spec pending_jobs() -> [#job{}].
pending_jobs() ->
    ets:match_object(?MODULE, #job{pid=undefined, _='_'}).


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


-spec reschedule(MaxJobs :: non_neg_integer(), MaxChurn :: non_neg_integer()) -> ok.
reschedule(MaxJobs, MaxChurn)
  when is_integer(MaxJobs), MaxJobs >= 0, is_integer(MaxChurn), MaxChurn > 0 ->
    Running = running_job_count(),
    Pending = pending_job_count(),
    stop_excess_jobs(MaxJobs, Running),
    start_pending_jobs(MaxJobs, Running, Pending),
    rotate_jobs(MaxJobs, MaxChurn, Running, Pending).


stop_excess_jobs(Max, Running) when Running > Max ->
    stop_jobs(Running - Max);

stop_excess_jobs(_, _) ->
    ok.


start_pending_jobs(Max) ->
    start_pending_jobs(Max, running_job_count(), pending_job_count()).


start_pending_jobs(Max, Running, Pending) when Running < Max, Pending > 0 ->
    start_jobs(Max - Running);

start_pending_jobs(_, _, _) ->
    ok.

rotate_jobs(MaxJobs, MaxChurn, Running, Pending) when Running == MaxJobs, Pending > 0 ->
    stop_jobs(min([Pending, Running, MaxChurn])),
    start_jobs(min([Pending, Running, MaxChurn]));

rotate_jobs(_, _, _, _) ->
    ok.


min(List) ->
    hd(lists:sort(List)).


-spec last_started(#job{}) -> erlang:timestamp().
last_started(#job{} = Job) ->
    Starts = [E || {started, _} = E <- Job#job.history],
    case Starts of
        [] ->
            {0, 0, 0};
        [{started, When} | _] ->
            When
    end.


-spec update_history(#job{}, event_type(), erlang:timestamp()) -> #job{}.
update_history(Job, Type, When) ->
    History0 = [{Type, When} | Job#job.history],
    History1 = lists:sublist(History0, ?MAX_HISTORY),
    Job#job{history = History1}.
