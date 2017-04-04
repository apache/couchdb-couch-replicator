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

-module(couch_replicator_manager).
-behaviour(gen_server).
-vsn(3).
-behaviour(config_listener).

% public API
-export([replication_started/1, replication_completed/2, replication_error/2]).
-export([continue/1, replication_usurped/2]).

-export([before_doc_update/2, after_doc_read/2]).

% gen_server callbacks
-export([start_link/0, init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

% changes callbacks
-export([changes_reader/3, changes_reader_cb/3]).

% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

-export([handle_db_event/3]).

%% exported but private
-export([start_replication/2]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include("couch_replicator.hrl").
-include("couch_replicator_js_functions.hrl").

-define(DOC_TO_REP, couch_rep_doc_id_to_rep_id).
-define(REP_TO_STATE, couch_rep_id_to_rep_state).
-define(INITIAL_WAIT, 2.5). % seconds
-define(MAX_WAIT, 600).     % seconds
-define(AVG_DELAY_MSEC, 100).
-define(MAX_DELAY_MSEC, 60000).
-define(OWNER, <<"owner">>).
-define(REPLICATOR_DB, <<"_replicator">>).

-define(DB_TO_SEQ, db_to_seq).
-define(CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}}).

-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).

-define(RELISTEN_DELAY, 5000).

-record(rep_state, {
    rep,
    starting,
    retries_left,
    max_retries,
    wait = ?INITIAL_WAIT
}).

-import(couch_util, [
    to_binary/1
]).

-record(state, {
    event_listener = nil,
    scan_pid = nil,
    rep_start_pids = [],
    max_retries,
    live = [],
    epoch = nil
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


replication_started(#rep{id = {BaseId, _} = RepId}) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep_state{rep = #rep{db_name = DbName, doc_id = DocId}} ->
        update_rep_doc(DbName, DocId, [
            {<<"_replication_state">>, <<"triggered">>},
            {<<"_replication_state_reason">>, undefined},
            {<<"_replication_id">>, ?l2b(BaseId)},
            {<<"_replication_stats">>, undefined}]),
        ok = gen_server:call(?MODULE, {rep_started, RepId}, infinity),
        couch_log:notice("Document `~s` from `~s` triggered replication `~s`",
            [DocId, DbName, pp_rep_id(RepId)])
    end.


replication_completed(#rep{id = RepId}, Stats) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep_state{rep = #rep{db_name = DbName, doc_id = DocId}} ->
        update_rep_doc(DbName, DocId, [
            {<<"_replication_state">>, <<"completed">>},
            {<<"_replication_state_reason">>, undefined},
            {<<"_replication_stats">>, {Stats}}]),
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` finished (triggered by document `~s`"
            " from `~s`)", [pp_rep_id(RepId), DocId, DbName])
    end.


replication_usurped(#rep{id = RepId}, By) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep_state{rep = #rep{db_name = DbName, doc_id = DocId}} ->
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` usurped by ~s (triggered by document"
            " `~s` from `~s`)", [pp_rep_id(RepId), By, DocId, DbName])
    end.


replication_error(#rep{id = {BaseId, _} = RepId}, Error) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep_state{rep = #rep{db_name = DbName, doc_id = DocId}} ->
        ok = timer:sleep(jitter(ets:info(?REP_TO_STATE, size))),
        update_rep_doc(DbName, DocId, [
            {<<"_replication_state">>, <<"error">>},
            {<<"_replication_state_reason">>, to_binary(error_reason(Error))},
            {<<"_replication_id">>, ?l2b(BaseId)}]),
        ok = gen_server:call(?MODULE, {rep_error, RepId, Error}, infinity)
    end.

continue(#rep{doc_id = null}) ->
    {true, no_owner};
continue(#rep{id = RepId}) ->
    Owner = gen_server:call(?MODULE, {owner, RepId}, infinity),
    {node() == Owner, Owner}.


handle_config_change("replicator", "max_replication_retry_count", V, _, S) ->
    ok = gen_server:cast(?MODULE, {set_max_retries, retries_value(V)}),
    {ok, S};
handle_config_change(_, _, _, _, S) ->
    {ok, S}.

handle_config_terminate(_, stop, _) ->
    ok;
handle_config_terminate(_Server, _Reason, _State) ->
    erlang:send_after(?RELISTEN_DELAY, whereis(?MODULE), restart_config_listener).

init(_) ->
    process_flag(trap_exit, true),
    net_kernel:monitor_nodes(true),
    Live = [node() | nodes()],
    ?DOC_TO_REP = ets:new(?DOC_TO_REP, [named_table, set, public]),
    ?REP_TO_STATE = ets:new(?REP_TO_STATE, [named_table, set, public]),
    ?DB_TO_SEQ = ets:new(?DB_TO_SEQ, [named_table, set, public]),
    Server = self(),
    ok = config:listen_for_changes(?MODULE, nil),
    Epoch = make_ref(),
    ScanPid = spawn_link(fun() -> scan_all_dbs(Server) end),
    % Automatically start node local changes feed loop
    ensure_rep_db_exists(?REPLICATOR_DB),
    Pid = start_changes_reader(?REPLICATOR_DB, 0, Epoch),
    {ok, #state{
        event_listener = start_event_listener(),
        scan_pid = ScanPid,
        max_retries = retries_value(
            config:get("replicator", "max_replication_retry_count", "10")),
        rep_start_pids = [{?REPLICATOR_DB, Pid}],
        live = Live,
        epoch = Epoch
    }}.

handle_call({owner, RepId}, _From, State) ->
    case rep_state(RepId) of
    nil ->
        {reply, nonode, State};
    #rep_state{rep = #rep{db_name = DbName, doc_id = DocId}} ->
        {reply, owner(DbName, DocId, State#state.live), State}
    end;

handle_call({rep_db_update, DbName, {ChangeProps} = Change}, _From, State) ->
    NewState = try
        process_update(State, DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        rep_db_update_error(Error, DbName, DocId),
        State
    end,
    {reply, ok, NewState};


handle_call({rep_started, RepId}, _From, State) ->
    case rep_state(RepId) of
    nil ->
        ok;
    RepState ->
        NewRepState = RepState#rep_state{
            starting = false,
            retries_left = State#state.max_retries,
            max_retries = State#state.max_retries,
            wait = ?INITIAL_WAIT
        },
        true = ets:insert(?REP_TO_STATE, {RepId, NewRepState})
    end,
    {reply, ok, State};

handle_call({rep_complete, RepId}, _From, State) ->
    true = ets:delete(?REP_TO_STATE, RepId),
    {reply, ok, State};

handle_call({rep_error, RepId, Error}, _From, State) ->
    {reply, ok, replication_error(State, RepId, Error)};

% Match changes epoch with the current epoch in the state.
% New epoch ref is created on a full rescan. Change feeds have to
% be replayed from the start to determine ownership in the new
% cluster configuration and epoch is used to match & checkpoint
% only changes from the current cluster configuration.
handle_call({rep_db_checkpoint, DbName, EndSeq, Epoch}, _From,
            #state{epoch = Epoch} = State) ->
    Entry = case ets:lookup(?DB_TO_SEQ, DbName) of
        [] ->
            {DbName, EndSeq, false};
        [{DbName, _OldSeq, Rescan}] ->
            {DbName, EndSeq, Rescan}
    end,
    true = ets:insert(?DB_TO_SEQ, Entry),
    {reply, ok, State};

% Ignore checkpoints from previous epoch.
handle_call({rep_db_checkpoint, _DbName, _EndSeq, _Epoch}, _From, State) ->
    {reply, ok, State};

handle_call(Msg, From, State) ->
    couch_log:error("Replication manager received unexpected call ~p from ~p",
        [Msg, From]),
    {stop, {error, {unexpected_call, Msg}}, State}.

handle_cast({resume_scan, DbName}, State) ->
    Pids = State#state.rep_start_pids,
    NewPids = case lists:keyfind(DbName, 1, Pids) of
        {DbName, _Pid} ->
            Entry = case ets:lookup(?DB_TO_SEQ, DbName) of
                [] ->
                    {DbName, 0, true};
                [{DbName, EndSeq, _Rescan}] ->
                    {DbName, EndSeq, true}
            end,
            true = ets:insert(?DB_TO_SEQ, Entry),
            Pids;
        false ->
            Since = case ets:lookup(?DB_TO_SEQ, DbName) of
                [] -> 0;
                [{DbName, EndSeq, _Rescan}] -> EndSeq
            end,
            true = ets:insert(?DB_TO_SEQ, {DbName, Since, false}),
            ensure_rep_ddoc_exists(DbName),
            Pid = start_changes_reader(DbName, Since, State#state.epoch),
            couch_log:debug("Scanning ~s from update_seq ~p", [DbName, Since]),
            [{DbName, Pid} | Pids]
    end,
    {noreply, State#state{rep_start_pids = NewPids}};

handle_cast({set_max_retries, MaxRetries}, State) ->
    {noreply, State#state{max_retries = MaxRetries}};

handle_cast(Msg, State) ->
    couch_log:error("Replication manager received unexpected cast ~p", [Msg]),
    {stop, {error, {unexpected_cast, Msg}}, State}.

handle_info({nodeup, Node}, State) ->
    couch_log:notice("Rescanning replicator dbs as ~s came up.", [Node]),
    Live = lists:usort([Node | State#state.live]),
    {noreply, rescan(State#state{live=Live})};

handle_info({nodedown, Node}, State) ->
    couch_log:notice("Rescanning replicator dbs ~s went down.", [Node]),
    Live = State#state.live -- [Node],
    {noreply, rescan(State#state{live=Live})};

handle_info({'EXIT', From, normal}, #state{scan_pid = From} = State) ->
    couch_log:debug("Background scan has completed.", []),
    {noreply, State#state{scan_pid=nil}};

handle_info({'EXIT', From, Reason}, #state{scan_pid = From} = State) ->
    couch_log:error("Background scanner died. Reason: ~p", [Reason]),
    {stop, {scanner_died, Reason}, State};

handle_info({'EXIT', From, Reason}, #state{event_listener = From} = State) ->
    couch_log:error("Database update notifier died. Reason: ~p", [Reason]),
    {stop, {db_update_notifier_died, Reason}, State};

handle_info({'EXIT', From, Reason}, #state{rep_start_pids = Pids} = State) ->
    case lists:keytake(From, 2, Pids) of
        {value, {DbName, From}, NewPids} ->
            if Reason == normal -> ok; true ->
                Fmt = "~s : Known replication or change feed pid ~w died :: ~w",
                couch_log:error(Fmt, [?MODULE, From, Reason])
            end,
            NewState = State#state{rep_start_pids = NewPids},
            case ets:lookup(?DB_TO_SEQ, DbName) of
                [{DbName, _EndSeq, true}] ->
                    handle_cast({resume_scan, DbName}, NewState);
                _ ->
                    {noreply, NewState}
            end;
        false when Reason == normal ->
            {noreply, State};
        false ->
            Fmt = "~s : Unknown pid ~w died :: ~w",
            couch_log:error(Fmt, [?MODULE, From, Reason]),
            {stop, {unexpected_exit, From, Reason}, State}
    end;

handle_info({'DOWN', _Ref, _, _, _}, State) ->
    % From a db monitor created by a replication process. Ignore.
    {noreply, State};

handle_info(shutdown, State) ->
    {stop, shutdown, State};

handle_info(restart_config_listener, State) ->
    ok = config:listen_for_changes(?MODULE, nil),
    {noreply, State};

handle_info(Msg, State) ->
    couch_log:error("Replication manager received unexpected message ~p", [Msg]),
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, State) ->
    #state{
        scan_pid = ScanPid,
        rep_start_pids = StartPids,
        event_listener = Listener
    } = State,
    stop_all_replications(),
    lists:foreach(
        fun({_Tag, Pid}) ->
            catch unlink(Pid),
            catch exit(Pid, stop)
        end,
        [{scanner, ScanPid} | StartPids]),
    true = ets:delete(?REP_TO_STATE),
    true = ets:delete(?DOC_TO_REP),
    true = ets:delete(?DB_TO_SEQ),
    couch_event:stop_listener(Listener).


code_change(1, State, _Extra) ->
    {ok, erlang:append_element(State, [node() | nodes()])};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_changes_reader(DbName, Since, Epoch) ->
    spawn_link(?MODULE, changes_reader, [{self(), Epoch}, DbName, Since]).

changes_reader({Server, Epoch}, DbName, Since) ->
    UserCtx = #user_ctx{roles = [<<"_admin">>, <<"_replicator">>]},
    DbOpenOptions = [{user_ctx, UserCtx}, sys_db],
    {ok, Db} = couch_db:open_int(DbName, DbOpenOptions),
    ChangesFeedFun = couch_changes:handle_db_changes(
        #changes_args{
            include_docs = true,
            since = Since,
            feed = "normal",
            timeout = infinity
        },
        {json_req, null},
        Db
    ),
    ChangesFeedFun({fun ?MODULE:changes_reader_cb/3, {Server, DbName, Epoch}}).

changes_reader_cb({change, Change, _}, _, {Server, DbName, Epoch}) ->
    case has_valid_rep_id(Change) of
        true ->
            Msg = {rep_db_update, DbName, Change},
            ok = gen_server:call(Server, Msg, infinity);
        false ->
            ok
    end,
    {Server, DbName, Epoch};
changes_reader_cb({stop, EndSeq}, _, {Server, DbName, Epoch}) ->
    Msg = {rep_db_checkpoint, DbName, EndSeq, Epoch},
    ok = gen_server:call(Server, Msg, infinity),
    {Server, DbName, Epoch};
changes_reader_cb(_, _, Acc) ->
    Acc.

has_valid_rep_id({Change}) ->
    has_valid_rep_id(get_json_value(<<"id">>, Change));
has_valid_rep_id(<<?DESIGN_DOC_PREFIX, _Rest/binary>>) ->
    false;
has_valid_rep_id(_Else) ->
    true.


start_event_listener() ->
    {ok, Pid} = couch_event:link_listener(
            ?MODULE, handle_db_event, self(), [all_dbs]
        ),
    Pid.


handle_db_event(DbName, created, Server) ->
    case is_replicator_db(DbName) of
	true ->
	    ensure_rep_ddoc_exists(DbName);
	_ ->
	    ok
    end,
    {ok, Server};
handle_db_event(DbName, updated, Server) ->
    case is_replicator_db(DbName) of
        true ->
	    Msg = {resume_scan, DbName},
	    ok = gen_server:cast(Server, Msg);
        _ ->
            ok
    end,
    {ok, Server};
handle_db_event(DbName, deleted, Server) ->
    case is_replicator_db(DbName) of
        true ->
            clean_up_replications(DbName);
        _ ->
            ok
    end,
    {ok, Server};
handle_db_event(_DbName, _Event, Server) ->
    {ok, Server}.

rescan(#state{scan_pid = nil} = State) ->
    true = ets:delete_all_objects(?DB_TO_SEQ),
    Server = self(),
    Epoch = make_ref(),
    NewScanPid = spawn_link(fun() -> scan_all_dbs(Server) end),
    State#state{scan_pid = NewScanPid, epoch = Epoch};
rescan(#state{scan_pid = ScanPid} = State) ->
    unlink(ScanPid),
    exit(ScanPid, exit),
    rescan(State#state{scan_pid = nil}).

process_update(State, DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    case {owner(DbName, DocId, State#state.live), get_json_value(deleted, Change, false)} of
    {_, true} ->
        rep_doc_deleted(DbName, DocId),
        State;
    {Owner, false} when Owner /= node() ->
        couch_log:notice("Not starting '~s' from '~s' as owner is ~s.",
            [DocId, DbName, Owner]),
        State;
    {_Owner, false} ->
        couch_log:notice("Maybe starting '~s' from '~s' as I'm the owner", [DocId, DbName]),
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            maybe_start_replication(State, DbName, DocId, JsonRepDoc);
        <<"triggered">> ->
            maybe_start_replication(State, DbName, DocId, JsonRepDoc);
        <<"completed">> ->
            replication_complete(DbName, DocId),
            State;
        <<"error">> ->
            case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
            [] ->
                maybe_start_replication(State, DbName, DocId, JsonRepDoc);
            _ ->
                State
            end
        end
    end.

owner(<<"shards/", _/binary>> = DbName, DocId, Live) ->
    Nodes = lists:sort([N || #shard{node=N} <- mem3:shards(mem3:dbname(DbName), DocId),
			     lists:member(N, Live)]),
    hd(mem3_util:rotate_list({DbName, DocId}, Nodes));
owner(_DbName, _DocId, _Live) ->
    node().

rep_db_update_error(Error, DbName, DocId) ->
    case Error of
    {bad_rep_doc, Reason} ->
        ok;
    _ ->
        Reason = to_binary(Error)
    end,
    couch_log:error("Replication manager, error processing document `~s`"
        " from `~s`: ~s", [DocId, DbName, Reason]),
    update_rep_doc(DbName, DocId, [{<<"_replication_state">>, <<"error">>},
                           {<<"_replication_state_reason">>, Reason}]).


rep_user_ctx({RepDoc}) ->
    case get_json_value(<<"user_ctx">>, RepDoc) of
    undefined ->
        #user_ctx{};
    {UserCtx} ->
        #user_ctx{
            name = get_json_value(<<"name">>, UserCtx, null),
            roles = get_json_value(<<"roles">>, UserCtx, [])
        }
    end.


maybe_start_replication(State, DbName, DocId, RepDoc) ->
    #rep{id = {BaseId, _} = RepId} = Rep0 = parse_rep_doc(RepDoc),
    Rep = Rep0#rep{db_name = DbName},
    case rep_state(RepId) of
    nil ->
        RepState = #rep_state{
            rep = Rep,
            starting = true,
            retries_left = State#state.max_retries,
            max_retries = State#state.max_retries
        },
        true = ets:insert(?REP_TO_STATE, {RepId, RepState}),
        true = ets:insert(?DOC_TO_REP, {{DbName, DocId}, RepId}),
        couch_log:notice("Attempting to start replication `~s` (document `~s`"
            " from `~s`).", [pp_rep_id(RepId), DocId, DbName]),
        StartDelaySecs = erlang:max(0,
            config:get_integer("replicator", "start_delay", 10)),
        StartSplaySecs = erlang:max(1,
            config:get_integer("replicator", "start_splay", 50)),
        DelaySecs = StartDelaySecs + random:uniform(StartSplaySecs),
        couch_log:notice("Delaying replication `~s` start by ~p seconds.",
            [pp_rep_id(RepId), DelaySecs]),
        Pid = spawn_link(?MODULE, start_replication, [Rep, DelaySecs]),
        State#state{
            rep_start_pids = [{rep_start, Pid} | State#state.rep_start_pids]
        };
    #rep_state{rep = #rep{doc_id = DocId}} ->
        State;
    #rep_state{starting = false, rep = #rep{db_name = DbName, doc_id = OtherDocId}} ->
        couch_log:notice("The replication specified by the document `~s` from"
            " `~s` was already triggered by the document `~s`",
            [DocId, DbName, OtherDocId]),
        maybe_tag_rep_doc(DbName, DocId, RepDoc, ?l2b(BaseId)),
        State;
    #rep_state{starting = true, rep = #rep{db_name = DbName, doc_id = OtherDocId}} ->
        couch_log:notice("The replication specified by the document `~s` from"
            " `~s` is already being triggered by the document `~s`",
            [DocId, DbName, OtherDocId]),
        maybe_tag_rep_doc(DbName, DocId, RepDoc, ?l2b(BaseId)),
        State
    end.


parse_rep_doc(RepDoc) ->
    {ok, Rep} = try
        couch_replicator_utils:parse_rep_doc(RepDoc, rep_user_ctx(RepDoc))
    catch
    throw:{error, Reason} ->
        throw({bad_rep_doc, Reason});
    Tag:Err ->
        throw({bad_rep_doc, to_binary({Tag, Err})})
    end,
    Rep.


maybe_tag_rep_doc(DbName, DocId, {RepProps}, RepId) ->
    case get_json_value(<<"_replication_id">>, RepProps) of
    RepId ->
        ok;
    _ ->
        update_rep_doc(DbName, DocId, [{<<"_replication_id">>, RepId}])
    end.

start_replication(Rep, Wait) ->
    ok = timer:sleep(Wait * 1000),
    case (catch couch_replicator:async_replicate(Rep)) of
    {ok, _} ->
        ok;
    Error ->
        replication_error(Rep, Error)
    end.

replication_complete(DbName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
    [{{DbName, DocId}, {BaseId, Ext} = RepId}] ->
        case rep_state(RepId) of
        nil ->
            % Prior to OTP R14B02, temporary child specs remain in
            % in the supervisor after a worker finishes - remove them.
            % We want to be able to start the same replication but with
            % eventually different values for parameters that don't
            % contribute to its ID calculation.
            case erlang:system_info(otp_release) < "R14B02" of
            true ->
                spawn(fun() ->
                    _ = supervisor:delete_child(couch_replicator_job_sup, BaseId ++ Ext)
                end);
            false ->
                ok
            end;
        #rep_state{} ->
            ok
        end,
        true = ets:delete(?DOC_TO_REP, {DbName, DocId});
    _ ->
        ok
    end.


rep_doc_deleted(DbName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
    [{{DbName, DocId}, RepId}] ->
        couch_replicator:cancel_replication(RepId),
        true = ets:delete(?REP_TO_STATE, RepId),
        true = ets:delete(?DOC_TO_REP, {DbName, DocId}),
        couch_log:notice("Stopped replication `~s` because replication document"
            " `~s` from `~s` was deleted", [pp_rep_id(RepId), DocId, DbName]);
    [] ->
        ok
    end.


replication_error(State, RepId, Error) ->
    case rep_state(RepId) of
    nil ->
        State;
    RepState ->
        maybe_retry_replication(RepState, Error, State)
    end.

maybe_retry_replication(#rep_state{retries_left = 0} = RepState, Error, State) ->
    #rep_state{
        rep = #rep{id = RepId, doc_id = DocId, db_name = DbName},
        max_retries = MaxRetries
    } = RepState,
    couch_replicator:cancel_replication(RepId),
    true = ets:delete(?REP_TO_STATE, RepId),
    true = ets:delete(?DOC_TO_REP, {DbName, DocId}),
    couch_log:error("Error in replication `~s` (triggered by document `~s` from"
        " `~s` ): ~s~nReached maximum retry attempts (~p).", [pp_rep_id(RepId),
        DocId, DbName, to_binary(error_reason(Error)), MaxRetries]),
    State;

maybe_retry_replication(RepState, Error, State) ->
    #rep_state{
        rep = #rep{id = RepId, doc_id = DocId, db_name = DbName} = Rep
    } = RepState,
    #rep_state{wait = Wait} = NewRepState = state_after_error(RepState),
    true = ets:insert(?REP_TO_STATE, {RepId, NewRepState}),
    couch_log:error("Error in replication `~s` (triggered by document `~s` from"
        " `~s` ): ~s~nRestarting replication in ~p seconds.", [pp_rep_id(RepId),
        DocId, DbName, to_binary(error_reason(Error)), Wait]),
    Pid = spawn_link(?MODULE, start_replication, [Rep, Wait]),
    State#state{
        rep_start_pids = [{rep_start, Pid} | State#state.rep_start_pids]
    }.


stop_all_replications() ->
    couch_log:notice("Stopping all ongoing replications", []),
    ets:foldl(
        fun({_, RepId}, _) ->
            couch_replicator:cancel_replication(RepId)
        end,
        ok, ?DOC_TO_REP),
    true = ets:delete_all_objects(?REP_TO_STATE),
    true = ets:delete_all_objects(?DOC_TO_REP),
    true = ets:delete_all_objects(?DB_TO_SEQ).

clean_up_replications(DbName) ->
    ets:foldl(
        fun({{Name, DocId}, RepId}, _) when Name =:= DbName ->
            couch_replicator:cancel_replication(RepId),
            ets:delete(?DOC_TO_REP,{Name, DocId}),
            ets:delete(?REP_TO_STATE, RepId);
           ({_,_}, _) ->
            ok
        end,
        ok, ?DOC_TO_REP),
    ets:delete(?DB_TO_SEQ,DbName).


update_rep_doc(RepDbName, RepDocId, KVs) ->
    update_rep_doc(RepDbName, RepDocId, KVs, 1).

update_rep_doc(RepDbName, RepDocId, KVs, Wait) when is_binary(RepDocId) ->
    try
        case open_rep_doc(RepDbName, RepDocId) of
            {ok, LastRepDoc} ->
                update_rep_doc(RepDbName, LastRepDoc, KVs, Wait * 2);
            _ ->
                ok
        end
    catch
        throw:conflict ->
            Msg = "Conflict when updating replication document `~s`. Retrying.",
            couch_log:error(Msg, [RepDocId]),
            ok = timer:sleep(random:uniform(erlang:min(128, Wait)) * 100),
            update_rep_doc(RepDbName, RepDocId, KVs, Wait * 2)
    end;
update_rep_doc(RepDbName, #doc{body = {RepDocBody}} = RepDoc, KVs, _Try) ->
    NewRepDocBody = lists:foldl(
        fun({K, undefined}, Body) ->
                lists:keydelete(K, 1, Body);
           ({<<"_replication_state">> = K, State} = KV, Body) ->
                case get_json_value(K, Body) of
                State ->
                    Body;
                _ ->
                    Body1 = lists:keystore(K, 1, Body, KV),
                    lists:keystore(
                        <<"_replication_state_time">>, 1, Body1,
                        {<<"_replication_state_time">>, timestamp()})
                end;
            ({K, _V} = KV, Body) ->
                lists:keystore(K, 1, Body, KV)
        end,
        RepDocBody, KVs),
    case NewRepDocBody of
    RepDocBody ->
        ok;
    _ ->
        % Might not succeed - when the replication doc is deleted right
        % before this update (not an error, ignore).
        save_rep_doc(RepDbName, RepDoc#doc{body = {NewRepDocBody}})
    end.

open_rep_doc(DbName, DocId) ->
    case couch_db:open_int(DbName, [?CTX, sys_db]) of
        {ok, Db} ->
            try
                couch_db:open_doc(Db, DocId, [ejson_body])
            after
                couch_db:close(Db)
            end;
        Else ->
            Else
    end.

save_rep_doc(DbName, Doc) ->
    {ok, Db} = couch_db:open_int(DbName, [?CTX, sys_db]),
    try
        couch_db:update_doc(Db, Doc, [])
    after
        couch_db:close(Db)
    end.

% RFC3339 timestamps.
% Note: doesn't include the time seconds fraction (RFC3339 says it's optional).
timestamp() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
    UTime = erlang:universaltime(),
    LocalTime = calendar:universal_time_to_local_time(UTime),
    DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(UTime),
    zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60),
    iolist_to_binary(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w~s",
            [Year, Month, Day, Hour, Min, Sec,
                zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60)])).

zone(Hr, Min) when Hr >= 0, Min >= 0 ->
    io_lib:format("+~2..0w:~2..0w", [Hr, Min]);
zone(Hr, Min) ->
    io_lib:format("-~2..0w:~2..0w", [abs(Hr), abs(Min)]).


ensure_rep_db_exists(<<"shards/", _/binary>>=DbName) ->
    ensure_rep_ddoc_exists(DbName),
    ok;
ensure_rep_db_exists(DbName) ->
    Db = case couch_db:open_int(DbName, [?CTX, sys_db, nologifmissing]) of
        {ok, Db0} ->
            Db0;
        _Error ->
            {ok, Db0} = couch_db:create(DbName, [?CTX, sys_db]),
            Db0
    end,
    ensure_rep_ddoc_exists(DbName),
    {ok, Db}.

ensure_rep_ddoc_exists(RepDb) ->
    DDocId = <<"_design/_replicator">>,
    case mem3:belongs(RepDb, DDocId) of
	true ->
	    ensure_rep_ddoc_exists(RepDb, DDocId);
	false ->
	    ok
    end.

ensure_rep_ddoc_exists(RepDb, DDocId) ->
    case open_rep_doc(RepDb, DDocId) of
        {not_found, no_db_file} ->
            %% database was deleted.
            ok;
        {not_found, _Reason} ->
            {ok, DDoc} = replication_design_doc(DDocId),
            couch_log:notice("creating replicator ddoc", []),
            {ok, _Rev} = save_rep_doc(RepDb, DDoc);
        {ok, Doc} ->
            {Props} = couch_doc:to_json_obj(Doc, []),
            case couch_util:get_value(<<"validate_doc_update">>, Props, []) of
                ?REP_DB_DOC_VALIDATE_FUN ->
                    ok;
                _ ->
                    Props1 = lists:keyreplace(<<"validate_doc_update">>, 1, Props,
                         {<<"validate_doc_update">>,
                        ?REP_DB_DOC_VALIDATE_FUN}),
                    DDoc = couch_doc:from_json_obj({Props1}),
                    couch_log:notice("updating replicator ddoc", []),
                    try
                        {ok, _} = save_rep_doc(RepDb, DDoc)
                    catch
                        throw:conflict ->
                            %% ignore, we'll retry next time
                            ok
                    end
            end
    end,
    ok.

replication_design_doc(DDocId) ->
    DocProps = [
        {<<"_id">>, DDocId},
        {<<"language">>, <<"javascript">>},
        {<<"validate_doc_update">>, ?REP_DB_DOC_VALIDATE_FUN}
   ],
   {ok, couch_doc:from_json_obj({DocProps})}.


% pretty-print replication id
pp_rep_id(#rep{id = RepId}) ->
    pp_rep_id(RepId);
pp_rep_id({Base, Extension}) ->
    Base ++ Extension.


rep_state(RepId) ->
    case ets:lookup(?REP_TO_STATE, RepId) of
    [{RepId, RepState}] ->
        RepState;
    [] ->
        nil
    end.


error_reason({error, {Error, Reason}})
  when is_atom(Error), is_binary(Reason) ->
    io_lib:format("~s: ~s", [Error, Reason]);
error_reason({error, Reason}) ->
    Reason;
error_reason(Reason) ->
    Reason.


retries_value("infinity") ->
    infinity;
retries_value(Value) ->
    list_to_integer(Value).


state_after_error(#rep_state{retries_left = Left, wait = Wait} = State) ->
    Wait2 = erlang:min(trunc(Wait * 2), ?MAX_WAIT),
    case Left of
    infinity ->
        State#rep_state{wait = Wait2};
    _ ->
        State#rep_state{retries_left = Left - 1, wait = Wait2}
    end.


before_doc_update(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db) ->
    Doc;
before_doc_update(#doc{body = {Body}} = Doc, Db) ->
    #user_ctx{roles = Roles, name = Name} = couch_db:get_user_ctx(Db),
    case lists:member(<<"_replicator">>, Roles) of
    true ->
        Doc;
    false ->
        case couch_util:get_value(?OWNER, Body) of
        undefined ->
            Doc#doc{body = {?replace(Body, ?OWNER, Name)}};
        Name ->
            Doc;
        Other ->
            case (catch couch_db:check_is_admin(Db)) of
            ok when Other =:= null ->
                Doc#doc{body = {?replace(Body, ?OWNER, Name)}};
            ok ->
                Doc;
            _ ->
                throw({forbidden, <<"Can't update replication documents",
                    " from other users.">>})
            end
        end
    end.


after_doc_read(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db) ->
    Doc;
after_doc_read(#doc{body = {Body}} = Doc, Db) ->
    #user_ctx{name = Name} = couch_db:get_user_ctx(Db),
    case (catch couch_db:check_is_admin(Db)) of
    ok ->
        Doc;
    _ ->
        case couch_util:get_value(?OWNER, Body) of
        Name ->
            Doc;
        _Other ->
            Source = strip_credentials(couch_util:get_value(<<"source">>,
Body)),
            Target = strip_credentials(couch_util:get_value(<<"target">>,
Body)),
            NewBody0 = ?replace(Body, <<"source">>, Source),
            NewBody = ?replace(NewBody0, <<"target">>, Target),
            #doc{revs = {Pos, [_ | Revs]}} = Doc,
            NewDoc = Doc#doc{body = {NewBody}, revs = {Pos - 1, Revs}},
            NewRevId = couch_db:new_revid(NewDoc),
            NewDoc#doc{revs = {Pos, [NewRevId | Revs]}}
        end
    end.


strip_credentials(undefined) ->
    undefined;
strip_credentials(Url) when is_binary(Url) ->
    re:replace(Url,
        "http(s)?://(?:[^:]+):[^@]+@(.*)$",
        "http\\1://\\2",
        [{return, binary}]);
strip_credentials({Props}) ->
    {lists:keydelete(<<"oauth">>, 1, Props)}.

scan_all_dbs(Server) when is_pid(Server) ->
    {ok, Db} = mem3_util:ensure_exists(
        config:get("mem3", "shards_db", "_dbs")),
    ChangesFun = couch_changes:handle_changes(#changes_args{}, nil, Db, nil),
    ChangesFun(fun({change, {Change}, _}, _) ->
        DbName = couch_util:get_value(<<"id">>, Change),
        case DbName of <<"_design/", _/binary>> -> ok; _Else ->
            case couch_replicator_utils:is_deleted(Change) of
            true ->
                ok;
            false ->
                try
                    [gen_server:cast(Server, {resume_scan, ShardName})
                        || ShardName <- replicator_shards(DbName)]
                catch error:database_does_not_exist ->
                    ok
                end
            end
        end;
        (_, _) -> ok
    end),
    couch_db:close(Db).


replicator_shards(DbName) ->
    case is_replicator_db(DbName) of
    false ->
        [];
    true ->
        [ShardName || #shard{name = ShardName} <- mem3:local_shards(DbName)]
    end.


% calculate random delay proportional to the number of replications
% on current node, in order to prevent a stampede:
%   - when a source with multiple replication targets fails
%   - when we restart couch_replication_manager
jitter(N) ->
    Range = min(2 * N * ?AVG_DELAY_MSEC, ?MAX_DELAY_MSEC),
    random:uniform(Range).

is_replicator_db(DbName) ->
    ?REPLICATOR_DB =:= couch_db:dbname_suffix(DbName).

get_json_value(Key, Props) ->
    get_json_value(Key, Props, undefined).

get_json_value(Key, Props, Default) when is_atom(Key) ->
    Ref = make_ref(),
    case couch_util:get_value(Key, Props, Ref) of
        Ref ->
            couch_util:get_value(?l2b(atom_to_list(Key)), Props, Default);
        Else ->
            Else
    end;
get_json_value(Key, Props, Default) when is_binary(Key) ->
    Ref = make_ref(),
    case couch_util:get_value(Key, Props, Ref) of
        Ref ->
            couch_util:get_value(list_to_atom(?b2l(Key)), Props, Default);
        Else ->
            Else
    end.


-ifdef(TEST).

-include_lib("couch/include/couch_eunit.hrl").

replicator_shards_test_() ->
{
      foreach,
      fun() -> test_util:start_couch([mem3, fabric]) end,
      fun(Ctx) -> test_util:stop_couch(Ctx) end,
      [
          t_pass_replicator_shard(),
          t_fail_non_replicator_shard()
     ]
}.


t_pass_replicator_shard() ->
    ?_test(begin
        DbName0 = ?tempdb(),
        DbName = <<DbName0/binary, "/_replicator">>,
        ok = fabric:create_db(DbName, [?CTX]),
        ?assertEqual(8, length(replicator_shards(DbName))),
        fabric:delete_db(DbName, [?CTX])
    end).


t_fail_non_replicator_shard() ->
    ?_test(begin
        DbName = ?tempdb(),
        ok = fabric:create_db(DbName, [?CTX]),
        ?assertEqual([], replicator_shards(DbName)),
        fabric:delete_db(DbName, [?CTX])
    end).


-endif.
