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
-vsn(2).
-behaviour(couch_multidb_changes).

% public API
-export([replication_started/1, replication_completed/2, replication_error/2]).
-export([continue/1, replication_usurped/2]).

% NV: TODO: These functions were moved to couch_replicator_docs
% but it is still called from fabric_doc_update. Keep it here for now
% later, update fabric to call couch_replicator_docs instead
-export([before_doc_update/2, after_doc_read/2]).

% gen_server callbacks
-export([start_link/0, init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

% multidb changes callback
-export([db_created/2, db_deleted/2, db_found/2, db_change/3]).

%% exported but private
-export([start_replication/1]).

% imports
-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("mem3/include/mem3.hrl").
-include("couch_replicator.hrl").


-define(DOC_TO_REP, couch_rep_doc_id_to_rep_id).
-define(REP_TO_STATE, couch_rep_id_to_rep_state).


-record(state, {
    mdb_listener = nil,
    rep_start_pids = [],
    live = []
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


replication_started(#rep{id = RepId}) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{db_name = DbName, doc_id = DocId} ->
        couch_replicator_docs:update_doc_triggered(DbName, DocId, RepId),
        %NV: TODO: This used to be
        % ok = gen_server:call(?MODULE, {rep_started, RepId}, infinity),
        % now just write triggered for compatibility, in the future do something
        % in the scheduler to handle repeated failed starts
        couch_log:notice("Document `~s` triggered replication `~s`",
            [DocId, pp_rep_id(RepId)])
    end.

replication_completed(#rep{id = RepId}, Stats) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{db_name = DbName, doc_id = DocId} ->
        couch_replicator_docs:update_doc_completed(DbName, DocId, Stats),
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` finished (triggered by document `~s`)",
            [pp_rep_id(RepId), DocId])
    end.


replication_usurped(#rep{id = RepId}, By) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{doc_id = DocId} ->
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` usurped by ~s (triggered by document `~s`)",
            [pp_rep_id(RepId), By, DocId])
    end.


replication_error(#rep{id = RepId}, Error) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{db_name = DbName, doc_id = DocId} ->
        % NV: TODO: We might want to do something else instead of update doc on every error
         couch_replicator_docs:update_doc_error(DbName, DocId, RepId, Error),
        ok = gen_server:call(?MODULE, {rep_error, RepId, Error}, infinity)
    end.

% NV: TODO: Here need to use the new cluster ownership bit.
continue(#rep{doc_id = null}) ->
    {true, no_owner};
continue(#rep{id = RepId}) ->
    Owner = gen_server:call(?MODULE, {owner, RepId}, infinity),
    {node() == Owner, Owner}.


init(_) ->
    process_flag(trap_exit, true),
    net_kernel:monitor_nodes(true),
    Live = [node() | nodes()],
    ?DOC_TO_REP = ets:new(?DOC_TO_REP, [named_table, set, public]),
    ?REP_TO_STATE = ets:new(?REP_TO_STATE, [named_table, set, public]),
    couch_replicator_docs:ensure_rep_db_exists(),
    {ok, #state{mdb_listener = start_mdb_listener(), live = Live}}.

% NV: TODO: Use new cluster membership module here. Possible return value
% could be 'unstable' in which case should keep old owner + possibly logging.
handle_call({owner, RepId}, _From, State) ->
    case rep_state(RepId) of
    nil ->
        {reply, nonode, State};
    #rep{db_name = DbName, doc_id = DocId} ->
        {reply, owner(DbName, DocId, State#state.live), State}
    end;

handle_call({rep_db_update, DbName, {ChangeProps} = Change}, _From, State) ->
    NewState = try
        process_update(State, DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_doc_process_error(DbName, DocId, Error),
        State
    end,
    {reply, ok, NewState};


handle_call({rep_complete, RepId}, _From, State) ->
    true = ets:delete(?REP_TO_STATE, RepId),
    {reply, ok, State};

handle_call({rep_error, RepId, Error}, _From, State) ->
    {reply, ok, replication_error(State, RepId, Error)};

handle_call(Msg, From, State) ->
    couch_log:error("Replication manager received unexpected call ~p from ~p",
        [Msg, From]),
    {stop, {error, {unexpected_call, Msg}}, State}.


handle_cast(Msg, State) ->
    couch_log:error("Replication manager received unexpected cast ~p", [Msg]),
    {stop, {error, {unexpected_cast, Msg}}, State}.

% NV: TODO: Remove when switching to new cluster membership module
handle_info({nodeup, Node}, State) ->
    couch_log:notice("Rescanning replicator dbs as ~s came up.", [Node]),
    Live = lists:usort([Node | State#state.live]),
    {noreply, rescan(State#state{live=Live})};

% NV: TODO: Remove when switching to new cluster membership module
handle_info({nodedown, Node}, State) ->
    couch_log:notice("Rescanning replicator dbs ~s went down.", [Node]),
    Live = State#state.live -- [Node],
    {noreply, rescan(State#state{live=Live})};

handle_info({'EXIT', From, Reason}, #state{mdb_listener = From} = State) ->
    couch_log:error("Database update notifier died. Reason: ~p", [Reason]),
    {stop, {db_update_notifier_died, Reason}, State};

handle_info({'EXIT', From, Reason}, #state{rep_start_pids = Pids} = State) ->
    case lists:keytake(From, 2, Pids) of
        {value, {rep_start, From}, NewPids} ->
            if Reason == normal -> ok; true ->
                Fmt = "~s : Known replication pid ~w died :: ~w",
                couch_log:error(Fmt, [?MODULE, From, Reason])
            end,
            {noreply, State#state{rep_start_pids = NewPids}};
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

handle_info(Msg, State) ->
    couch_log:error("Replication manager received unexpected message ~p", [Msg]),
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, State) ->
    #state{
        rep_start_pids = StartPids,
        mdb_listener = Listener
    } = State,
    stop_all_replications(),
    lists:foreach(
        fun({_Tag, Pid}) ->
            catch unlink(Pid),
            catch exit(Pid, stop)
        end,
        StartPids),
    true = ets:delete(?REP_TO_STATE),
    true = ets:delete(?DOC_TO_REP),
    catch unlink(Listener),
    catch exit(Listener).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


start_mdb_listener() ->
    {ok, Pid} = couch_multidb_changes:start_link(
        <<"_replicator">>, ?MODULE, self(), [skip_ddocs]),
    Pid.


%%%%%% multidb changes callbacks

db_created(DbName, Server) ->
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_deleted(DbName, Server) ->
    clean_up_replications(DbName),
    Server.

db_found(DbName, Server) ->
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.

db_change(DbName, Change, Server) ->
    ok = gen_server:call(Server, {rep_db_update, DbName, Change}, infinity),
    Server.


% NV: TODO: This will change when using the new clustering module.
% Rescan should happend when cluster membership changes, clustering module
% handles with an appropriate back-off. mdb_listener should probably live
% in a supervisor and that supervisor should be asked to restart it.
rescan(#state{mdb_listener = nil} = State) ->
    State#state{mdb_listener = start_mdb_listener()};
rescan(#state{mdb_listener = MPid} = State) ->
    unlink(MPid),
    exit(MPid, exit),
    rescan(State#state{mdb_listener = nil}).


process_update(State, DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    case {owner(DbName, DocId, State#state.live), get_json_value(deleted, Change, false)} of
    {_, true} ->
        rep_doc_deleted(DbName, DocId),
        State;
    {Owner, false} when Owner /= node() ->
        couch_log:notice("Not starting '~s' as owner is ~s.", [DocId, Owner]),
        State;
    {_Owner, false} ->
        couch_log:notice("Maybe starting '~s' as I'm the owner", [DocId]),
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



maybe_start_replication(State, DbName, DocId, RepDoc) ->
    Rep0 = couch_replicator_docs:parse_rep_doc(RepDoc),
    #rep{id = {BaseId, _} = RepId} = Rep0,
    Rep = Rep0#rep{db_name = DbName},
    case rep_state(RepId) of
    nil ->
        true = ets:insert(?REP_TO_STATE, {RepId, Rep}),
        true = ets:insert(?DOC_TO_REP, {{DbName, DocId}, RepId}),
        couch_log:notice("Attempting to start replication `~s` (document `~s`).",
            [pp_rep_id(RepId), DocId]),
        Pid = spawn_link(?MODULE, start_replication, [Rep]),
        State#state{
            rep_start_pids = [{rep_start, Pid} | State#state.rep_start_pids]
        };
    #rep{doc_id = DocId} ->
        State;
    #rep{db_name = DbName, doc_id = OtherDocId} ->
        couch_log:notice("The replication specified by the document `~s` already started"
            " triggered by the document `~s`", [DocId, OtherDocId]),
        maybe_tag_rep_doc(DbName, DocId, RepDoc, ?l2b(BaseId)),
        State
    end.


maybe_tag_rep_doc(DbName, DocId, {RepProps}, RepId) ->
    case get_json_value(<<"_replication_id">>, RepProps) of
    RepId ->
        ok;
    _ ->
        couch_replicator_docs:update_doc_replication_id(DbName, DocId, RepId)
    end.

start_replication(Rep) ->
    % NV: TODO: Removed splay and back-off sleep on error. Instead to replace that
    % temporarily add some random sleep here. To avoid repeated failed restarts in
    % a loop if source doc is broken
    timer:sleep(random:uniform(1000)),
    case (catch couch_replicator:async_replicate(Rep)) of
    {ok, _} ->
        ok;
    Error ->
        replication_error(Rep, Error)
    end.

replication_complete(DbName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
    [{{DbName, DocId}, _RepId}] ->
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
        couch_log:notice("Stopped replication `~s` because replication document `~s`"
            " was deleted", [pp_rep_id(RepId), DocId]);
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

maybe_retry_replication(#rep{id = RepId, doc_id = DocId} = Rep, Error, State) ->
    ErrorBinary = couch_replicator_utils:rep_error_to_binary(Error),
    couch_log:error("Error in replication `~s` (triggered by `~s`): ~s",
        [pp_rep_id(RepId), DocId, ErrorBinary]),
    % NV: TODO: Removed repeated failed restarts handling. Will do that some
    % other way in scheduler code
    Pid = spawn_link(?MODULE, start_replication, [Rep]),
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
    true = ets:delete_all_objects(?DOC_TO_REP).


clean_up_replications(DbName) ->
    ets:foldl(
        fun({{Name, DocId}, RepId}, _) when Name =:= DbName ->
            couch_replicator:cancel_replication(RepId),
            ets:delete(?DOC_TO_REP,{Name, DocId}),
            ets:delete(?REP_TO_STATE, RepId);
           ({_,_}, _) ->
            ok
        end,
        ok, ?DOC_TO_REP).


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


% NV: TODO: This function was moved to couch_replicator_docs
% but it is still called from fabric_doc_update. Keep it here for now
% later, update fabric to call couch_replicator_docs instead
before_doc_update(Doc, Db) ->
    couch_replicator_docs:before_doc_update(Doc, Db).

after_doc_read(Doc, Db) ->
    couch_replicator_doc:after_doc_read(Doc, Db).
