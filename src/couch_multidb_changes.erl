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

-module(couch_multidb_changes).
-behaviour(gen_server).

-export([start_link/4]).

-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-export([changes_reader/3, changes_reader_cb/3]).
-export([handle_db_event/3]).

-include_lib("couch/include/couch_db.hrl").

-define(CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}}).


-record(state, {
    tid :: ets:tid(),
    mod :: atom(),
    ctx :: term(),
    suffix :: binary(),
    event_listener :: pid(),
    scanner :: nil | pid(),
    pids :: [{binary(),pid()}],
    skip_ddocs :: boolean()
}).



% Behavior API

% For each db shard with a matching suffix, report created,
% deleted, found (discovered) and change events.

-callback db_created(DbName :: binary(), Context :: term()) ->
    Context :: term().

-callback db_deleted(DbName :: binary(), Context :: term()) ->
    Context :: term().

-callback db_found(DbName :: binary(), Context :: term()) ->
    Context :: term().

-callback db_change(DbName :: binary(), Change :: term(), Context :: term()) ->
    Context :: term().



% External API


% Opts list can contain:
%  - `skip_ddocs` : Skip design docs

-spec start_link(binary(), module(), term(), list()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(DbSuffix, Module, Context, Opts) when
    is_binary(DbSuffix), is_atom(Module), is_list(Opts) ->
    gen_server:start_link(?MODULE, [DbSuffix, Module, Context, Opts], []).


% gen_server callbacks

init([DbSuffix, Module, Context, Opts]) ->
    process_flag(trap_exit, true),
    Server = self(),
    {ok, #state{
        tid = ets:new(?MODULE, [set, protected]),
        mod = Module,
        ctx = Context,
        suffix = DbSuffix,
        event_listener = start_event_listener(DbSuffix),
        scanner = spawn_link(fun() -> scan_all_dbs(Server, DbSuffix) end),
        pids = [],
        skip_ddocs = proplists:is_defined(skip_ddocs, Opts)
    }}.


handle_call({change, DbName, Change}, _From,
    #state{skip_ddocs=SkipDDocs, mod=Mod, ctx=Ctx} = State) ->
    case {SkipDDocs, is_design_doc(Change)} of
        {true, true} ->
            {reply, ok, State};
        {false, _} ->
            {reply, ok, State#state{ctx=Mod:db_change(DbName, Change, Ctx)}}
    end;

handle_call({created, DbName}, _From, #state{mod=Mod, ctx=Ctx} = State) ->
    {reply, ok, State#state{ctx=Mod:db_created(DbName, Ctx)}};

handle_call({deleted, DbName}, _From, #state{mod=Mod, ctx=Ctx} = State) ->
    {reply, ok, State#state{ctx=Mod:db_deleted(DbName, Ctx)}};

handle_call({checkpoint, DbName, EndSeq}, _From, #state{tid=Ets} = State) ->
    case ets:lookup(Ets, DbName) of
        [] ->
            true = ets:insert(Ets, {DbName, EndSeq, false});
        [{DbName, _OldSeq, Rescan}] ->
            true = ets:insert(Ets, {DbName, EndSeq, Rescan})
    end,
    {reply, ok, State}.


handle_cast({resume_scan, DbName}, #state{pids=Pids, tid=Ets} = State) ->
    {noreply, case {lists:keyfind(DbName, 1, Pids), ets:lookup(Ets, DbName)} of
        {{DbName, _}, []} ->
            % Found existing change feed, but not entry in ETS
            % Flag a need to rescan from begining
            true = ets:insert(Ets, {DbName, 0, true}),
            State;
        {{DbName, _}, [{DbName, EndSeq, _}]} ->
            % Found existing change feed and entry in ETS
            % Flag a need to rescan from last ETS checkpoint
            true = ets:insert(Ets, {DbName, EndSeq, true}),
            State;
        {false, []} ->
            % No existing change feed running. No entry in ETS.
            % This is first time seeing this db shard.
            % Notify user with a found callback. Insert checkpoint
            % entry in ETS to start from 0. And start a change feed.
            true = ets:insert(Ets, {DbName, 0, false}),
            Mod = State#state.mod,
            Ctx = Mod:db_found(DbName, State#state.ctx),
            Pid = start_changes_reader(DbName, 0),
            State#state{ctx=Ctx, pids=[{DbName, Pid} | Pids]};
        {false, [{DbName, EndSeq, _}]} ->
            % No existing change feed running. Found existing checkpoint.
            % Start a new change reader from last checkpoint.
            true = ets:insert(Ets, {DbName, EndSeq, false}),
            Pid = start_changes_reader(DbName, EndSeq),
            State#state{pids=[{DbName, Pid} | Pids]}
     end}.


handle_info({'EXIT', From, normal}, #state{scanner = From} = State) ->
    couch_log:info("multidb_changes ~p scanner pid exited ~p",[State#state.suffix, From]),
    {noreply, State#state{scanner=nil}};

handle_info({'EXIT', From, Reason}, #state{scanner = From} = State) ->
    {stop, {scanner_died, Reason}, State};

handle_info({'EXIT', From, Reason}, #state{event_listener = From} = State) ->
    {stop, {db_update_notifier_died, Reason}, State};

handle_info({'EXIT', From, Reason}, #state{pids = Pids} = State) ->
    couch_log:info("~p change feed exited ~p",[State#state.suffix, From]),
    case lists:keytake(From, 2, Pids) of
        {value, {DbName, From}, NewPids} ->
            if Reason == normal -> ok; true ->
                Fmt = "~s : Known change feed ~w died :: ~w",
                couch_log:error(Fmt, [?MODULE, From, Reason])
            end,
            NewState = State#state{pids = NewPids},
            case ets:lookup(State#state.tid, DbName) of
                [{DbName, _EndSeq, true}] ->
                    handle_cast({resume_scan, DbName}, NewState);
                _ ->
                    {noreply, NewState}
            end;
        false when Reason == normal ->
            {noreply, State};
        false ->
            Fmt = "~s(~p) : Unknown pid ~w died :: ~w",
            couch_log:error(Fmt, [?MODULE, State#state.suffix, From, Reason]),
            {stop, {unexpected_exit, From, Reason}, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Private functions

start_changes_reader(DbName, Since) ->
    spawn_link(?MODULE, changes_reader, [self(), DbName, Since]).


changes_reader(Server, DbName, Since) ->
    {ok, Db} = couch_db:open_int(DbName, [?CTX, sys_db]),
    ChFun = couch_changes:handle_db_changes(
        #changes_args{
	    include_docs = true,
	    since = Since,
	    feed = "normal",
	    timeout = infinity
	}, {json_req, null}, Db),
    ChFun({fun ?MODULE:changes_reader_cb/3, {Server, DbName}}).


changes_reader_cb({change, Change, _}, _, {Server, DbName}) ->
    ok = gen_server:call(Server, {change, DbName, Change}, infinity),
    {Server, DbName};

changes_reader_cb({stop, EndSeq, _Pending}, _, {Server, DbName}) ->
    ok = gen_server:call(Server, {checkpoint, DbName, EndSeq}, infinity),
    {Server, DbName};

changes_reader_cb(_, _, Acc) ->
    Acc.


start_event_listener(DbSuffix) ->
    {ok, Pid} = couch_event:link_listener(
        ?MODULE, handle_db_event, {self(), DbSuffix}, [all_dbs]),
    Pid.

handle_db_event(DbName, created, {Server, DbSuffix}) ->
    case suffix_match(DbName, DbSuffix) of
	true ->
	    ok = gen_server:call(Server, {created, DbName});
	_ ->
	    ok
    end,
    {ok, {Server, DbSuffix}};

handle_db_event(DbName, deleted, {Server, DbSuffix}) ->
    case suffix_match(DbName, DbSuffix) of
        true ->
            ok = gen_server:call(Server, {deleted, DbName});
        _ ->
            ok
    end,
    {ok, {Server, DbSuffix}};

handle_db_event(DbName, updated, {Server, DbSuffix}) ->
    case suffix_match(DbName, DbSuffix) of
        true ->
	    ok = gen_server:cast(Server, {resume_scan, DbName});
        _ ->
            ok
    end,
    {ok, {Server, DbSuffix}};

handle_db_event(_DbName, _Event, {Server, DbSuffix}) ->
    {ok, {Server, DbSuffix}}.


scan_all_dbs(Server, DbSuffix) when is_pid(Server) ->
    Root = config:get("couchdb", "database_dir", "."),
    NormRoot = couch_util:normpath(Root),
    Pat = io_lib:format("~s(\\.[0-9]{10,})?.couch$", [DbSuffix]),
    filelib:fold_files(Root, lists:flatten(Pat), true,
        fun(Filename, _) ->
	    % shamelessly stolen from couch_server.erl
            NormFilename = couch_util:normpath(Filename),
            case NormFilename -- NormRoot of
                [$/ | RelativeFilename] -> ok;
                RelativeFilename -> ok
            end,
            DbName = ?l2b(filename:rootname(RelativeFilename, ".couch")),
	    gen_server:cast(Server, {resume_scan, DbName}),
	    ok
	end, ok).


suffix_match(DbName, DbSuffix) ->
    case lists:last(binary:split(mem3:dbname(DbName), <<"/">>, [global])) of
        DbSuffix ->
            true;
        _ ->
            false
    end.

is_design_doc({Change}) ->
    case lists:keyfind(<<"id">>, 1, Change) of
        false ->
            false;
        {Id, _} ->
            is_design_doc_id(Id)
    end.


is_design_doc_id(<<?DESIGN_DOC_PREFIX, _/binary>>) ->
    true;

is_design_doc_id(_) ->
    false.
