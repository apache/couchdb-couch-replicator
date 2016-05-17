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



start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



%%%%%% Multidb changes callbacks

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


-spec replication_started(#rep{}) -> ok.
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
            [DocId, pp_rep_id(RepId)]),
        ok
    end.

-spec replication_completed(#rep{}, list()) -> ok.
replication_completed(#rep{id = RepId}, Stats) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{db_name = DbName, doc_id = DocId} ->
        couch_replicator_docs:update_doc_completed(DbName, DocId, Stats),
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` finished (triggered by document `~s`)",
            [pp_rep_id(RepId), DocId]),
        ok
    end.


-spec replication_usurped(#rep{}, node()) -> ok.
replication_usurped(#rep{id = RepId}, By) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{doc_id = DocId} ->
        ok = gen_server:call(?MODULE, {rep_complete, RepId}, infinity),
        couch_log:notice("Replication `~s` usurped by ~s (triggered by document `~s`)",
            [pp_rep_id(RepId), By, DocId]),
        ok
    end.

-spec replication_error(#rep{}, any()) -> ok.
replication_error(#rep{id = RepId}, Error) ->
    case rep_state(RepId) of
    nil ->
        ok;
    #rep{db_name = DbName, doc_id = DocId} ->
        % NV: TODO: later, perhaps don't update doc on each error
        couch_replicator_docs:update_doc_error(DbName, DocId, RepId, Error)
    end.

% NV: TODO: Here need to use the new cluster ownership bit.
-spec continue(#rep{}) -> {true, no_owner | unstable | node()} |
    {false, node()}.
continue(#rep{doc_id = null}) ->
    {true, no_owner};
continue(#rep{id = RepId}) ->
    case rep_state(RepId) of
    nil ->
        {false, nonode};
    #rep{db_name = DbName, doc_id = DocId} ->
	case couch_replicator_clustering:owner(DbName, DocId) of
        {ok, no_owner} ->
	    {true, no_owner};
	{ok, Owner} ->
	    {node() == Owner, Owner};
	{error, unstable} ->
	    {true, unstable}
        end
    end.


init(_) ->
    ?DOC_TO_REP = ets:new(?DOC_TO_REP, [named_table, set, public]),
    ?REP_TO_STATE = ets:new(?REP_TO_STATE, [named_table, set, public]),
    couch_replicator_docs:ensure_rep_db_exists(),
    {ok, nil}.


handle_call({rep_db_update, DbName, {ChangeProps} = Change}, _From, State) ->
    try
        process_update(DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_doc_process_error(DbName, DocId, Error)
    end,
    {reply, ok, State};


handle_call({rep_complete, RepId}, _From, State) ->
    true = ets:delete(?REP_TO_STATE, RepId),
    {reply, ok, State}.


handle_cast(Msg, State) ->
    couch_log:error("Replication manager received unexpected cast ~p", [Msg]),
    {stop, {error, {unexpected_cast, Msg}}, State}.


handle_info(Msg, State) ->
    couch_log:error("Replication manager received unexpected message ~p", [Msg]),
    {stop, {unexpected_msg, Msg}, State}.



terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec process_update(binary(), tuple()) -> ok.
process_update(DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    OwnerRes = couch_replicator_clustering:owner(DbName, DocId),
    case {OwnerRes, get_json_value(deleted, Change, false)} of
    {_, true} ->
        rep_doc_deleted(DbName, DocId);
    {{ok, Owner}, false} when Owner /= node() ->
        couch_log:notice("Not starting '~s' as owner is ~s.", [DocId, Owner]);
    {{error, unstable}, false} ->
	couch_log:notice("Not starting '~s' as cluster is unstable", [DocId]);
    {{ok,_Owner}, false} ->
        couch_log:notice("Maybe starting '~s' as I'm the owner", [DocId]),
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"triggered">> ->
            maybe_start_replication(DbName, DocId, JsonRepDoc);
        <<"completed">> ->
            replication_complete(DbName, DocId);
        <<"error">> ->
            case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
            [] ->
                maybe_start_replication(DbName, DocId, JsonRepDoc);
            _ ->
                ok
            end
        end
    end,
    ok.

-spec maybe_start_replication(binary(), binary(), tuple()) -> ok.
maybe_start_replication(DbName, DocId, RepDoc) ->
    Rep0 = couch_replicator_docs:parse_rep_doc(RepDoc),
    #rep{id = {BaseId, _} = RepId} = Rep0,
    Rep = Rep0#rep{db_name = DbName},
    case rep_state(RepId) of
    nil ->
        true = ets:insert(?REP_TO_STATE, {RepId, Rep}),
        true = ets:insert(?DOC_TO_REP, {{DbName, DocId}, RepId}),
        couch_log:notice("Attempting to start replication `~s` (document `~s`).",
            [pp_rep_id(RepId), DocId]),
        ok = start_replication(Rep);
    #rep{doc_id = DocId} ->
        ok;
    #rep{db_name = DbName, doc_id = OtherDocId} ->
        couch_log:notice("The replication specified by the document `~s` already started"
            " triggered by the document `~s`", [DocId, OtherDocId]),
        maybe_tag_rep_doc(DbName, DocId, RepDoc, ?l2b(BaseId))
    end,
    ok.

-spec maybe_tag_rep_doc(binary(), binary(), tuple(), binary()) -> ok.
maybe_tag_rep_doc(DbName, DocId, {RepProps}, RepId) ->
    case get_json_value(<<"_replication_id">>, RepProps) of
    RepId ->
        ok;
    _ ->
        couch_replicator_docs:update_doc_replication_id(DbName, DocId, RepId)
    end.

-spec start_replication(#rep{}) -> ok.
start_replication(Rep) ->
    case couch_replicator_scheduler:add_job(Rep) of
    ok ->
        ok;
    {error, already_added} ->
        couch_log:error("replicator scheduler add_job ~p was already added", [Rep])
    end.

-spec replication_complete(binary(), binary()) -> ok.
replication_complete(DbName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
    [{{DbName, DocId}, _RepId}] ->
        true = ets:delete(?DOC_TO_REP, {DbName, DocId}),
        ok;
    _ ->
        ok
    end.

-spec rep_doc_deleted(binary(), binary()) -> ok.
rep_doc_deleted(DbName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {DbName, DocId}) of
    [{{DbName, DocId}, RepId}] ->
        couch_replicator_scheduler:remove_job(RepId),
        true = ets:delete(?REP_TO_STATE, RepId),
        true = ets:delete(?DOC_TO_REP, {DbName, DocId}),
        couch_log:notice("Stopped replication `~s` because replication document `~s`"
            " was deleted", [pp_rep_id(RepId), DocId]),
        ok;
    [] ->
        ok
    end.


-spec clean_up_replications(binary()) -> ok.
clean_up_replications(DbName) ->
    ets:foldl(
        fun({{Name, DocId}, RepId}, _) when Name =:= DbName ->
            couch_replicator_scheduler:remove_job(RepId),
            ets:delete(?DOC_TO_REP,{Name, DocId}),
            ets:delete(?REP_TO_STATE, RepId);
           ({_,_}, _) ->
            ok
        end,
        ok, ?DOC_TO_REP),
    ok.


% pretty-print replication id
-spec pp_rep_id(#rep{}) -> string().
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
    couch_replicator_docs:after_doc_read(Doc, Db).
