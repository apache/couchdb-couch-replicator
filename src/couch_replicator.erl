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

-module(couch_replicator).

-export([replicate/2, ensure_rep_db_exists/0]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).


-spec replicate({[_]}, #user_ctx{}) ->
    {ok, {continuous, binary()}} |
    {ok, {[_]}} |
    {ok, {cancelled, binary()}} |
    {error, any()}.
replicate(PostBody, Ctx) ->
    {ok, #rep{id = RepId, options = Options, user_ctx = UserCtx} = Rep} =
        couch_replicator_utils:parse_rep_doc(PostBody, Ctx),
    case get_value(cancel, Options, false) of
    true ->
        case get_value(id, Options, nil) of
        nil ->
            cancel_replication(RepId);
        RepId2 ->
            cancel_replication(RepId2, UserCtx)
        end;
    false ->
        {ok, Listener} = rep_result_listener(RepId),
        Result = do_replication_loop(Rep),
        couch_replicator_notifier:stop(Listener),
        Result
    end.


% This is called from supervisor. Must respect supervisor protocol so
% it returns `ignore`.
-spec ensure_rep_db_exists() -> ignore.
ensure_rep_db_exists() ->
    {ok, _Db} = couch_replicator_docs:ensure_rep_db_exists(),
    couch_log:notice("~p : created local _replicator database", [?MODULE]),
    ignore.


-spec do_replication_loop(#rep{}) ->
    {ok, {continuous, binary()}} | {ok, tuple()} | {error, any()}.
do_replication_loop(#rep{id = {BaseId, Ext} = Id, options = Options} = Rep) ->
    case couch_replicator_scheduler:add_job(Rep) of
    ok ->
        ok;
    {error, already_added} ->
        couch_log:notice("Replication '~s' already running", [BaseId ++ Ext]),
        ok
    end,
    case get_value(continuous, Options, false) of
    true ->
        {ok, {continuous, ?l2b(BaseId ++ Ext)}};
    false ->
        wait_for_result(Id)
    end.


-spec rep_result_listener(rep_id()) -> {ok, pid()}.
rep_result_listener(RepId) ->
    ReplyTo = self(),
    {ok, _Listener} = couch_replicator_notifier:start_link(
        fun({_, RepId2, _} = Ev) when RepId2 =:= RepId ->
                ReplyTo ! Ev;
            (_) ->
                ok
        end).


-spec wait_for_result(rep_id()) ->
    {ok, any()} | {error, any()}.
wait_for_result(RepId) ->
    receive
    {finished, RepId, RepResult} ->
        {ok, RepResult};
    {error, RepId, Reason} ->
        {error, Reason}
    end.


-spec cancel_replication(rep_id()) ->
    {ok, {cancelled, binary()}} | {error, not_found}.
cancel_replication({BasedId, Extension} = RepId) ->
    FullRepId = BasedId ++ Extension,
    couch_log:notice("Canceling replication '~s' ...", [FullRepId]),
    case couch_replicator_scheduler:rep_state(RepId) of
    #rep{} ->
        ok = couch_replicator_scheduler:remove_job(RepId),
        couch_log:notice("Replication '~s' cancelled", [FullRepId]),
        {ok, {cancelled, ?l2b(FullRepId)}};
    nil ->
        couch_log:notice("Replication '~s' not found", [FullRepId]),
        {error, not_found}
    end.


-spec cancel_replication(rep_id(), #user_ctx{}) ->
    {ok, {cancelled, binary()}} | {error, not_found}.
cancel_replication(RepId, #user_ctx{name = Name, roles = Roles}) ->
    case lists:member(<<"_admin">>, Roles) of
    true ->
        cancel_replication(RepId);
    false ->
        case couch_replicator_scheduler:rep_state(RepId) of
        #rep{user_ctx = #user_ctx{name = Name}} ->
            cancel_replication(RepId);
        #rep{user_ctx = #user_ctx{name = _Other}} ->
            throw({unauthorized,
                <<"Can't cancel a replication triggered by another user">>});
        nil ->
            {error, not_found}
        end
     end.



