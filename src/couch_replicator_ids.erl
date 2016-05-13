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

-module(couch_replicator_ids).

-export([replication_id/1, replication_id/2]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator_api_wrap.hrl").
-include("couch_replicator.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).


replication_id(#rep{options = Options} = Rep) ->
    BaseId = replication_id(Rep, ?REP_ID_VERSION),
    {BaseId, maybe_append_options([continuous, create_target], Options)}.

% Versioned clauses for generating replication IDs.
% If a change is made to how replications are identified,
% please add a new clause and increase ?REP_ID_VERSION.

replication_id(#rep{user_ctx = UserCtx} = Rep, 3) ->
    UUID = couch_server:get_uuid(),
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([UUID, Src, Tgt], Rep);

replication_id(#rep{user_ctx = UserCtx} = Rep, 2) ->
    {ok, HostName} = inet:gethostname(),
    Port = case (catch mochiweb_socket_server:get(couch_httpd, port)) of
    P when is_number(P) ->
        P;
    _ ->
        % On restart we might be called before the couch_httpd process is
        % started.
        % TODO: we might be under an SSL socket server only, or both under
        % SSL and a non-SSL socket.
        % ... mochiweb_socket_server:get(https, port)
        list_to_integer(config:get("httpd", "port", "5984"))
    end,
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([HostName, Port, Src, Tgt], Rep);

replication_id(#rep{user_ctx = UserCtx} = Rep, 1) ->
    {ok, HostName} = inet:gethostname(),
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([HostName, Src, Tgt], Rep).


% Private functions

maybe_append_filters(Base,
        #rep{source = Source, user_ctx = UserCtx, options = Options}) ->
    Filter = get_value(filter, Options),
    DocIds = get_value(doc_ids, Options),
    Selector = get_value(selector, Options),
    Base2 = Base ++
        case {Filter, DocIds, Selector} of
        {undefined, undefined, undefined} ->
            [];
        {<<"_", _/binary>>, undefined, undefined} ->
            [Filter, get_value(query_params, Options, {[]})];
        {_, undefined, undefined} ->
            [filter_code(Filter, Source, UserCtx),
                get_value(query_params, Options, {[]})];
        {undefined, _, undefined} ->
            [DocIds];
        {undefined, undefined, _} ->
            [ejsort(mango_selector:normalize(Selector))];
        _ ->
            throw({error, <<"`selector`, `filter` and `doc_ids` fields are mutually exclusive">>})
        end,
    couch_util:to_hex(couch_crypto:hash(md5, term_to_binary(Base2))).


filter_code(Filter, Source, UserCtx) ->
    {DDocName, FilterName} =
    case re:run(Filter, "(.*?)/(.*)", [{capture, [1, 2], binary}]) of
    {match, [DDocName0, FilterName0]} ->
        {DDocName0, FilterName0};
    _ ->
        throw({error, <<"Invalid filter. Must match `ddocname/filtername`.">>})
    end,
    Db = case (catch couch_replicator_api_wrap:db_open(Source, [{user_ctx, UserCtx}])) of
    {ok, Db0} ->
        Db0;
    DbError ->
        DbErrorMsg = io_lib:format("Could not open source database `~s`: ~s",
           [couch_replicator_api_wrap:db_uri(Source), couch_util:to_binary(DbError)]),
        throw({error, iolist_to_binary(DbErrorMsg)})
    end,
    try
        Body = case (catch couch_replicator_api_wrap:open_doc(
            Db, <<"_design/", DDocName/binary>>, [ejson_body])) of
        {ok, #doc{body = Body0}} ->
            Body0;
        DocError ->
            DocErrorMsg = io_lib:format(
                "Couldn't open document `_design/~s` from source "
                "database `~s`: ~s", [DDocName, couch_replicator_api_wrap:db_uri(Source),
                    couch_util:to_binary(DocError)]),
            throw({error, iolist_to_binary(DocErrorMsg)})
        end,
        Code = couch_util:get_nested_json_value(
            Body, [<<"filters">>, FilterName]),
        re:replace(Code, [$^, "\s*(.*?)\s*", $$], "\\1", [{return, binary}])
    after
        couch_replicator_api_wrap:db_close(Db)
    end.


maybe_append_options(Options, RepOptions) ->
    lists:foldl(fun(Option, Acc) ->
        Acc ++
        case get_value(Option, RepOptions, false) of
        true ->
            "+" ++ atom_to_list(Option);
        false ->
            ""
        end
    end, [], Options).


get_rep_endpoint(_UserCtx, #httpdb{url=Url, headers=Headers, oauth=OAuth}) ->
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    case OAuth of
    nil ->
        {remote, Url, Headers -- DefaultHeaders};
    #oauth{} ->
        {remote, Url, Headers -- DefaultHeaders, OAuth}
    end;
get_rep_endpoint(UserCtx, <<DbName/binary>>) ->
    {local, DbName, UserCtx}.


% Sort an EJSON object's properties to attempt
% to generate a unique representation. This is used
% to reduce the chance of getting different
% replication checkpoints for the same Mango selector
ejsort({V})->
    ejsort_props(V, []);
ejsort(V) when is_list(V) ->
    ejsort_array(V, []);
ejsort(V) ->
    V.

ejsort_props([], Acc)->
    {lists:keysort(1, Acc)};
ejsort_props([{K, V}| R], Acc) ->
    ejsort_props(R, [{K, ejsort(V)} | Acc]).

ejsort_array([], Acc)->
    lists:reverse(Acc);
ejsort_array([V | R], Acc) ->
    ejsort_array(R, [ejsort(V) | Acc]).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

ejsort_basic_values_test() ->
    ?assertEqual(ejsort(0), 0),
    ?assertEqual(ejsort(<<"a">>), <<"a">>),
    ?assertEqual(ejsort(true), true),
    ?assertEqual(ejsort([]), []),
    ?assertEqual(ejsort({[]}), {[]}).

ejsort_compound_values_test() ->
    ?assertEqual(ejsort([2, 1, 3 ,<<"a">>]), [2, 1, 3, <<"a">>]),
    Ej1 = {[{<<"a">>, 0}, {<<"c">>, 0},  {<<"b">>, 0}]},
    Ej1s =  {[{<<"a">>, 0}, {<<"b">>, 0}, {<<"c">>, 0}]},
    ?assertEqual(ejsort(Ej1), Ej1s),
    Ej2 = {[{<<"x">>, Ej1}, {<<"z">>, Ej1}, {<<"y">>, [Ej1, Ej1]}]},
    ?assertEqual(ejsort(Ej2),
        {[{<<"x">>, Ej1s}, {<<"y">>, [Ej1s, Ej1s]}, {<<"z">>, Ej1s}]}).

-endif.
