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

-module(couch_replicator_docs).

-export([parse_rep_doc/1, parse_rep_doc/2]).
-export([before_doc_update/2, after_doc_read/2]).
-export([ensure_rep_db_exists/0, ensure_rep_ddoc_exists/1]).
-export([
    remove_state_fields/2,
    update_doc_completed/3,
    update_doc_replication_id/3,
    update_doc_process_error/3
]).

-define(REP_DB_NAME, <<"_replicator">>).
-define(REP_DESIGN_DOC, <<"_design/_replicator">>).

-include_lib("couch/include/couch_db.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").
-include("couch_replicator_api_wrap.hrl").
-include("couch_replicator.hrl").

-include("couch_replicator_js_functions.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1
]).

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).


-define(OWNER, <<"owner">>).
-define(CTX, {user_ctx, #user_ctx{roles=[<<"_admin">>, <<"_replicator">>]}}).

-define(replace(L, K, V), lists:keystore(K, 1, L, {K, V})).

remove_state_fields(DbName, DocId) ->
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, undefined},
        {<<"_replication_state_time">>, undefined},
        {<<"_replication_state_reason">>, undefined},
        {<<"_replication_stats">>, undefined}]).

-spec update_doc_completed(binary(), binary(), [_]) -> any().
update_doc_completed(DbName, DocId, Stats) ->
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"completed">>},
        {<<"_replication_state_reason">>, undefined},
        {<<"_replication_stats">>, {Stats}}]).


-spec update_doc_process_error(binary(), binary(), any()) -> any().
update_doc_process_error(DbName, DocId, Error) ->
    Reason = case Error of
        {bad_rep_doc, Reas} ->
            Reas;
        _ ->
            to_binary(Error)
    end,
    couch_log:error("Error processing replication doc `~s`: ~s", [DocId, Reason]),
    update_rep_doc(DbName, DocId, [
        {<<"_replication_state">>, <<"failed">>},
        {<<"_replication_state_reason">>, Reason}]).


-spec update_doc_replication_id(binary(), binary(), binary()) -> any().
update_doc_replication_id(DbName, DocId, RepId) ->
    update_rep_doc(DbName, DocId, [{<<"_replication_id">>, RepId}]).


-spec ensure_rep_db_exists() -> {ok, #db{}}.
ensure_rep_db_exists() ->
    Db = case couch_db:open_int(?REP_DB_NAME, [?CTX, sys_db, nologifmissing]) of
        {ok, Db0} ->
            Db0;
        _Error ->
            {ok, Db0} = couch_db:create(?REP_DB_NAME, [?CTX, sys_db]),
            Db0
    end,
    ok = ensure_rep_ddoc_exists(?REP_DB_NAME),
    {ok, Db}.


-spec ensure_rep_ddoc_exists(binary()) -> ok.
ensure_rep_ddoc_exists(RepDb) ->
    case mem3:belongs(RepDb, ?REP_DESIGN_DOC) of
	true ->
	    ensure_rep_ddoc_exists(RepDb, ?REP_DESIGN_DOC);
	false ->
	    ok
    end.

-spec ensure_rep_ddoc_exists(binary(), binary()) -> ok.
ensure_rep_ddoc_exists(RepDb, DDocId) ->
    case open_rep_doc(RepDb, DDocId) of
        {ok, _Doc} ->
            ok;
        _ ->
            DDoc = couch_doc:from_json_obj({[
                {<<"_id">>, DDocId},
                {<<"language">>, <<"javascript">>},
                {<<"validate_doc_update">>, ?REP_DB_DOC_VALIDATE_FUN}
            ]}),
            try
                {ok, _} = save_rep_doc(RepDb, DDoc),
                ok
            catch
                throw:conflict ->
                    % NFC what to do about this other than
                    % not kill the process.
                    ok
            end
    end.


-spec parse_rep_doc({[_]}) -> #rep{}.
parse_rep_doc(RepDoc) ->
    {ok, Rep} = try
        parse_rep_doc(RepDoc, rep_user_ctx(RepDoc))
    catch
    throw:{error, Reason} ->
        throw({bad_rep_doc, Reason});
    Tag:Err ->
        throw({bad_rep_doc, to_binary({Tag, Err})})
    end,
    Rep.


-spec parse_rep_doc({[_]}, #user_ctx{}) -> {ok, #rep{}}.
parse_rep_doc({Props}, UserCtx) ->
    ProxyParams = parse_proxy_params(get_value(<<"proxy">>, Props, <<>>)),
    Options = make_options(Props),
    case get_value(cancel, Options, false) andalso
        (get_value(id, Options, nil) =/= nil) of
    true ->
        {ok, #rep{options = Options, user_ctx = UserCtx}};
    false ->
        Source = parse_rep_db(get_value(<<"source">>, Props),
                              ProxyParams, Options),
        Target = parse_rep_db(get_value(<<"target">>, Props),
                              ProxyParams, Options),


        {RepType, View} = case get_value(<<"filter">>, Props) of
                <<"_view">> ->
                    {QP}  = get_value(query_params, Options, {[]}),
                    ViewParam = get_value(<<"view">>, QP),
                    View1 = case re:split(ViewParam, <<"/">>) of
                        [DName, ViewName] ->
                            {<< "_design/", DName/binary >>, ViewName};
                        _ ->
                            throw({bad_request, "Invalid `view` parameter."})
                    end,
                    {view, View1};
                _ ->
                    {db, nil}
            end,

        Rep = #rep{
            source = Source,
            target = Target,
            options = Options,
            user_ctx = UserCtx,
            type = RepType,
            view = View,
            doc_id = get_value(<<"_id">>, Props, null)
        },
        {ok, Rep#rep{id = couch_replicator_ids:replication_id(Rep)}}
    end.


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
    {ok, Db} = couch_db:open_int(DbName, [?CTX, sys_db]),
    try
        couch_db:open_doc(Db, DocId, [ejson_body])
    after
        couch_db:close(Db)
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
-spec timestamp() -> binary().
timestamp() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_local_time(os:timestamp()),
    UTime = erlang:universaltime(),
    LocalTime = calendar:universal_time_to_local_time(UTime),
    DiffSecs = calendar:datetime_to_gregorian_seconds(LocalTime) -
        calendar:datetime_to_gregorian_seconds(UTime),
    zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60),
    iolist_to_binary(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w~s",
            [Year, Month, Day, Hour, Min, Sec,
                zone(DiffSecs div 3600, (DiffSecs rem 3600) div 60)])).

-spec zone(integer(), integer()) -> iolist().
zone(Hr, Min) when Hr >= 0, Min >= 0 ->
    io_lib:format("+~2..0w:~2..0w", [Hr, Min]);
zone(Hr, Min) ->
    io_lib:format("-~2..0w:~2..0w", [abs(Hr), abs(Min)]).


-spec rep_user_ctx({[_]}) -> #user_ctx{}.
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

-spec parse_rep_db({[_]} | binary(), [_], [_]) -> #httpd{} | binary().
parse_rep_db({Props}, ProxyParams, Options) ->
    Url = maybe_add_trailing_slash(get_value(<<"url">>, Props)),
    {AuthProps} = get_value(<<"auth">>, Props, {[]}),
    {BinHeaders} = get_value(<<"headers">>, Props, {[]}),
    Headers = lists:ukeysort(1, [{?b2l(K), ?b2l(V)} || {K, V} <- BinHeaders]),
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    OAuth = case get_value(<<"oauth">>, AuthProps) of
    undefined ->
        nil;
    {OauthProps} ->
        #oauth{
            consumer_key = ?b2l(get_value(<<"consumer_key">>, OauthProps)),
            token = ?b2l(get_value(<<"token">>, OauthProps)),
            token_secret = ?b2l(get_value(<<"token_secret">>, OauthProps)),
            consumer_secret = ?b2l(get_value(<<"consumer_secret">>, OauthProps)),
            signature_method =
                case get_value(<<"signature_method">>, OauthProps) of
                undefined ->        hmac_sha1;
                <<"PLAINTEXT">> ->  plaintext;
                <<"HMAC-SHA1">> ->  hmac_sha1;
                <<"RSA-SHA1">> ->   rsa_sha1
                end
        }
    end,
    #httpdb{
        url = Url,
        oauth = OAuth,
        headers = lists:ukeymerge(1, Headers, DefaultHeaders),
        ibrowse_options = lists:keysort(1,
            [{socket_options, get_value(socket_options, Options)} |
                ProxyParams ++ ssl_params(Url)]),
        timeout = get_value(connection_timeout, Options),
        http_connections = get_value(http_connections, Options),
        retries = get_value(retries, Options)
    };
parse_rep_db(<<"http://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<"https://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<DbName/binary>>, _ProxyParams, _Options) ->
    DbName.

-spec maybe_add_trailing_slash(binary() | list()) -> list().
maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.

-spec make_options([_]) -> [_].
make_options(Props) ->
    Options0 = lists:ukeysort(1, convert_options(Props)),
    Options = check_options(Options0),
    DefWorkers = config:get("replicator", "worker_processes", "4"),
    DefBatchSize = config:get("replicator", "worker_batch_size", "500"),
    DefConns = config:get("replicator", "http_connections", "20"),
    DefTimeout = config:get("replicator", "connection_timeout", "30000"),
    DefRetries = config:get("replicator", "retries_per_request", "10"),
    UseCheckpoints = config:get("replicator", "use_checkpoints", "true"),
    DefCheckpointInterval = config:get("replicator", "checkpoint_interval", "30000"),
    {ok, DefSocketOptions} = couch_util:parse_term(
        config:get("replicator", "socket_options",
            "[{keepalive, true}, {nodelay, false}]")),
    lists:ukeymerge(1, Options, lists:keysort(1, [
        {connection_timeout, list_to_integer(DefTimeout)},
        {retries, list_to_integer(DefRetries)},
        {http_connections, list_to_integer(DefConns)},
        {socket_options, DefSocketOptions},
        {worker_batch_size, list_to_integer(DefBatchSize)},
        {worker_processes, list_to_integer(DefWorkers)},
        {use_checkpoints, list_to_existing_atom(UseCheckpoints)},
        {checkpoint_interval, list_to_integer(DefCheckpointInterval)}
    ])).


-spec convert_options([_]) -> [_].
convert_options([])->
    [];
convert_options([{<<"cancel">>, V} | R]) ->
    [{cancel, V} | convert_options(R)];
convert_options([{IdOpt, V} | R]) when IdOpt =:= <<"_local_id">>;
        IdOpt =:= <<"replication_id">>; IdOpt =:= <<"id">> ->
    Id = lists:splitwith(fun(X) -> X =/= $+ end, ?b2l(V)),
    [{id, Id} | convert_options(R)];
convert_options([{<<"create_target">>, V} | R]) ->
    [{create_target, V} | convert_options(R)];
convert_options([{<<"continuous">>, V} | R]) ->
    [{continuous, V} | convert_options(R)];
convert_options([{<<"filter">>, V} | R]) ->
    [{filter, V} | convert_options(R)];
convert_options([{<<"query_params">>, V} | R]) ->
    [{query_params, V} | convert_options(R)];
convert_options([{<<"doc_ids">>, null} | R]) ->
    convert_options(R);
convert_options([{<<"doc_ids">>, V} | _R]) when not is_list(V) ->
    throw({bad_request, <<"parameter `doc_ids` must be an array">>});
convert_options([{<<"doc_ids">>, V} | R]) ->
    % Ensure same behaviour as old replicator: accept a list of percent
    % encoded doc IDs.
    DocIds = [?l2b(couch_httpd:unquote(Id)) || Id <- V],
    [{doc_ids, DocIds} | convert_options(R)];
convert_options([{<<"selector">>, V} | _R]) when not is_tuple(V) ->
    throw({bad_request, <<"parameter `selector` must be a JSON object">>});
convert_options([{<<"selector">>, V} | R]) ->
    [{selector, V} | convert_options(R)];
convert_options([{<<"worker_processes">>, V} | R]) ->
    [{worker_processes, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"worker_batch_size">>, V} | R]) ->
    [{worker_batch_size, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"http_connections">>, V} | R]) ->
    [{http_connections, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"connection_timeout">>, V} | R]) ->
    [{connection_timeout, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"retries_per_request">>, V} | R]) ->
    [{retries, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"socket_options">>, V} | R]) ->
    {ok, SocketOptions} = couch_util:parse_term(V),
    [{socket_options, SocketOptions} | convert_options(R)];
convert_options([{<<"since_seq">>, V} | R]) ->
    [{since_seq, V} | convert_options(R)];
convert_options([{<<"use_checkpoints">>, V} | R]) ->
    [{use_checkpoints, V} | convert_options(R)];
convert_options([{<<"checkpoint_interval">>, V} | R]) ->
    [{checkpoint_interval, couch_util:to_integer(V)} | convert_options(R)];
convert_options([_ | R]) -> % skip unknown option
    convert_options(R).

-spec check_options([_]) -> [_].
check_options(Options) ->
    DocIds = lists:keyfind(doc_ids, 1, Options),
    Filter = lists:keyfind(filter, 1, Options),
    Selector = lists:keyfind(selector, 1, Options),
    case {DocIds, Filter, Selector} of
        {false, false, false} -> Options;
        {false, false, _} -> Options;
        {false, _, false} -> Options;
        {_, false, false} -> Options;
        _ ->
            throw({bad_request, "`doc_ids`, `filter`, `selector` are mutually exclusive options"})
    end.

-spec parse_proxy_params(binary() | [_]) -> [_].
parse_proxy_params(ProxyUrl) when is_binary(ProxyUrl) ->
    parse_proxy_params(?b2l(ProxyUrl));
parse_proxy_params([]) ->
    [];
parse_proxy_params(ProxyUrl) ->
    #url{
        host = Host,
        port = Port,
        username = User,
        password = Passwd,
        protocol = Protocol
    } = ibrowse_lib:parse_url(ProxyUrl),
    [{proxy_protocol, Protocol}, {proxy_host, Host}, {proxy_port, Port}] ++
        case is_list(User) andalso is_list(Passwd) of
        false ->
            [];
        true ->
            [{proxy_user, User}, {proxy_password, Passwd}]
        end.

-spec ssl_params([_]) -> [_].
ssl_params(Url) ->
    case ibrowse_lib:parse_url(Url) of
    #url{protocol = https} ->
        Depth = list_to_integer(
            config:get("replicator", "ssl_certificate_max_depth", "3")
        ),
        VerifyCerts = config:get("replicator", "verify_ssl_certificates"),
        CertFile = config:get("replicator", "cert_file", undefined),
        KeyFile = config:get("replicator", "key_file", undefined),
        Password = config:get("replicator", "password", undefined),
        SslOpts = [{depth, Depth} | ssl_verify_options(VerifyCerts =:= "true")],
        SslOpts1 = case CertFile /= undefined andalso KeyFile /= undefined of
            true ->
                case Password of
                    undefined ->
                        [{certfile, CertFile}, {keyfile, KeyFile}] ++ SslOpts;
                    _ ->
                        [{certfile, CertFile}, {keyfile, KeyFile},
                            {password, Password}] ++ SslOpts
                end;
            false -> SslOpts
        end,
        [{is_ssl, true}, {ssl_options, SslOpts1}];
    #url{protocol = http} ->
        []
    end.

-spec ssl_verify_options(true | false) -> [_].
ssl_verify_options(true) ->
    CAFile = config:get("replicator", "ssl_trusted_certificates_file"),
    [{verify, verify_peer}, {cacertfile, CAFile}];
ssl_verify_options(false) ->
    [{verify, verify_none}].


-spec before_doc_update(#doc{}, #db{}) -> #doc{}.
before_doc_update(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db) ->
    Doc;
before_doc_update(#doc{body = {Body}} = Doc, #db{user_ctx=UserCtx} = Db) ->
    #user_ctx{roles = Roles, name = Name} = UserCtx,
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


-spec after_doc_read(#doc{}, #db{}) -> #doc{}.
after_doc_read(#doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>} = Doc, _Db) ->
    Doc;
after_doc_read(#doc{body = {Body}} = Doc, #db{user_ctx=UserCtx} = Db) ->
    #user_ctx{name = Name} = UserCtx,
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


-spec strip_credentials(undefined) -> undefined;
    (binary()) -> binary();
    ({[_]}) -> {[_]}.
strip_credentials(undefined) ->
    undefined;
strip_credentials(Url) when is_binary(Url) ->
    re:replace(Url,
        "http(s)?://(?:[^:]+):[^@]+@(.*)$",
        "http\\1://\\2",
        [{return, binary}]);
strip_credentials({Props}) ->
    {lists:keydelete(<<"oauth">>, 1, Props)}.



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

check_options_pass_values_test() ->
    ?assertEqual(check_options([]), []),
    ?assertEqual(check_options([baz, {other,fiz}]), [baz, {other, fiz}]),
    ?assertEqual(check_options([{doc_ids, x}]), [{doc_ids, x}]),
    ?assertEqual(check_options([{filter, x}]), [{filter, x}]),
    ?assertEqual(check_options([{selector, x}]), [{selector, x}]).

check_options_fail_values_test() ->
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {filter, y}])),
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {selector, y}])),
    ?assertThrow({bad_request, _},
        check_options([{filter, x}, {selector, y}])),
    ?assertThrow({bad_request, _},
        check_options([{doc_ids, x}, {selector, y}, {filter, z}])).

-endif.


