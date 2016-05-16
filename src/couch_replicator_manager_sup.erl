%
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

-module(couch_replicator_manager_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([restart_mdb_listener/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    MdbChangesArgs = [
        <<"_replicator">>,        % DbSuffix
	couch_replicator_manager, % Module
	couch_replicator_manager, % Context(Server)
        [skip_ddocs]              % Options
    ],
    Children = [
        {couch_replicator_clustering,
            {couch_replicator_clustering, start_link, []},
            permanent,
            brutal_kill,
            worker,
            [couch_replicator_clustering]},
        {couch_replicator_manager,
            {couch_replicator_manager, start_link, []},
            permanent,
            brutal_kill,
            worker,
            [couch_replicator_manager]},
        {couch_multidb_changes,
            {couch_multidb_changes, start_link, MdbChangesArgs},
            permanent,
            brutal_kill,
            worker,
            [couch_multidb_changes]}
    ],
    {ok, {{rest_for_one,10,1}, Children}}.


restart_mdb_listener() ->
    ok = supervisor:terminate_child(?MODULE, couch_multidb_changes),
    {ok, ChildPid} = supervisor:restart_child(?MODULE, couch_multidb_changes),
    ChildPid.

