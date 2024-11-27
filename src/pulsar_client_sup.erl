%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(pulsar_client_sup).

%% @doc
%% pulsar_client_sup (1) (one_for_one)
%%  |
%%  +-- pulsar_clients_sup (0..N) (rest_for_one)
%%        |
%%        +-- pulsar_client (1) (worker) (proxies calls and spawns new workers)
%%        |
%%        +-- pulsar_client_worker_sup (1) (one_for_one)
%%              |
%%              +-- pulsar_client_worker (0..N)

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/1, find_worker_sup/1]).

-define(SUPERVISOR, ?MODULE).

start_link() -> supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 5
               },
    Children = [], %% dynamically added/stopped
    {ok, {SupFlags, Children}}.

%% ensure a client started under supervisor
ensure_present(ClientId, Hosts, Opts) ->
    ChildSpec = child_spec(ClientId, Hosts, Opts),
    case supervisor:start_child(?SUPERVISOR, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, {already_started, _Pid}} ->
            ok;
        {error, already_present} ->
            ensure_absence(ClientId),
            {error, client_not_running};
        {error, Reason} ->
            ensure_absence(ClientId),
            {error, map_start_error(Reason)}
    end.

%% ensure client stopped and deleted under supervisor
ensure_absence(ClientId) ->
    case supervisor:terminate_child(?SUPERVISOR, child_id(ClientId)) of
        ok -> ok = supervisor:delete_child(?SUPERVISOR, child_id(ClientId));
        {error, not_found} -> ok
    end.

find_worker_sup(ClientId) ->
    Children = supervisor:which_children(?SUPERVISOR),
    case lists:keyfind(ClientId, 1, Children) of
        {ClientId, Pid, _, _} when is_pid(Pid) ->
            pulsar_clients_sup:find_worker_sup(Pid, ClientId);
        {ClientId, Restarting, _, _} ->
            {error, Restarting};
        false ->
            {error, not_found}
    end.

child_id(ClientId) ->
    ClientId.

child_spec(ClientId, Hosts, Opts) ->
    #{id => child_id(ClientId),
      start => {pulsar_clients_sup, start_link, [ClientId, Hosts, Opts]},
      restart => permanent,
      type => supervisor,
      shutdown => infinity
    }.

map_start_error({{shutdown, {failed_to_start_child,
                             {worker_sup, _},
                             {shutdown, {failed_to_start_child, _, Reason}}}}, _}) ->
    Reason;
map_start_error(Reason) ->
    Reason.
