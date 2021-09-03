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

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/1, find_client/1]).

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
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, already_present} -> {error, client_not_running}
    end.

%% ensure client stopped and deleted under supervisor
ensure_absence(ClientId) ->
    case supervisor:terminate_child(?SUPERVISOR, ClientId) of
        ok -> ok = supervisor:delete_child(?SUPERVISOR, ClientId);
        {error, not_found} -> ok
    end.

%% find client pid from client id
find_client(ClientId) ->
    Children = supervisor:which_children(?SUPERVISOR),
    case lists:keyfind(ClientId, 1, Children) of
        {ClientId, Client, _, _} when is_pid(Client) ->
            {ok, Client};
        {ClientId, Restarting, _, _} ->
            {error, Restarting};
        false ->
            {error, {no_such_client, ClientId}}
    end.

child_spec(ClientId, Hosts, Opts) ->
    #{id => ClientId,
      start => {pulsar_client, start_link, [ClientId, Hosts, Opts]},
      restart => transient,
      type => worker,
      modules => [pulsar_client]
    }.
