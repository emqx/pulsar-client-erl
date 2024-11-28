%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(pulsar_clients_sup).

-behaviour(supervisor).

-export([start_link/3, find_worker_sup/2, init/1]).

start_link(ClientId, Servers, Opts) ->
    supervisor:start_link(?MODULE, [ClientId, Servers, Opts]).

find_worker_sup(Sup, ClientId) ->
    Children = supervisor:which_children(Sup),
    Key = {worker_sup, ClientId},
    case lists:keyfind(Key, 1, Children) of
        {Key, Pid, _, _} when is_pid(Pid) ->
            {ok, Pid};
        {Key, Restarting, _, _} ->
            {error, Restarting};
        false ->
            {error, not_found}
    end.

init([ClientId, Servers, Opts]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 10,
                 period => 5
               },
    Children = [ client_spec(ClientId, Opts)
               , client_worker_sup_spec(ClientId, Servers, Opts)
               ],
    {ok, {SupFlags, Children}}.

client_spec(ClientId, Opts) ->
    #{id => {client, ClientId},
      start => {pulsar_client_manager, start_link, [ClientId, Opts]},
      restart => permanent,
      type => worker
    }.

client_worker_sup_spec(ClientId, Servers, Opts) ->
    #{id => {worker_sup, ClientId},
      start => {pulsar_client_worker_sup, start_link, [ClientId, Servers, Opts]},
      restart => permanent,
      type => supervisor,
      shutdown => infinity
    }.
