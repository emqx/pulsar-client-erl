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
-module(pulsar_client_worker_sup).

-behaviour(supervisor).

-export([start_link/3, start_worker/4]).

-export([init/1]).

start_link(ClientId, Hosts, Opts) ->
    supervisor:start_link(?MODULE, [ClientId, Hosts, Opts]).

start_worker(Sup, ClientId, Server, Opts) ->
    case supervisor:start_child(Sup, worker_spec(ClientId, Server, Opts)) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, already_present} -> {error, client_not_running};
        {error, Reason} -> {error, Reason}
    end.

init([ClientId, Servers, Opts]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 5
               },
    Children = lists:map(fun(Server) -> worker_spec(ClientId, Server, Opts) end, Servers),
    {ok, {SupFlags, Children}}.

worker_spec(ClientId, Server, Opts) ->
    #{id => Server,
      start => {pulsar_client_worker, start_link, [ClientId, Server, Opts]},
      restart => transient,
      type => worker,
      shutdown => 5_000
    }.
