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

-module(pulsar_producers_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/2]).

-define(SUPERVISOR, ?MODULE).
-define(WORKER_ID(ClientId, Name), {ClientId, Name}).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 5
               },
    Children = [], %% dynamically added/stopped
    {ok, {SupFlags, Children}}.

%% ensure a client started under supervisor
ensure_present(ClientId, Topic, ProducerOpts0) ->
    ProducerOpts = pulsar_utils:wrap_secrets(ProducerOpts0),
    ChildSpec = child_spec(ClientId, Topic, ProducerOpts),
    case supervisor:start_child(?SUPERVISOR, ChildSpec) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, {{already_started, Pid}, _}} -> {ok, Pid};
        {error, already_present} -> {error, not_running}
    end.

%% ensure client stopped and deleted under supervisor
ensure_absence(ClientId, Name) ->
    Id = ?WORKER_ID(ClientId, Name),
    case supervisor:terminate_child(?SUPERVISOR, Id) of
        ok ->
            case supervisor:delete_child(?SUPERVISOR, Id) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        {error, not_found} -> ok
    end.

child_spec(ClientId, Topic, ProducerOpts) ->
    #{id => ?WORKER_ID(ClientId, get_name(ProducerOpts)),
      start => {pulsar_producers, start_link, [ClientId, Topic, ProducerOpts]},
      restart => transient,
      type => worker,
      modules => [pulsar_producers]
    }.

get_name(ProducerOpts) ->
    maps:get(name, ProducerOpts, pulsar_producers).
