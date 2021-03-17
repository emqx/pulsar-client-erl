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

-module(pulsar_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 10,
                 period => 5},
    Children = [client_sup(), producers_sup(), consumers_sup()],
    {ok, {SupFlags, Children}}.

client_sup() ->
    #{id => pulsar_client_sup,
      start => {pulsar_client_sup, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [pulsar_client_sup]
     }.

producers_sup() ->
    #{id => pulsar_producers_sup,
      start => {pulsar_producers_sup, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [pulsar_producers_sup]
    }.
consumers_sup() ->
    #{id => pulsar_consumers_sup,
      start => {pulsar_consumers_sup, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [pulsar_consumers_sup]
    }.
