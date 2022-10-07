%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_fake_producer).

-behaviour(gen_statem).

-compile([nowarn_export_all, export_all]).

start_link() ->
    Parent = self(),
    gen_statem:start_link(?MODULE, [Parent], []).

callback_mode() ->
    [handle_event_function].

init([Parent]) ->
    {ok, statem, #{parent => Parent, received => []}}.

handle_event(EventType, EventContent, State, Data = #{received := Received0}) ->
    Received = Received0 ++ [{State, EventType, EventContent}],
    {keep_state, Data#{received := Received}}.

code_change(Direction, State, Data = #{parent := Parent}, Extra) ->
    Parent ! {code_change, Direction, State, Data, Extra},
    {ok, State, Data}.
