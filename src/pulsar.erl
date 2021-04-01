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

-module(pulsar).

-export([start/0]).

%% Supervised client management APIs
-export([ ensure_supervised_client/3
        , stop_and_delete_supervised_client/1
        ]).

%% Primitive producer worker management APIs
-export([ ensure_supervised_producers/3
        , stop_and_delete_supervised_producers/1
        ]).

%% Primitive consumer worker management APIs
-export([ ensure_supervised_consumers/3
        , stop_and_delete_supervised_consumers/1
        ]).

%% Messaging APIs
-export([ send/2
        , send_sync/2
        , send_sync/3
        ]).

start() ->
    application:start(crc32cer),
    application:start(pulsar).

ensure_supervised_client(ClientId, Hosts, Opts) ->
    pulsar_client_sup:ensure_present(ClientId, Hosts, Opts).

stop_and_delete_supervised_client(ClientId) ->
    pulsar_client_sup:ensure_absence(ClientId).

ensure_supervised_producers(ClientId, Topic, Opts) ->
    pulsar_producers:start_supervised(ClientId, Topic, Opts).

stop_and_delete_supervised_producers(Producers) ->
    pulsar_producers:stop_supervised(Producers).

ensure_supervised_consumers(ClientId, Topic, Opts) ->
    pulsar_consumers:start_supervised(ClientId, Topic, Opts).

stop_and_delete_supervised_consumers(Consumers) ->
    pulsar_consumers:stop_supervised(Consumers).


send(Producers, Batch) ->
    {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
    pulsar_producer:send(ProducerPid, Batch).

send_sync(Producers, Batch, Timeout) ->
    {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
    pulsar_producer:send_sync(ProducerPid, Batch, Timeout).

send_sync(Producers, Batch) ->
    {_Partition, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
    pulsar_producer:send_sync(ProducerPid, Batch).