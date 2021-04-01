%%%%--------------------------------------------------------------------
%%%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%%%
%%%% Licensed under the Apache License, Version 2.0 (the "License");
%%%% you may not use this file except in compliance with the License.
%%%% You may obtain a copy of the License at
%%%%
%%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%%
%%%% Unless required by applicable law or agreed to in writing, software
%%%% distributed under the License is distributed on an "AS IS" BASIS,
%%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%% See the License for the specific language governing permissions and
%%%% limitations under the License.
%%%%--------------------------------------------------------------------
-module(pulsar_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(TEST_SUIT_CLIENT, client_erl_suit).
-define(BATCH_SIZE , 100).
-define(PULSAR_HOST, {"pulsar", 6650}).
%%-define(PULSAR_HOST, {"127.0.0.1", 6650}).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [
        t_pulsar_client
        , t_pulsar_produce
        , t_pulsar_consumer
    ].

init_per_suite(Cfg) ->
    Cfg.

end_per_suite(_Args) ->
    ok.

set_special_configs(_Args) ->
    ok.
%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_pulsar_client(_Args) ->
    application:ensure_all_started(pulsar),

    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),

    timer:sleep(500),

    ?assertMatch({_, 4}, pulsar_client:get_topic_metadata(ClientPid, <<"test">>)),

    ?assertEqual("pulsar://localhost:6650", pulsar_client:lookup_topic(ClientPid, <<"test-partition-0">>)),

    ?assertEqual(true, pulsar_client:get_status(ClientPid)),

    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),

    timer:sleep(50),

    ?assertEqual(false, is_process_alive(ClientPid)),
    application:stop(pulsar).

t_pulsar_produce(_) ->
    application:ensure_all_started(pulsar),
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),
    ConsumerOpts = #{
        cb_init_args => no_args,
        cb_module => ?MODULE,
        sub_type => 'Shared',
        subscription => "pulsar_test_suite_subscription",
        max_consumer_num => 1,
        name => pulsar_test_suite_subscription
    },
    %% start producers
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),

    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
%%        strategy => random
        strategy => roundrobin
    },
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),
    %% wait server connect
    timer:sleep(500),
%%    todo pulsar in docker will change brokers uri
    ?assertMatch(#{sequence_id := _}, pulsar:send_sync(Producers, [Data], 300)),
    timer:sleep(500),
    %% match the send_sync message
    ?assertEqual(1, pulsar_metrics:consumer()),
    ?assertEqual(1, pulsar_metrics:producer()),
    %% loop send data
    lists:foreach(fun(_) -> pulsar:send(Producers, [Data]) end, lists:seq(1, ?BATCH_SIZE)),
    timer:sleep(500),
    %% should be equal BatchSize
    %% send ==  consumer
    ?assertEqual(pulsar_metrics:producer(), pulsar_metrics:consumer()),
    %% stop consumers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_consumers(Consumers)),
    %% stop producers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_producers(Producers)),
    %% stop clients
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),
    %% check alive
    ?assertEqual(false, is_process_alive(ClientPid)),
    application:stop(pulsar).

t_pulsar_consumer(Args) ->
    t_pulsar_consumer_(Args, 1),
    t_pulsar_consumer_(Args, 2),
    t_pulsar_consumer_(Args, 3).

t_pulsar_consumer_(_, ConsumersMaxNum) ->
    application:ensure_all_started(pulsar),
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),
    %% start counter server
    ConsumerOpts = #{
        cb_init_args => no_args,
        cb_module => ?MODULE,
        sub_type => 'Shared',
        subscription => "pulsar_test_suite_subscription",
        max_consumer_num => ConsumersMaxNum
    },
    %% start consumers
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),
    %% loop time :BatchSize
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => roundrobin
    },
    %% start produce date
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),
    timer:sleep(100),
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    lists:foreach(fun(_) -> pulsar:send(Producers, [Data]) end, lists:seq(1, ?BATCH_SIZE)),
    timer:sleep(500),
    %% stop producers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_producers(Producers)),
    %% wait consumer
    timer:sleep(500),
    %% send ==  consumer
    ?assertEqual(pulsar_metrics:producer(), pulsar_metrics:consumer()),
    %% stop consumers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_consumers(Consumers)),
    %% stop client
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),
    timer:sleep(50),
    ?assertEqual(false, is_process_alive(ClientPid)),
    %% stop app
    application:stop(pulsar).

%%----------------------
%% pulsar callback
%%  #{highest_sequence_id => 18446744073709551615,message_id => #{ack_set => [],entryId => 13,ledgerId => 225},producer_id => 1,sequence_id => 1}
%%----------------------
init(_Topic, Args) ->
    {ok, Args}.
handle_message(_Msg, _Payloads, Loop) ->
    {ok, 'Individual', Loop}.

