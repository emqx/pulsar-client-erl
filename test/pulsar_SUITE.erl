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
%-define(PULSAR_HOST, {"localhost", 6650}).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [ t_pulsar_client
    , t_pulsar
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
%%    pulsar:start(),
    {ok, _} = application:ensure_all_started(pulsar),

    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),

    timer:sleep(1000),
    %% for coverage
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),

    ?assertMatch({ok,{<<"test">>, PNum}} when is_integer(PNum),
        pulsar_client:get_topic_metadata(ClientPid, <<"test">>)),

    ?assertMatch({ok, #{
            brokerServiceUrl := BrokerServiceUrl,
            proxy_through_service_url := IsProxy
        }} when (is_list(BrokerServiceUrl) orelse is_binary(BrokerServiceUrl))
                andalso is_boolean(IsProxy),
        pulsar_client:lookup_topic(ClientPid, <<"test-partition-0">>)),

    ?assertEqual(true, pulsar_client:get_status(ClientPid)),

    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),

    timer:sleep(50),

    ?assertEqual(false, is_process_alive(ClientPid)),
    application:stop(pulsar).

t_pulsar(_) ->
    t_pulsar_(random),
    t_pulsar_(roundrobin).
t_pulsar_(Strategy) ->
    {ok, _} = application:ensure_all_started(pulsar),
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [?PULSAR_HOST], #{}),
    ConsumerOpts = #{
        cb_init_args => [],
        cb_module => ?MODULE,
        sub_type => 'Shared',
        subscription => "pulsar_test_suite_subscription",
        max_consumer_num => 1,
        name => pulsar_test_suite_subscription
    },
    %% start producers
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),
    timer:sleep(200),
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),

    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => Strategy,
        callback => {?MODULE, producer_callback, []}
    },
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),
    %% wait server connect
    timer:sleep(500),
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),


    ?assertMatch(#{sequence_id := _}, pulsar:send_sync(Producers, [Data], 300)),
    %% keepalive test
    timer:sleep(30*1000 + 5*1000),
    ?assertMatch(#{sequence_id := _}, pulsar:send_sync(Producers, [Data])),
    timer:sleep(500),

    %% restart test
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers, [Data]),
    [{_, ConsumersPid, _, _} | _] = supervisor:which_children(pulsar_consumers_sup),
    {_, _, _, _, _, _, ConsumerMap} = sys:get_state(ConsumersPid),
    [ConsumerPid | _] = maps:keys(ConsumerMap),
    erlang:exit(ProducerPid, kill),
    erlang:exit(ConsumerPid, kill),
    timer:sleep(2000),

    %% match the send_sync message
    ?assertEqual(2, pulsar_metrics:consumer()),
    ?assertEqual(2, pulsar_metrics:producer()),
    %% loop send data
    lists:foreach(fun(_) ->
            pulsar:send(Producers, [Data]), timer:sleep(30)
        end, lists:seq(1, ?BATCH_SIZE)),
    timer:sleep(2000),
    %% should be equal BatchSize
    %% send == consumer
    ?assertEqual(pulsar_metrics:producer(), pulsar_metrics:consumer()),
    %% stop consumers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_consumers(Consumers)),
    %% stop producers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_producers(Producers)),
    %% stop clients
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),
    %% check alive
    ?assertEqual(false, is_process_alive(ClientPid)),
    %% metrics test
    [{producer, PCount}, {consumer, CCounter}] = pulsar_metrics:all(),
    ?assertEqual(PCount, CCounter),
    %% 4 producer , 4 consumer , 2 all
    AllMetrics = pulsar_metrics:all_detail(),
    ?assertMatch(N when N > 0, length(AllMetrics)),
    ?assertEqual({PCount, CCounter}, count_test(AllMetrics)),
    ?assertEqual(0, pulsar_metrics:producer("no exist topic name")),
    ?assertEqual(0, pulsar_metrics:consumer("no exist topic name")),
    application:stop(pulsar).

count_test(AllMetrics)-> count_test(AllMetrics, 0, 0).
count_test([], PCountNow, CCountNow) ->
    {PCountNow, CCountNow};
count_test([{{_, all}, _}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow, CCountNow);
count_test([{{producer, _}, Data}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow + Data, CCountNow);
count_test([{{consumer, _}, Data}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow, CCountNow + Data).

%%----------------------
%% pulsar callback
%%  #{highest_sequence_id => 18446744073709551615,message_id => #{ack_set => [],entryId => 13,ledgerId => 225},producer_id => 1,sequence_id => 1}
%%----------------------
init(_Topic, Args) ->
    {ok, Args}.
handle_message(_Msg, _Payloads, Loop) ->
    {ok, 'Individual', Loop}.

producer_callback(_Args) -> ok.
