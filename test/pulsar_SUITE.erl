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
-define(PULSAR_HOST, "pulsar://pulsar:6650").
-define(PULSAR_BASIC_AUTH_HOST, "pulsar://pulsar-basic-auth:6650").
-define(PULSAR_TOKEN_AUTH_HOST, "pulsar://pulsar-token-auth:6650").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [ t_pulsar_client
    , t_pulsar_basic_auth
    , t_pulsar_token_auth
    , t_pulsar
    , {group, resilience}
    ].

resilience_tests() ->
    resilience_multi_failure_tests() ++ resilience_single_failure_tests().

resilience_multi_failure_tests() ->
    [ t_pulsar_replayq
    , t_pulsar_replayq_producer_restart
    , t_pulsar_drop_expired_batch
    ].

resilience_single_failure_tests() ->
    [ t_pulsar_drop_expired_batch_resend_inflight
    ].

groups() ->
    [ {resilience, [ {group, timeout}
                   , {group, down}
                   | resilience_single_failure_tests()
                   ]}
    , {timeout, resilience_multi_failure_tests()}
    , {down, resilience_multi_failure_tests()}
    ].

init_per_suite(Cfg) ->
    ct:timetrap({minutes, 3}),
    Cfg.

end_per_suite(_Args) ->
    ok.

init_per_group(resilience, Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?PULSAR_HOST),
    ProxyHost = os:getenv("PROXY_HOST", "proxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    UpstreamHost = os:getenv("PROXY_PULSAR_HOST", PulsarHost),
    %% when testing locally; externally exposed port for container
    FakePulsarPort = list_to_integer(os:getenv("PROXY_PULSAR_PORT", "6650")),
    FakePulsarHost = pulsar_test_utils:populate_proxy(ProxyHost, ProxyPort, FakePulsarPort, UpstreamHost),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    , {proxy_host, ProxyHost}
    , {proxy_port, ProxyPort}
    , {fake_pulsar_host, FakePulsarHost}
    , {resilience_test, true}
    | Config];
init_per_group(timeout, Config) ->
    [ {failure_type, timeout}
    | Config];
init_per_group(down, Config) ->
    [ {failure_type, down}
    | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    ok.

init_per_testcase(t_pulsar_basic_auth, Config) ->
    PulsarHost = os:getenv("PULSAR_BASIC_AUTH_HOST", ?PULSAR_BASIC_AUTH_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    | Config];
init_per_testcase(t_pulsar_token_auth, Config) ->
    PulsarHost = os:getenv("PULSAR_TOKEN_AUTH_HOST", ?PULSAR_TOKEN_AUTH_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    | Config];
init_per_testcase(_TestCase, Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?PULSAR_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    group_initializations(Config),
    [ {pulsar_host, PulsarHost}
    | Config].

end_per_testcase(TestCase, Config)
  when TestCase =:= t_pulsar_replayq;
       TestCase =:= t_pulsar_replayq_producer_restart;
       TestCase =:= t_pulsar_drop_expired_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    application:stop(pulsar),
    snabbkaffe:stop(),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    file:del_dir_r("/tmp/replayq1"),
    meck:unload([pulsar_client]),
    ok;
end_per_testcase(t_pulsar_drop_expired_batch_resend_inflight, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    application:stop(pulsar),
    snabbkaffe:stop(),
    catch meck:unload([pulsar_producer, pulsar_client]),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    file:del_dir_r("/tmp/replayq2"),
    ok;
end_per_testcase(_TestCase, _Config) ->
    application:stop(pulsar),
    snabbkaffe:stop(),
    ok.

group_initializations(Config) ->
    IsResilienceTest = ?config(resilience_test, Config),
    case IsResilienceTest of
        true ->
            FakePulsarHost = ?config(fake_pulsar_host, Config),
            fix_broker_service_url(FakePulsarHost),
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_pulsar_client(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

    timer:sleep(1000),
    %% for coverage
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

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
    ok.

t_pulsar_basic_auth(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    ConnOpts = #{ auth_data => <<"super:secretpass">>
                , auth_method_name => <<"basic">>
                },
    {ok, _ClientPid} = pulsar:ensure_supervised_client(
                         ?TEST_SUIT_CLIENT,
                         [PulsarHost],
                         #{conn_opts => ConnOpts}),

    ConsumerOpts = #{ cb_init_args => #{send_to => self()}
                    , cb_module => pulsar_echo_consumer
                    , sub_type => 'Shared'
                    , subscription => "my-subscription"
                    , max_consumer_num => 1
                    , name => my_test
                    , consumer_id => 1
                    , conn_opts => ConnOpts
                    },
    {ok, _Consumer} = pulsar:ensure_supervised_consumers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ConsumerOpts
                                                        ),
    timer:sleep(2000),
    ProducerOpts = #{ batch_size => ?BATCH_SIZE
                    , strategy => random
                    , callback => {?MODULE, producer_callback, []}
                    , conn_opts => ConnOpts
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ProducerOpts
                                                        ),
    timer:sleep(1000),
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, PubRes} = pulsar:send_sync(Producers, [Data]),
    ?assertNotMatch({error, _}, PubRes),
    receive
        {pulsar_message, _Topic, _Message, Payloads} ->
            ?assertEqual([<<"hello world">>], Payloads),
            ok
    after
        2000 ->
            error("message not received!")
    end,
    ok.

t_pulsar_token_auth(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    ConnOpts = #{ auth_data => <<"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.RVPrnEzgEG-iKfpUWKryC39JgWdFXs7MJMUWnHA4ZSg">>
                , auth_method_name => <<"token">>
                },
    {ok, _ClientPid} = pulsar:ensure_supervised_client(
                         ?TEST_SUIT_CLIENT,
                         [PulsarHost],
                         #{conn_opts => ConnOpts}),

    ConsumerOpts = #{ cb_init_args => #{send_to => self()}
                    , cb_module => pulsar_echo_consumer
                    , sub_type => 'Shared'
                    , subscription => "my-subscription"
                    , max_consumer_num => 1
                    , name => my_test
                    , consumer_id => 1
                    , conn_opts => ConnOpts
                    },
    {ok, _Consumer} = pulsar:ensure_supervised_consumers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ConsumerOpts
                                                        ),
    timer:sleep(2000),
    ProducerOpts = #{ batch_size => ?BATCH_SIZE
                    , strategy => random
                    , callback => {?MODULE, producer_callback, []}
                    , conn_opts => ConnOpts
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ProducerOpts
                                                        ),
    timer:sleep(1000),
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, PubRes} = pulsar:send_sync(Producers, [Data]),
    ?assertNotMatch({error, _}, PubRes),
    receive
        {pulsar_message, _Topic, _Message, Payloads} ->
            ?assertEqual([<<"hello world">>], Payloads),
            ok
    after
        2000 ->
            error("message not received!")
    end,
    ok.

t_pulsar(Config) ->
    t_pulsar_(random, Config),
    t_pulsar_(roundrobin, Config).
t_pulsar_(Strategy, Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    {ok, ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
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


    ?assertMatch({ok, #{sequence_id := _}}, pulsar:send_sync(Producers, [Data], 300)),
    %% keepalive test
    timer:sleep(30*1000 + 5*1000),
    ?assertMatch({ok, #{sequence_id := _}}, pulsar:send_sync(Producers, [Data])),
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

t_pulsar_drop_expired_batch(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),

    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_expired",
        max_consumer_num => 1,
        name => pulsar_test_expired
    },
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    RetentionPeriodMS = 1_000,
    ProducerOpts = #{
        %% to speed up the test a bit
        send_timeout => 5_000,
        connnect_timeout => 5_000,
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, echo_callback, [self()]},
        retention_period => RetentionPeriodMS
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),

    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),
    ct:sleep(StabilizationPeriod),

    %% Produce messages that'll expire
    ct:pal("sending messages..."),
    pulsar:send(Producers,
                [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                 || SeqNo <- lists:seq(1, 150)]),
    ct:pal("waiting for retention period to expire..."),
    ct:sleep(RetentionPeriodMS * 2),

    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),
    ct:sleep(StabilizationPeriod * 2),

    ct:pal("waiting for producer to be connected again"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<>>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    ct:pal("producer connected"),

    receive
        {pulsar_message, _Topic, _Receipt, Payloads} ->
            error({should_have_expired, Payloads})
    after
        1_000 ->
            ok
    end,
    receive
        {produce_response, {error, expired}} ->
            ok
    after
        1_000 ->
            error(should_have_invoked_callback)
    end,

    ct:pal("producing message that should be received now"),
    pulsar:send(Producers, [#{key => <<"k">>, value => <<"should receive">>}]),

    receive
        {pulsar_message, _Topic1, _Receipt1, [<<"should receive">>]} ->
            ok
    after
        15_000 ->
            error(timeout)
    end,

    ok.

t_pulsar_drop_expired_batch_resend_inflight(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = down,
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),

    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_expired",
        max_consumer_num => 1,
        name => pulsar_test_expired
    },
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),

    meck:new(pulsar_producer, [non_strict, no_history, no_link, passthrough]),
    meck:expect(pulsar_producer, handle_response,
      fun({send_receipt, _}, State) ->
              %% ignore receipts so we can test resend
              State;
         (Arg, State) ->
              meck:passthrough([Arg, State])
      end),
    RetentionPeriodMS = 1_000,
    ProducerOpts = #{
        %% to speed up the test a bit
        send_timeout => 5_000,
        connnect_timeout => 5_000,
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, echo_callback, [self()]},
        replayq_dir => "/tmp/replayq2",
        replayq_seg_bytes => 20 * 1024 * 1024,
        replayq_offload_mode => false,
        replayq_max_total_bytes => 1_000_000_000,
        retention_period => RetentionPeriodMS
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<>>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),


    %% 1. produce some messages and, while we are processing them,
    %% simulate a disconnect message to change state to `idle'.
    %% 2. also cut the connection with pulsar, so it'll stay idle
    %% while the messages expire.
    %% 3. restore the connection so that inflight messages will be
    %% resent.
    ct:pal("forcing disconnect"),
    ?check_trace(
       begin
         ?force_ordering(#{?snk_kind := pulsar_producer_send_req_enter},
                         #{?snk_kind := will_force_disconnect}),
         ?force_ordering(#{?snk_kind := force_disconnect},
                         #{?snk_kind := pulsar_producer_send_req_exit}),
         spawn_link(fun() ->
           ?tp(will_force_disconnect, #{}),
           ProducerPid ! {tcp_closed, port},
           pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
           ?tp(force_disconnect, #{}),
           ok
         end),

         %% Produce messages that'll expire when resent
         pulsar:send(Producers,
                     [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                      || SeqNo <- lists:seq(1, 150)]),
         ?block_until(#{?snk_kind := pulsar_producer_send_req_exit}),
         ct:sleep(RetentionPeriodMS * 2),


         ct:pal("healing failure..."),
         pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
         ct:pal("failure healed"),
         ct:sleep(StabilizationPeriod * 2),
         ct:pal("waiting for producer to be connected again"),
         {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                           [#{key => <<"k">>, value => <<>>}]),
         pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
         ct:pal("producer connected"),

         %% we can't assert that the messages have been expired, as
         %% the produce request might have succeeded in Pulsar's side,
         %% but our producer doens't know that because of the network
         %% partition.  we can assert that the messages were deemed
         %% expired and dropped by checking the trace, and by checking
         %% that the callback was called with the expiration error.
         receive
             {produce_response, {error, expired}} ->
                 ok
         after
             1_000 ->
                 error(should_have_invoked_callback)
         end,

         ct:pal("producing message that should be received now"),
         pulsar:send(Producers, [#{key => <<"k">>, value => <<"should receive">>}]),

         receive
             {pulsar_message, _Topic1, _Receipt1, [<<"should receive">>]} ->
                 ok
         after
             15_000 ->
                 error(timeout)
         end,
         ok
       end,
       fun(Trace) ->
         ?assertMatch([_], ?of_kind(pulsar_producer_resend_all_expired,
                                    Trace)),
         ok
       end),

    ok.

t_pulsar_replayq(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),

    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_replayq",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, producer_callback, []}
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    ct:sleep(2_000),

    TestPid = self(),
    ProduceInterval = 100,
    StartSequentialProducer =
        fun Go (SeqNo0) ->
          receive
            stop -> TestPid ! {done, SeqNo0}
          after
            0 ->
              SeqNo = SeqNo0 + 1,
              pulsar:send(Producers, [#{key => <<"k">>, value => integer_to_binary(SeqNo)}]),
              SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
              timer:sleep(ProduceInterval),
              Go(SeqNo)
          end
        end,

    SequentialProducer = spawn(fun() -> StartSequentialProducer(0) end),
    ct:pal("started sequential producer"),

    %% produce some messages in the connected state.
    wait_until_produced(100, 100 * ProduceInterval + 100),
    ct:pal("produced 100 messages"),

    %% cut the connection and produce more messages
    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),

    timer:sleep(StabilizationPeriod),
    wait_until_produced(500, 400 * ProduceInterval + 100),
    ct:pal("produced 500 messages"),

    %% reestablish connection and produce some more
    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),
    timer:sleep(StabilizationPeriod),
    wait_until_produced(600, 100 * ProduceInterval + 100),
    ct:pal("produced 600 messages"),

    %% stop producer and check we received all msgs
    SequentialProducer ! stop,
    TotalProduced =
        receive
            {done, Total} -> Total
        after
            10_000 ->
                error(producer_didnt_stop)
        end,

    ct:pal("produced ~b messages in total", [TotalProduced]),

    ExpectedPayloads = sets:from_list([integer_to_binary(N) || N <- lists:seq(1, TotalProduced)],
                                      [{version, 2}]),
    wait_until_consumed(ExpectedPayloads, timer:seconds(30)),

    ct:pal("all ~b expected messages were received", [TotalProduced]),

    ok.

t_pulsar_replayq_producer_restart(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),

    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_replayq",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, producer_callback, []},
        replayq_dir => "/tmp/replayq1",
        replayq_seg_bytes => 20 * 1024 * 1024,
        replayq_offload_mode => false,
        replayq_max_total_bytes => 1_000_000_000,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    ct:pal("producer connected"),

    TestPid = self(),
    ProduceInterval = 100,
    StartSequentialProducer =
        fun Go (SeqNo0) ->
          receive
            stop -> TestPid ! {done, SeqNo0}
          after
            0 ->
              SeqNo = SeqNo0 + 1,
              pulsar:send(Producers, [#{key => <<"k">>, value => integer_to_binary(SeqNo)}]),
              SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
              timer:sleep(ProduceInterval),
              Go(SeqNo)
          end
        end,

    SequentialProducer = spawn(fun() -> StartSequentialProducer(0) end),
    ct:pal("started sequential producer"),

    %% produce some messages in the connected state.
    wait_until_produced(100, 100 * ProduceInterval + 100),
    ct:pal("produced 100 messages"),

    %% cut the connection and produce more messages
    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),

    timer:sleep(StabilizationPeriod),
    wait_until_produced(250, 150 * ProduceInterval + 100),
    ct:pal("produced 250 messages"),

    %% stop producing and kill the producer
    SequentialProducer ! stop,
    TotalProduced =
        receive
            {done, Total} -> Total
        after
            10_000 ->
                error(producer_didnt_stop)
        end,
    ct:pal("produced ~b messages in total", [TotalProduced]),
    %% give it some time for the producer to enqueue them before
    %% killing it.
    ct:sleep(5_000),
    Ref = monitor(process, ProducerPid),
    exit(ProducerPid, kill),
    receive
        {'DOWN', Ref, process, ProducerPid, killed} ->
            ok
    after
        500 ->
            error(producer_still_alive)
    end,

    %% reestablish connection and wait until producer catches up
    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),
    timer:sleep(StabilizationPeriod),

    ExpectedPayloads = sets:from_list([integer_to_binary(N) || N <- lists:seq(1, TotalProduced)],
                                      [{version, 2}]),
    wait_until_consumed(ExpectedPayloads, timer:seconds(30)),

    ct:pal("all ~b expected messages were received", [TotalProduced]),

    ok.

wait_until_produced(ExpectedSeqNo, Timeout) ->
    receive
        {sent, ExpectedSeqNo} ->
            ok
    after
        Timeout ->
            error({timeout, ExpectedSeqNo})
    end.

wait_until_consumed(ExpectedPayloads0, Timeout) ->
    case sets:is_empty(ExpectedPayloads0) of
        true -> ok;
        false ->
            receive
                {pulsar_message, _Topic, _Receipt, Payloads} ->
                    PayloadsSet = sets:from_list(Payloads, [{version, 2}]),
                    ExpectedPayloads = sets:subtract(ExpectedPayloads0, PayloadsSet),
                    wait_until_consumed(ExpectedPayloads, Timeout)
            after
                Timeout ->
                    error({missing_messages, lists:sort(sets:to_list(ExpectedPayloads0))})
            end
    end.

fix_broker_service_url(FakePulsarHost) ->
    %% since the broker reports a proxy URL to itself, we need to
    %% patch that so we always use toxiproxy.
    ok = meck:new(pulsar_client, [non_strict, passthrough, no_history]),
    ok = meck:expect(
           pulsar_client, lookup_topic,
           fun(Pid, PartitionTopic) ->
             case meck:passthrough([Pid, PartitionTopic]) of
               {ok, Resp} -> {ok, Resp#{brokerServiceUrl => FakePulsarHost}};
               Error -> Error
             end
           end),
    ok.

%%----------------------
%% pulsar callback
%%  #{highest_sequence_id => 18446744073709551615,message_id => #{ack_set => [],entryId => 13,ledgerId => 225},producer_id => 1,sequence_id => 1}
%%----------------------
init(_Topic, Args) ->
    {ok, Args}.
handle_message(_Msg, _Payloads, Loop) ->
    {ok, 'Individual', Loop}.

producer_callback(_Args) -> ok.

echo_callback(Resp, SendTo) ->
    SendTo ! {produce_response, Resp}.
