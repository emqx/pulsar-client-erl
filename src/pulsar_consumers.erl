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
-module(pulsar_consumers).
-define(MAX_CONSUMER_ID, 65535).


%% APIs
-export([start_supervised/3, stop_supervised/1, start_link/3]).
-export([all_connected/1]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        , format_status/1
        , format_status/2
        ]).

-record(state, {topic,
                client_id,
                partitions,
                consumer_opts,
                consumer_id = 0,
                consumers = #{}}).

-type consumers() :: #{ client := atom()
                      , topic := binary()
                      , name := atom()
                      }.

-define(T_RETRY_START, 5000).

%% @doc Start supervised consumer.
start_supervised(ClientId, Topic, ConsumerOpts) ->
    {ok, _Pid} = pulsar_consumers_sup:ensure_present(ClientId, Topic, ConsumerOpts),
    {ok, #{client => ClientId, topic => Topic, name => get_name(ConsumerOpts)}}.

stop_supervised(#{client := ClientId, name := Name}) ->
    pulsar_consumers_sup:ensure_absence(ClientId, Name).

-spec all_connected(consumers()) -> boolean().
all_connected(#{name := Name}) ->
    try
      ConsumerToPartitionTopicMap = gen_server:call(Name, get_consumers, 5_000),
      NumConsumers = map_size(ConsumerToPartitionTopicMap),
      (NumConsumers =/= 0) andalso
          lists:all(
            fun(Pid) ->
                    connected =:= pulsar_consumer:get_state(Pid)
            end,
            maps:keys(ConsumerToPartitionTopicMap))
    catch
        _:_ ->
            false
    end.

%% @doc start pulsar_consumers gen_server
start_link(ClientId, Topic, ConsumerOpts) ->
    gen_server:start_link({local, get_name(ConsumerOpts)}, ?MODULE, [ClientId, Topic, ConsumerOpts], []).

init([ClientId, Topic, ConsumerOpts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{topic = Topic,
                client_id = ClientId,
                consumer_opts = ConsumerOpts}, 0}.

handle_call(get_consumers, _From, State = #state{consumers = Consumers}) ->
    {reply, Consumers, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State = #state{client_id = ClientId, topic = Topic}) ->
    case pulsar_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            case pulsar_client:get_topic_metadata(Pid, Topic) of
                {ok, {_, Partitions}} ->
                    PartitionTopics = create_partition_topic(Topic, Partitions),
                    NewState = lists:foldl(
                        fun(PartitionTopic, CurrentState) ->
                            start_consumer(Pid, PartitionTopic, CurrentState)
                        end,
                        State, PartitionTopics),
                    {noreply, NewState#state{partitions = length(PartitionTopics)}};
                {error, Reason} ->
                    log_error("get topic metatdata failed: ~p", [Reason]),
                    {stop, {shutdown, Reason}, State}
            end;
        {error, Reason} ->
            {stop, {shutdown, Reason}, State}
    end;

handle_info({'EXIT', Pid, _Error}, State = #state{consumers = Consumers}) ->
    case maps:get(Pid, Consumers, undefined) of
        undefined ->
            log_error("Not find Pid:~p consumer", [Pid]),
            {noreply, State};
        PartitionTopic ->
            restart_producer_later(PartitionTopic),
            {noreply, State#state{consumers = maps:remove(Pid, Consumers)}}
    end;

handle_info({restart_consumer, PartitionTopic}, State = #state{client_id = ClientId}) ->
    case pulsar_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            {noreply, start_consumer(Pid, PartitionTopic, State)};
        {error, Reason} ->
            {stop, {shutdown, Reason}, State}
    end;

handle_info(_Info, State) ->
    log_error("Receive unknown message:~p~n", [_Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, _St) -> ok.

format_status(Status) ->
    maps:map(
      fun(state, State) ->
              censor_secrets(State);
         (_Key, Value)->
              Value
      end,
      Status).

%% `format_status/2' is deprecated as of OTP 25.0
format_status(_Opt, [_PDict, State0]) ->
    State = censor_secrets(State0),
    [{data, [{"State", State}]}].

censor_secrets(State0 = #state{consumer_opts = Opts0}) ->
    Opts1 = censor_conn_opts(Opts0),
    Opts =
        maps:map(
          fun(cb_init_args, CBInitArgs) ->
                  censor_conn_opts(CBInitArgs);
             (_Key, Val) ->
                  Val
          end,
          Opts1),
    State0#state{consumer_opts = Opts}.

censor_conn_opts(Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := _}}) ->
    Opts0#{conn_opts := ConnOpts0#{auth_data := "******"}};
censor_conn_opts(Opts) ->
    Opts.

create_partition_topic(Topic, 0) ->
    [Topic];
create_partition_topic(Topic, Partitions) ->
    lists:map(fun(Partition) ->
        lists:concat([Topic, "-partition-", Partition])
    end,lists:seq(0, Partitions-1)).

get_name(ConsumerOpts) -> maps:get(name, ConsumerOpts, ?MODULE).

log_error(Fmt, Args) ->
    do_log(error, Fmt, Args).

do_log(Level, Fmt, Args) ->
    logger:log(Level, Fmt, Args, #{domain => [pulsar, consumers]}).

start_consumer(Pid, PartitionTopic, #state{consumer_opts = ConsumerOpts} = State) ->
    try
        {ok, #{ brokerServiceUrl := BrokerServiceURL
              , proxy_through_service_url := IsProxy
              }} =
            pulsar_client:lookup_topic(Pid, PartitionTopic),
        {MaxConsumerMum, ConsumerOpts1} = case maps:take(max_consumer_num, ConsumerOpts) of
            error -> {1, ConsumerOpts};
            Res -> Res
        end,
        lists:foldl(
            fun(_, #state{consumer_id = CurrentID, consumers = Consumers} = CurrentState) ->
                ConsumerOptsWithConsumerID = maps:put(consumer_id, CurrentID, ConsumerOpts1),
                {AlivePulsarURL, ProxyToBrokerURL} = case IsProxy of
                    false ->
                        {BrokerServiceURL, undefined};
                    true ->
                        {ok, URL} = pulsar_client:get_alive_pulsar_url(Pid),
                        {URL, BrokerServiceURL}
                end,
                {ok, Consumer} =
                    pulsar_consumer:start_link(PartitionTopic, AlivePulsarURL,
                        ProxyToBrokerURL, ConsumerOptsWithConsumerID),
                NewState = next_consumer_id(CurrentState),
                NewState#state{consumers = maps:put(Consumer, PartitionTopic, Consumers)}
            end,
            State, lists:seq(1, MaxConsumerMum))
    catch
        Error : Reason : Stacktrace ->
            log_error("Start consumer: ~p, ~p", [Error, {Reason, Stacktrace}]),
            restart_producer_later(PartitionTopic),
            State
    end.

restart_producer_later(PartitionTopic) ->
    erlang:send_after(?T_RETRY_START, self(), {restart_consumer, PartitionTopic}).

next_consumer_id(#state{consumer_id = ?MAX_CONSUMER_ID} = Stat) ->
    Stat#state{consumer_id = 0};
next_consumer_id(#state{consumer_id = ID} = Stat) ->
    Stat#state{consumer_id = ID + 1}.
