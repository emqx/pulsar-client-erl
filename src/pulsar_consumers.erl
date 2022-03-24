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

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-record(state, {topic,
                client_id,
                partitions,
                consumer_opts,
                consumer_id = 0,
                consumers = #{}}).

%% @doc Start supervised consumer.
start_supervised(ClientId, Topic, ConsumerOpts) ->
    {ok, _Pid} = pulsar_consumers_sup:ensure_present(ClientId, Topic, ConsumerOpts),
    {ok, #{client => ClientId, topic => Topic, name => get_name(ConsumerOpts)}}.

stop_supervised(#{client := ClientId, name := Name}) ->
    pulsar_consumers_sup:ensure_absence(ClientId, Name).

%% @doc start pulsar_consumers gen_server
start_link(ClientId, Topic, ConsumerOpts) ->
    gen_server:start_link({local, get_name(ConsumerOpts)}, ?MODULE, [ClientId, Topic, ConsumerOpts], []).

init([ClientId, Topic, ConsumerOpts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{topic = Topic,
                client_id = ClientId,
                consumer_opts = ConsumerOpts}, 0}.

handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State = #state{client_id = ClientId, topic = Topic}) ->
    case pulsar_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            {ok, {_, Partitions}} = pulsar_client:get_topic_metadata(Pid, Topic),
            PartitionTopics = create_partition_topic(Topic, Partitions),
            NewState = lists:foldl(
                fun(PartitionTopic, CurrentState) ->
                    start_consumer(Pid, PartitionTopic, CurrentState)
                end,
                State, PartitionTopics),
            {noreply, NewState#state{partitions = length(PartitionTopics)}};
        {error, Reason} ->
            {stop, {shutdown, Reason}, State}
    end;

handle_info({'EXIT', Pid, _Error}, State = #state{consumers = Consumers}) ->
    case maps:get(Pid, Consumers, undefined) of
        undefined ->
            log_error("Not find Pid:~p consumer", [Pid]),
            {noreply, State};
        PartitionTopic ->
            self() ! {restart_consumer, PartitionTopic},
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

create_partition_topic(Topic, 0) ->
    [Topic];
create_partition_topic(Topic, Partitions) ->
    lists:map(fun(Partition) ->
        lists:concat([Topic, "-partition-", Partition])
    end,lists:seq(0, Partitions-1)).

get_name(ConsumerOpts) -> maps:get(name, ConsumerOpts, ?MODULE).

log_error(Fmt, Args) -> logger:error(Fmt, Args).

start_consumer(Pid, PartitionTopic, #state{consumer_opts = ConsumerOpts} = State) ->
    try
        {ok, BrokerServiceUrl} = pulsar_client:lookup_topic(Pid, PartitionTopic),
        {MaxConsumerMum, ConsumerOpts1} = case maps:take(max_consumer_num, ConsumerOpts) of
            error -> {1, ConsumerOpts};
            Res -> Res
        end,
        lists:foldl(
            fun(_, #state{consumer_id = CurrentID, consumers = Consumers} = CurrentState) ->
                ConsumerOptsWithConsumerID = maps:put(consumer_id, CurrentID, ConsumerOpts1),
                {ok, Consumer} =
                    pulsar_consumer:start_link(PartitionTopic, BrokerServiceUrl, ConsumerOptsWithConsumerID),
                NewState = next_consumer_id(CurrentState),
                NewState#state{consumers = maps:put(Consumer, PartitionTopic, Consumers)}
            end,
            State, lists:seq(1, MaxConsumerMum))
    catch
        Error : Reason : Stacktrace ->
            log_error("Start consumer: ~p, ~p", [Error, {Reason, Stacktrace}]),
            self() ! {restart_consumer, PartitionTopic},
            State
    end.

next_consumer_id(#state{consumer_id = ?MAX_CONSUMER_ID} = Stat) ->
    Stat#state{consumer_id = 0};
next_consumer_id(#state{consumer_id = ID} = Stat) ->
    Stat#state{consumer_id = ID + 1}.
