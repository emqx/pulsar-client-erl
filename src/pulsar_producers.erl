%% Copyright (c) 2013-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_producers).

-define(MAX_PRODUCER_ID, 65535).

%% APIs
-export([start_supervised/3, stop_supervised/1, start_link/3]).

-export ([ pick_producer/2
         , all_connected/1
         ]).

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

-export([report_status/2]).

-record(state, {topic,
                client_id,
                workers,
                partitions,
                producer_opts,
                producer_id = 0,
                producers = #{}}).

-type clientid() :: atom().
-type topic() :: string().
-type produce_strategy() :: roundrobin | random.
-type producers() :: #{ client := clientid()
                      , topic := topic()
                      , workers := _Workers
                      , partitions := _Partitions
                      , strategy := produce_strategy()
                      }.

-export_type([producers/0]).

-define(T_RETRY_START, 5000).
-define(PRODUCER_STATE_INDEX, 3).
-define(GET_TOPIC_METADATA_TIMEOUT, 30_000).
-define(LOOKUP_TOPIC_TIMEOUT, 30_000).
-define(GET_ALIVE_PULSAR_URL_TIMEOUT, 5_000).

%% @doc Start supervised producers.
-spec start_supervised(clientid(), topic(), map()) -> {ok, producers()}.
start_supervised(ClientId, Topic, ProducerOpts) ->
  {ok, Pid} = pulsar_producers_sup:ensure_present(ClientId, Topic, ProducerOpts),
  {Partitions, Workers} = gen_server:call(Pid, get_workers, infinity),
  {ok, #{client => ClientId,
         topic => Topic,
         workers => Workers,
         partitions => Partitions,
         strategy => maps:get(strategy, ProducerOpts, random)
        }}.

-spec stop_supervised(producers()) -> ok.
stop_supervised(#{client := ClientId, workers := Workers}) ->
  pulsar_producers_sup:ensure_absence(ClientId, Workers).

%% @doc start pulsar_producers gen_server
start_link(ClientId, Topic, ProducerOpts) ->
    gen_server:start_link({local, get_name(ProducerOpts)}, ?MODULE, [ClientId, Topic, ProducerOpts], []).

-spec all_connected(producers()) -> boolean().
all_connected(#{workers := WorkersTable}) ->
    NumWorkers = ets:info(WorkersTable, size),
    (NumWorkers =/= 0) andalso
        ets:foldl(
          fun({_Partition, _ProducerPid, ProducerState}, Acc) ->
            Acc andalso ProducerState =:= connected
          end,
          true,
          WorkersTable).

pick_producer(#{workers := Workers, partitions := Partitions, strategy := Strategy}, Batch) ->
    Partition = pick_partition(Partitions, Strategy, Batch),
    do_pick_producer(Strategy, Partition, Partitions, Workers).

do_pick_producer(Strategy, Partition, Partitions, Workers) ->
    Pid = lookup_producer(Workers, Partition),
    case is_pid(Pid) andalso is_process_alive(Pid) of
        true ->
            {Partition, Pid};
        false when Strategy =:= random ->
            pick_next_alive(Workers, Partition, Partitions);
        false when Strategy =:= roundrobin ->
            R = pick_next_alive(Workers, Partition, Partitions),
            _ = put(pulsar_roundrobin, (Partition + 1) rem Partitions),
            R;
        false ->
            {error, producer_down}
    end.

pick_next_alive(Workers, Partition, Partitions) ->
    pick_next_alive(Workers, (Partition + 1) rem Partitions, Partitions, _Tried = 1).

pick_next_alive(_Workers, _Partition, Partitions, Partitions) ->
    {error, no_producers_available};
pick_next_alive(Workers, Partition, Partitions, Tried) ->
    Pid = lookup_producer(Workers, Partition),
    case is_alive(Pid) of
        true -> {Partition, Pid};
        false -> pick_next_alive(Workers, (Partition + 1) rem Partitions, Partitions, Tried + 1)
    end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

lookup_producer(#{workers := Workers}, Partition) ->
    lookup_producer(Workers, Partition);
lookup_producer(Workers, Partition) ->
    case ets:lookup(Workers, Partition) of
        [{Partition, ProducerPid, _ProducerState}] -> ProducerPid;
        _ -> undefined
    end.

pick_partition(Partitions, random, _) ->
    rand:uniform(Partitions) - 1;
pick_partition(Partitions, roundrobin, _) ->
    Partition = case get(pulsar_roundrobin) of
        undefined -> 0;
        Number    -> Number
    end,
    _ = put(pulsar_roundrobin, (Partition + 1) rem Partitions),
    Partition;
pick_partition(Partitions, first_key_dispatch, [#{key := Key} | _]) ->
  murmerl3:hash_32(Key) rem Partitions.

init([ClientId, Topic, ProducerOpts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{topic = Topic,
                client_id = ClientId,
                producer_opts = ProducerOpts,
                workers = ets:new(get_name(ProducerOpts), [protected, named_table, {read_concurrency, true}])}, 0}.

handle_call(get_workers, _From, State = #state{workers = Workers, partitions = Partitions}) ->
    {reply, {Partitions, Workers}, State};
handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

%% TODO: should be a `continue'.
handle_info(timeout, State = #state{client_id = ClientId, topic = Topic}) ->
    case pulsar_client_manager:get_topic_metadata(ClientId, Topic, ?GET_TOPIC_METADATA_TIMEOUT) of
        {ok, {_, Partitions}} ->
            PartitionTopics = create_partition_topic(Topic, Partitions),
            NewState = lists:foldl(
               fun({PartitionTopic, Partition}, CurrentState) ->
                 start_producer(ClientId, Partition, PartitionTopic, CurrentState)
               end,
               State,
               PartitionTopics),
            {noreply, NewState#state{partitions = length(PartitionTopics)}};
        {error, Reason} ->
            log_error("get topic metatdata failed: ~p", [Reason]),
            {stop, {failed_to_get_metadata, Reason}, State}
    end;
handle_info({'EXIT', Pid, Error}, State = #state{workers = Workers, producers = Producers}) ->
    log_error("Received EXIT from ~p, error: ~p", [Pid, Error]),
    case maps:get(Pid, Producers, undefined) of
        undefined ->
            log_error("Cannot find ~p from producers", [Pid]),
            {noreply, State};
        {Partition, PartitionTopic} ->
            ets:delete(Workers, Partition),
            log_error("Producer ~p down, restart it later", [Pid]),
            restart_producer_later(Partition, PartitionTopic),
            {noreply, State#state{producers = maps:remove(Pid, Producers)}}
    end;
handle_info({restart_producer, Partition, PartitionTopic}, State = #state{client_id = ClientId}) ->
    {noreply, start_producer(ClientId, Partition, PartitionTopic, State)};
handle_info({producer_state_change, ProducerPid, ProducerState},
            State = #state{producers = Producers, workers = WorkersTable})
  when is_map_key(ProducerPid, Producers) ->
    #{ProducerPid := {Partition, _PartitionTopic}} = Producers,
    true = ets:update_element(WorkersTable, Partition, {?PRODUCER_STATE_INDEX, ProducerState}),
    {noreply, State};
handle_info(_Info, State) ->
    log_error("Received unknown message: ~p~n", [_Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, _St) -> ok.

format_status(Status) ->
    maps:map(
      fun(state, State0) ->
              censor_secrets(State0);
         (_Key, Value)->
              Value
      end,
      Status).

%% `format_status/2' is deprecated as of OTP 25.0
format_status(_Opt, [_PDict, State0]) ->
    State = censor_secrets(State0),
    [{data, [{"State", State}]}].

censor_secrets(State0 = #state{producer_opts = Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := _}}}) ->
    State0#state{producer_opts = Opts0#{conn_opts := ConnOpts0#{auth_data := "******"}}};
censor_secrets(State) ->
    State.

restart_producer_later(Partition, PartitionTopic) ->
    erlang:send_after(?T_RETRY_START, self(), {restart_producer, Partition, PartitionTopic}).

create_partition_topic(Topic, 0) ->
    [{Topic, 0}];
create_partition_topic(Topic, Partitions) ->
    lists:map(fun(Partition) ->
        {lists:concat([Topic, "-partition-", Partition]), Partition}
    end, lists:seq(0, Partitions-1)).

get_name(ProducerOpts) -> maps:get(name, ProducerOpts, ?MODULE).

log_error(Fmt, Args) ->
    do_log(error, Fmt, Args).

do_log(Level, Fmt, Args) ->
    logger:log(Level, "[pulsar_producers] " ++ Fmt, Args, #{domain => [pulsar, producers]}).

start_producer(ClientId, Partition, PartitionTopic, State) ->
    try
        case pulsar_client_manager:lookup_topic(ClientId, PartitionTopic, ?LOOKUP_TOPIC_TIMEOUT) of
            {ok, #{ brokerServiceUrl := BrokerServiceURL
                  , proxy_through_service_url := IsProxy
                  }} ->
                do_start_producer(State, ClientId, Partition, PartitionTopic,
                    BrokerServiceURL, IsProxy);
            {error, Reason0} ->
                log_error("Lookup topic failed: ~p", [Reason0]),
                restart_producer_later(Partition, PartitionTopic),
                State
        end
    catch
        Error : Reason : Stacktrace ->
            log_error("Start producer error: ~p, ~p", [Error, {Reason, Stacktrace}]),
            restart_producer_later(Partition, PartitionTopic),
            State
    end.

do_start_producer(#state{
        client_id = ClientId,
        producers = Producers,
        workers = Workers,
        producer_opts = ProducerOpts0,
        producer_id = ProducerID} = State, Pid, Partition, PartitionTopic, BrokerServiceURL, IsProxy) ->
    NextID = next_producer_id(ProducerID),
    {AlivePulsarURL, ProxyToBrokerURL} = case IsProxy of
            false -> {BrokerServiceURL, undefined};
            true ->
                {ok, URL} = pulsar_client_manager:get_alive_pulsar_url(
                              Pid, ?GET_ALIVE_PULSAR_URL_TIMEOUT),
                {URL, BrokerServiceURL}
        end,
    ParentPid = self(),
    StateObserverCallback = {fun ?MODULE:report_status/2, [ParentPid]},
    ProducerOpts = ProducerOpts0#{ producer_id => NextID
                                 , clientid => ClientId
                                 , state_observer_callback => StateObserverCallback
                                 , parent_id => ParentPid
                                 },
    {ok, ProducerPid} = pulsar_producer:start_link(PartitionTopic,
        AlivePulsarURL, ProxyToBrokerURL, ProducerOpts),
    ProducerState = idle,
    ets:insert(Workers, {Partition, ProducerPid, ProducerState}),
    State#state{
        producers = maps:put(ProducerPid, {Partition, PartitionTopic}, Producers),
        producer_id = NextID
    }.

next_producer_id(?MAX_PRODUCER_ID) -> 0;
next_producer_id(ProducerID) ->
    ProducerID + 1.

%% Called by `pulsar_producer' when there's a state change.
report_status(ProducerState, ParentPid) ->
    ProducerPid = self(),
    ParentPid ! {producer_state_change, ProducerPid, ProducerState},
    ok.
