%%%-------------------------------------------------------------------
%%% @author DDDHuang
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(pulsar_metrics).
-include("pulsar.hrl").
%% APIs
-export([all/0, all_detail/0]).

-export([send/2, recv/2]).

-export([
    %% gauges
    queuing_set/2,
    queuing_bytes_set/2,
    inflight_set/2,
    %% counters
    dropped_inc/1,
    dropped_inc/2,
    dropped_queue_full_inc/1,
    dropped_queue_full_inc/2
]).

-export([consumer/0, consumer/1, producer/0, producer/1]).

all() ->
    [{producer, producer()}, {consumer, consumer()}].

all_detail() ->
    get_all().

producer() ->
    get_stat(producer).
producer(Topic) ->
    get_stat(producer, Topic).

consumer() ->
    get_stat(consumer).
consumer(Topic) ->
    get_stat(consumer, Topic).

%%-------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------

%% Gauges (value can go both up and down):
%% --------------------------------------

%% @doc Count of requests (batches of messages) that are currently queuing. [Gauge]
queuing_set(Config, Val) ->
    telemetry:execute([pulsar, queuing],
                      #{gauge_set => Val},
                      telemetry_metadata(Config)).

%% @doc Number of bytes (RAM and/or disk) currently queuing. [Gauge]
queuing_bytes_set(Config, Val) ->
    telemetry:execute([pulsar, queuing_bytes],
                      #{gauge_set => Val},
                      telemetry_metadata(Config)).

%% @doc Count of messages that were sent asynchronously but ACKs are not
%% received. [Gauge]
inflight_set(Config, Val) ->
    telemetry:execute([pulsar, inflight],
                      #{gauge_set => Val},
                      telemetry_metadata(Config)).

%% Counters (value can only got up):
%% --------------------------------------

%% @doc Count of messages dropped due to TTL.
dropped_inc(Config) ->
    dropped_inc(Config, 1).

dropped_inc(Config, Val) ->
    telemetry:execute([pulsar, dropped],
                      #{counter_inc => Val, reason => expired},
                      telemetry_metadata(Config)).

%% @doc Count of messages dropped because the queue was full.
dropped_queue_full_inc(Config) ->
    dropped_queue_full_inc(Config, 1).

dropped_queue_full_inc(Config, Val) ->
    telemetry:execute([pulsar, dropped],
                      #{counter_inc => Val, reason => queue_full},
                      telemetry_metadata(Config)).

%%-------------------------------------------------------------------------
%% internal api
%%-------------------------------------------------------------------------
send(Topic, Inc) ->
    ok = bump_counter({producer, all}, Inc),
    ok = bump_counter({producer, Topic}, Inc).

recv(Topic, Inc) ->
    ok = bump_counter({consumer, all}, Inc),
    ok = bump_counter({consumer, Topic}, Inc).

%%-------------------------------------------------------------------------
%% internal function
%%-------------------------------------------------------------------------
get_stat(ProducerOrConsumer) ->
    get_counter({ProducerOrConsumer, all}).
get_stat(ProducerOrConsumer, Topic) ->
    get_counter({ProducerOrConsumer, Topic}).

bump_counter(Key, Inc) ->
    try _ = ets:update_counter(?PULSAR_METRICS_ETS, Key, Inc, {Key, 0}), ok
    catch _ : _ -> ok
    end.

get_counter(Key) ->
    case ets:lookup(?PULSAR_METRICS_ETS, Key) of
        [] -> 0;
        [{_, Value}] -> Value
    end.

get_all()->
    try _ = ets:tab2list(?PULSAR_METRICS_ETS)
    catch _ : _ -> []
    end.

telemetry_metadata(Config) ->
    maps:get(telemetry_metadata, Config).
