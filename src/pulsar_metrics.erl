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



