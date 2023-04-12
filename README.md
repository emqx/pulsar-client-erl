# Apache Pulsar client for Erlang/Elixir

[![Coverage Status](https://coveralls.io/repos/github/emqx/pulsar-client-erl/badge.svg?branch=master)](https://coveralls.io/github/emqx/pulsar-client-erl?branch=master)

## Example Code

### Async Produce

```
{ok, Pid} = pulsar:ensure_supervised_client('client1', [{"127.0.0.1", 6650}], #{}),
{ok, Producers} = pulsar:ensure_supervised_producers('client1', "persistent://public/default/test", #{}),
{ok, WorkerPid} = pulsar:send(Producers, [#{key => "key", value => <<"hello">>}]),
ok = pulsar:stop_and_delete_supervised_producers(Producers),
ok = pulsar:stop_and_delete_supervised_client('client1').
```
### Sync Produce

```
{ok, Pid} = pulsar:ensure_supervised_client('client1', [{"127.0.0.1", 6650}], #{}),
{ok, Producers} = pulsar:ensure_supervised_producers('client1', "persistent://public/default/test", #{}),
ok = pulsar:send_sync(Producers, [#{key => "key", value => <<"hello">>}], 5000),
ok = pulsar:stop_and_delete_supervised_producers(Producers),
ok = pulsar:stop_and_delete_supervised_client('client1').
```

### Supervised Producers

```
application:ensure_all_started(pulsar).
Client = 'client1',
Opts = #{},
{ok, _ClientPid} = pulsar:ensure_supervised_client(Client, [{"127.0.0.1", 6650}], Opts),
Callback = fun(SendReceipt) ->
            io:format("message produced receipt:~p~n",[SendReceipt]),
            ok
         end,
ProducerOpts = #{batch_size => 1000, callback => Callback, tcp_opts => []},
{ok, Producers} = pulsar:ensure_supervised_producers(Client, <<"persistent://public/default/test">>, ProducerOpts),
ok = pulsar:stop_and_delete_supervised_producers(Producers),
ok = pulsar:stop_and_delete_supervised_client('client1').
```

### Example of consumer to Pulsar

```erlang
-module(my_subscriber).

-export([start/0]).
-export([init/2, handle_message/3]). %% callback api

%% behaviour callback
init(Topic, _Arg) -> {ok, []}.

%% behaviour callback
-spec handle_message(map(), list(), any()) -> {ok, 'Individual' , any()} | {ok, 'Cumulative' , any()}.
handle_message(Msg, Payloads, CState) ->
    #{consumer_id := ConsumerId,
      message_id := #{entryId := EntryId,ledgerId := LedgerId}} = Msg,
    io:format("Receive payloads:~p~n", [Payloads]),
    {ok, 'Individual', State}.

-spec start(atom()) -> {ok, pid()}.
start() ->
    Client = 'client1',
    Topic = "persistent://public/default/test",
    ConsumerOpts = #{cb_init_args => [],
                     cb_module => ?MODULE,
                     sub_type => 'Shared',
                     subscription => "SubscriptionName",
                     max_consumer_num => 1,
                     name = 'consumer1'},
    application:ensure_all_started(pulsar),
    {ok, Pid} = pulsar:ensure_supervised_client(Client, [{"127.0.0.1", 6650}], #{}),
    pulsar:ensure_supervised_consumers(Client, Topic, ConsumerOpts).
```

## License

Apache License Version 2.0
