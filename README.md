# pulsar-client-erl
A Erlang client library for Apache Pulsar

## Example Code

### Async Produce

```
{ok, Pid} = pulsar:ensure_supervised_client('client1', [{"127.0.0.1", 6650}], #{}),
{ok, Producers} = pulsar:ensure_supervised_producers('client1', "persistent://public/default/turtle", #{}),
ok = pulsar:send(Producers, [#{key => "key", value => <<"hello">>}]),
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
            io:format("message produced  receipt:~p~n",[SendReceipt]),
            ok
         end,
ProducerOpts = #{batch_size => 1000, callback => Callback, tcp_opts => []},
{ok, Producers} = pulsar:ensure_supervised_producers(Client, <<"persistent://public/default/test">>, ProducerOpts),
ok = pulsar:stop_and_delete_supervised_producers(Producers),
ok = pulsar:stop_and_delete_supervised_client('client1').
```

## License

Apache License Version 2.0

## Author

EMQ X Team.