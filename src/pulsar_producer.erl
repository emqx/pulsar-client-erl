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

-module(pulsar_producer).

-behaviour(gen_statem).

-export([ send/2
        , send_sync/2
        , send_sync/3
        ]).

-export([ start_link/3
        , idle/3
        , connecting/3
        , connected/3
        ]).

-export([ callback_mode/0
        , init/1
        , terminate/3
        , code_change/4
        ]).

callback_mode() -> [state_functions].

-define(TIMEOUT, 60000).

-define(MAX_QUE_ID, 4294836225).
-define(MAX_SEQ_ID, 18445618199572250625).

-define(TCPOPTIONS, [
    binary,
    {packet,    raw},
    {reuseaddr, true},
    {nodelay,   true},
    {active,    true},
    {reuseaddr, true},
    {send_timeout, ?TIMEOUT}]).

-record(state, {partitiontopic,
                broker_service_url,
                sock,
                request_id = 1,
                producer_id = 1,
                sequence_id = 1,
                producer_name,
                opts = [],
                callback,
                batch_size = 0,
                requests = #{},
                last_bin = <<>>}).

start_link(PartitionTopic, BrokerServiceUrl, ProducerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, BrokerServiceUrl, ProducerOpts], []).


send(Pid, Message) ->
    gen_statem:cast(Pid, {send, Message}).

send_sync(Pid, Message) ->
    send_sync(Pid, Message, 5000).

send_sync(Pid, Message, Timeout) ->
    gen_statem:call(Pid, {send, Message}, Timeout).


%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([PartitionTopic, BrokerServiceUrl, ProducerOpts]) when is_binary(BrokerServiceUrl)->
    init([PartitionTopic, binary_to_list(BrokerServiceUrl), ProducerOpts]);
init([PartitionTopic, BrokerServiceUrl, ProducerOpts]) ->
    Compression = maps:get(compression, ProducerOpts, no_compression),
    erlang:put(compression, Compression),
    State = #state{partitiontopic = PartitionTopic,
                   producer_id = maps:get(producer_id, ProducerOpts),
                   producer_name = maps:get(producer_name, ProducerOpts, pulsar_producer),
                   callback = maps:get(callback, ProducerOpts, undefined),
                   batch_size = maps:get(batch_size, ProducerOpts, 0),
                   broker_service_url = BrokerServiceUrl,
                   opts = maps:get(tcp_opts, ProducerOpts, [])},
    self() ! connecting,
    {ok, idle, State}.

idle(_, connecting, State = #state{opts = Opts, broker_service_url = BrokerServiceUrl}) ->
    {Host, Port} = format_url(BrokerServiceUrl),
    case gen_tcp:connect(Host, Port, merge_opts(Opts, ?TCPOPTIONS), ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            connect(Sock),
            {next_state, connecting, State#state{sock = Sock}};
        Error ->
            log_error("connect error: ~p, server: ~p~n", [Error, {Host, Port}]),
            {stop, {shutdown, Error}, State}
    end.

connecting(_EventType, {tcp, _, Bin}, State) ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State);

connecting({call, From}, _, State) ->
    {keep_state, State, [{reply, From ,{fail, producer_connecting}}]};

connecting(cast, {send, _Message}, _State) ->
    keep_state_and_data.

connected(_EventType, {tcp_closed, Sock}, State = #state{sock = Sock, partitiontopic = Topic}) ->
    log_error("connection closed by peer, topic: ~p~n", [Topic]),
    erlang:send_after(5000, self(), connecting),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {tcp, _, Bin}, State = #state{last_bin = LastBin}) ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);

connected(_EventType, ping, State = #state{sock = Sock}) ->
    ping(Sock),
    {keep_state, State};

connected({call, From}, {send, Message}, State = #state{sequence_id = SequenceId, requests = Reqs}) ->
    send_batch_payload(Message, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, From, Reqs)})};

connected(cast, {send, Message}, State = #state{batch_size = BatchSize, sequence_id = SequenceId, requests = Reqs}) ->
    BatchMessage = Message ++ collect_send_calls(BatchSize),
    send_batch_payload(BatchMessage, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, {SequenceId, length(BatchMessage)}, Reqs)})};

connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, _State) ->
    ok.

parse({undefined, Bin}, State) ->
    {keep_state, State#state{last_bin = Bin}};
parse({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#state{last_bin = <<>>});
parse({Cmd, LastBin}, State) ->
    State2 = case handle_response(Cmd, State) of
        {_, State1} -> State1;
        {_, _, State1} -> State1
    end,
    parse(pulsar_protocol_frame:parse(LastBin), State2).

handle_response({connected, _ConnectedData}, State = #state{sock = Sock,
                                                            request_id = RequestId,
                                                            producer_id = ProId,
                                                            partitiontopic = Topic}) ->
    start_keepalive(),
    create_producer(Sock, Topic, RequestId, ProId),
    {keep_state, next_request_id(State)};

handle_response({producer_success, #{producer_name := ProName}}, State) ->
    {next_state, connected, State#state{producer_name = ProName}};

handle_response({pong, #{}}, State) ->
    start_keepalive(),
    {keep_state, State};
handle_response({ping, #{}}, State = #state{sock = Sock}) ->
    pong(Sock),
    {keep_state, State};
handle_response({close_producer, #{}}, State = #state{partitiontopic = Topic}) ->
    log_error("Close producer: ~p~n", [Topic]),
    {stop, {shutdown, closed_producer}, State};
handle_response({send_receipt, Resp = #{sequence_id := SequenceId}},
                State = #state{callback = undefined, requests = Reqs}) ->
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            {keep_state, State};
        SequenceId ->
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        {SequenceId, _} ->
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        From ->
            gen_statem:reply(From, Resp),
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}}
    end;
handle_response({send_receipt, Resp = #{sequence_id := SequenceId}},
                State = #state{callback = Callback, requests = Reqs}) ->
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            case Callback of
                {M, F, A} -> erlang:apply(M, F, [Resp] ++ A);
                _ -> Callback(Resp)
            end,
            {keep_state, State};
        SequenceId ->
            case Callback of
                {M, F, A} -> erlang:apply(M, F, [Resp] ++ A);
                _ -> Callback(Resp)
            end,
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        {SequenceId, BatchLen} ->
            case Callback of
                {M, F, A} ->
                    lists:foreach(fun(_) ->
                        erlang:apply(M, F, [Resp] ++ A)
                    end,  lists:seq(1, BatchLen));
                _ ->
                    lists:foreach(fun(_) ->
                        Callback(Resp)
                    end,  lists:seq(1, BatchLen))
            end,
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        From ->
            gen_statem:reply(From, Resp),
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}}
    end;


handle_response({error, #{error := Error, message := Msg}}, State) ->
    log_error("Response error:~p, msg:~p~n", [Error, Msg]),
    {stop, {shutdown, Error}, State};

handle_response(Msg, State) ->
    log_error("Receive unknown message:~p~n", [Msg]),
    {keep_state, State}.

connect(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:connect()).

send_batch_payload(Messages, #state{
                                    partitiontopic = PartitionTopic,
                                    sequence_id = SequenceId,
                                    producer_id = ProducerId,
                                    producer_name = ProducerName,
                                    sock = Sock}) ->
    Len = length(Messages),
    Send = case Len > 1 of
        true ->
            #{producer_id => ProducerId,
              sequence_id => SequenceId,
              num_messages => Len};
        false ->
            #{producer_id => ProducerId,
              sequence_id => SequenceId}
    end,
    Metadata = #{producer_name => ProducerName,
                 sequence_id => SequenceId,
                 publish_time => erlang:system_time(millisecond),
                 compression => 'NONE'},
    {Metadata1, BatchMessage} = batch_message(Metadata, Len, Messages),
    gen_tcp:send(Sock, pulsar_protocol_frame:send(Send, Metadata1, BatchMessage)),
    pulsar_metrics:send(PartitionTopic, Len).

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

ping(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:pong()).

create_producer(Sock, Topic, RequestId, ProducerId) ->
    Producer = #{
        topic => Topic,
        producer_id => ProducerId,
        request_id => RequestId
    },
    gen_tcp:send(Sock, pulsar_protocol_frame:create_producer(Producer)).

batch_message(Metadata, Len, Messages) ->
    Metadata1 = Metadata#{num_messages_in_batch => Len},
    Compression = case erlang:get(compression) of
        snappy -> 'SNAPPY';
        zlib -> 'ZLIB';
        _ -> 'NONE'
    end,
    BatchMessage = lists:foldl(fun(#{key := Key, value := Message}, Acc) ->
        Message1 = maybe_compression(Message, Compression),
        SMetadata = case Key =:= undefined of
            true  -> #{payload_size => size(Message1), compression => Compression};
            false -> #{payload_size => size(Message1), partition_key => Key, compression => Compression}
        end,
        SMetadataBin = pulsar_api:encode_msg(SMetadata, 'SingleMessageMetadata'),
        SMetadataBinSize = size(SMetadataBin),
        <<Acc/binary, SMetadataBinSize:32, SMetadataBin/binary, Message1/binary>>
    end, <<>>, Messages),
    {Metadata1, BatchMessage}.


collect_send_calls(0) ->
    [];
collect_send_calls(Cnt) when Cnt > 0 ->
    collect_send_calls(Cnt, []).

collect_send_calls(0, Acc) ->
    lists:reverse(Acc);

collect_send_calls(Cnt, Acc) ->
    receive
        {'$gen_cast', {send, Messages}} ->
            collect_send_calls(Cnt - 1, Messages ++ Acc)
    after 0 ->
          lists:reverse(Acc)
    end.

tune_buffer(Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]} = inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

merge_opts(Defaults, Options) ->
    lists:foldl(
        fun({Opt, Val}, Acc) ->
                case lists:keymember(Opt, 1, Acc) of
                    true ->
                        lists:keyreplace(Opt, 1, Acc, {Opt, Val});
                    false ->
                        [{Opt, Val}|Acc]
                end;
            (Opt, Acc) ->
                case lists:member(Opt, Acc) of
                    true -> Acc;
                    false -> [Opt | Acc]
                end
        end, Defaults, Options).

format_url("pulsar://" ++ Url) ->
    [Host, Port] = string:tokens(Url, ":"),
    {Host, list_to_integer(Port)};
format_url(_) ->
    {"127.0.0.1", 6650}.

next_request_id(State = #state{request_id = ?MAX_QUE_ID}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

next_sequence_id(State = #state{sequence_id = ?MAX_SEQ_ID}) ->
    State#state{sequence_id = 1};
next_sequence_id(State = #state{sequence_id = SequenceId}) ->
    State#state{sequence_id = SequenceId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-producer] " ++ Fmt, Args).

maybe_compression(Bin, 'SNAPPY') ->
    {ok, Compressed} = snappyer:compress(Bin),
    Compressed;

maybe_compression(Bin, 'ZLIB') ->
    zlib:compress(Bin);

maybe_compression(Bin, _) ->
    iolist_to_binary(Bin).
