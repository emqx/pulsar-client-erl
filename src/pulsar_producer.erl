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

-export([ start_link/4
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
                broker_server,
                sock,
                request_id = 1,
                producer_id = 1,
                sequence_id = 1,
                producer_name,
                opts = #{},
                callback,
                batch_size = 0,
                requests = #{},
                last_bin = <<>>}).

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts], []).

send(Pid, Message) ->
    gen_statem:cast(Pid, {send, Message}).

send_sync(Pid, Message) ->
    send_sync(Pid, Message, 5000).

send_sync(Pid, Message, Timeout) ->
    gen_statem:call(Pid, {send, Message}, Timeout).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts]) ->
    State = #state{partitiontopic = PartitionTopic,
                   producer_id = maps:get(producer_id, ProducerOpts),
                   producer_name = maps:get(producer_name, ProducerOpts, pulsar_producer),
                   callback = maps:get(callback, ProducerOpts, undefined),
                   batch_size = maps:get(batch_size, ProducerOpts, 0),
                   broker_server = pulsar_protocol_frame:uri_to_host_port(Server),
                   opts = ProducerOpts},
    %% use process dict to avoid the trouble of relup
    erlang:put(proxy_to_broker_url, ProxyToBrokerUrl),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

idle(_, do_connect, State) ->
    do_connect(State).

connecting(_, do_connect, State) ->
    do_connect(State);

connecting(_EventType, {tcp, _, Bin}, State) ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State);

connecting({call, From}, _, State) ->
    {keep_state, State, [{reply, From ,{fail, producer_connecting}}]};

connecting(cast, {send, _Message}, _State) ->
    keep_state_and_data.

connected(_, do_connect, _State) ->
    keep_state_and_data;

connected(_EventType, {tcp_closed, Sock}, State = #state{sock = Sock, partitiontopic = Topic}) ->
    log_error("connection closed by peer, topic: ~p~n", [Topic]),
    erlang:send_after(5000, self(), do_connect),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {tcp, _, Bin}, State = #state{last_bin = LastBin}) ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);

connected(_EventType, ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {keep_state, State};

connected({call, From}, {send, Message}, State = #state{sequence_id = SequenceId, requests = Reqs}) ->
    send_batch_payload(Message, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, From, Reqs)})};

connected(cast, {send, Message}, State = #state{batch_size = BatchSize, sequence_id = SequenceId, requests = Reqs}) ->
    BatchMessage = Message ++ pulsar_utils:collect_send_calls(BatchSize),
    send_batch_payload(BatchMessage, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, {SequenceId, length(BatchMessage)}, Reqs)})};

connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

do_connect(State = #state{opts = Opts, broker_server = {Host, Port}}) ->
    case pulsar_socket:connect(Host, Port, Opts) of
        {ok, Sock} ->
            pulsar_socket:send_connect_packet(Sock, erlang:get(proxy_to_broker_url), Opts),
            {next_state, connecting, State#state{sock = Sock}};
        {error, _Reason} = Error ->
             {stop, {shutdown, Error}, State}
    end.

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

handle_response({connected, _ConnectedData}, State = #state{
        sock = Sock,
        opts = Opts,
        request_id = RequestId,
        producer_id = ProducerId,
        partitiontopic = Topic
    }) ->
    start_keepalive(),
    pulsar_socket:send_create_producer_packet(Sock, Topic, RequestId, ProducerId, Opts),
    {keep_state, next_request_id(State)};

handle_response({producer_success, #{producer_name := ProName}}, State) ->
    {next_state, connected, State#state{producer_name = ProName}};

handle_response({pong, #{}}, State) ->
    start_keepalive(),
    {keep_state, State};
handle_response({ping, #{}}, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:pong(Sock, Opts),
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

send_batch_payload(Messages, #state{
            partitiontopic = Topic,
            sequence_id = SequenceId,
            producer_id = ProducerId,
            producer_name = ProducerName,
            sock = Sock,
            opts = Opts
        }) ->
    pulsar_socket:send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId,
        ProducerName, Opts).

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

next_request_id(State = #state{request_id = ?MAX_QUE_ID}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

next_sequence_id(State = #state{sequence_id = ?MAX_SEQ_ID}) ->
    State#state{sequence_id = 1};
next_sequence_id(State = #state{sequence_id = SequenceId}) ->
    State#state{sequence_id = SequenceId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-producer] " ++ Fmt, Args).
