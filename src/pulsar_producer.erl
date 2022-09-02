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

-type statem() :: idle | connecting | connected.
-type send_receipt() :: #{ sequence_id := integer()
                         , producer_id := integer()
                         , highest_sequence_id := integer()
                         , message_id := map()
                         , any() => term()
                         }.

-define(TIMEOUT, 60000).

-define(MAX_REQ_ID, 4294836225).
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

callback_mode() -> [state_functions].

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts], []).

-spec send(gen_statem:server_ref(), [pulsar:message()]) -> ok.
send(Pid, Message) ->
    gen_statem:cast(Pid, {send, Message}).

-spec send_sync(gen_statem:server_ref(), [pulsar:message()]) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Message) ->
    send_sync(Pid, Message, 5_000).

-spec send_sync(gen_statem:server_ref(), [pulsar:message()], timeout()) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Message, Timeout) ->
    gen_statem:call(Pid, {send, Message}, Timeout).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts]) ->
    {Transport, BrokerServer} = pulsar_utils:parse_url(Server),
    State = #state{
        partitiontopic = PartitionTopic,
        producer_id = maps:get(producer_id, ProducerOpts),
        producer_name = maps:get(producer_name, ProducerOpts, pulsar_producer),
        callback = maps:get(callback, ProducerOpts, undefined),
        batch_size = maps:get(batch_size, ProducerOpts, 0),
        broker_server = BrokerServer,
        opts = pulsar_utils:maybe_enable_ssl_opts(Transport, ProducerOpts)
    },
    %% use process dict to avoid the trouble of relup
    erlang:put(proxy_to_broker_url, ProxyToBrokerUrl),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

%% idle state
-spec idle(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
idle(_, do_connect, State) ->
    do_connect(State);
idle({call, From}, _Event, _State) ->
    {keep_state_and_data, [{reply, From, {error, producer_disconnected}}]};
idle(cast, _Event, _State) ->
    {keep_state_and_data, [postpone]};
idle(_EventType, _Event, _State) ->
    keep_state_and_data.

%% connecting state
-spec connecting(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
connecting(_, do_connect, State) ->
    do_connect(State);
connecting(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State);
connecting({call, From}, _, State) ->
    {keep_state, State, [{reply, From, {error, producer_connecting}}]};
connecting(cast, {send, _Message}, _State) ->
    {keep_state_and_data, [postpone]}.

%% connected state
-spec connected(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
connected(_, do_connect, _State) ->
    keep_state_and_data;
connected(_EventType, {InetClose, Sock}, State = #state{sock = Sock, partitiontopic = Topic})
        when InetClose == tcp_closed; InetClose == ssl_closed ->
    log_error("connection closed by peer, topic: ~p~n", [Topic]),
    erlang:send_after(5000, self(), do_connect),
    {next_state, idle, State#state{sock = undefined}};
connected(_EventType, {InetError, _Sock, Reason}, State = #state{partitiontopic = Topic})
        when InetError == tcp_error; InetError == ssl_error ->
    log_error("connection error on topic: ~p, error: ~p~n", [Topic, Reason]),
    erlang:send_after(5000, self(), do_connect),
    {next_state, idle, State#state{sock = undefined}};
connected(_EventType, {Inet, _, Bin}, State = #state{last_bin = LastBin})
        when Inet == tcp; Inet == ssl ->
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
            pulsar_socket:send_connect_packet(Sock,
                pulsar_utils:maybe_add_proxy_to_broker_url_opts(Opts,
                    erlang:get(proxy_to_broker_url))),
            {next_state, connecting, State#state{sock = Sock}};
        {error, _Reason} = Error ->
             {stop, {shutdown, Error}, State}
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, _State) ->
    ok.

parse({incomplete, Bin}, State) ->
    {keep_state, State#state{last_bin = Bin}};
parse({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#state{last_bin = <<>>});
parse({Cmd, LastBin}, State) ->
    State2 = case handle_response(Cmd, State) of
        {_, State1} -> State1;
        {_, _, State1} -> State1
    end,
    parse(pulsar_protocol_frame:parse(LastBin), State2).

-spec handle_response(_EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
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
                State = #state{callback = Callback, requests = Reqs}) ->
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            _ = invoke_callback(Callback, Resp),
            {keep_state, State};
        SequenceId ->
            _ = invoke_callback(Callback, Resp),
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        {SequenceId, BatchLen} ->
            _ = invoke_callback(Callback, Resp, BatchLen),
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

next_request_id(State = #state{request_id = ?MAX_REQ_ID}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

next_sequence_id(State = #state{sequence_id = ?MAX_SEQ_ID}) ->
    State#state{sequence_id = 1};
next_sequence_id(State = #state{sequence_id = SequenceId}) ->
    State#state{sequence_id = SequenceId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-producer] " ++ Fmt, Args).

invoke_callback(Callback, Resp) ->
    invoke_callback(Callback, Resp, _BatchLen = 1).

invoke_callback(_Callback = undefined, _Resp, _BatchLen) ->
    ok;
invoke_callback({M, F, A}, Resp, BatchLen) ->
    lists:foreach(
      fun(_) ->
        erlang:apply(M, F, [Resp] ++ A)
      end,  lists:seq(1, BatchLen));
invoke_callback(Callback, Resp, BatchLen) when is_function(Callback, 1) ->
    lists:foreach(
      fun(_) ->
        Callback(Resp)
      end,  lists:seq(1, BatchLen)).
