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
-module(pulsar_consumer).

-behaviour(gen_statem).

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

-define(MAX_QUE_ID, 4294836225).

-record(state, {partitiontopic,
                broker_server,
                sock,
                request_id = 1,
                consumer_id = 1,
                consumer_name,
                opts = #{},
                cb_module,
                cb_state,
                last_bin = <<>>,
                flow,
                flow_rate}).

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ConsumerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, Server, ProxyToBrokerUrl, ConsumerOpts], []).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([PartitionTopic, Server, ProxyToBrokerUrl, ConsumerOpts]) ->
    {CbModule, ConsumerOpts1} = maps:take(cb_module, ConsumerOpts),
    {CbInitArg, ConsumerOpts2} = maps:take(cb_init_args, ConsumerOpts1),
    {ok, CbState} = CbModule:init(PartitionTopic, CbInitArg),
    {Transport, BrokerServer} = pulsar_utils:parse_url(Server),
    State = #state{consumer_id = maps:get(consumer_id, ConsumerOpts),
                   partitiontopic = PartitionTopic,
                   cb_module = CbModule,
                   cb_state = CbState,
                   opts = pulsar_utils:maybe_enable_ssl_opts(Transport, ConsumerOpts2),
                   broker_server = BrokerServer,
                   flow = maps:get(flow, ConsumerOpts, 1000)},
    %% use process dict to avoid the trouble of relup
    erlang:put(proxy_to_broker_url, ProxyToBrokerUrl),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

idle(_, do_connect, State) ->
    do_connect(State);
idle({call, _From}, _Event, _State) ->
    keep_state_and_data;
idle(cast, _Event, _State) ->
    {keep_state_and_data, [postpone]};
idle(_EventType, _Event, _State) ->
    keep_state_and_data.

connecting(_, do_connect, State) ->
    do_connect(State);

connecting(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State).

connected(_, do_connect, _State) ->
    keep_state_and_data;

connected(_EventType, {InetClose, Sock}, State = #state{sock = Sock, partitiontopic = Topic})
        when InetClose == tcp_closed; InetClose == ssl_closed ->
    log_error("tcp closed on topic: ~p~n", [Topic]),
    erlang:send_after(5000, self(), do_connect),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {InetError, _Sock, Reason}, State = #state{partitiontopic = Topic})
        when InetError == tcp_error; InetError == ssl_error ->
    log_error("tcp error on topic: ~p, error: ~p~n", [Topic, Reason]),
    erlang:send_after(5000, self(), do_connect),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {Inet, _, Bin}, State = #state{last_bin = LastBin})
        when Inet == tcp; Inet == ssl ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);

connected(_EventType, ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {keep_state, State};

connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

do_connect(State = #state{broker_server = {Host, Port}, opts = Opts}) ->
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

handle_response({connected, _ConnectedData}, State = #state{sock = Sock,
                                                            request_id = RequestId,
                                                            consumer_id = ConsumerId,
                                                            partitiontopic = Topic,
                                                            opts = Opts}) ->
    start_keepalive(),
    subscribe(Sock, Topic, RequestId, ConsumerId, Opts),
    {next_state, connected, next_request_id(State)};

handle_response({pong, #{}}, State) ->
    start_keepalive(),
    {keep_state, State};
handle_response({ping, #{}}, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:pong(Sock, Opts),
    {keep_state, State};
handle_response({subscribe_success, #{}}, State = #state{
        sock = Sock, opts = Opts,
        consumer_id = ConsumerId,
        flow = Flow}) ->
    pulsar_socket:send_set_flow_packet(Sock, ConsumerId, Flow, Opts),
    {keep_state, State};
handle_response({message, Msg, Payloads}, State = #state{
            partitiontopic = PartitionTopic,
            sock = Sock,
            consumer_id = ConsumerId,
            cb_module = CbModule,
            cb_state = CbState,
            opts = Opts
        }) ->
    pulsar_metrics:recv(PartitionTopic, length(Payloads)),
    case CbModule:handle_message(Msg, Payloads, CbState) of
        {ok, AckType, NCbState} ->
            pulsar_socket:send_ack_packet(Sock, ConsumerId, AckType,
                [maps:get(message_id, Msg)], Opts),
            NState = maybe_set_flow(length(Payloads), State),
            {keep_state, NState#state{cb_state = NCbState}};
        _ ->
            {keep_state, State}
    end;

handle_response({close_consumer, #{}}, State = #state{partitiontopic = Topic}) ->
    log_error("Close consumer: ~p~n", [Topic]),
    {stop, {shutdown, close_consumer}, State};
handle_response(Msg, State) ->
    log_error("Consumer Receive unknown message:~p~n", [Msg]),
    {keep_state, State}.

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

subscribe(Sock, Topic, RequestId, ConsumerId, Opts) ->
    SubType = maps:get(sub_type, Opts, 'Shared'),
    Subscription = maps:get(subscription, Opts, "my-subscription-name"),
    pulsar_socket:send_subscribe_packet(Sock, Topic, RequestId, ConsumerId, Subscription,
        SubType, Opts).

maybe_set_flow(Len, State = #state{sock = Sock,
                                   consumer_id = ConsumerId,
                                   flow = Flow,
                                   opts = Opts}) ->
    InitFlow = maps:get(flow, Opts, 1000),
    case (InitFlow div 2) > Flow of
        true ->
            pulsar_socket:send_set_flow_packet(Sock, ConsumerId, InitFlow - (Flow-Len), Opts),
            State#state{flow = InitFlow};
        false ->
            State#state{flow = Flow - Len}
    end.

next_request_id(State = #state{request_id = ?MAX_QUE_ID}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-consumer] " ++ Fmt, Args).
