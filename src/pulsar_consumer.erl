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
                consumer_id = 1,
                consumer_name,
                opts = [],
                cb_module,
                cb_state,
                last_bin = <<>>,
                flow,
                flow_rate}).

-define (FRAME, pulsar_protocol_frame).

start_link(PartitionTopic, BrokerServiceUrl, ConsumerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, BrokerServiceUrl, ConsumerOpts], []).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([PartitionTopic, BrokerServiceUrl, ConsumerOpts]) when is_binary(BrokerServiceUrl) ->
    init([PartitionTopic, binary_to_list(BrokerServiceUrl), ConsumerOpts]);
init([PartitionTopic, BrokerServiceUrl, ConsumerOpts]) ->
    {CbModule, ConsumerOpts1} = maps:take(cb_module, ConsumerOpts),
    {CbInitArg, ConsumerOpts2} = maps:take(cb_init_args, ConsumerOpts1),
    {ok, CbState} = CbModule:init(PartitionTopic, CbInitArg),
    State = #state{partitiontopic = PartitionTopic,
                   cb_module = CbModule,
                   cb_state = CbState,
                   opts = ConsumerOpts2,
                   broker_service_url = BrokerServiceUrl,
                   flow = maps:get(flow, ConsumerOpts, 1000)},
    self() ! connecting,
    {ok, idle, State}.

idle(_, connecting, State = #state{broker_service_url = BrokerServiceUrl}) ->
    {Host, Port} = format_url(BrokerServiceUrl),
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
        {ok, Sock} ->
            gen_tcp:controlling_process(Sock, self()),
            connect(Sock),
            {next_state, connecting, State#state{sock = Sock}};
        Error ->
            {stop, {shutdown, Error}, State}
    end;
idle(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

connecting(_EventType, {tcp, _, Bin}, State) ->
    {Cmd, _} = ?FRAME:parse(Bin),
    handle_response(Cmd, State).

connected(_EventType, {tcp_closed, Sock}, State = #state{sock = Sock, partitiontopic = Topic}) ->
    log_error("TcpClosed consumer: ~p~n", [Topic]),
    erlang:send_after(5000, self(), connecting),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {tcp, _, Bin}, State = #state{last_bin = LastBin}) ->
    parse(?FRAME:parse(<<LastBin/binary, Bin/binary>>), State);

connected(_EventType, ping, State = #state{sock = Sock}) ->
    ping(Sock),
    {keep_state, State};

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
    parse(?FRAME:parse(LastBin), State2).

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
handle_response({ping, #{}}, State = #state{sock = Sock}) ->
    pong(Sock),
    {keep_state, State};
handle_response({subscribe_success, #{}}, State = #state{sock = Sock,
                                                         consumer_id = ConsumerId,
                                                         flow = Flow}) ->
    set_flow(Sock, ConsumerId, Flow),
    {keep_state, State};
handle_response({message, Msg, Payloads}, State = #state{sock = Sock,
                                                         consumer_id = ConsumerId,
                                                         cb_module = CbModule,
                                                         cb_state = CbState}) ->
    case CbModule:handle_message(Msg, Payloads, CbState) of
        {ok, AckType, NCbState} ->
            ack(Sock, ConsumerId, AckType, Msg),
            NState = maybe_set_flow(length(Payloads), State),
            {keep_state, NState#state{cb_state = NCbState}};
        _ ->
            {keep_state, State}
    end;

handle_response({close_consumer, #{}}, State = #state{partitiontopic = Topic}) ->
    log_error("Close consumer: ~p~n", [Topic]),
    {stop, {shutdown, close_consumer}, State};
handle_response(Msg, State) ->
    log_error("Receive unknown message:~p~n", [Msg]),
    {keep_state, State}.

connect(Sock) ->
    Conn = #{client_version => "Pulsar-Client-Erlang-v0.0.1",
             protocol_version => 6},
    gen_tcp:send(Sock, ?FRAME:connect(Conn)).

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

ping(Sock) ->
    gen_tcp:send(Sock, ?FRAME:ping()).

pong(Sock) ->
    gen_tcp:send(Sock, ?FRAME:pong()).

subscribe(Sock, Topic, RequestId, ConsumerId, Opts) ->
    SubType = maps:get(sub_type, Opts, 'Shared'),
    Subscription = maps:get(subscription, Opts, "my-subscription-name"),
    SubInfo = #{
        topic => Topic,
        subscription => Subscription,
        subType => SubType,
        consumer_id => ConsumerId,
        request_id => RequestId
    },
    gen_tcp:send(Sock, ?FRAME:create_subscribe(SubInfo)).

maybe_set_flow(Len, State = #state{sock = Sock,
                                   consumer_id = ConsumerId,
                                   flow = Flow,
                                   opts = Opts}) ->
    InitFlow = maps:get(flow, Opts, 1000),
    case (InitFlow div 2) > Flow of
        true ->
            set_flow(Sock, ConsumerId, InitFlow - (Flow - Len)),
            State#state{flow = InitFlow};
        false ->
            State#state{flow = Flow - Len}
    end.

set_flow(Sock, ConsumerId, FlowSize) ->
    FlowInfo = #{
        consumer_id => ConsumerId,
        messagePermits => FlowSize
    },
    gen_tcp:send(Sock, ?FRAME:set_flow(FlowInfo)).

ack(Sock, ConsumerId, AckType, Msg) ->
    Ack = #{
        consumer_id => ConsumerId,
        ack_type => AckType,
        message_id => [maps:get(message_id, Msg)]
    },
    gen_tcp:send(Sock, ?FRAME:ack(Ack)).

format_url("pulsar://" ++ Url) ->
    [Host, Port] = string:tokens(Url, ":"),
    {Host, list_to_integer(Port)};
format_url(_) ->
    {"127.0.0.1", 6650}.

next_request_id(State = #state{request_id = ?MAX_QUE_ID}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

log_error(Fmt, Args) -> error_logger:error_msg(Fmt, Args).
