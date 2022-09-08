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

%% gen_statem API
-export([ callback_mode/0
        , init/1
        , terminate/3
        , code_change/4
        ]).

%% replayq API
-export([ queue_item_sizer/1
        , queue_item_marshaller/1
        ]).

-type statem() :: idle | connecting | connected.
-type send_receipt() :: #{ sequence_id := integer()
                         , producer_id := integer()
                         , highest_sequence_id := integer()
                         , message_id := map()
                         , any() => term()
                         }.
-type config() :: #{ replayq_dir := string()
                   , replayq_max_total_bytes => pos_integer()
                   , replayq_seg_bytes => pos_integer()
                   , replayq_offload_mode => boolean()
                   , producer_name => atom()
                   , callback => undefined | mfa() | fun((map()) -> ok)
                   , batch_size => non_neg_integer()
                   , retention_period => timeout()
                   }.
-export_type([ config/0
             ]).

-define(TIMEOUT, 60_000).
-define(RECONNECT_TIMEOUT, 5_000).

-define(MAX_REQ_ID, 4294836225).
-define(MAX_SEQ_ID, 18445618199572250625).

-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2_000_000_000).
-define(Q_ITEM(From, Ts, Messages), {From, Ts, Messages}).
-define(SEND_REQ(From, Messages), {send, From, Messages}).

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

callback_mode() -> [state_functions, state_enter].

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts) ->
    gen_statem:start_link(?MODULE, [PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts], []).

-spec send(gen_statem:server_ref(), [pulsar:message()]) -> ok.
send(Pid, Messages) ->
    From = undefined,
    erlang:send(Pid, ?SEND_REQ(From, Messages)),
    ok.

-spec send_sync(gen_statem:server_ref(), [pulsar:message()]) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Messages) ->
    send_sync(Pid, Messages, 5_000).

-spec send_sync(gen_statem:server_ref(), [pulsar:message()], timeout()) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Messages, Timeout) ->
    Caller = self(),
    MRef = erlang:monitor(process, Pid),
    erlang:send(Pid, ?SEND_REQ({Caller, MRef}, Messages)),
    receive
        {MRef, Response} ->
            erlang:demonitor(MRef, [flush]),
            Response;
        {'DOWN', MRef, process, Pid, Reason} ->
            error({producer_down, Reason})
    after
        Timeout ->
            erlang:demonitor(MRef, [flush]),
            receive
                {MRef, Response} ->
                    Response
            after
                0 ->
                    error(timeout)
            end
    end.

%%--------------------------------------------------------------------
%% gen_statem callback
%%--------------------------------------------------------------------

init([PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts0]) ->
    process_flag(trap_exit, true),
    {Transport, BrokerServer} = pulsar_utils:parse_url(Server),
    ProducerID = maps:get(producer_id, ProducerOpts0),
    ReplayqCfg0 =
        case maps:get(replayq_dir, ProducerOpts0, false) of
            false ->
                #{mem_only => true};
            BaseDir ->
                Dir = filename:join([BaseDir, PartitionTopic, integer_to_list(ProducerID)]),
                SegBytes = maps:get(replayq_seg_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_SEG_BYTES),
                Offload = maps:get(replayq_offload_mode, ProducerOpts0, false),
                #{dir => Dir, seg_bytes => SegBytes, offload => Offload}
        end,
    MaxTotalBytes = maps:get(replayq_max_total_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_LIMIT),
    ReplayqCfg =
        ReplayqCfg0#{ sizer => fun ?MODULE:queue_item_sizer/1
                    , marshaller => fun ?MODULE:queue_item_marshaller/1
                    , max_total_bytes => MaxTotalBytes
                    },
    Q = replayq:open(ReplayqCfg),
    ProducerOpts = maps:put(replayq, Q, ProducerOpts0),
    State = #state{
        partitiontopic = PartitionTopic,
        producer_id = ProducerID,
        producer_name = maps:get(producer_name, ProducerOpts, pulsar_producer),
        callback = maps:get(callback, ProducerOpts, undefined),
        batch_size = maps:get(batch_size, ProducerOpts, 0),
        broker_server = BrokerServer,
        opts = pulsar_utils:maybe_enable_ssl_opts(Transport, ProducerOpts)
    },
    %% use process dict to avoid the trouble of relup
    put(proxy_to_broker_url, ProxyToBrokerUrl),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

%% idle state
-spec idle(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
idle(enter, _OldState, _State) ->
    keep_state_and_data;
idle(_, do_connect, State) ->
    do_connect(State);
idle({call, From}, _Event, _State) ->
    {keep_state_and_data, [{reply, From, {error, producer_disconnected}}]};
idle(cast, _Event, _State) ->
    {keep_state_and_data, [postpone]};
idle(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
idle(_EventType, _Event, _State) ->
    keep_state_and_data.

%% connecting state
-spec connecting(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
connecting(enter, _OldState, _State) ->
    keep_state_and_data;
connecting(_, do_connect, State) ->
    do_connect(State);
connecting(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
connecting(info, {CloseEvent, _Sock}, State0 = #state{})
  when CloseEvent =:= tcp_closed; CloseEvent =:= ssl_closed ->
    try_close_socket(State0),
    {next_state, idle, State0#state{sock = undefined}, [{next_event, internal, do_connect}]};
connecting(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State);
connecting(info, Msg, _State) ->
    logger:info("[pulsar-producer][connecting] unknown message received ~p~n  ~p", [Msg, _State]),
    keep_state_and_data;
connecting({call, From}, _, State) ->
    {keep_state, State, [{reply, From, {error, producer_connecting}}]};
connecting(cast, {send, _Message}, _State) ->
    {keep_state_and_data, [postpone]}.

%% connected state
-spec connected(gen_statem:event_type(), _EventContent, #state{}) ->
          gen_statem:event_handler_result(statem()).
connected(enter, _OldState, State0) ->
    resend_sent_requests(State0),
    State = maybe_send_to_pulsar(State0),
    {keep_state, State};
connected(_, do_connect, _State) ->
    keep_state_and_data;
connected(info, ?SEND_REQ(_, _) = SendRequest, State0 = #state{batch_size = BatchSize}) ->
    SendRequests = collect_send_requests([SendRequest], BatchSize),
    State1 = enqueue_send_requests(SendRequests, State0),
    State = maybe_send_to_pulsar(State1),
    {keep_state, State};
connected(_EventType, {InetClose, _Sock}, State = #state{partitiontopic = Topic})
        when InetClose == tcp_closed; InetClose == ssl_closed ->
    log_error("connection closed by peer, topic: ~p~n", [Topic]),
    try_close_socket(State),
    {next_state, idle, State#state{sock = undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
connected(_EventType, {InetError, _Sock, Reason}, State = #state{partitiontopic = Topic})
        when InetError == tcp_error; InetError == ssl_error ->
    log_error("connection error on topic: ~p, error: ~p~n", [Topic, Reason]),
    try_close_socket(State),
    {next_state, idle, State#state{sock = undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
connected(_EventType, {Inet, _, Bin}, State = #state{last_bin = LastBin})
        when Inet == tcp; Inet == ssl ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);
connected(_EventType, ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {keep_state, State};
connected({call, From}, {send, Message}, State = #state{sequence_id = SequenceId, requests = Reqs}) ->
    send_batch_payload(Message, State#state.sequence_id, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, From, Reqs)})};
connected(cast, {send, Message}, State = #state{batch_size = BatchSize, sequence_id = SequenceId, requests = Reqs}) ->
    BatchMessage = Message ++ pulsar_utils:collect_send_calls(BatchSize),
    send_batch_payload(BatchMessage, State#state.sequence_id, State),
    {keep_state, next_sequence_id(State#state{requests = maps:put(SequenceId, {SequenceId, length(BatchMessage)}, Reqs)})};
connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

do_connect(State = #state{opts = Opts, broker_server = {Host, Port}}) ->
    try pulsar_socket:connect(Host, Port, Opts) of
        {ok, Sock} ->
            pulsar_socket:send_connect_packet(Sock,
                pulsar_utils:maybe_add_proxy_to_broker_url_opts(Opts,
                    erlang:get(proxy_to_broker_url))),
            {next_state, connecting, State#state{sock = Sock}};
        {error, Reason} ->
            log_error("error connecting: ~p", [Reason]),
            try_close_socket(State),
            {next_state, idle, State#state{sock = undefined},
             [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]}
    catch
        Kind:Error:Stacktrace ->
            log_error("exception connecting: ~p -> ~p~n  ~p", [Kind, Error, Stacktrace]),
            try_close_socket(State),
            {next_state, idle, State#state{sock = undefined},
             [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]}
    end.

code_change({down, _Vsn}, State, Data0, _Extra) ->
    Data = ensure_replayq_absent(Data0),
    {ok, State, Data};
code_change(_Vsn, State, Data0, _Extra) ->
    Data = ensure_replayq_present(Data0),
    {ok, State, Data}.

terminate(_Reason, _StateName, _State) ->
    ok.

parse({incomplete, Bin}, State) ->
    {keep_state, State#state{last_bin = Bin}};
parse({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#state{last_bin = <<>>});
parse({Cmd, LastBin}, State) ->
    State2 = case handle_response(Cmd, State) of
        keep_state_and_data -> State;
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
handle_response({pong, #{}}, _State) ->
    start_keepalive(),
    keep_state_and_data;
handle_response({ping, #{}}, #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:pong(Sock, Opts),
    keep_state_and_data;
handle_response({close_producer, #{}}, State = #state{partitiontopic = Topic}) ->
    log_error("Close producer: ~p~n", [Topic]),
    try_close_socket(State),
    {next_state, idle, State#state{sock = undefined}, [{next_event, internal, do_connect}]};
handle_response({send_receipt, Resp = #{sequence_id := SequenceId}},
                State = #state{callback = Callback, requests = Reqs,
                               opts = #{replayq := Q}}) ->
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            _ = invoke_callback(Callback, Resp),
            {keep_state, State};
        %% impossible case!?!??
        %% SequenceId ->
        %%     _ = invoke_callback(Callback, Resp),
        %%     {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};
        {QAckRef, Froms, _Messages} ->
            ok = replayq:ack(Q, QAckRef),
            lists:foreach(
              fun(undefined) ->
                   ok;
                 (From) ->
                   gen_statem:reply(From, {ok, Resp})
              end,
              Froms),
            {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}}
    end;
handle_response({error, #{error := Error, message := Msg}}, State) ->
    log_error("Response error:~p, msg:~p~n", [Error, Msg]),
    try_close_socket(State),
    {next_state, idle, State#state{sock = undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
handle_response(Msg, _State) ->
    log_error("Receive unknown message:~p~n", [Msg]),
    keep_state_and_data.

send_batch_payload(Messages, SequenceId, #state{
            partitiontopic = Topic,
            producer_id = ProducerId,
            producer_name = ProducerName,
            sock = Sock,
            opts = Opts
        }) ->
    pulsar_socket:send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId,
        ProducerName, Opts).

start_keepalive() ->
    erlang:send_after(30_000, self(), ping).

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

queue_item_sizer(?Q_ITEM(_CallId, _Ts, Batch)) ->
    length(Batch).

queue_item_marshaller(?Q_ITEM(_, _, _) = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  binary_to_term(Bin).

now_ts() ->
    erlang:system_time(millisecond).

make_queue_item(From, Messages) ->
    ?Q_ITEM(From, now_ts(), Messages).

enqueue_send_requests(Requests, State = #state{opts = #{replayq := Q} = Opts0}) ->
    QItems = lists:map(
               fun(?SEND_REQ(From, Messages)) ->
                 make_queue_item(From, Messages)
               end,
               Requests),
    NewQ = replayq:append(Q, QItems),
    State#state{opts = Opts0#{replayq := NewQ}}.

maybe_send_to_pulsar(State0) ->
    #state{ batch_size = BatchSize
          , sequence_id = SequenceId
          , requests = Requests0
          , opts = ProducerOpts0 = #{replayq := Q}
          } = State0,
    case replayq:count(Q) =:= 0 of
        true ->
            State0;
        false ->
            {NewQ, QAckRef, Items} = replayq:pop(Q, #{count_limit => BatchSize}),
            RetentionPeriod = maps:get(retention_period, ProducerOpts0, infinity),
            Now = now_ts(),
            {Froms, Messages} =
                lists:foldr(
                  fun(?Q_ITEM(From, Timestamp, Msgs), {Froms, AccMsgs}) ->
                    case {From, is_batch_expired(Timestamp, RetentionPeriod, Now)} of
                      {_, true} -> {Froms, AccMsgs};
                      {undefined, false} -> {Froms, Msgs ++ AccMsgs};
                      {From, false} -> {[From | Froms], Msgs ++ AccMsgs}
                    end
                  end,
                  {[], []},
                  Items),
            send_batch_payload(Messages, State0#state.sequence_id, State0),
            Requests = Requests0#{SequenceId => {QAckRef, Froms, Messages}},
            State1 = State0#state{ opts = ProducerOpts0#{replayq := NewQ}
                                 , requests = Requests
                                 },
            State = next_sequence_id(State1),
            maybe_send_to_pulsar(State)
    end.

collect_send_requests(Acc, Limit) ->
    Count = 0,
    do_collect_send_requests(Acc, Count, Limit).

do_collect_send_requests(Acc, Count, Limit) when Count >= Limit ->
    lists:reverse(Acc);
do_collect_send_requests(Acc, Count, Limit) ->
    receive
        ?SEND_REQ(_, _) = Req ->
            do_collect_send_requests([Req | Acc], Count + 1, Limit)
    after
        0 ->
            lists:reverse(Acc)
    end.

try_close_socket(#state{sock = undefined}) ->
    ok;
try_close_socket(#state{sock = Sock, opts = Opts}) ->
    catch pulsar_socket:close(Sock, Opts),
    ok.

resend_sent_requests(State = #state{requests = Requests}) ->
    lists:foreach(
      fun({SequenceId, {_QAckRef, _Froms, Messages}}) ->
        send_batch_payload(Messages, SequenceId, State)
      end,
      maps:to_list(Requests)),
    ok.

is_batch_expired(_Timestamp, infinity = _RetentionPeriod, _Now) ->
    false;
is_batch_expired(Timestamp, RetentionPeriod, Now) ->
    Timestamp =< Now - RetentionPeriod.

ensure_replayq_present(Data = #state{opts = ProducerOpts0}) ->
    RetentionPeriod = maps:get(retention_period, ProducerOpts0, infinity),
    MaxTotalBytes = maps:get(replayq_max_total_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_LIMIT),
    ReplayqCfg = #{ mem_only => true
                  , sizer => fun ?MODULE:queue_item_sizer/1
                  , marshaller => fun ?MODULE:queue_item_marshaller/1
                  , max_total_bytes => MaxTotalBytes
                  },
    Q = replayq:open(ReplayqCfg),
    ProducerOpts = ProducerOpts0#{ replayq => Q
                                 , retention_period => RetentionPeriod
                                 },
    Data#state{opts = ProducerOpts}.

ensure_replayq_absent(Data = #state{opts = ProducerOpts0}) ->
    ProducerOpts = case maps:take(replayq, ProducerOpts0) of
        {Q, ProducerOpts1} ->
            _ = replayq:close(Q),
            maps:without([retention_period], ProducerOpts1);
        error ->
            ProducerOpts0
    end,
    Data#state{opts = ProducerOpts}.
