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

-include("include/pulsar_producer_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
        , format_status/1
        , format_status/2
        ]).

%% replayq API
-export([ queue_item_sizer/1
        , queue_item_marshaller/1
        ]).

%% for testing only
-ifdef(TEST).
-export([ code_change_requests_down/1
        , from_old_state_record/1
        , to_old_state_record/1
        ]).
-endif.

-type statem() :: idle | connecting | connected.
-type sequence_id() :: integer().
-type send_receipt() :: #{ sequence_id := sequence_id()
                         , producer_id := integer()
                         , highest_sequence_id := sequence_id()
                         , message_id := map()
                         , any() => term()
                         }.
-type timestamp() :: integer().
-type callback() :: undefined | mfa() | fun((map()) -> ok).
-type callback_input() :: {ok, send_receipt()} | {error, expired}.
-type config() :: #{ replayq_dir := string()
                   , replayq_max_total_bytes => pos_integer()
                   , replayq_seg_bytes => pos_integer()
                   , replayq_offload_mode => boolean()
                   , max_batch_bytes => pos_integer()
                   , producer_name => atom()
                   , callback => callback()
                   , batch_size => non_neg_integer()
                   , retention_period => timeout()
                   }.
-export_type([ config/0
             ]).

-define(RECONNECT_TIMEOUT, 5_000).

-define(MAX_REQ_ID, 4294836225).
-define(MAX_SEQ_ID, 18445618199572250625).

-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2_000_000_000).
-define(DEFAULT_MAX_BATCH_BYTES, 1_000_000).
-define(Q_ITEM(From, Ts, Messages), {From, Ts, Messages}).
-define(INFLIGHT_REQ(QAckRef, FromsToMessages), {inflight_req, QAckRef, FromsToMessages}).

-type state() :: #{
                   batch_size := non_neg_integer(),
                   broker_server := {binary(), pos_integer()},
                   callback := undefined | mfa() | fun((map()) -> ok),
                   last_bin := binary(),
                   opts := map(),
                   partitiontopic := binary(),
                   producer_id := integer(),
                   producer_name := atom(),
                   replayq := replayq:q(),
                   request_id := integer(),
                   requests := #{sequence_id() =>
                                     ?INFLIGHT_REQ(
                                       replayq:ack_ref(),
                                        [{gen_statem:from() | undefined,
                                          {timestamp(), [pulsar:message()]}}])},
                   sequence_id := sequence_id(),
                   sock := undefined | port()
                  }.
-type handler_result() :: gen_statem:event_handler_result(statem(), state()).

callback_mode() -> [state_functions, state_enter].

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts) ->
    gen_statem:start_link(?MODULE, {PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts}, []).

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
    %% Mimicking gen_statem's From, so the reply can be sent with
    %% `gen_statem:reply/2'
    From = {Caller, MRef},
    erlang:send(Pid, ?SEND_REQ(From, Messages)),
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

-spec init({string(), string(), string() | undefined, config()}) ->
          gen_statem:init_result(statem(), state()).
init({PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts0}) ->
    process_flag(trap_exit, true),
    {Transport, BrokerServer} = pulsar_utils:parse_url(Server),
    ProducerID = maps:get(producer_id, ProducerOpts0),
    ReplayqCfg0 =
        case maps:get(replayq_dir, ProducerOpts0, false) of
            false ->
                #{mem_only => true};
            BaseDir ->
                PartitionTopicPath = escape(PartitionTopic),
                Dir = filename:join([BaseDir, PartitionTopicPath]),
                SegBytes = maps:get(replayq_seg_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_SEG_BYTES),
                Offload = maps:get(replayq_offload_mode, ProducerOpts0, false),
                #{dir => Dir, seg_bytes => SegBytes, offload => Offload}
        end,
    MaxTotalBytes = maps:get(replayq_max_total_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_LIMIT),
    MaxBatchBytes = maps:get(max_batch_bytes, ProducerOpts0, ?DEFAULT_MAX_BATCH_BYTES),
    ReplayqCfg =
        ReplayqCfg0#{ sizer => fun ?MODULE:queue_item_sizer/1
                    , marshaller => fun ?MODULE:queue_item_marshaller/1
                    , max_total_bytes => MaxTotalBytes
                    },
    Q = replayq:open(ReplayqCfg),
    ProducerOpts1 = ProducerOpts0#{max_batch_bytes => MaxBatchBytes},
    %% drop replayq options, now that it's open.
    ProducerOpts = maps:without([ replayq_dir
                                , replayq_seg_bytes
                                , replayq_offload_mode
                                , replayq_max_total_bytes
                                ],
                                ProducerOpts1),
    State = #{
        batch_size => maps:get(batch_size, ProducerOpts, 0),
        broker_server => BrokerServer,
        callback => maps:get(callback, ProducerOpts, undefined),
        last_bin => <<>>,
        opts => pulsar_utils:maybe_enable_ssl_opts(Transport, ProducerOpts),
        partitiontopic => PartitionTopic,
        producer_id => ProducerID,
        producer_name => maps:get(producer_name, ProducerOpts, pulsar_producer),
        replayq => Q,
        request_id => 1,
        requests => #{},
        sequence_id => 1,
        sock => undefined
    },
    %% use process dict to avoid the trouble of relup
    put(proxy_to_broker_url, ProxyToBrokerUrl),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

%% idle state
-spec idle(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
idle(enter, _OldState, _State) ->
    keep_state_and_data;
idle(_, do_connect, State) ->
    do_connect(State);
idle({call, From}, {send, Messages}, State0) ->
    %% for race conditions when upgrading from previous versions only
    SendRequest = ?SEND_REQ(From, Messages),
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
idle({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
idle(cast, {send, Messages}, State0) ->
    %% for race conditions when upgrading from previous versions only
    From = undefined,
    SendRequest = ?SEND_REQ(From, Messages),
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
idle(cast, _EventContent, _State) ->
    keep_state_and_data;
idle(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
idle(_EventType, _Event, _State) ->
    keep_state_and_data.

%% connecting state
-spec connecting(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
connecting(enter, _OldState, _State) ->
    keep_state_and_data;
connecting(_, do_connect, State) ->
    do_connect(State);
connecting(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
connecting(info, {CloseEvent, _Sock}, State0 = #{})
  when CloseEvent =:= tcp_closed; CloseEvent =:= ssl_closed ->
    try_close_socket(State0),
    {next_state, idle, State0#{sock := undefined}, [{next_event, internal, do_connect}]};
connecting(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    {Cmd, _} = pulsar_protocol_frame:parse(Bin),
    handle_response(Cmd, State);
connecting(info, Msg, _State) ->
    logger:info("[pulsar-producer][connecting] unknown message received ~p~n  ~p", [Msg, _State]),
    keep_state_and_data;
connecting({call, From}, {send, Messages}, State0) ->
    %% for race conditions when upgrading from previous versions only
    SendRequest = ?SEND_REQ(From, Messages),
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
connecting({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
connecting(cast, {send, Messages}, State0) ->
    %% for race conditions when upgrading from previous versions only
    From = undefined,
    SendRequest = ?SEND_REQ(From, Messages),
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
connecting(cast, _EventContent, _State) ->
   keep_state_and_data.

%% connected state
-spec connected(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
connected(enter, _OldState, State0) ->
    State1 = resend_sent_requests(State0),
    State = maybe_send_to_pulsar(State1),
    {keep_state, State};
connected(_, do_connect, _State) ->
    keep_state_and_data;
connected(info, ?SEND_REQ(_, _) = SendRequest, State0 = #{batch_size := BatchSize}) ->
    ?tp(pulsar_producer_send_req_enter, #{}),
    SendRequests = collect_send_requests([SendRequest], BatchSize),
    State1 = enqueue_send_requests(SendRequests, State0),
    State = maybe_send_to_pulsar(State1),
    ?tp(pulsar_producer_send_req_exit, #{}),
    {keep_state, State};
connected(_EventType, {InetClose, _Sock}, State = #{partitiontopic := Topic})
        when InetClose == tcp_closed; InetClose == ssl_closed ->
    log_error("connection closed by peer, topic: ~p~n", [Topic], State),
    try_close_socket(State),
    {next_state, idle, State#{sock := undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
connected(_EventType, {InetError, _Sock, Reason}, State = #{partitiontopic := Topic})
        when InetError == tcp_error; InetError == ssl_error ->
    log_error("connection error on topic: ~p, error: ~p~n", [Topic, Reason], State),
    try_close_socket(State),
    {next_state, idle, State#{sock := undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
connected(_EventType, {Inet, _, Bin}, State = #{last_bin := LastBin})
        when Inet == tcp; Inet == ssl ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);
connected(_EventType, ping, State = #{sock := Sock, opts := Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {keep_state, State};
connected({call, From}, {send, Messages}, State) ->
    %% for race conditions when upgrading from previous versions only
    SendRequest = ?SEND_REQ(From, Messages),
    self() ! SendRequest,
    {keep_state, State};
connected({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
connected(cast, {send, Messages}, State) ->
    %% for race conditions when upgrading from previous versions only
    From = undefined,
    SendRequest = ?SEND_REQ(From, Messages),
    self() ! SendRequest,
    {keep_state, State};
connected(cast, _EventContent, _State) ->
    keep_state_and_data;
connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

-spec do_connect(state()) -> handler_result().
do_connect(State = #{opts := Opts, broker_server := {Host, Port}}) ->
    try pulsar_socket:connect(Host, Port, Opts) of
        {ok, Sock} ->
            pulsar_socket:send_connect_packet(Sock,
                pulsar_utils:maybe_add_proxy_to_broker_url_opts(Opts,
                    erlang:get(proxy_to_broker_url))),
            {next_state, connecting, State#{sock := Sock}};
        {error, Reason} ->
            log_error("error connecting: ~p", [Reason], State),
            try_close_socket(State),
            {next_state, idle, State#{sock := undefined},
             [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]}
    catch
        Kind:Error:Stacktrace ->
            log_error("exception connecting: ~p -> ~p~n  ~p", [Kind, Error, Stacktrace], State),
            try_close_socket(State),
            {next_state, idle, State#{sock := undefined},
             [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]}
    end.

format_status(Status) ->
    maps:map(
      fun(data, Data0) ->
              censor_secrets(Data0);
         (_Key, Value)->
              Value
      end,
      Status).

%% `format_status/2' is deprecated as of OTP 25.0
format_status(_Opt, [_PDict, _State0, Data0]) ->
    Data = censor_secrets(Data0),
    [{data, [{"State", Data}]}].

censor_secrets(Data0 = #{opts := Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := _}}}) ->
    Data0#{opts := Opts0#{conn_opts := ConnOpts0#{auth_data := "******"}}};
censor_secrets(Data) ->
    Data.

code_change({down, _ToVsn}, State, Data, Extra) when is_map(Data) ->
    #{to_version := ToVsn} = Extra,
    case pulsar_relup:is_before_replayq(ToVsn) of
        true ->
            do_code_change_down_replayq(State, Data, Extra);
        false ->
            {ok, State, Data}
    end;
code_change({down, _Vsn}, State, Data, _Extra) ->
    {ok, State, Data};
code_change(_ToVsn, State, DataRec, Extra) when is_tuple(DataRec) ->
    #{to_version := ToVsn} = Extra,
    case pulsar_relup:is_before_replayq(ToVsn) of
        true ->
            {ok, State, DataRec};
        false ->
            do_code_change_up_replayq(State, DataRec, Extra)
    end;
code_change(_ToVsn, State, Data, _Extra) ->
    {ok, State, Data}.

do_code_change_down_replayq(State, Data0, _Extra) ->
    downgrade_buffered_send_requests(Data0),
    Data1 = ensure_replayq_absent(Data0),
    Requests0 = maps:get(requests, Data0),
    Requests = code_change_requests_down(Requests0),
    DataMap = Data1#{requests := Requests},
    DataRec = to_old_state_record(DataMap),
    {ok, State, DataRec}.

do_code_change_up_replayq(State, DataRec, _Extra) ->
    Data0 = from_old_state_record(DataRec),
    Data = ensure_replayq_present(Data0),
    {ok, State, Data}.

terminate(_Reason, _StateName, _State = #{replayq := Q}) ->
    ok = replayq:close(Q),
    ok.

parse({incomplete, Bin}, State) ->
    {keep_state, State#{last_bin := Bin}};
parse({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#{last_bin := <<>>});
parse({Cmd, LastBin}, State) ->
    State2 = case handle_response(Cmd, State) of
        keep_state_and_data -> State;
        {_, State1} -> State1;
        {_, _, State1} -> State1
    end,
    parse(pulsar_protocol_frame:parse(LastBin), State2).

-spec handle_response(_EventContent, state()) ->
          handler_result().
handle_response({connected, _ConnectedData}, State = #{
        sock := Sock,
        opts := Opts,
        request_id := RequestId,
        producer_id := ProducerId,
        partitiontopic := Topic
    }) ->
    start_keepalive(),
    pulsar_socket:send_create_producer_packet(Sock, Topic, RequestId, ProducerId, Opts),
    {keep_state, next_request_id(State)};
handle_response({producer_success, #{producer_name := ProName}}, State) ->
    {next_state, connected, State#{producer_name := ProName}};
handle_response({pong, #{}}, _State) ->
    start_keepalive(),
    keep_state_and_data;
handle_response({ping, #{}}, #{sock := Sock, opts := Opts}) ->
    pulsar_socket:pong(Sock, Opts),
    keep_state_and_data;
handle_response({close_producer, #{}}, State = #{partitiontopic := Topic}) ->
    log_error("Close producer: ~p~n", [Topic], State),
    try_close_socket(State),
    {next_state, idle, State#{sock := undefined}, [{next_event, internal, do_connect}]};
handle_response({send_receipt, Resp = #{sequence_id := SequenceId}},
                State = #{callback := Callback, requests := Reqs,
                          replayq := Q}) ->
    ?tp(pulsar_producer_recv_send_receipt, #{receipt => Resp}),
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            _ = invoke_callback(Callback, {ok, Resp}),
            {keep_state, State};
        %% impossible case!?!??
        %% SequenceId ->
        %%     _ = invoke_callback(Callback, {ok, Resp}),
        %%     {keep_state, State#state{requests = maps:remove(SequenceId, Reqs)}};

        %% State transferred from hot-upgrade; it doesn't have enough
        %% info to migrate to the new format.
        {SequenceId, BatchLen} ->
            _ = invoke_callback(Callback, {ok, Resp}, BatchLen),
            {keep_state, State#{requests := maps:remove(SequenceId, Reqs)}};
        %% State transferred from hot-upgrade; it doesn't have enough
        %% info to migrate to the new format.
        {_FromPID, _Alias} = OldFrom ->
            gen_statem:reply(OldFrom, {ok, Resp}),
            {keep_state, State#{requests := maps:remove(SequenceId, Reqs)}};
        ?INFLIGHT_REQ(QAckRef, FromsToMessages) ->
            ok = replayq:ack(Q, QAckRef),
            lists:foreach(
              fun({undefined, {_TS, Messages}}) ->
                   BatchLen = length(Messages),
                   _ = invoke_callback(Callback, {ok, Resp}, BatchLen),
                   ok;
                 ({From, {_TS, _Messages}}) ->
                   gen_statem:reply(From, {ok, Resp})
              end,
              FromsToMessages),
            {keep_state, State#{requests := maps:remove(SequenceId, Reqs)}}
    end;
handle_response({error, #{error := Error, message := Msg}}, State) ->
    log_error("Response error:~p, msg:~p~n", [Error, Msg], State),
    try_close_socket(State),
    {next_state, idle, State#{sock := undefined},
     [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]};
handle_response(Msg, State) ->
    log_error("Receive unknown message:~p~n", [Msg], State),
    keep_state_and_data.

-spec send_batch_payload([{timestamp(), [pulsar:message()]}], sequence_id(), state()) -> ok.
send_batch_payload(Messages, SequenceId, #{
            partitiontopic := Topic,
            producer_id := ProducerId,
            producer_name := ProducerName,
            sock := Sock,
            opts := Opts
        }) ->
    pulsar_socket:send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId,
        ProducerName, Opts).

start_keepalive() ->
    erlang:send_after(30_000, self(), ping).

next_request_id(State = #{request_id := ?MAX_REQ_ID}) ->
    State#{request_id := 1};
next_request_id(State = #{request_id := RequestId}) ->
    State#{request_id := RequestId + 1}.

next_sequence_id(State = #{sequence_id := ?MAX_SEQ_ID}) ->
    State#{sequence_id := 1};
next_sequence_id(State = #{sequence_id := SequenceId}) ->
    State#{sequence_id := SequenceId + 1}.

-spec log_error(string(), [term()], state()) -> ok.
log_error(Fmt, Args, #{partitiontopic := PartitionTopic}) ->
    logger:error("[pulsar-producer][~s] " ++ Fmt, [PartitionTopic | Args]).

-spec invoke_callback(callback(), callback_input()) -> ok.
invoke_callback(Callback, Resp) ->
    invoke_callback(Callback, Resp, _BatchLen = 1).

-spec invoke_callback(callback(), callback_input(), non_neg_integer()) -> ok.
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

queue_item_sizer(?Q_ITEM(_CallId, _Ts, _Batch) = Item) ->
    erlang:external_size(Item).

queue_item_marshaller(?Q_ITEM(_, _, _) = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  binary_to_term(Bin).

now_ts() ->
    erlang:system_time(millisecond).

make_queue_item(From, Messages) ->
    ?Q_ITEM(From, now_ts(), Messages).

enqueue_send_requests(Requests, State = #{replayq := Q}) ->
    QItems = lists:map(
               fun(?SEND_REQ(From, Messages)) ->
                 make_queue_item(From, Messages)
               end,
               Requests),
    NewQ = replayq:append(Q, QItems),
    State#{replayq := NewQ}.

maybe_send_to_pulsar(State) ->
    #{replayq := Q} = State,
    case replayq:count(Q) =:= 0 of
        true ->
            State;
        false ->
            do_send_to_pulsar(State)
    end.

do_send_to_pulsar(State0) ->
    #{ batch_size := BatchSize
     , sequence_id := SequenceId
     , requests := Requests0
     , replayq := Q
     , opts := ProducerOpts
     } = State0,
    MaxBatchBytes = maps:get(max_batch_bytes, ProducerOpts, ?DEFAULT_MAX_BATCH_BYTES),
    {NewQ, QAckRef, Items} = replayq:pop(Q, #{ count_limit => BatchSize
                                             , bytes_limit => MaxBatchBytes
                                             }),
    State1 = State0#{replayq := NewQ},
    RetentionPeriod = maps:get(retention_period, ProducerOpts, infinity),
    Now = now_ts(),
    {Expired, FromsToMessages} =
       lists:foldr(
         fun(?Q_ITEM(From, Timestamp, Msgs), {Expired, Acc}) ->
           case is_batch_expired(Timestamp, RetentionPeriod, Now) of
             true ->
               {[{From, Msgs} | Expired], Acc};
             false ->
               {Expired, [{From, {Timestamp, Msgs}} | Acc]}
           end
         end,
         {[], []},
         Items),
    reply_expired_messages(Expired, State1),
    case FromsToMessages of
        [] ->
            %% all expired, immediately ack replayq batch and continue
            ok = replayq:ack(Q, QAckRef),
            maybe_send_to_pulsar(State1);
        [_ | _] ->
            send_batch_payload([Msg || {_From, {_Timestamp, Msgs}} <-
                                           FromsToMessages,
                                       Msg <- Msgs],
                               SequenceId, State0),
            Requests = Requests0#{SequenceId => ?INFLIGHT_REQ(QAckRef, FromsToMessages)},
            State2 = State1#{requests := Requests},
            State = next_sequence_id(State2),
            maybe_send_to_pulsar(State)
    end.

-spec reply_expired_messages([{gen_statem:from() | undefined, [pulsar:message()]}],
                             state()) -> ok.
reply_expired_messages(Expired, #{callback := Callback}) ->
    lists:foreach(
      fun({undefined, Msgs}) ->
              invoke_callback(Callback, {error, expired}, length(Msgs));
         ({From, _Msgs}) ->
              gen_statem:reply(From, {error, expired})
      end,
      Expired).

collect_send_requests(Acc, Limit) ->
    Count = length(Acc),
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

try_close_socket(#{sock := undefined}) ->
    ok;
try_close_socket(#{sock := Sock, opts := Opts}) ->
    _ = pulsar_socket:close(Sock, Opts),
    ok.

resend_sent_requests(State) ->
    ?tp(pulsar_producer_resend_sent_requests_enter, #{}),
    #{ requests := Requests0
     , replayq := Q
     , opts := ProducerOpts
     } = State,
    Now = now_ts(),
    RetentionPeriod = maps:get(retention_period, ProducerOpts, infinity),
    Requests =
        maps:fold(
          fun(SequenceId, ?INFLIGHT_REQ(QAckRef, FromsToMessages), Acc) ->
               {Messages, Expired} =
                  lists:partition(
                    fun({_From, {Ts, _Msgs}}) ->
                      not is_batch_expired(Ts, RetentionPeriod, Now)
                    end,
                    FromsToMessages),
               lists:foreach(
                 fun({From, {_Ts, Msgs}}) ->
                   reply_expired_messages([{From, Msgs}], State)
                 end,
                 Expired),
               case Messages of
                   [] ->
                       ?tp(pulsar_producer_resend_all_expired, #{}),
                       ok = replayq:ack(Q, QAckRef),
                       Acc;
                   [_ | _] ->
                       send_batch_payload([Msg || {_From, {_Ts, Msgs}} <- Messages,
                                                  Msg <- Msgs],
                                          SequenceId, State),
                       Acc#{SequenceId => ?INFLIGHT_REQ(QAckRef, Messages)}
               end;
             (SequenceId, Req = {_SequenceId1, _BatchLen}, Acc) ->
               %% this clause is when one hot-upgrades from a version
               %% without replayq.  we don't have enough info to
               %% resend nor expire.
               Acc#{SequenceId => Req}
          end,
          #{},
          Requests0),
    State#{requests := Requests}.

is_batch_expired(_Timestamp, infinity = _RetentionPeriod, _Now) ->
    false;
is_batch_expired(Timestamp, RetentionPeriod, Now) ->
    Timestamp =< Now - RetentionPeriod.

-spec ensure_replayq_present(map()) -> state().
ensure_replayq_present(Data = #{opts := ProducerOpts0}) ->
    RetentionPeriod = maps:get(retention_period, ProducerOpts0, infinity),
    MaxTotalBytes = maps:get(replayq_max_total_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_LIMIT),
    ReplayqCfg = #{ mem_only => true
                  , sizer => fun ?MODULE:queue_item_sizer/1
                  , marshaller => fun ?MODULE:queue_item_marshaller/1
                  , max_total_bytes => MaxTotalBytes
                  },
    Q = replayq:open(ReplayqCfg),
    ProducerOpts = ProducerOpts0#{retention_period => RetentionPeriod},
    Data#{opts := ProducerOpts, replayq => Q}.

-spec ensure_replayq_absent(state()) -> map().
ensure_replayq_absent(Data0) ->
    Data = case maps:take(replayq, Data0) of
        {Q, Data1 = #{opts := ProducerOpts0}} ->
            _ = replayq:close(Q),
            ProducerOpts = maps:without([retention_period, replayq], ProducerOpts0),
            Data1#{opts := ProducerOpts};
        error ->
            Data0
    end,
    Data.

code_change_requests_down(Requests) ->
    maps:map(
      fun(SequenceId, ?INFLIGHT_REQ(_QAckRef, FromsToMessages)) ->
        lists:foldl(
         fun({_From, {_Ts, Messages}}, {_SequenceId, BatchLen}) ->
           {SequenceId, BatchLen + length(Messages)}
         end,
         {SequenceId, 0},
         FromsToMessages)
      end,
      Requests).

%% when downgrading to a previous version before replayq, there may be
%% some `?SEND_REQ' messages in the mailbox that'll be unknown to the
%% old code.  We attempt to convert them to call/casts to avoid losing
%% them.  Calls might still be lost if the downgrade + processing the
%% message afterwards takes longer than the call timeout.
downgrade_buffered_send_requests(#{replayq := Q}) ->
    {NewQ, QAckRef, BufferedItems} = replayq:pop(Q, #{count_limit => 10_000}),
    replayq:ack(NewQ, QAckRef),
    lists:foreach(
      fun(?Q_ITEM(_From = undefined, _Timestamp, Messages)) ->
              self() ! {'$gen_cast', {send, Messages}};
         (?Q_ITEM(From, _Timestamp, Messages)) ->
              self() ! {'$gen_call', From, {send, Messages}}
      end,
      BufferedItems).

%% -record(state, {partitiontopic,
%%                 broker_server,
%%                 sock,
%%                 request_id = 1,
%%                 producer_id = 1,
%%                 sequence_id = 1,
%%                 producer_name,
%%                 opts = #{},
%%                 callback,
%%                 batch_size = 0,
%%                 requests = #{},
%%                 last_bin = <<>>}).
to_old_state_record(StateMap = #{}) ->
    { state
    , maps:get(partitiontopic, StateMap)
    , maps:get(broker_server, StateMap)
    , maps:get(sock, StateMap)
    , maps:get(request_id, StateMap)
    , maps:get(producer_id, StateMap)
    , maps:get(sequence_id, StateMap)
    , maps:get(producer_name, StateMap)
    , maps:get(opts, StateMap)
    , maps:get(callback, StateMap)
    , maps:get(batch_size, StateMap)
    , maps:get(requests, StateMap)
    , maps:get(last_bin, StateMap)
    }.

from_old_state_record(StateRec) ->
    #{ partitiontopic => element(2, StateRec)
     , broker_server => element(3, StateRec)
     , sock => element(4, StateRec)
     , request_id => element(5, StateRec)
     , producer_id => element(6, StateRec)
     , sequence_id => element(7, StateRec)
     , producer_name => element(8, StateRec)
     , opts => element(9, StateRec)
     , callback => element(10, StateRec)
     , batch_size => element(11, StateRec)
     , requests => element(12, StateRec)
     , last_bin => element(13, StateRec)
     }.

-spec escape(string()) -> binary().
escape(Str) ->
    NormalizedStr = unicode:characters_to_nfd_list(Str),
    iolist_to_binary(pulsar_utils:escape_uri(NormalizedStr)).
