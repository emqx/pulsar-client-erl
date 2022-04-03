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

-module(pulsar_socket).

-export([ peername/2
        , tune_buffer/2
        , connect/3
        ]).

-export([ send_connect_packet/3
        , send_lookup_topic_packet/4
        , send_topic_metadata_packet/4
        , send_subscribe_packet/7
        , send_set_flow_packet/4
        , send_ack_packet/5
        , send_batch_message_packet/7
        , send_create_producer_packet/5
        , ping/2
        , pong/2
        ]).

-define(SEND_TIMEOUT, 60000).
-define(CONN_TIMEOUT, 30000).

-define(INTERNAL_TCP_OPTS,
    [ binary
    , {packet, raw}
    , {reuseaddr, true}
    , {active, true}
    , {reuseaddr, true}
    ]).

-define(DEF_TCP_OPTS,
    [ {nodelay, true}
    , {send_timeout, ?SEND_TIMEOUT}
    ]).

send_connect_packet(Sock, undefined, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:connect());
send_connect_packet(Sock, ProxyToBrokerUrl, Opts) ->
    Mod = tcp_module(Opts),
    Fields = #{proxy_to_broker_url => uri_to_url(ProxyToBrokerUrl)},
    Mod:send(Sock, pulsar_protocol_frame:connect(Fields)).

send_topic_metadata_packet(Sock, Topic, RequestId, Opts) ->
    Mod = tcp_module(Opts),
    Metadata = topic_metadata_cmd(Topic, RequestId),
    Mod:send(Sock, pulsar_protocol_frame:topic_metadata(Metadata)).

send_lookup_topic_packet(Sock, Topic, RequestId, Opts) ->
    Mod = tcp_module(Opts),
    LookupCmd = lookup_topic_cmd(Topic, RequestId),
    Mod:send(Sock, pulsar_protocol_frame:lookup_topic(LookupCmd)).

send_subscribe_packet(Sock, Topic, RequestId, ConsumerId, Subscription, SubType, Opts) ->
    Mod = tcp_module(Opts),
    SubInfo = #{
        topic => Topic,
        subscription => Subscription,
        subType => SubType,
        consumer_id => ConsumerId,
        request_id => RequestId
    },
    Mod:send(Sock, pulsar_protocol_frame:create_subscribe(SubInfo)).

send_set_flow_packet(Sock, ConsumerId, FlowSize, Opts) ->
    Mod = tcp_module(Opts),
    FlowInfo = #{
        consumer_id => ConsumerId,
        messagePermits => FlowSize
    },
    Mod:send(Sock, pulsar_protocol_frame:set_flow(FlowInfo)).

send_ack_packet(Sock, ConsumerId, AckType, MsgIds, Opts) ->
    Mod = tcp_module(Opts),
    Ack = #{
        consumer_id => ConsumerId,
        ack_type => AckType,
        message_id => MsgIds
    },
    Mod:send(Sock, pulsar_protocol_frame:ack(Ack)).

send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId, ProducerName, Opts) ->
    Mod = tcp_module(Opts),
    Len = length(Messages),
    SendCmd = message_snd_cmd(Len, ProducerId, SequenceId),
    BatchMsg = batch_message_cmd(Messages, Opts),
    MsgMetadata = batch_message_cmd_metadata(ProducerName, SequenceId, Len),
    pulsar_metrics:send(Topic, length(Messages)),
    Mod:send(Sock, pulsar_protocol_frame:send(SendCmd, MsgMetadata, BatchMsg)).

send_create_producer_packet(Sock, Topic, RequestId, ProducerId, Opts) ->
    Mod = tcp_module(Opts),
    Producer = #{
        topic => Topic,
        producer_id => ProducerId,
        request_id => RequestId
    },
    Mod:send(Sock, pulsar_protocol_frame:create_producer(Producer)).

ping(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:pong()).

connect(Host, Port, Opts) ->
    TcpMod = tcp_module(Opts),
    {ConnOpts, Timeout} = connect_opts(Opts),
    case TcpMod:connect(Host, Port, ConnOpts, Timeout) of
        {ok, Sock} ->
            tune_buffer(inet_module(Opts), Sock),
            TcpMod:controlling_process(Sock, self()),
            {ok, Sock};
        {error, _} = Error ->
            Error
    end.

connect_opts(Opts) ->
    TcpOpts = maps:get(tcp_opts, Opts, []),
    SslOpts = maps:get(ssl_opts, Opts, []),
    ConnTimeout = maps:get(connect_timeout, Opts, ?CONN_TIMEOUT),
    {pulsar_utils:merge_opts([?DEF_TCP_OPTS, TcpOpts, SslOpts, ?INTERNAL_TCP_OPTS]),
     ConnTimeout}.

topic_metadata_cmd(Topic, RequestId) ->
    #{
        topic => Topic,
        request_id => RequestId
    }.

lookup_topic_cmd(Topic, RequestId) ->
    #{
        topic => Topic,
        request_id => RequestId
    }.

message_snd_cmd(Len, ProducerId, SequenceId) when is_integer(Len) ->
    #{
        producer_id => ProducerId,
        sequence_id => SequenceId,
        num_messages => Len
    }.

batch_message_cmd(Messages, Opts) ->
    Compression = compression_type(maps:get(compression, Opts, no_compression)),
    lists:foldl(fun(#{key := Key, value := Msg}, Acc) ->
            Msg1 = maybe_compression(Msg, Compression),
            SMetadata = single_message_metadata(Key, erlang:iolist_size(Msg), Compression),
            SMetadataBin = pulsar_api:encode_msg(SMetadata, 'SingleMessageMetadata'),
            SMetadataBinSize = erlang:iolist_size(SMetadataBin),
            <<Acc/binary, SMetadataBinSize:32, SMetadataBin/binary, Msg1/binary>>
        end, <<>>, Messages).

single_message_metadata(undefined, Size, Compression) ->
    #{ payload_size => Size
     , compression => Compression
     };
single_message_metadata(Key, Size, Compression) ->
    (single_message_metadata(undefined, Size, Compression))#{partition_key => Key}.

batch_message_cmd_metadata(ProducerName, SequenceId, Len) ->
    #{
        producer_name => ProducerName,
        sequence_id => SequenceId,
        publish_time => erlang:system_time(millisecond),
        compression => 'NONE',
        num_messages_in_batch => Len
    }.

compression_type(snappy) ->'SNAPPY';
compression_type(zlib) ->'ZLIB';
compression_type(_) ->'NONE'.

maybe_compression(Bin, 'SNAPPY') ->
    {ok, Compressed} = snappyer:compress(Bin),
    Compressed;
maybe_compression(Bin, 'ZLIB') ->
    zlib:compress(Bin);
maybe_compression(Bin, _) ->
    Bin.

%% =======================================================================================
%% Helpers
%% =======================================================================================

peername(Sock, Opts) ->
    Mod = inet_module(Opts),
    Mod:peername(Sock).

tcp_module(Opts) ->
    case maps:get(enable_ssl, Opts, false) of
        false -> gen_tcp;
        true -> ssl
    end.

inet_module(Opts) ->
    case maps:get(enable_ssl, Opts, false) of
        false -> inet;
        true -> ssl
    end.

tune_buffer(InetM, Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]}
        = InetM:getopts(Sock, [recbuf, sndbuf]),
    InetM:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

uri_to_url(URI) ->
    case string:split(URI, "://", leading) of
        [URI] -> URI;
        [_Scheme, URL] -> URL
    end.
