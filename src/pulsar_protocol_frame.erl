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

-module(pulsar_protocol_frame).

-define(CONNECT, 'CONNECT').
-define(CONNECTED, 'CONNECTED').
-define(PARTITIONED_METADATA, 'PARTITIONED_METADATA').
-define(PARTITIONED_METADATA_RESPONSE, 'PARTITIONED_METADATA_RESPONSE').
-define(LOOKUP, 'LOOKUP').
-define(LOOKUP_RESPONSE, 'LOOKUP_RESPONSE').
-define(PRODUCER, 'PRODUCER').
-define(SUBSCRIBE, 'SUBSCRIBE').
-define(SUCCESS, 'SUCCESS').
-define(FLOW, 'FLOW').
-define(MESSAGE, 'MESSAGE').
-define(ACK, 'ACK').
-define(PRODUCER_SUCCESS, 'PRODUCER_SUCCESS').
-define(SEND, 'SEND').
-define(SEND_RECEIPT, 'SEND_RECEIPT').
-define(PING, 'PING').
-define(PONG, 'PONG').
-define(CLOSE_PRODUCER, 'CLOSE_PRODUCER').
-define(CLOSE_CONSUMER, 'CLOSE_CONSUMER').
-define(SIMPLE_SIZE, 4).
-define(PAYLOAD_SIZE, 10).
-define(MAGIC_NUMBER, 3585).

%% Use protocol version 10 to work with pulsar proxy
-define(PROTO_VSN, 10).
-define(CLIENT_VSN, "Pulsar-Client-Erlang-v0.0.2").

-export ([ connect/0
         , connect/1
         , topic_metadata/1
         , lookup_topic/1
         , create_producer/1
         , create_subscribe/1
         , set_flow/1
         , ack/1
         , ping/0
         , pong/0
         , serialized_simple_command/1
         , serialized_payload_command/3
         , parse/1
         , send/3
         ]).

connect() ->
    connect(#{}).

connect(CommandConnect) ->
    serialized_simple_command(#{
        type => ?CONNECT,
        connect => maps:merge(default_connect_fields(), CommandConnect)
    }).

topic_metadata(PartitionMetadata) ->
    serialized_simple_command(#{
        type => ?PARTITIONED_METADATA,
        partitionMetadata => PartitionMetadata
    }).

lookup_topic(LookupTopic) ->
    serialized_simple_command(#{
        type => ?LOOKUP,
        lookupTopic => LookupTopic
    }).

create_producer(Producer) ->
    serialized_simple_command(#{
        type => ?PRODUCER,
        producer => Producer
    }).

create_subscribe(SubInfo) ->
    serialized_simple_command(#{
        type => ?SUBSCRIBE,
        subscribe => SubInfo
    }).

set_flow(FlowInfo) ->
    serialized_simple_command(#{
        type => ?FLOW,
        flow => FlowInfo
    }).

ack(Ack) ->
    serialized_simple_command(#{
        type => ?ACK,
        ack => Ack
    }).

send(Send, Metadata, BatchPayload) ->
    serialized_payload_command(#{
        type => ?SEND,
        send => Send
    }, pulsar_api:encode_msg(Metadata, 'MessageMetadata'), BatchPayload).

ping() ->
    serialized_simple_command(#{
        type => ?PING,
        ping => #{}
    }).

pong() ->
    serialized_simple_command(#{
        type => ?PONG,
        pong => #{}
    }).

default_connect_fields() ->
    #{ client_version => ?CLIENT_VSN
     , protocol_version => ?PROTO_VSN
     }.

parse(<<TotalSize:32, CmdBin:TotalSize/binary, Rest/binary>>) ->
    <<CommandSize:32, Command:CommandSize/binary, CmdRest/binary>> = CmdBin,
    BaseCommand = try_decode(CommandSize, Command),
    Resp = case maps:get(type, BaseCommand, unknown) of
        ?MESSAGE ->
            <<MetadataSize:32, Metadata:MetadataSize/binary, Payload0/binary>> = CmdRest,
            MetadataCmd = pulsar_api:decode_msg(<<MetadataSize:32, Metadata/binary>>, 'MessageMetadata'),
            Payloads = parse_batch_message(Payload0, maps:get(num_messages_in_batch, MetadataCmd, 1)),
            {message, maps:get(message, BaseCommand), Payloads};
        ?CONNECTED ->
            {connected, maps:get(connected, BaseCommand)};
        ?PARTITIONED_METADATA_RESPONSE ->
            {partitionMetadataResponse, maps:get(partitionMetadataResponse, BaseCommand)};
        ?LOOKUP_RESPONSE ->
            {lookupTopicResponse, maps:get(lookupTopicResponse, BaseCommand)};
        ?PRODUCER_SUCCESS ->
            {producer_success, maps:get(producer_success, BaseCommand)};
        ?SEND_RECEIPT ->
            {send_receipt, maps:get(send_receipt, BaseCommand)};
        ?PING ->
            {ping, maps:get(ping, BaseCommand)};
        ?PONG ->
            {pong, maps:get(pong, BaseCommand)};
        ?CLOSE_PRODUCER ->
            {close_producer, maps:get(close_producer, BaseCommand)};
        ?CLOSE_CONSUMER ->
            {close_consumer, maps:get(close_consumer, BaseCommand)};
        ?SUCCESS ->
            {subscribe_success, maps:get(success, BaseCommand)};
        'ERROR' ->
            {error, maps:get(error, BaseCommand)};
        _Type ->
            logger:error("parse unknown type:~p~n", [BaseCommand]),
            unknown
    end,
    {Resp, Rest};

parse(Bin) ->
    {undefined, Bin}.

serialized_simple_command(BaseCommand) ->
    BaseCommandBin = pulsar_api:encode_msg(BaseCommand, 'BaseCommand'),
    Size = size(BaseCommandBin),
    TotalSize = Size + ?SIMPLE_SIZE,
    <<TotalSize:32, Size:32, BaseCommandBin/binary>>.

serialized_payload_command(BaseCommand, Metadata, BatchPayload) ->
    BaseCommandBin = pulsar_api:encode_msg(BaseCommand, 'BaseCommand'),
    BaseCommandSize = size(BaseCommandBin),
    MetadataSize = size(Metadata),
    Payload = <<MetadataSize:32, Metadata/binary, BatchPayload/binary>>,
    Checksum = crc32cer:nif(Payload),
    TotalSize = BaseCommandSize + size(Payload) + ?PAYLOAD_SIZE,
    <<TotalSize:32, BaseCommandSize:32, BaseCommandBin/binary, ?MAGIC_NUMBER:16, Checksum:32, Payload/binary>>.

parse_batch_message(Payloads, Size) ->
    parse_batch_message(Payloads, Size, []).
parse_batch_message(_Payloads, 0, Acc) ->
    lists:reverse(Acc);
parse_batch_message(Payloads, Size, Acc) ->
    <<SMetadataSize:32, SMetadata:SMetadataSize/binary, Rest/binary>> = Payloads,
    SingleMessageMetadata = pulsar_api:decode_msg(<<SMetadataSize:32, SMetadata/binary>>, 'SingleMessageMetadata'),
    PayloadSize = maps:get(payload_size, SingleMessageMetadata),
    <<Payload:PayloadSize/binary, Rest1/binary>> = Rest,
    parse_batch_message(Rest1, Size - 1, [Payload | Acc]).

try_decode(CommandSize, Command) ->
    try pulsar_api:decode_msg(<<CommandSize:32, Command/binary>>, 'BaseCommand') of
        BaseCommand -> BaseCommand
    catch _:_ ->
        pulsar_api:decode_msg(Command, 'BaseCommand')
    end.
