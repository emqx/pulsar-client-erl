-module(pulsar_protocol_frame_tests).

-include_lib("eunit/include/eunit.hrl").

parse_single_message_without_num_messages_in_batch_test() ->
    Payload = <<"pulsar">>,
    %% This metadata is captured from a real broker response where
    %% `num_messages_in_batch` is absent.
    Metadata = <<10, 14, 115, 116, 97, 110, 100, 97, 108, 111, 110, 101, 45,
                 48, 45, 56, 16, 0, 24, 205, 234, 247, 203, 187, 50, 72, 6>>,
    Packet = message_packet(Metadata, Payload),
    {{message, _MsgInfo, Payloads}, <<>>} = pulsar_protocol_frame:parse(Packet),
    ?assertEqual([Payload], Payloads).

parse_batch_message_when_num_messages_in_batch_present_test() ->
    Payload = <<"pulsar">>,
    BatchPayload = batch_payload(Payload),
    Metadata = pulsar_api:encode_msg(
        #{
            producer_name => "standalone-0-8",
            sequence_id => 0,
            publish_time => 12345,
            uncompressed_size => byte_size(Payload),
            num_messages_in_batch => 1
        },
        'MessageMetadata'
    ),
    Packet = message_packet(Metadata, BatchPayload),
    {{message, _MsgInfo, Payloads}, <<>>} = pulsar_protocol_frame:parse(Packet),
    ?assertEqual([Payload], Payloads).

batch_payload(Payload) ->
    SingleMessageMetadata = pulsar_api:encode_msg(
        #{payload_size => byte_size(Payload)},
        'SingleMessageMetadata'
    ),
    <<(byte_size(SingleMessageMetadata)):32, SingleMessageMetadata/binary, Payload/binary>>.

message_packet(Metadata, Payload) ->
    %% BaseCommand(Type=MESSAGE) + minimal CommandMessage
    Command = <<8, 9, 74, 11, 8, 228, 2, 18, 6, 8, 13, 16, 0, 24, 4>>,
    CommandSize = byte_size(Command),
    MetadataSize = byte_size(Metadata),
    TotalSize = 4 + CommandSize + 4 + MetadataSize + byte_size(Payload),
    <<TotalSize:32, CommandSize:32, Command/binary,
      MetadataSize:32, Metadata/binary, Payload/binary>>.
