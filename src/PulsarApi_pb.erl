-file("src/PulsarApi_pb.erl", 1).

-module('PulsarApi_pb').

-export([encode_basecommand/1, decode_basecommand/1,
	 delimited_decode_basecommand/1,
	 encode_commandgetschemaresponse/1,
	 decode_commandgetschemaresponse/1,
	 delimited_decode_commandgetschemaresponse/1,
	 encode_commandgetschema/1, decode_commandgetschema/1,
	 delimited_decode_commandgetschema/1,
	 encode_commandgettopicsofnamespaceresponse/1,
	 decode_commandgettopicsofnamespaceresponse/1,
	 delimited_decode_commandgettopicsofnamespaceresponse/1,
	 encode_commandgettopicsofnamespace/1,
	 decode_commandgettopicsofnamespace/1,
	 delimited_decode_commandgettopicsofnamespace/1,
	 encode_commandgetlastmessageidresponse/1,
	 decode_commandgetlastmessageidresponse/1,
	 delimited_decode_commandgetlastmessageidresponse/1,
	 encode_commandgetlastmessageid/1,
	 decode_commandgetlastmessageid/1,
	 delimited_decode_commandgetlastmessageid/1,
	 encode_commandconsumerstatsresponse/1,
	 decode_commandconsumerstatsresponse/1,
	 delimited_decode_commandconsumerstatsresponse/1,
	 encode_commandconsumerstats/1,
	 decode_commandconsumerstats/1,
	 delimited_decode_commandconsumerstats/1,
	 encode_commandpong/1, decode_commandpong/1,
	 delimited_decode_commandpong/1, encode_commandping/1,
	 decode_commandping/1, delimited_decode_commandping/1,
	 encode_commanderror/1, decode_commanderror/1,
	 delimited_decode_commanderror/1,
	 encode_commandproducersuccess/1,
	 decode_commandproducersuccess/1,
	 delimited_decode_commandproducersuccess/1,
	 encode_commandsuccess/1, decode_commandsuccess/1,
	 delimited_decode_commandsuccess/1,
	 encode_commandredeliverunacknowledgedmessages/1,
	 decode_commandredeliverunacknowledgedmessages/1,
	 delimited_decode_commandredeliverunacknowledgedmessages/1,
	 encode_commandcloseconsumer/1,
	 decode_commandcloseconsumer/1,
	 delimited_decode_commandcloseconsumer/1,
	 encode_commandcloseproducer/1,
	 decode_commandcloseproducer/1,
	 delimited_decode_commandcloseproducer/1,
	 encode_commandreachedendoftopic/1,
	 decode_commandreachedendoftopic/1,
	 delimited_decode_commandreachedendoftopic/1,
	 encode_commandseek/1, decode_commandseek/1,
	 delimited_decode_commandseek/1,
	 encode_commandunsubscribe/1,
	 decode_commandunsubscribe/1,
	 delimited_decode_commandunsubscribe/1,
	 encode_commandflow/1, decode_commandflow/1,
	 delimited_decode_commandflow/1,
	 encode_commandactiveconsumerchange/1,
	 decode_commandactiveconsumerchange/1,
	 delimited_decode_commandactiveconsumerchange/1,
	 encode_commandack/1, decode_commandack/1,
	 delimited_decode_commandack/1, encode_commandmessage/1,
	 decode_commandmessage/1,
	 delimited_decode_commandmessage/1,
	 encode_commandsenderror/1, decode_commandsenderror/1,
	 delimited_decode_commandsenderror/1,
	 encode_commandsendreceipt/1,
	 decode_commandsendreceipt/1,
	 delimited_decode_commandsendreceipt/1,
	 encode_commandsend/1, decode_commandsend/1,
	 delimited_decode_commandsend/1,
	 encode_commandproducer/1, decode_commandproducer/1,
	 delimited_decode_commandproducer/1,
	 encode_commandlookuptopicresponse/1,
	 decode_commandlookuptopicresponse/1,
	 delimited_decode_commandlookuptopicresponse/1,
	 encode_commandlookuptopic/1,
	 decode_commandlookuptopic/1,
	 delimited_decode_commandlookuptopic/1,
	 encode_commandpartitionedtopicmetadataresponse/1,
	 decode_commandpartitionedtopicmetadataresponse/1,
	 delimited_decode_commandpartitionedtopicmetadataresponse/1,
	 encode_commandpartitionedtopicmetadata/1,
	 decode_commandpartitionedtopicmetadata/1,
	 delimited_decode_commandpartitionedtopicmetadata/1,
	 encode_commandsubscribe/1, decode_commandsubscribe/1,
	 delimited_decode_commandsubscribe/1, encode_authdata/1,
	 decode_authdata/1, delimited_decode_authdata/1,
	 encode_commandauthchallenge/1,
	 decode_commandauthchallenge/1,
	 delimited_decode_commandauthchallenge/1,
	 encode_commandauthresponse/1,
	 decode_commandauthresponse/1,
	 delimited_decode_commandauthresponse/1,
	 encode_commandconnected/1, decode_commandconnected/1,
	 delimited_decode_commandconnected/1,
	 encode_commandconnect/1, decode_commandconnect/1,
	 delimited_decode_commandconnect/1,
	 encode_singlemessagemetadata/1,
	 decode_singlemessagemetadata/1,
	 delimited_decode_singlemessagemetadata/1,
	 encode_messagemetadata/1, decode_messagemetadata/1,
	 delimited_decode_messagemetadata/1,
	 encode_encryptionkeys/1, decode_encryptionkeys/1,
	 delimited_decode_encryptionkeys/1,
	 encode_keylongvalue/1, decode_keylongvalue/1,
	 delimited_decode_keylongvalue/1, encode_keyvalue/1,
	 decode_keyvalue/1, delimited_decode_keyvalue/1,
	 encode_messageiddata/1, decode_messageiddata/1,
	 delimited_decode_messageiddata/1, encode_schema/1,
	 decode_schema/1, delimited_decode_schema/1]).

-export([has_extension/2, extension_size/1,
	 get_extension/2, set_extension/3]).

-export([decode_extensions/1]).

-export([encode/1, decode/2, delimited_decode/2]).

-export([int_to_enum/2, enum_to_int/2]).

-record(basecommand,
	{type, connect, connected, subscribe, producer, send,
	 send_receipt, send_error, message, ack, flow,
	 unsubscribe, success, error, close_producer,
	 close_consumer, producer_success, ping, pong,
	 redeliverunacknowledgedmessages, partitionmetadata,
	 partitionmetadataresponse, lookuptopic,
	 lookuptopicresponse, consumerstats,
	 consumerstatsresponse, reachedendoftopic, seek,
	 getlastmessageid, getlastmessageidresponse,
	 active_consumer_change, gettopicsofnamespace,
	 gettopicsofnamespaceresponse, getschema,
	 getschemaresponse, authchallenge, authresponse}).

-record(commandgetschemaresponse,
	{request_id, error_code, error_message, schema,
	 schema_version}).

-record(commandgetschema,
	{request_id, topic, schema_version}).

-record(commandgettopicsofnamespaceresponse,
	{request_id, topics}).

-record(commandgettopicsofnamespace,
	{request_id, namespace, mode}).

-record(commandgetlastmessageidresponse,
	{last_message_id, request_id}).

-record(commandgetlastmessageid,
	{consumer_id, request_id}).

-record(commandconsumerstatsresponse,
	{request_id, error_code, error_message, msgrateout,
	 msgthroughputout, msgrateredeliver, consumername,
	 availablepermits, unackedmessages,
	 blockedconsumeronunackedmsgs, address, connectedsince,
	 type, msgrateexpired, msgbacklog}).

-record(commandconsumerstats,
	{request_id, consumer_id}).

-record(commandpong, {}).

-record(commandping, {}).

-record(commanderror, {request_id, error, message}).

-record(commandproducersuccess,
	{request_id, producer_name, last_sequence_id,
	 schema_version}).

-record(commandsuccess, {request_id, schema}).

-record(commandredeliverunacknowledgedmessages,
	{consumer_id, message_ids}).

-record(commandcloseconsumer,
	{consumer_id, request_id}).

-record(commandcloseproducer,
	{producer_id, request_id}).

-record(commandreachedendoftopic, {consumer_id}).

-record(commandseek,
	{consumer_id, request_id, message_id,
	 message_publish_time}).

-record(commandunsubscribe, {consumer_id, request_id}).

-record(commandflow, {consumer_id, messagepermits}).

-record(commandactiveconsumerchange,
	{consumer_id, is_active}).

-record(commandack,
	{consumer_id, ack_type, message_id, validation_error,
	 properties}).

-record(commandmessage,
	{consumer_id, message_id, redelivery_count}).

-record(commandsenderror,
	{producer_id, sequence_id, error, message}).

-record(commandsendreceipt,
	{producer_id, sequence_id, message_id}).

-record(commandsend,
	{producer_id, sequence_id, num_messages}).

-record(commandproducer,
	{topic, producer_id, request_id, producer_name,
	 encrypted, metadata, schema}).

-record(commandlookuptopicresponse,
	{brokerserviceurl, brokerserviceurltls, response,
	 request_id, authoritative, error, message,
	 proxy_through_service_url}).

-record(commandlookuptopic,
	{topic, request_id, authoritative, original_principal,
	 original_auth_data, original_auth_method}).

-record(commandpartitionedtopicmetadataresponse,
	{partitions, request_id, response, error, message}).

-record(commandpartitionedtopicmetadata,
	{topic, request_id, original_principal,
	 original_auth_data, original_auth_method}).

-record(commandsubscribe,
	{topic, subscription, subtype, consumer_id, request_id,
	 consumer_name, priority_level, durable,
	 start_message_id, metadata, read_compacted, schema,
	 initialposition}).

-record(authdata, {auth_method_name, auth_data}).

-record(commandauthchallenge,
	{server_version, challenge, protocol_version}).

-record(commandauthresponse,
	{client_version, response, protocol_version}).

-record(commandconnected,
	{server_version, protocol_version}).

-record(commandconnect,
	{client_version, auth_method, auth_data,
	 protocol_version, auth_method_name, proxy_to_broker_url,
	 original_principal, original_auth_data,
	 original_auth_method}).

-record(singlemessagemetadata,
	{properties, partition_key, payload_size, compacted_out,
	 event_time, partition_key_b64_encoded, ordering_key}).

-record(messagemetadata,
	{producer_name, sequence_id, publish_time, properties,
	 replicated_from, partition_key, replicate_to,
	 compression, uncompressed_size, num_messages_in_batch,
	 event_time, encryption_keys, encryption_algo,
	 encryption_param, schema_version,
	 partition_key_b64_encoded, ordering_key}).

-record(encryptionkeys, {key, value, metadata}).

-record(keylongvalue, {key, value}).

-record(keyvalue, {key, value}).

-record(messageiddata,
	{ledgerid, entryid, partition, batch_index}).

-record(schema, {name, schema_data, type, properties}).

-dialyzer(no_match).

encode([]) -> [];
encode(Records) when is_list(Records) ->
    delimited_encode(Records);
encode(Record) -> encode(element(1, Record), Record).

encode_basecommand(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_basecommand(Record)
    when is_record(Record, basecommand) ->
    encode(basecommand, Record).

encode_commandgetschemaresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgetschemaresponse(Record)
    when is_record(Record, commandgetschemaresponse) ->
    encode(commandgetschemaresponse, Record).

encode_commandgetschema(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgetschema(Record)
    when is_record(Record, commandgetschema) ->
    encode(commandgetschema, Record).

encode_commandgettopicsofnamespaceresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgettopicsofnamespaceresponse(Record)
    when is_record(Record,
		   commandgettopicsofnamespaceresponse) ->
    encode(commandgettopicsofnamespaceresponse, Record).

encode_commandgettopicsofnamespace(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgettopicsofnamespace(Record)
    when is_record(Record, commandgettopicsofnamespace) ->
    encode(commandgettopicsofnamespace, Record).

encode_commandgetlastmessageidresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgetlastmessageidresponse(Record)
    when is_record(Record,
		   commandgetlastmessageidresponse) ->
    encode(commandgetlastmessageidresponse, Record).

encode_commandgetlastmessageid(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandgetlastmessageid(Record)
    when is_record(Record, commandgetlastmessageid) ->
    encode(commandgetlastmessageid, Record).

encode_commandconsumerstatsresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandconsumerstatsresponse(Record)
    when is_record(Record, commandconsumerstatsresponse) ->
    encode(commandconsumerstatsresponse, Record).

encode_commandconsumerstats(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandconsumerstats(Record)
    when is_record(Record, commandconsumerstats) ->
    encode(commandconsumerstats, Record).

encode_commandpong(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandpong(Record)
    when is_record(Record, commandpong) ->
    encode(commandpong, Record).

encode_commandping(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandping(Record)
    when is_record(Record, commandping) ->
    encode(commandping, Record).

encode_commanderror(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commanderror(Record)
    when is_record(Record, commanderror) ->
    encode(commanderror, Record).

encode_commandproducersuccess(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandproducersuccess(Record)
    when is_record(Record, commandproducersuccess) ->
    encode(commandproducersuccess, Record).

encode_commandsuccess(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandsuccess(Record)
    when is_record(Record, commandsuccess) ->
    encode(commandsuccess, Record).

encode_commandredeliverunacknowledgedmessages(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandredeliverunacknowledgedmessages(Record)
    when is_record(Record,
		   commandredeliverunacknowledgedmessages) ->
    encode(commandredeliverunacknowledgedmessages, Record).

encode_commandcloseconsumer(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandcloseconsumer(Record)
    when is_record(Record, commandcloseconsumer) ->
    encode(commandcloseconsumer, Record).

encode_commandcloseproducer(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandcloseproducer(Record)
    when is_record(Record, commandcloseproducer) ->
    encode(commandcloseproducer, Record).

encode_commandreachedendoftopic(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandreachedendoftopic(Record)
    when is_record(Record, commandreachedendoftopic) ->
    encode(commandreachedendoftopic, Record).

encode_commandseek(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandseek(Record)
    when is_record(Record, commandseek) ->
    encode(commandseek, Record).

encode_commandunsubscribe(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandunsubscribe(Record)
    when is_record(Record, commandunsubscribe) ->
    encode(commandunsubscribe, Record).

encode_commandflow(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandflow(Record)
    when is_record(Record, commandflow) ->
    encode(commandflow, Record).

encode_commandactiveconsumerchange(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandactiveconsumerchange(Record)
    when is_record(Record, commandactiveconsumerchange) ->
    encode(commandactiveconsumerchange, Record).

encode_commandack(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandack(Record)
    when is_record(Record, commandack) ->
    encode(commandack, Record).

encode_commandmessage(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandmessage(Record)
    when is_record(Record, commandmessage) ->
    encode(commandmessage, Record).

encode_commandsenderror(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandsenderror(Record)
    when is_record(Record, commandsenderror) ->
    encode(commandsenderror, Record).

encode_commandsendreceipt(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandsendreceipt(Record)
    when is_record(Record, commandsendreceipt) ->
    encode(commandsendreceipt, Record).

encode_commandsend(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandsend(Record)
    when is_record(Record, commandsend) ->
    encode(commandsend, Record).

encode_commandproducer(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandproducer(Record)
    when is_record(Record, commandproducer) ->
    encode(commandproducer, Record).

encode_commandlookuptopicresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandlookuptopicresponse(Record)
    when is_record(Record, commandlookuptopicresponse) ->
    encode(commandlookuptopicresponse, Record).

encode_commandlookuptopic(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandlookuptopic(Record)
    when is_record(Record, commandlookuptopic) ->
    encode(commandlookuptopic, Record).

encode_commandpartitionedtopicmetadataresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandpartitionedtopicmetadataresponse(Record)
    when is_record(Record,
		   commandpartitionedtopicmetadataresponse) ->
    encode(commandpartitionedtopicmetadataresponse, Record).

encode_commandpartitionedtopicmetadata(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandpartitionedtopicmetadata(Record)
    when is_record(Record,
		   commandpartitionedtopicmetadata) ->
    encode(commandpartitionedtopicmetadata, Record).

encode_commandsubscribe(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandsubscribe(Record)
    when is_record(Record, commandsubscribe) ->
    encode(commandsubscribe, Record).

encode_authdata(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_authdata(Record)
    when is_record(Record, authdata) ->
    encode(authdata, Record).

encode_commandauthchallenge(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandauthchallenge(Record)
    when is_record(Record, commandauthchallenge) ->
    encode(commandauthchallenge, Record).

encode_commandauthresponse(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandauthresponse(Record)
    when is_record(Record, commandauthresponse) ->
    encode(commandauthresponse, Record).

encode_commandconnected(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_commandconnected(Record)
    when is_record(Record, commandconnected) ->
    encode(commandconnected, Record).

encode_commandconnect(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_commandconnect(Record)
    when is_record(Record, commandconnect) ->
    encode(commandconnect, Record).

encode_singlemessagemetadata(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_singlemessagemetadata(Record)
    when is_record(Record, singlemessagemetadata) ->
    encode(singlemessagemetadata, Record).

encode_messagemetadata(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_messagemetadata(Record)
    when is_record(Record, messagemetadata) ->
    encode(messagemetadata, Record).

encode_encryptionkeys(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_encryptionkeys(Record)
    when is_record(Record, encryptionkeys) ->
    encode(encryptionkeys, Record).

encode_keylongvalue(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_keylongvalue(Record)
    when is_record(Record, keylongvalue) ->
    encode(keylongvalue, Record).

encode_keyvalue(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_keyvalue(Record)
    when is_record(Record, keyvalue) ->
    encode(keyvalue, Record).

encode_messageiddata(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_messageiddata(Record)
    when is_record(Record, messageiddata) ->
    encode(messageiddata, Record).

encode_schema(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_schema(Record) when is_record(Record, schema) ->
    encode(schema, Record).

encode(schema, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(schema, Record) ->
    [iolist(schema, Record) | encode_extensions(Record)];
encode(messageiddata, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(messageiddata, Record) ->
    [iolist(messageiddata, Record)
     | encode_extensions(Record)];
encode(keyvalue, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(keyvalue, Record) ->
    [iolist(keyvalue, Record) | encode_extensions(Record)];
encode(keylongvalue, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(keylongvalue, Record) ->
    [iolist(keylongvalue, Record)
     | encode_extensions(Record)];
encode(encryptionkeys, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(encryptionkeys, Record) ->
    [iolist(encryptionkeys, Record)
     | encode_extensions(Record)];
encode(messagemetadata, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(messagemetadata, Record) ->
    [iolist(messagemetadata, Record)
     | encode_extensions(Record)];
encode(singlemessagemetadata, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(singlemessagemetadata, Record) ->
    [iolist(singlemessagemetadata, Record)
     | encode_extensions(Record)];
encode(commandconnect, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandconnect, Record) ->
    [iolist(commandconnect, Record)
     | encode_extensions(Record)];
encode(commandconnected, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandconnected, Record) ->
    [iolist(commandconnected, Record)
     | encode_extensions(Record)];
encode(commandauthresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandauthresponse, Record) ->
    [iolist(commandauthresponse, Record)
     | encode_extensions(Record)];
encode(commandauthchallenge, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandauthchallenge, Record) ->
    [iolist(commandauthchallenge, Record)
     | encode_extensions(Record)];
encode(authdata, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(authdata, Record) ->
    [iolist(authdata, Record) | encode_extensions(Record)];
encode(commandsubscribe, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandsubscribe, Record) ->
    [iolist(commandsubscribe, Record)
     | encode_extensions(Record)];
encode(commandpartitionedtopicmetadata, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandpartitionedtopicmetadata, Record) ->
    [iolist(commandpartitionedtopicmetadata, Record)
     | encode_extensions(Record)];
encode(commandpartitionedtopicmetadataresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandpartitionedtopicmetadataresponse,
       Record) ->
    [iolist(commandpartitionedtopicmetadataresponse, Record)
     | encode_extensions(Record)];
encode(commandlookuptopic, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandlookuptopic, Record) ->
    [iolist(commandlookuptopic, Record)
     | encode_extensions(Record)];
encode(commandlookuptopicresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandlookuptopicresponse, Record) ->
    [iolist(commandlookuptopicresponse, Record)
     | encode_extensions(Record)];
encode(commandproducer, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandproducer, Record) ->
    [iolist(commandproducer, Record)
     | encode_extensions(Record)];
encode(commandsend, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandsend, Record) ->
    [iolist(commandsend, Record)
     | encode_extensions(Record)];
encode(commandsendreceipt, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandsendreceipt, Record) ->
    [iolist(commandsendreceipt, Record)
     | encode_extensions(Record)];
encode(commandsenderror, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandsenderror, Record) ->
    [iolist(commandsenderror, Record)
     | encode_extensions(Record)];
encode(commandmessage, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandmessage, Record) ->
    [iolist(commandmessage, Record)
     | encode_extensions(Record)];
encode(commandack, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandack, Record) ->
    [iolist(commandack, Record)
     | encode_extensions(Record)];
encode(commandactiveconsumerchange, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandactiveconsumerchange, Record) ->
    [iolist(commandactiveconsumerchange, Record)
     | encode_extensions(Record)];
encode(commandflow, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandflow, Record) ->
    [iolist(commandflow, Record)
     | encode_extensions(Record)];
encode(commandunsubscribe, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandunsubscribe, Record) ->
    [iolist(commandunsubscribe, Record)
     | encode_extensions(Record)];
encode(commandseek, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandseek, Record) ->
    [iolist(commandseek, Record)
     | encode_extensions(Record)];
encode(commandreachedendoftopic, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandreachedendoftopic, Record) ->
    [iolist(commandreachedendoftopic, Record)
     | encode_extensions(Record)];
encode(commandcloseproducer, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandcloseproducer, Record) ->
    [iolist(commandcloseproducer, Record)
     | encode_extensions(Record)];
encode(commandcloseconsumer, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandcloseconsumer, Record) ->
    [iolist(commandcloseconsumer, Record)
     | encode_extensions(Record)];
encode(commandredeliverunacknowledgedmessages, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandredeliverunacknowledgedmessages,
       Record) ->
    [iolist(commandredeliverunacknowledgedmessages, Record)
     | encode_extensions(Record)];
encode(commandsuccess, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandsuccess, Record) ->
    [iolist(commandsuccess, Record)
     | encode_extensions(Record)];
encode(commandproducersuccess, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandproducersuccess, Record) ->
    [iolist(commandproducersuccess, Record)
     | encode_extensions(Record)];
encode(commanderror, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commanderror, Record) ->
    [iolist(commanderror, Record)
     | encode_extensions(Record)];
encode(commandping, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandping, Record) ->
    [iolist(commandping, Record)
     | encode_extensions(Record)];
encode(commandpong, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(commandpong, Record) ->
    [iolist(commandpong, Record)
     | encode_extensions(Record)];
encode(commandconsumerstats, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandconsumerstats, Record) ->
    [iolist(commandconsumerstats, Record)
     | encode_extensions(Record)];
encode(commandconsumerstatsresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandconsumerstatsresponse, Record) ->
    [iolist(commandconsumerstatsresponse, Record)
     | encode_extensions(Record)];
encode(commandgetlastmessageid, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgetlastmessageid, Record) ->
    [iolist(commandgetlastmessageid, Record)
     | encode_extensions(Record)];
encode(commandgetlastmessageidresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgetlastmessageidresponse, Record) ->
    [iolist(commandgetlastmessageidresponse, Record)
     | encode_extensions(Record)];
encode(commandgettopicsofnamespace, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgettopicsofnamespace, Record) ->
    [iolist(commandgettopicsofnamespace, Record)
     | encode_extensions(Record)];
encode(commandgettopicsofnamespaceresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgettopicsofnamespaceresponse, Record) ->
    [iolist(commandgettopicsofnamespaceresponse, Record)
     | encode_extensions(Record)];
encode(commandgetschema, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgetschema, Record) ->
    [iolist(commandgetschema, Record)
     | encode_extensions(Record)];
encode(commandgetschemaresponse, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(commandgetschemaresponse, Record) ->
    [iolist(commandgetschemaresponse, Record)
     | encode_extensions(Record)];
encode(basecommand, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(basecommand, Record) ->
    [iolist(basecommand, Record)
     | encode_extensions(Record)].

encode_extensions(_) -> [].

delimited_encode(Records) ->
    lists:map(fun (Record) ->
		      IoRec = encode(Record),
		      Size = iolist_size(IoRec),
		      [protobuffs:encode_varint(Size), IoRec]
	      end,
	      Records).

iolist(schema, Record) ->
    [pack(1, required,
	  with_default(Record#schema.name, none), string, []),
     pack(3, required,
	  with_default(Record#schema.schema_data, none), bytes,
	  []),
     pack(4, required,
	  with_default(Record#schema.type, none), schema_type,
	  []),
     pack(5, repeated,
	  with_default(Record#schema.properties, none), keyvalue,
	  [])];
iolist(messageiddata, Record) ->
    [pack(1, required,
	  with_default(Record#messageiddata.ledgerid, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#messageiddata.entryid, none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#messageiddata.partition, -1), int32,
	  []),
     pack(4, optional,
	  with_default(Record#messageiddata.batch_index, -1),
	  int32, [])];
iolist(keyvalue, Record) ->
    [pack(1, required,
	  with_default(Record#keyvalue.key, none), string, []),
     pack(2, required,
	  with_default(Record#keyvalue.value, none), string, [])];
iolist(keylongvalue, Record) ->
    [pack(1, required,
	  with_default(Record#keylongvalue.key, none), string,
	  []),
     pack(2, required,
	  with_default(Record#keylongvalue.value, none), uint64,
	  [])];
iolist(encryptionkeys, Record) ->
    [pack(1, required,
	  with_default(Record#encryptionkeys.key, none), string,
	  []),
     pack(2, required,
	  with_default(Record#encryptionkeys.value, none), bytes,
	  []),
     pack(3, repeated,
	  with_default(Record#encryptionkeys.metadata, none),
	  keyvalue, [])];
iolist(messagemetadata, Record) ->
    [pack(1, required,
	  with_default(Record#messagemetadata.producer_name,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#messagemetadata.sequence_id, none),
	  uint64, []),
     pack(3, required,
	  with_default(Record#messagemetadata.publish_time, none),
	  uint64, []),
     pack(4, repeated,
	  with_default(Record#messagemetadata.properties, none),
	  keyvalue, []),
     pack(5, optional,
	  with_default(Record#messagemetadata.replicated_from,
		       none),
	  string, []),
     pack(6, optional,
	  with_default(Record#messagemetadata.partition_key,
		       none),
	  string, []),
     pack(7, repeated,
	  with_default(Record#messagemetadata.replicate_to, none),
	  string, []),
     pack(8, optional,
	  with_default(Record#messagemetadata.compression,
		       'NONE'),
	  compressiontype, []),
     pack(9, optional,
	  with_default(Record#messagemetadata.uncompressed_size,
		       0),
	  uint32, []),
     pack(11, optional,
	  with_default(Record#messagemetadata.num_messages_in_batch,
		       1),
	  int32, []),
     pack(12, optional,
	  with_default(Record#messagemetadata.event_time, 0),
	  uint64, []),
     pack(13, repeated,
	  with_default(Record#messagemetadata.encryption_keys,
		       none),
	  encryptionkeys, []),
     pack(14, optional,
	  with_default(Record#messagemetadata.encryption_algo,
		       none),
	  string, []),
     pack(15, optional,
	  with_default(Record#messagemetadata.encryption_param,
		       none),
	  bytes, []),
     pack(16, optional,
	  with_default(Record#messagemetadata.schema_version,
		       none),
	  bytes, []),
     pack(17, optional,
	  with_default(Record#messagemetadata.partition_key_b64_encoded,
		       false),
	  bool, []),
     pack(18, optional,
	  with_default(Record#messagemetadata.ordering_key, none),
	  bytes, [])];
iolist(singlemessagemetadata, Record) ->
    [pack(1, repeated,
	  with_default(Record#singlemessagemetadata.properties,
		       none),
	  keyvalue, []),
     pack(2, optional,
	  with_default(Record#singlemessagemetadata.partition_key,
		       none),
	  string, []),
     pack(3, required,
	  with_default(Record#singlemessagemetadata.payload_size,
		       none),
	  int32, []),
     pack(4, optional,
	  with_default(Record#singlemessagemetadata.compacted_out,
		       false),
	  bool, []),
     pack(5, optional,
	  with_default(Record#singlemessagemetadata.event_time,
		       0),
	  uint64, []),
     pack(6, optional,
	  with_default(Record#singlemessagemetadata.partition_key_b64_encoded,
		       false),
	  bool, []),
     pack(7, optional,
	  with_default(Record#singlemessagemetadata.ordering_key,
		       none),
	  bytes, [])];
iolist(commandconnect, Record) ->
    [pack(1, required,
	  with_default(Record#commandconnect.client_version,
		       none),
	  string, []),
     pack(2, optional,
	  with_default(Record#commandconnect.auth_method, none),
	  authmethod, []),
     pack(3, optional,
	  with_default(Record#commandconnect.auth_data, none),
	  bytes, []),
     pack(4, optional,
	  with_default(Record#commandconnect.protocol_version, 0),
	  int32, []),
     pack(5, optional,
	  with_default(Record#commandconnect.auth_method_name,
		       none),
	  string, []),
     pack(6, optional,
	  with_default(Record#commandconnect.proxy_to_broker_url,
		       none),
	  string, []),
     pack(7, optional,
	  with_default(Record#commandconnect.original_principal,
		       none),
	  string, []),
     pack(8, optional,
	  with_default(Record#commandconnect.original_auth_data,
		       none),
	  string, []),
     pack(9, optional,
	  with_default(Record#commandconnect.original_auth_method,
		       none),
	  string, [])];
iolist(commandconnected, Record) ->
    [pack(1, required,
	  with_default(Record#commandconnected.server_version,
		       none),
	  string, []),
     pack(2, optional,
	  with_default(Record#commandconnected.protocol_version,
		       0),
	  int32, [])];
iolist(commandauthresponse, Record) ->
    [pack(1, optional,
	  with_default(Record#commandauthresponse.client_version,
		       none),
	  string, []),
     pack(2, optional,
	  with_default(Record#commandauthresponse.response, none),
	  authdata, []),
     pack(3, optional,
	  with_default(Record#commandauthresponse.protocol_version,
		       0),
	  int32, [])];
iolist(commandauthchallenge, Record) ->
    [pack(1, optional,
	  with_default(Record#commandauthchallenge.server_version,
		       none),
	  string, []),
     pack(2, optional,
	  with_default(Record#commandauthchallenge.challenge,
		       none),
	  authdata, []),
     pack(3, optional,
	  with_default(Record#commandauthchallenge.protocol_version,
		       0),
	  int32, [])];
iolist(authdata, Record) ->
    [pack(1, optional,
	  with_default(Record#authdata.auth_method_name, none),
	  string, []),
     pack(2, optional,
	  with_default(Record#authdata.auth_data, none), bytes,
	  [])];
iolist(commandsubscribe, Record) ->
    [pack(1, required,
	  with_default(Record#commandsubscribe.topic, none),
	  string, []),
     pack(2, required,
	  with_default(Record#commandsubscribe.subscription,
		       none),
	  string, []),
     pack(3, required,
	  with_default(Record#commandsubscribe.subtype, none),
	  commandsubscribe_subtype, []),
     pack(4, required,
	  with_default(Record#commandsubscribe.consumer_id, none),
	  uint64, []),
     pack(5, required,
	  with_default(Record#commandsubscribe.request_id, none),
	  uint64, []),
     pack(6, optional,
	  with_default(Record#commandsubscribe.consumer_name,
		       none),
	  string, []),
     pack(7, optional,
	  with_default(Record#commandsubscribe.priority_level,
		       none),
	  int32, []),
     pack(8, optional,
	  with_default(Record#commandsubscribe.durable, true),
	  bool, []),
     pack(9, optional,
	  with_default(Record#commandsubscribe.start_message_id,
		       none),
	  messageiddata, []),
     pack(10, repeated,
	  with_default(Record#commandsubscribe.metadata, none),
	  keyvalue, []),
     pack(11, optional,
	  with_default(Record#commandsubscribe.read_compacted,
		       none),
	  bool, []),
     pack(12, optional,
	  with_default(Record#commandsubscribe.schema, none),
	  schema, []),
     pack(13, optional,
	  with_default(Record#commandsubscribe.initialposition,
		       'Latest'),
	  commandsubscribe_initialposition, [])];
iolist(commandpartitionedtopicmetadata, Record) ->
    [pack(1, required,
	  with_default(Record#commandpartitionedtopicmetadata.topic,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#commandpartitionedtopicmetadata.request_id,
		       none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandpartitionedtopicmetadata.original_principal,
		       none),
	  string, []),
     pack(4, optional,
	  with_default(Record#commandpartitionedtopicmetadata.original_auth_data,
		       none),
	  string, []),
     pack(5, optional,
	  with_default(Record#commandpartitionedtopicmetadata.original_auth_method,
		       none),
	  string, [])];
iolist(commandpartitionedtopicmetadataresponse,
       Record) ->
    [pack(1, optional,
	  with_default(Record#commandpartitionedtopicmetadataresponse.partitions,
		       none),
	  uint32, []),
     pack(2, required,
	  with_default(Record#commandpartitionedtopicmetadataresponse.request_id,
		       none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandpartitionedtopicmetadataresponse.response,
		       none),
	  commandpartitionedtopicmetadataresponse_lookuptype, []),
     pack(4, optional,
	  with_default(Record#commandpartitionedtopicmetadataresponse.error,
		       none),
	  servererror, []),
     pack(5, optional,
	  with_default(Record#commandpartitionedtopicmetadataresponse.message,
		       none),
	  string, [])];
iolist(commandlookuptopic, Record) ->
    [pack(1, required,
	  with_default(Record#commandlookuptopic.topic, none),
	  string, []),
     pack(2, required,
	  with_default(Record#commandlookuptopic.request_id,
		       none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandlookuptopic.authoritative,
		       false),
	  bool, []),
     pack(4, optional,
	  with_default(Record#commandlookuptopic.original_principal,
		       none),
	  string, []),
     pack(5, optional,
	  with_default(Record#commandlookuptopic.original_auth_data,
		       none),
	  string, []),
     pack(6, optional,
	  with_default(Record#commandlookuptopic.original_auth_method,
		       none),
	  string, [])];
iolist(commandlookuptopicresponse, Record) ->
    [pack(1, optional,
	  with_default(Record#commandlookuptopicresponse.brokerserviceurl,
		       none),
	  string, []),
     pack(2, optional,
	  with_default(Record#commandlookuptopicresponse.brokerserviceurltls,
		       none),
	  string, []),
     pack(3, optional,
	  with_default(Record#commandlookuptopicresponse.response,
		       none),
	  commandlookuptopicresponse_lookuptype, []),
     pack(4, required,
	  with_default(Record#commandlookuptopicresponse.request_id,
		       none),
	  uint64, []),
     pack(5, optional,
	  with_default(Record#commandlookuptopicresponse.authoritative,
		       false),
	  bool, []),
     pack(6, optional,
	  with_default(Record#commandlookuptopicresponse.error,
		       none),
	  servererror, []),
     pack(7, optional,
	  with_default(Record#commandlookuptopicresponse.message,
		       none),
	  string, []),
     pack(8, optional,
	  with_default(Record#commandlookuptopicresponse.proxy_through_service_url,
		       false),
	  bool, [])];
iolist(commandproducer, Record) ->
    [pack(1, required,
	  with_default(Record#commandproducer.topic, none),
	  string, []),
     pack(2, required,
	  with_default(Record#commandproducer.producer_id, none),
	  uint64, []),
     pack(3, required,
	  with_default(Record#commandproducer.request_id, none),
	  uint64, []),
     pack(4, optional,
	  with_default(Record#commandproducer.producer_name,
		       none),
	  string, []),
     pack(5, optional,
	  with_default(Record#commandproducer.encrypted, false),
	  bool, []),
     pack(6, repeated,
	  with_default(Record#commandproducer.metadata, none),
	  keyvalue, []),
     pack(7, optional,
	  with_default(Record#commandproducer.schema, none),
	  schema, [])];
iolist(commandsend, Record) ->
    [pack(1, required,
	  with_default(Record#commandsend.producer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandsend.sequence_id, none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandsend.num_messages, 1), int32,
	  [])];
iolist(commandsendreceipt, Record) ->
    [pack(1, required,
	  with_default(Record#commandsendreceipt.producer_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandsendreceipt.sequence_id,
		       none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandsendreceipt.message_id,
		       none),
	  messageiddata, [])];
iolist(commandsenderror, Record) ->
    [pack(1, required,
	  with_default(Record#commandsenderror.producer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandsenderror.sequence_id, none),
	  uint64, []),
     pack(3, required,
	  with_default(Record#commandsenderror.error, none),
	  servererror, []),
     pack(4, required,
	  with_default(Record#commandsenderror.message, none),
	  string, [])];
iolist(commandmessage, Record) ->
    [pack(1, required,
	  with_default(Record#commandmessage.consumer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandmessage.message_id, none),
	  messageiddata, []),
     pack(3, optional,
	  with_default(Record#commandmessage.redelivery_count, 0),
	  uint32, [])];
iolist(commandack, Record) ->
    [pack(1, required,
	  with_default(Record#commandack.consumer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandack.ack_type, none),
	  commandack_acktype, []),
     pack(3, repeated,
	  with_default(Record#commandack.message_id, none),
	  messageiddata, []),
     pack(4, optional,
	  with_default(Record#commandack.validation_error, none),
	  commandack_validationerror, []),
     pack(5, repeated,
	  with_default(Record#commandack.properties, none),
	  keylongvalue, [])];
iolist(commandactiveconsumerchange, Record) ->
    [pack(1, required,
	  with_default(Record#commandactiveconsumerchange.consumer_id,
		       none),
	  uint64, []),
     pack(2, optional,
	  with_default(Record#commandactiveconsumerchange.is_active,
		       false),
	  bool, [])];
iolist(commandflow, Record) ->
    [pack(1, required,
	  with_default(Record#commandflow.consumer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandflow.messagepermits, none),
	  uint32, [])];
iolist(commandunsubscribe, Record) ->
    [pack(1, required,
	  with_default(Record#commandunsubscribe.consumer_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandunsubscribe.request_id,
		       none),
	  uint64, [])];
iolist(commandseek, Record) ->
    [pack(1, required,
	  with_default(Record#commandseek.consumer_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandseek.request_id, none),
	  uint64, []),
     pack(3, optional,
	  with_default(Record#commandseek.message_id, none),
	  messageiddata, []),
     pack(4, optional,
	  with_default(Record#commandseek.message_publish_time,
		       none),
	  uint64, [])];
iolist(commandreachedendoftopic, Record) ->
    [pack(1, required,
	  with_default(Record#commandreachedendoftopic.consumer_id,
		       none),
	  uint64, [])];
iolist(commandcloseproducer, Record) ->
    [pack(1, required,
	  with_default(Record#commandcloseproducer.producer_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandcloseproducer.request_id,
		       none),
	  uint64, [])];
iolist(commandcloseconsumer, Record) ->
    [pack(1, required,
	  with_default(Record#commandcloseconsumer.consumer_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandcloseconsumer.request_id,
		       none),
	  uint64, [])];
iolist(commandredeliverunacknowledgedmessages,
       Record) ->
    [pack(1, required,
	  with_default(Record#commandredeliverunacknowledgedmessages.consumer_id,
		       none),
	  uint64, []),
     pack(2, repeated,
	  with_default(Record#commandredeliverunacknowledgedmessages.message_ids,
		       none),
	  messageiddata, [])];
iolist(commandsuccess, Record) ->
    [pack(1, required,
	  with_default(Record#commandsuccess.request_id, none),
	  uint64, []),
     pack(2, optional,
	  with_default(Record#commandsuccess.schema, none),
	  schema, [])];
iolist(commandproducersuccess, Record) ->
    [pack(1, required,
	  with_default(Record#commandproducersuccess.request_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandproducersuccess.producer_name,
		       none),
	  string, []),
     pack(3, optional,
	  with_default(Record#commandproducersuccess.last_sequence_id,
		       -1),
	  int64, []),
     pack(4, optional,
	  with_default(Record#commandproducersuccess.schema_version,
		       none),
	  bytes, [])];
iolist(commanderror, Record) ->
    [pack(1, required,
	  with_default(Record#commanderror.request_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commanderror.error, none),
	  servererror, []),
     pack(3, required,
	  with_default(Record#commanderror.message, none), string,
	  [])];
iolist(commandping, _Record) -> [];
iolist(commandpong, _Record) -> [];
iolist(commandconsumerstats, Record) ->
    [pack(1, required,
	  with_default(Record#commandconsumerstats.request_id,
		       none),
	  uint64, []),
     pack(4, required,
	  with_default(Record#commandconsumerstats.consumer_id,
		       none),
	  uint64, [])];
iolist(commandconsumerstatsresponse, Record) ->
    [pack(1, required,
	  with_default(Record#commandconsumerstatsresponse.request_id,
		       none),
	  uint64, []),
     pack(2, optional,
	  with_default(Record#commandconsumerstatsresponse.error_code,
		       none),
	  servererror, []),
     pack(3, optional,
	  with_default(Record#commandconsumerstatsresponse.error_message,
		       none),
	  string, []),
     pack(4, optional,
	  with_default(Record#commandconsumerstatsresponse.msgrateout,
		       none),
	  double, []),
     pack(5, optional,
	  with_default(Record#commandconsumerstatsresponse.msgthroughputout,
		       none),
	  double, []),
     pack(6, optional,
	  with_default(Record#commandconsumerstatsresponse.msgrateredeliver,
		       none),
	  double, []),
     pack(7, optional,
	  with_default(Record#commandconsumerstatsresponse.consumername,
		       none),
	  string, []),
     pack(8, optional,
	  with_default(Record#commandconsumerstatsresponse.availablepermits,
		       none),
	  uint64, []),
     pack(9, optional,
	  with_default(Record#commandconsumerstatsresponse.unackedmessages,
		       none),
	  uint64, []),
     pack(10, optional,
	  with_default(Record#commandconsumerstatsresponse.blockedconsumeronunackedmsgs,
		       none),
	  bool, []),
     pack(11, optional,
	  with_default(Record#commandconsumerstatsresponse.address,
		       none),
	  string, []),
     pack(12, optional,
	  with_default(Record#commandconsumerstatsresponse.connectedsince,
		       none),
	  string, []),
     pack(13, optional,
	  with_default(Record#commandconsumerstatsresponse.type,
		       none),
	  string, []),
     pack(14, optional,
	  with_default(Record#commandconsumerstatsresponse.msgrateexpired,
		       none),
	  double, []),
     pack(15, optional,
	  with_default(Record#commandconsumerstatsresponse.msgbacklog,
		       none),
	  uint64, [])];
iolist(commandgetlastmessageid, Record) ->
    [pack(1, required,
	  with_default(Record#commandgetlastmessageid.consumer_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandgetlastmessageid.request_id,
		       none),
	  uint64, [])];
iolist(commandgetlastmessageidresponse, Record) ->
    [pack(1, required,
	  with_default(Record#commandgetlastmessageidresponse.last_message_id,
		       none),
	  messageiddata, []),
     pack(2, required,
	  with_default(Record#commandgetlastmessageidresponse.request_id,
		       none),
	  uint64, [])];
iolist(commandgettopicsofnamespace, Record) ->
    [pack(1, required,
	  with_default(Record#commandgettopicsofnamespace.request_id,
		       none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandgettopicsofnamespace.namespace,
		       none),
	  string, []),
     pack(3, optional,
	  with_default(Record#commandgettopicsofnamespace.mode,
		       'PERSISTENT'),
	  commandgettopicsofnamespace_mode, [])];
iolist(commandgettopicsofnamespaceresponse, Record) ->
    [pack(1, required,
	  with_default(Record#commandgettopicsofnamespaceresponse.request_id,
		       none),
	  uint64, []),
     pack(2, repeated,
	  with_default(Record#commandgettopicsofnamespaceresponse.topics,
		       none),
	  string, [])];
iolist(commandgetschema, Record) ->
    [pack(1, required,
	  with_default(Record#commandgetschema.request_id, none),
	  uint64, []),
     pack(2, required,
	  with_default(Record#commandgetschema.topic, none),
	  string, []),
     pack(3, optional,
	  with_default(Record#commandgetschema.schema_version,
		       none),
	  bytes, [])];
iolist(commandgetschemaresponse, Record) ->
    [pack(1, required,
	  with_default(Record#commandgetschemaresponse.request_id,
		       none),
	  uint64, []),
     pack(2, optional,
	  with_default(Record#commandgetschemaresponse.error_code,
		       none),
	  servererror, []),
     pack(3, optional,
	  with_default(Record#commandgetschemaresponse.error_message,
		       none),
	  string, []),
     pack(4, optional,
	  with_default(Record#commandgetschemaresponse.schema,
		       none),
	  schema, []),
     pack(5, optional,
	  with_default(Record#commandgetschemaresponse.schema_version,
		       none),
	  bytes, [])];
iolist(basecommand, Record) ->
    [pack(1, required,
	  with_default(Record#basecommand.type, none),
	  basecommand_type, []),
     pack(2, optional,
	  with_default(Record#basecommand.connect, none),
	  commandconnect, []),
     pack(3, optional,
	  with_default(Record#basecommand.connected, none),
	  commandconnected, []),
     pack(4, optional,
	  with_default(Record#basecommand.subscribe, none),
	  commandsubscribe, []),
     pack(5, optional,
	  with_default(Record#basecommand.producer, none),
	  commandproducer, []),
     pack(6, optional,
	  with_default(Record#basecommand.send, none),
	  commandsend, []),
     pack(7, optional,
	  with_default(Record#basecommand.send_receipt, none),
	  commandsendreceipt, []),
     pack(8, optional,
	  with_default(Record#basecommand.send_error, none),
	  commandsenderror, []),
     pack(9, optional,
	  with_default(Record#basecommand.message, none),
	  commandmessage, []),
     pack(10, optional,
	  with_default(Record#basecommand.ack, none), commandack,
	  []),
     pack(11, optional,
	  with_default(Record#basecommand.flow, none),
	  commandflow, []),
     pack(12, optional,
	  with_default(Record#basecommand.unsubscribe, none),
	  commandunsubscribe, []),
     pack(13, optional,
	  with_default(Record#basecommand.success, none),
	  commandsuccess, []),
     pack(14, optional,
	  with_default(Record#basecommand.error, none),
	  commanderror, []),
     pack(15, optional,
	  with_default(Record#basecommand.close_producer, none),
	  commandcloseproducer, []),
     pack(16, optional,
	  with_default(Record#basecommand.close_consumer, none),
	  commandcloseconsumer, []),
     pack(17, optional,
	  with_default(Record#basecommand.producer_success, none),
	  commandproducersuccess, []),
     pack(18, optional,
	  with_default(Record#basecommand.ping, none),
	  commandping, []),
     pack(19, optional,
	  with_default(Record#basecommand.pong, none),
	  commandpong, []),
     pack(20, optional,
	  with_default(Record#basecommand.redeliverunacknowledgedmessages,
		       none),
	  commandredeliverunacknowledgedmessages, []),
     pack(21, optional,
	  with_default(Record#basecommand.partitionmetadata,
		       none),
	  commandpartitionedtopicmetadata, []),
     pack(22, optional,
	  with_default(Record#basecommand.partitionmetadataresponse,
		       none),
	  commandpartitionedtopicmetadataresponse, []),
     pack(23, optional,
	  with_default(Record#basecommand.lookuptopic, none),
	  commandlookuptopic, []),
     pack(24, optional,
	  with_default(Record#basecommand.lookuptopicresponse,
		       none),
	  commandlookuptopicresponse, []),
     pack(25, optional,
	  with_default(Record#basecommand.consumerstats, none),
	  commandconsumerstats, []),
     pack(26, optional,
	  with_default(Record#basecommand.consumerstatsresponse,
		       none),
	  commandconsumerstatsresponse, []),
     pack(27, optional,
	  with_default(Record#basecommand.reachedendoftopic,
		       none),
	  commandreachedendoftopic, []),
     pack(28, optional,
	  with_default(Record#basecommand.seek, none),
	  commandseek, []),
     pack(29, optional,
	  with_default(Record#basecommand.getlastmessageid, none),
	  commandgetlastmessageid, []),
     pack(30, optional,
	  with_default(Record#basecommand.getlastmessageidresponse,
		       none),
	  commandgetlastmessageidresponse, []),
     pack(31, optional,
	  with_default(Record#basecommand.active_consumer_change,
		       none),
	  commandactiveconsumerchange, []),
     pack(32, optional,
	  with_default(Record#basecommand.gettopicsofnamespace,
		       none),
	  commandgettopicsofnamespace, []),
     pack(33, optional,
	  with_default(Record#basecommand.gettopicsofnamespaceresponse,
		       none),
	  commandgettopicsofnamespaceresponse, []),
     pack(34, optional,
	  with_default(Record#basecommand.getschema, none),
	  commandgetschema, []),
     pack(35, optional,
	  with_default(Record#basecommand.getschemaresponse,
		       none),
	  commandgetschemaresponse, []),
     pack(36, optional,
	  with_default(Record#basecommand.authchallenge, none),
	  commandauthchallenge, []),
     pack(37, optional,
	  with_default(Record#basecommand.authresponse, none),
	  commandauthresponse, [])].

with_default(Default, Default) -> undefined;
with_default(Val, _) -> Val.

pack(_, optional, undefined, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(_, repeated_packed, undefined, _, _) -> [];
pack(_, repeated_packed, [], _, _) -> [];
pack(FNum, required, undefined, Type, _) ->
    exit({error,
	  {required_field_is_undefined, FNum, Type}});
pack(_, repeated, [], _, Acc) -> lists:reverse(Acc);
pack(FNum, repeated, [Head | Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type,
	 [pack(FNum, optional, Head, Type, []) | Acc]);
pack(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed(FNum, Data, Type);
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName | _] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, Type, _)
    when Type =:= bool;
	 Type =:= int32;
	 Type =:= uint32;
	 Type =:= int64;
	 Type =:= uint64;
	 Type =:= sint32;
	 Type =:= sint64;
	 Type =:= fixed32;
	 Type =:= sfixed32;
	 Type =:= fixed64;
	 Type =:= sfixed64;
	 Type =:= string;
	 Type =:= bytes;
	 Type =:= float;
	 Type =:= double ->
    protobuffs:encode(FNum, Data, Type);
pack(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode(FNum, enum_to_int(Type, Data), enum).

enum_to_int(basecommand_type, 'AUTH_RESPONSE') -> 37;
enum_to_int(basecommand_type, 'AUTH_CHALLENGE') -> 36;
enum_to_int(basecommand_type, 'GET_SCHEMA_RESPONSE') ->
    35;
enum_to_int(basecommand_type, 'GET_SCHEMA') -> 34;
enum_to_int(basecommand_type,
	    'GET_TOPICS_OF_NAMESPACE_RESPONSE') ->
    33;
enum_to_int(basecommand_type,
	    'GET_TOPICS_OF_NAMESPACE') ->
    32;
enum_to_int(basecommand_type,
	    'ACTIVE_CONSUMER_CHANGE') ->
    31;
enum_to_int(basecommand_type,
	    'GET_LAST_MESSAGE_ID_RESPONSE') ->
    30;
enum_to_int(basecommand_type, 'GET_LAST_MESSAGE_ID') ->
    29;
enum_to_int(basecommand_type, 'SEEK') -> 28;
enum_to_int(basecommand_type, 'REACHED_END_OF_TOPIC') ->
    27;
enum_to_int(basecommand_type,
	    'CONSUMER_STATS_RESPONSE') ->
    26;
enum_to_int(basecommand_type, 'CONSUMER_STATS') -> 25;
enum_to_int(basecommand_type, 'LOOKUP_RESPONSE') -> 24;
enum_to_int(basecommand_type, 'LOOKUP') -> 23;
enum_to_int(basecommand_type,
	    'PARTITIONED_METADATA_RESPONSE') ->
    22;
enum_to_int(basecommand_type, 'PARTITIONED_METADATA') ->
    21;
enum_to_int(basecommand_type,
	    'REDELIVER_UNACKNOWLEDGED_MESSAGES') ->
    20;
enum_to_int(basecommand_type, 'PONG') -> 19;
enum_to_int(basecommand_type, 'PING') -> 18;
enum_to_int(basecommand_type, 'PRODUCER_SUCCESS') -> 17;
enum_to_int(basecommand_type, 'CLOSE_CONSUMER') -> 16;
enum_to_int(basecommand_type, 'CLOSE_PRODUCER') -> 15;
enum_to_int(basecommand_type, 'ERROR') -> 14;
enum_to_int(basecommand_type, 'SUCCESS') -> 13;
enum_to_int(basecommand_type, 'UNSUBSCRIBE') -> 12;
enum_to_int(basecommand_type, 'FLOW') -> 11;
enum_to_int(basecommand_type, 'ACK') -> 10;
enum_to_int(basecommand_type, 'MESSAGE') -> 9;
enum_to_int(basecommand_type, 'SEND_ERROR') -> 8;
enum_to_int(basecommand_type, 'SEND_RECEIPT') -> 7;
enum_to_int(basecommand_type, 'SEND') -> 6;
enum_to_int(basecommand_type, 'PRODUCER') -> 5;
enum_to_int(basecommand_type, 'SUBSCRIBE') -> 4;
enum_to_int(basecommand_type, 'CONNECTED') -> 3;
enum_to_int(basecommand_type, 'CONNECT') -> 2;
enum_to_int(commandgettopicsofnamespace_mode, 'ALL') ->
    2;
enum_to_int(commandgettopicsofnamespace_mode,
	    'NON_PERSISTENT') ->
    1;
enum_to_int(commandgettopicsofnamespace_mode,
	    'PERSISTENT') ->
    0;
enum_to_int(commandack_acktype, 'Cumulative') -> 1;
enum_to_int(commandack_acktype, 'Individual') -> 0;
enum_to_int(commandack_validationerror,
	    'DecryptionError') ->
    4;
enum_to_int(commandack_validationerror,
	    'BatchDeSerializeError') ->
    3;
enum_to_int(commandack_validationerror,
	    'ChecksumMismatch') ->
    2;
enum_to_int(commandack_validationerror,
	    'DecompressionError') ->
    1;
enum_to_int(commandack_validationerror,
	    'UncompressedSizeCorruption') ->
    0;
enum_to_int(commandlookuptopicresponse_lookuptype,
	    'Failed') ->
    2;
enum_to_int(commandlookuptopicresponse_lookuptype,
	    'Connect') ->
    1;
enum_to_int(commandlookuptopicresponse_lookuptype,
	    'Redirect') ->
    0;
enum_to_int(commandpartitionedtopicmetadataresponse_lookuptype,
	    'Failed') ->
    1;
enum_to_int(commandpartitionedtopicmetadataresponse_lookuptype,
	    'Success') ->
    0;
enum_to_int(commandsubscribe_subtype, 'Key_Shared') ->
    3;
enum_to_int(commandsubscribe_subtype, 'Failover') -> 2;
enum_to_int(commandsubscribe_subtype, 'Shared') -> 1;
enum_to_int(commandsubscribe_subtype, 'Exclusive') -> 0;
enum_to_int(commandsubscribe_initialposition,
	    'Earliest') ->
    1;
enum_to_int(commandsubscribe_initialposition,
	    'Latest') ->
    0;
enum_to_int(schema_type, 'KeyValue') -> 15;
enum_to_int(schema_type, 'Timestamp') -> 14;
enum_to_int(schema_type, 'Time') -> 13;
enum_to_int(schema_type, 'Date') -> 12;
enum_to_int(schema_type, 'Double') -> 11;
enum_to_int(schema_type, 'Float') -> 10;
enum_to_int(schema_type, 'Int64') -> 9;
enum_to_int(schema_type, 'Int32') -> 8;
enum_to_int(schema_type, 'Int16') -> 7;
enum_to_int(schema_type, 'Int8') -> 6;
enum_to_int(schema_type, 'Bool') -> 5;
enum_to_int(schema_type, 'Avro') -> 4;
enum_to_int(schema_type, 'Protobuf') -> 3;
enum_to_int(schema_type, 'Json') -> 2;
enum_to_int(schema_type, 'String') -> 1;
enum_to_int(schema_type, 'None') -> 0;
enum_to_int(protocolversion, v14) -> 14;
enum_to_int(protocolversion, v13) -> 13;
enum_to_int(protocolversion, v12) -> 12;
enum_to_int(protocolversion, v11) -> 11;
enum_to_int(protocolversion, v10) -> 10;
enum_to_int(protocolversion, v9) -> 9;
enum_to_int(protocolversion, v8) -> 8;
enum_to_int(protocolversion, v7) -> 7;
enum_to_int(protocolversion, v6) -> 6;
enum_to_int(protocolversion, v5) -> 5;
enum_to_int(protocolversion, v4) -> 4;
enum_to_int(protocolversion, v3) -> 3;
enum_to_int(protocolversion, v2) -> 2;
enum_to_int(protocolversion, v1) -> 1;
enum_to_int(protocolversion, v0) -> 0;
enum_to_int(authmethod, 'AuthMethodAthens') -> 2;
enum_to_int(authmethod, 'AuthMethodYcaV1') -> 1;
enum_to_int(authmethod, 'AuthMethodNone') -> 0;
enum_to_int(servererror, 'ConsumerAssignError') -> 19;
enum_to_int(servererror, 'IncompatibleSchema') -> 18;
enum_to_int(servererror, 'InvalidTopicName') -> 17;
enum_to_int(servererror, 'ProducerBusy') -> 16;
enum_to_int(servererror, 'TopicTerminatedError') -> 15;
enum_to_int(servererror, 'TooManyRequests') -> 14;
enum_to_int(servererror, 'ConsumerNotFound') -> 13;
enum_to_int(servererror, 'SubscriptionNotFound') -> 12;
enum_to_int(servererror, 'TopicNotFound') -> 11;
enum_to_int(servererror, 'UnsupportedVersionError') ->
    10;
enum_to_int(servererror, 'ChecksumError') -> 9;
enum_to_int(servererror,
	    'ProducerBlockedQuotaExceededException') ->
    8;
enum_to_int(servererror,
	    'ProducerBlockedQuotaExceededError') ->
    7;
enum_to_int(servererror, 'ServiceNotReady') -> 6;
enum_to_int(servererror, 'ConsumerBusy') -> 5;
enum_to_int(servererror, 'AuthorizationError') -> 4;
enum_to_int(servererror, 'AuthenticationError') -> 3;
enum_to_int(servererror, 'PersistenceError') -> 2;
enum_to_int(servererror, 'MetadataError') -> 1;
enum_to_int(servererror, 'UnknownError') -> 0;
enum_to_int(compressiontype, 'ZSTD') -> 3;
enum_to_int(compressiontype, 'ZLIB') -> 2;
enum_to_int(compressiontype, 'LZ4') -> 1;
enum_to_int(compressiontype, 'NONE') -> 0.

int_to_enum(basecommand_type, 37) -> 'AUTH_RESPONSE';
int_to_enum(basecommand_type, 36) -> 'AUTH_CHALLENGE';
int_to_enum(basecommand_type, 35) ->
    'GET_SCHEMA_RESPONSE';
int_to_enum(basecommand_type, 34) -> 'GET_SCHEMA';
int_to_enum(basecommand_type, 33) ->
    'GET_TOPICS_OF_NAMESPACE_RESPONSE';
int_to_enum(basecommand_type, 32) ->
    'GET_TOPICS_OF_NAMESPACE';
int_to_enum(basecommand_type, 31) ->
    'ACTIVE_CONSUMER_CHANGE';
int_to_enum(basecommand_type, 30) ->
    'GET_LAST_MESSAGE_ID_RESPONSE';
int_to_enum(basecommand_type, 29) ->
    'GET_LAST_MESSAGE_ID';
int_to_enum(basecommand_type, 28) -> 'SEEK';
int_to_enum(basecommand_type, 27) ->
    'REACHED_END_OF_TOPIC';
int_to_enum(basecommand_type, 26) ->
    'CONSUMER_STATS_RESPONSE';
int_to_enum(basecommand_type, 25) -> 'CONSUMER_STATS';
int_to_enum(basecommand_type, 24) -> 'LOOKUP_RESPONSE';
int_to_enum(basecommand_type, 23) -> 'LOOKUP';
int_to_enum(basecommand_type, 22) ->
    'PARTITIONED_METADATA_RESPONSE';
int_to_enum(basecommand_type, 21) ->
    'PARTITIONED_METADATA';
int_to_enum(basecommand_type, 20) ->
    'REDELIVER_UNACKNOWLEDGED_MESSAGES';
int_to_enum(basecommand_type, 19) -> 'PONG';
int_to_enum(basecommand_type, 18) -> 'PING';
int_to_enum(basecommand_type, 17) -> 'PRODUCER_SUCCESS';
int_to_enum(basecommand_type, 16) -> 'CLOSE_CONSUMER';
int_to_enum(basecommand_type, 15) -> 'CLOSE_PRODUCER';
int_to_enum(basecommand_type, 14) -> 'ERROR';
int_to_enum(basecommand_type, 13) -> 'SUCCESS';
int_to_enum(basecommand_type, 12) -> 'UNSUBSCRIBE';
int_to_enum(basecommand_type, 11) -> 'FLOW';
int_to_enum(basecommand_type, 10) -> 'ACK';
int_to_enum(basecommand_type, 9) -> 'MESSAGE';
int_to_enum(basecommand_type, 8) -> 'SEND_ERROR';
int_to_enum(basecommand_type, 7) -> 'SEND_RECEIPT';
int_to_enum(basecommand_type, 6) -> 'SEND';
int_to_enum(basecommand_type, 5) -> 'PRODUCER';
int_to_enum(basecommand_type, 4) -> 'SUBSCRIBE';
int_to_enum(basecommand_type, 3) -> 'CONNECTED';
int_to_enum(basecommand_type, 2) -> 'CONNECT';
int_to_enum(commandgettopicsofnamespace_mode, 2) ->
    'ALL';
int_to_enum(commandgettopicsofnamespace_mode, 1) ->
    'NON_PERSISTENT';
int_to_enum(commandgettopicsofnamespace_mode, 0) ->
    'PERSISTENT';
int_to_enum(commandack_acktype, 1) -> 'Cumulative';
int_to_enum(commandack_acktype, 0) -> 'Individual';
int_to_enum(commandack_validationerror, 4) ->
    'DecryptionError';
int_to_enum(commandack_validationerror, 3) ->
    'BatchDeSerializeError';
int_to_enum(commandack_validationerror, 2) ->
    'ChecksumMismatch';
int_to_enum(commandack_validationerror, 1) ->
    'DecompressionError';
int_to_enum(commandack_validationerror, 0) ->
    'UncompressedSizeCorruption';
int_to_enum(commandlookuptopicresponse_lookuptype, 2) ->
    'Failed';
int_to_enum(commandlookuptopicresponse_lookuptype, 1) ->
    'Connect';
int_to_enum(commandlookuptopicresponse_lookuptype, 0) ->
    'Redirect';
int_to_enum(commandpartitionedtopicmetadataresponse_lookuptype,
	    1) ->
    'Failed';
int_to_enum(commandpartitionedtopicmetadataresponse_lookuptype,
	    0) ->
    'Success';
int_to_enum(commandsubscribe_subtype, 3) ->
    'Key_Shared';
int_to_enum(commandsubscribe_subtype, 2) -> 'Failover';
int_to_enum(commandsubscribe_subtype, 1) -> 'Shared';
int_to_enum(commandsubscribe_subtype, 0) -> 'Exclusive';
int_to_enum(commandsubscribe_initialposition, 1) ->
    'Earliest';
int_to_enum(commandsubscribe_initialposition, 0) ->
    'Latest';
int_to_enum(schema_type, 15) -> 'KeyValue';
int_to_enum(schema_type, 14) -> 'Timestamp';
int_to_enum(schema_type, 13) -> 'Time';
int_to_enum(schema_type, 12) -> 'Date';
int_to_enum(schema_type, 11) -> 'Double';
int_to_enum(schema_type, 10) -> 'Float';
int_to_enum(schema_type, 9) -> 'Int64';
int_to_enum(schema_type, 8) -> 'Int32';
int_to_enum(schema_type, 7) -> 'Int16';
int_to_enum(schema_type, 6) -> 'Int8';
int_to_enum(schema_type, 5) -> 'Bool';
int_to_enum(schema_type, 4) -> 'Avro';
int_to_enum(schema_type, 3) -> 'Protobuf';
int_to_enum(schema_type, 2) -> 'Json';
int_to_enum(schema_type, 1) -> 'String';
int_to_enum(schema_type, 0) -> 'None';
int_to_enum(protocolversion, 14) -> v14;
int_to_enum(protocolversion, 13) -> v13;
int_to_enum(protocolversion, 12) -> v12;
int_to_enum(protocolversion, 11) -> v11;
int_to_enum(protocolversion, 10) -> v10;
int_to_enum(protocolversion, 9) -> v9;
int_to_enum(protocolversion, 8) -> v8;
int_to_enum(protocolversion, 7) -> v7;
int_to_enum(protocolversion, 6) -> v6;
int_to_enum(protocolversion, 5) -> v5;
int_to_enum(protocolversion, 4) -> v4;
int_to_enum(protocolversion, 3) -> v3;
int_to_enum(protocolversion, 2) -> v2;
int_to_enum(protocolversion, 1) -> v1;
int_to_enum(protocolversion, 0) -> v0;
int_to_enum(authmethod, 2) -> 'AuthMethodAthens';
int_to_enum(authmethod, 1) -> 'AuthMethodYcaV1';
int_to_enum(authmethod, 0) -> 'AuthMethodNone';
int_to_enum(servererror, 19) -> 'ConsumerAssignError';
int_to_enum(servererror, 18) -> 'IncompatibleSchema';
int_to_enum(servererror, 17) -> 'InvalidTopicName';
int_to_enum(servererror, 16) -> 'ProducerBusy';
int_to_enum(servererror, 15) -> 'TopicTerminatedError';
int_to_enum(servererror, 14) -> 'TooManyRequests';
int_to_enum(servererror, 13) -> 'ConsumerNotFound';
int_to_enum(servererror, 12) -> 'SubscriptionNotFound';
int_to_enum(servererror, 11) -> 'TopicNotFound';
int_to_enum(servererror, 10) ->
    'UnsupportedVersionError';
int_to_enum(servererror, 9) -> 'ChecksumError';
int_to_enum(servererror, 8) ->
    'ProducerBlockedQuotaExceededException';
int_to_enum(servererror, 7) ->
    'ProducerBlockedQuotaExceededError';
int_to_enum(servererror, 6) -> 'ServiceNotReady';
int_to_enum(servererror, 5) -> 'ConsumerBusy';
int_to_enum(servererror, 4) -> 'AuthorizationError';
int_to_enum(servererror, 3) -> 'AuthenticationError';
int_to_enum(servererror, 2) -> 'PersistenceError';
int_to_enum(servererror, 1) -> 'MetadataError';
int_to_enum(servererror, 0) -> 'UnknownError';
int_to_enum(compressiontype, 3) -> 'ZSTD';
int_to_enum(compressiontype, 2) -> 'ZLIB';
int_to_enum(compressiontype, 1) -> 'LZ4';
int_to_enum(compressiontype, 0) -> 'NONE';
int_to_enum(_, Val) -> Val.

decode_basecommand(Bytes) when is_binary(Bytes) ->
    decode(basecommand, Bytes).

decode_commandgetschemaresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandgetschemaresponse, Bytes).

decode_commandgetschema(Bytes) when is_binary(Bytes) ->
    decode(commandgetschema, Bytes).

decode_commandgettopicsofnamespaceresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandgettopicsofnamespaceresponse, Bytes).

decode_commandgettopicsofnamespace(Bytes)
    when is_binary(Bytes) ->
    decode(commandgettopicsofnamespace, Bytes).

decode_commandgetlastmessageidresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandgetlastmessageidresponse, Bytes).

decode_commandgetlastmessageid(Bytes)
    when is_binary(Bytes) ->
    decode(commandgetlastmessageid, Bytes).

decode_commandconsumerstatsresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandconsumerstatsresponse, Bytes).

decode_commandconsumerstats(Bytes)
    when is_binary(Bytes) ->
    decode(commandconsumerstats, Bytes).

decode_commandpong(Bytes) when is_binary(Bytes) ->
    decode(commandpong, Bytes).

decode_commandping(Bytes) when is_binary(Bytes) ->
    decode(commandping, Bytes).

decode_commanderror(Bytes) when is_binary(Bytes) ->
    decode(commanderror, Bytes).

decode_commandproducersuccess(Bytes)
    when is_binary(Bytes) ->
    decode(commandproducersuccess, Bytes).

decode_commandsuccess(Bytes) when is_binary(Bytes) ->
    decode(commandsuccess, Bytes).

decode_commandredeliverunacknowledgedmessages(Bytes)
    when is_binary(Bytes) ->
    decode(commandredeliverunacknowledgedmessages, Bytes).

decode_commandcloseconsumer(Bytes)
    when is_binary(Bytes) ->
    decode(commandcloseconsumer, Bytes).

decode_commandcloseproducer(Bytes)
    when is_binary(Bytes) ->
    decode(commandcloseproducer, Bytes).

decode_commandreachedendoftopic(Bytes)
    when is_binary(Bytes) ->
    decode(commandreachedendoftopic, Bytes).

decode_commandseek(Bytes) when is_binary(Bytes) ->
    decode(commandseek, Bytes).

decode_commandunsubscribe(Bytes)
    when is_binary(Bytes) ->
    decode(commandunsubscribe, Bytes).

decode_commandflow(Bytes) when is_binary(Bytes) ->
    decode(commandflow, Bytes).

decode_commandactiveconsumerchange(Bytes)
    when is_binary(Bytes) ->
    decode(commandactiveconsumerchange, Bytes).

decode_commandack(Bytes) when is_binary(Bytes) ->
    decode(commandack, Bytes).

decode_commandmessage(Bytes) when is_binary(Bytes) ->
    decode(commandmessage, Bytes).

decode_commandsenderror(Bytes) when is_binary(Bytes) ->
    decode(commandsenderror, Bytes).

decode_commandsendreceipt(Bytes)
    when is_binary(Bytes) ->
    decode(commandsendreceipt, Bytes).

decode_commandsend(Bytes) when is_binary(Bytes) ->
    decode(commandsend, Bytes).

decode_commandproducer(Bytes) when is_binary(Bytes) ->
    decode(commandproducer, Bytes).

decode_commandlookuptopicresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandlookuptopicresponse, Bytes).

decode_commandlookuptopic(Bytes)
    when is_binary(Bytes) ->
    decode(commandlookuptopic, Bytes).

decode_commandpartitionedtopicmetadataresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandpartitionedtopicmetadataresponse, Bytes).

decode_commandpartitionedtopicmetadata(Bytes)
    when is_binary(Bytes) ->
    decode(commandpartitionedtopicmetadata, Bytes).

decode_commandsubscribe(Bytes) when is_binary(Bytes) ->
    decode(commandsubscribe, Bytes).

decode_authdata(Bytes) when is_binary(Bytes) ->
    decode(authdata, Bytes).

decode_commandauthchallenge(Bytes)
    when is_binary(Bytes) ->
    decode(commandauthchallenge, Bytes).

decode_commandauthresponse(Bytes)
    when is_binary(Bytes) ->
    decode(commandauthresponse, Bytes).

decode_commandconnected(Bytes) when is_binary(Bytes) ->
    decode(commandconnected, Bytes).

decode_commandconnect(Bytes) when is_binary(Bytes) ->
    decode(commandconnect, Bytes).

decode_singlemessagemetadata(Bytes)
    when is_binary(Bytes) ->
    decode(singlemessagemetadata, Bytes).

decode_messagemetadata(Bytes) when is_binary(Bytes) ->
    decode(messagemetadata, Bytes).

decode_encryptionkeys(Bytes) when is_binary(Bytes) ->
    decode(encryptionkeys, Bytes).

decode_keylongvalue(Bytes) when is_binary(Bytes) ->
    decode(keylongvalue, Bytes).

decode_keyvalue(Bytes) when is_binary(Bytes) ->
    decode(keyvalue, Bytes).

decode_messageiddata(Bytes) when is_binary(Bytes) ->
    decode(messageiddata, Bytes).

decode_schema(Bytes) when is_binary(Bytes) ->
    decode(schema, Bytes).

delimited_decode_schema(Bytes) ->
    delimited_decode(schema, Bytes).

delimited_decode_messageiddata(Bytes) ->
    delimited_decode(messageiddata, Bytes).

delimited_decode_keyvalue(Bytes) ->
    delimited_decode(keyvalue, Bytes).

delimited_decode_keylongvalue(Bytes) ->
    delimited_decode(keylongvalue, Bytes).

delimited_decode_encryptionkeys(Bytes) ->
    delimited_decode(encryptionkeys, Bytes).

delimited_decode_messagemetadata(Bytes) ->
    delimited_decode(messagemetadata, Bytes).

delimited_decode_singlemessagemetadata(Bytes) ->
    delimited_decode(singlemessagemetadata, Bytes).

delimited_decode_commandconnect(Bytes) ->
    delimited_decode(commandconnect, Bytes).

delimited_decode_commandconnected(Bytes) ->
    delimited_decode(commandconnected, Bytes).

delimited_decode_commandauthresponse(Bytes) ->
    delimited_decode(commandauthresponse, Bytes).

delimited_decode_commandauthchallenge(Bytes) ->
    delimited_decode(commandauthchallenge, Bytes).

delimited_decode_authdata(Bytes) ->
    delimited_decode(authdata, Bytes).

delimited_decode_commandsubscribe(Bytes) ->
    delimited_decode(commandsubscribe, Bytes).

delimited_decode_commandpartitionedtopicmetadata(Bytes) ->
    delimited_decode(commandpartitionedtopicmetadata,
		     Bytes).

delimited_decode_commandpartitionedtopicmetadataresponse(Bytes) ->
    delimited_decode(commandpartitionedtopicmetadataresponse,
		     Bytes).

delimited_decode_commandlookuptopic(Bytes) ->
    delimited_decode(commandlookuptopic, Bytes).

delimited_decode_commandlookuptopicresponse(Bytes) ->
    delimited_decode(commandlookuptopicresponse, Bytes).

delimited_decode_commandproducer(Bytes) ->
    delimited_decode(commandproducer, Bytes).

delimited_decode_commandsend(Bytes) ->
    delimited_decode(commandsend, Bytes).

delimited_decode_commandsendreceipt(Bytes) ->
    delimited_decode(commandsendreceipt, Bytes).

delimited_decode_commandsenderror(Bytes) ->
    delimited_decode(commandsenderror, Bytes).

delimited_decode_commandmessage(Bytes) ->
    delimited_decode(commandmessage, Bytes).

delimited_decode_commandack(Bytes) ->
    delimited_decode(commandack, Bytes).

delimited_decode_commandactiveconsumerchange(Bytes) ->
    delimited_decode(commandactiveconsumerchange, Bytes).

delimited_decode_commandflow(Bytes) ->
    delimited_decode(commandflow, Bytes).

delimited_decode_commandunsubscribe(Bytes) ->
    delimited_decode(commandunsubscribe, Bytes).

delimited_decode_commandseek(Bytes) ->
    delimited_decode(commandseek, Bytes).

delimited_decode_commandreachedendoftopic(Bytes) ->
    delimited_decode(commandreachedendoftopic, Bytes).

delimited_decode_commandcloseproducer(Bytes) ->
    delimited_decode(commandcloseproducer, Bytes).

delimited_decode_commandcloseconsumer(Bytes) ->
    delimited_decode(commandcloseconsumer, Bytes).

delimited_decode_commandredeliverunacknowledgedmessages(Bytes) ->
    delimited_decode(commandredeliverunacknowledgedmessages,
		     Bytes).

delimited_decode_commandsuccess(Bytes) ->
    delimited_decode(commandsuccess, Bytes).

delimited_decode_commandproducersuccess(Bytes) ->
    delimited_decode(commandproducersuccess, Bytes).

delimited_decode_commanderror(Bytes) ->
    delimited_decode(commanderror, Bytes).

delimited_decode_commandping(Bytes) ->
    delimited_decode(commandping, Bytes).

delimited_decode_commandpong(Bytes) ->
    delimited_decode(commandpong, Bytes).

delimited_decode_commandconsumerstats(Bytes) ->
    delimited_decode(commandconsumerstats, Bytes).

delimited_decode_commandconsumerstatsresponse(Bytes) ->
    delimited_decode(commandconsumerstatsresponse, Bytes).

delimited_decode_commandgetlastmessageid(Bytes) ->
    delimited_decode(commandgetlastmessageid, Bytes).

delimited_decode_commandgetlastmessageidresponse(Bytes) ->
    delimited_decode(commandgetlastmessageidresponse,
		     Bytes).

delimited_decode_commandgettopicsofnamespace(Bytes) ->
    delimited_decode(commandgettopicsofnamespace, Bytes).

delimited_decode_commandgettopicsofnamespaceresponse(Bytes) ->
    delimited_decode(commandgettopicsofnamespaceresponse,
		     Bytes).

delimited_decode_commandgetschema(Bytes) ->
    delimited_decode(commandgetschema, Bytes).

delimited_decode_commandgetschemaresponse(Bytes) ->
    delimited_decode(commandgetschemaresponse, Bytes).

delimited_decode_basecommand(Bytes) ->
    delimited_decode(basecommand, Bytes).

delimited_decode(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode(Type, Bytes, []).

delimited_decode(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
      {Size, Rest} when size(Rest) < Size ->
	  {lists:reverse(Acc), Bytes};
      {Size, Rest} ->
	  <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
	  Message = decode(Type, MessageBytes),
	  delimited_decode(Type, Rest2, [Message | Acc])
    catch
      _What:_Why -> {lists:reverse(Acc), Bytes}
    end.

decode(enummsg_values, 1) -> value1;
decode(schema, Bytes) when is_binary(Bytes) ->
    Types = [{5, properties, keyvalue,
	      [is_record, repeated]},
	     {4, type, schema_type, []}, {3, schema_data, bytes, []},
	     {1, name, string, []}],
    Defaults = [{5, properties, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(schema, Decoded);
decode(messageiddata, Bytes) when is_binary(Bytes) ->
    Types = [{4, batch_index, int32, []},
	     {3, partition, int32, []}, {2, entryid, uint64, []},
	     {1, ledgerid, uint64, []}],
    Defaults = [{3, partition, -1}, {4, batch_index, -1}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(messageiddata, Decoded);
decode(keyvalue, Bytes) when is_binary(Bytes) ->
    Types = [{2, value, string, []}, {1, key, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(keyvalue, Decoded);
decode(keylongvalue, Bytes) when is_binary(Bytes) ->
    Types = [{2, value, uint64, []}, {1, key, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(keylongvalue, Decoded);
decode(encryptionkeys, Bytes) when is_binary(Bytes) ->
    Types = [{3, metadata, keyvalue, [is_record, repeated]},
	     {2, value, bytes, []}, {1, key, string, []}],
    Defaults = [{3, metadata, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(encryptionkeys, Decoded);
decode(messagemetadata, Bytes) when is_binary(Bytes) ->
    Types = [{18, ordering_key, bytes, []},
	     {17, partition_key_b64_encoded, bool, []},
	     {16, schema_version, bytes, []},
	     {15, encryption_param, bytes, []},
	     {14, encryption_algo, string, []},
	     {13, encryption_keys, encryptionkeys,
	      [is_record, repeated]},
	     {12, event_time, uint64, []},
	     {11, num_messages_in_batch, int32, []},
	     {9, uncompressed_size, uint32, []},
	     {8, compression, compressiontype, []},
	     {7, replicate_to, string, [repeated]},
	     {6, partition_key, string, []},
	     {5, replicated_from, string, []},
	     {4, properties, keyvalue, [is_record, repeated]},
	     {3, publish_time, uint64, []},
	     {2, sequence_id, uint64, []},
	     {1, producer_name, string, []}],
    Defaults = [{4, properties, []}, {7, replicate_to, []},
		{8, compression, 'NONE'}, {9, uncompressed_size, 0},
		{11, num_messages_in_batch, 1}, {12, event_time, 0},
		{13, encryption_keys, []},
		{17, partition_key_b64_encoded, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(messagemetadata, Decoded);
decode(singlemessagemetadata, Bytes)
    when is_binary(Bytes) ->
    Types = [{7, ordering_key, bytes, []},
	     {6, partition_key_b64_encoded, bool, []},
	     {5, event_time, uint64, []},
	     {4, compacted_out, bool, []},
	     {3, payload_size, int32, []},
	     {2, partition_key, string, []},
	     {1, properties, keyvalue, [is_record, repeated]}],
    Defaults = [{1, properties, []},
		{4, compacted_out, false}, {5, event_time, 0},
		{6, partition_key_b64_encoded, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(singlemessagemetadata, Decoded);
decode(commandconnect, Bytes) when is_binary(Bytes) ->
    Types = [{9, original_auth_method, string, []},
	     {8, original_auth_data, string, []},
	     {7, original_principal, string, []},
	     {6, proxy_to_broker_url, string, []},
	     {5, auth_method_name, string, []},
	     {4, protocol_version, int32, []},
	     {3, auth_data, bytes, []},
	     {2, auth_method, authmethod, []},
	     {1, client_version, string, []}],
    Defaults = [{4, protocol_version, 0}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandconnect, Decoded);
decode(commandconnected, Bytes) when is_binary(Bytes) ->
    Types = [{2, protocol_version, int32, []},
	     {1, server_version, string, []}],
    Defaults = [{2, protocol_version, 0}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandconnected, Decoded);
decode(commandauthresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, protocol_version, int32, []},
	     {2, response, authdata, [is_record]},
	     {1, client_version, string, []}],
    Defaults = [{3, protocol_version, 0}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandauthresponse, Decoded);
decode(commandauthchallenge, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, protocol_version, int32, []},
	     {2, challenge, authdata, [is_record]},
	     {1, server_version, string, []}],
    Defaults = [{3, protocol_version, 0}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandauthchallenge, Decoded);
decode(authdata, Bytes) when is_binary(Bytes) ->
    Types = [{2, auth_data, bytes, []},
	     {1, auth_method_name, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(authdata, Decoded);
decode(commandsubscribe, Bytes) when is_binary(Bytes) ->
    Types = [{13, initialposition,
	      commandsubscribe_initialposition, []},
	     {12, schema, schema, [is_record]},
	     {11, read_compacted, bool, []},
	     {10, metadata, keyvalue, [is_record, repeated]},
	     {9, start_message_id, messageiddata, [is_record]},
	     {8, durable, bool, []}, {7, priority_level, int32, []},
	     {6, consumer_name, string, []},
	     {5, request_id, uint64, []},
	     {4, consumer_id, uint64, []},
	     {3, subtype, commandsubscribe_subtype, []},
	     {2, subscription, string, []}, {1, topic, string, []}],
    Defaults = [{8, durable, true}, {10, metadata, []},
		{13, initialposition, 'Latest'}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandsubscribe, Decoded);
decode(commandpartitionedtopicmetadata, Bytes)
    when is_binary(Bytes) ->
    Types = [{5, original_auth_method, string, []},
	     {4, original_auth_data, string, []},
	     {3, original_principal, string, []},
	     {2, request_id, uint64, []}, {1, topic, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandpartitionedtopicmetadata, Decoded);
decode(commandpartitionedtopicmetadataresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{5, message, string, []},
	     {4, error, servererror, []},
	     {3, response,
	      commandpartitionedtopicmetadataresponse_lookuptype, []},
	     {2, request_id, uint64, []},
	     {1, partitions, uint32, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandpartitionedtopicmetadataresponse,
	      Decoded);
decode(commandlookuptopic, Bytes)
    when is_binary(Bytes) ->
    Types = [{6, original_auth_method, string, []},
	     {5, original_auth_data, string, []},
	     {4, original_principal, string, []},
	     {3, authoritative, bool, []},
	     {2, request_id, uint64, []}, {1, topic, string, []}],
    Defaults = [{3, authoritative, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandlookuptopic, Decoded);
decode(commandlookuptopicresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{8, proxy_through_service_url, bool, []},
	     {7, message, string, []}, {6, error, servererror, []},
	     {5, authoritative, bool, []},
	     {4, request_id, uint64, []},
	     {3, response, commandlookuptopicresponse_lookuptype,
	      []},
	     {2, brokerserviceurltls, string, []},
	     {1, brokerserviceurl, string, []}],
    Defaults = [{5, authoritative, false},
		{8, proxy_through_service_url, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandlookuptopicresponse, Decoded);
decode(commandproducer, Bytes) when is_binary(Bytes) ->
    Types = [{7, schema, schema, [is_record]},
	     {6, metadata, keyvalue, [is_record, repeated]},
	     {5, encrypted, bool, []},
	     {4, producer_name, string, []},
	     {3, request_id, uint64, []},
	     {2, producer_id, uint64, []}, {1, topic, string, []}],
    Defaults = [{5, encrypted, false}, {6, metadata, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandproducer, Decoded);
decode(commandsend, Bytes) when is_binary(Bytes) ->
    Types = [{3, num_messages, int32, []},
	     {2, sequence_id, uint64, []},
	     {1, producer_id, uint64, []}],
    Defaults = [{3, num_messages, 1}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandsend, Decoded);
decode(commandsendreceipt, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, message_id, messageiddata, [is_record]},
	     {2, sequence_id, uint64, []},
	     {1, producer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandsendreceipt, Decoded);
decode(commandsenderror, Bytes) when is_binary(Bytes) ->
    Types = [{4, message, string, []},
	     {3, error, servererror, []},
	     {2, sequence_id, uint64, []},
	     {1, producer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandsenderror, Decoded);
decode(commandmessage, Bytes) when is_binary(Bytes) ->
    Types = [{3, redelivery_count, uint32, []},
	     {2, message_id, messageiddata, [is_record]},
	     {1, consumer_id, uint64, []}],
    Defaults = [{3, redelivery_count, 0}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandmessage, Decoded);
decode(commandack, Bytes) when is_binary(Bytes) ->
    Types = [{5, properties, keylongvalue,
	      [is_record, repeated]},
	     {4, validation_error, commandack_validationerror, []},
	     {3, message_id, messageiddata, [is_record, repeated]},
	     {2, ack_type, commandack_acktype, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [{3, message_id, []}, {5, properties, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandack, Decoded);
decode(commandactiveconsumerchange, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, is_active, bool, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [{2, is_active, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandactiveconsumerchange, Decoded);
decode(commandflow, Bytes) when is_binary(Bytes) ->
    Types = [{2, messagepermits, uint32, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandflow, Decoded);
decode(commandunsubscribe, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, request_id, uint64, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandunsubscribe, Decoded);
decode(commandseek, Bytes) when is_binary(Bytes) ->
    Types = [{4, message_publish_time, uint64, []},
	     {3, message_id, messageiddata, [is_record]},
	     {2, request_id, uint64, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandseek, Decoded);
decode(commandreachedendoftopic, Bytes)
    when is_binary(Bytes) ->
    Types = [{1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandreachedendoftopic, Decoded);
decode(commandcloseproducer, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, request_id, uint64, []},
	     {1, producer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandcloseproducer, Decoded);
decode(commandcloseconsumer, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, request_id, uint64, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandcloseconsumer, Decoded);
decode(commandredeliverunacknowledgedmessages, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, message_ids, messageiddata,
	      [is_record, repeated]},
	     {1, consumer_id, uint64, []}],
    Defaults = [{2, message_ids, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandredeliverunacknowledgedmessages,
	      Decoded);
decode(commandsuccess, Bytes) when is_binary(Bytes) ->
    Types = [{2, schema, schema, [is_record]},
	     {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandsuccess, Decoded);
decode(commandproducersuccess, Bytes)
    when is_binary(Bytes) ->
    Types = [{4, schema_version, bytes, []},
	     {3, last_sequence_id, int64, []},
	     {2, producer_name, string, []},
	     {1, request_id, uint64, []}],
    Defaults = [{3, last_sequence_id, -1}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandproducersuccess, Decoded);
decode(commanderror, Bytes) when is_binary(Bytes) ->
    Types = [{3, message, string, []},
	     {2, error, servererror, []},
	     {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commanderror, Decoded);
decode(commandping, Bytes) when is_binary(Bytes) ->
    Types = [],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandping, Decoded);
decode(commandpong, Bytes) when is_binary(Bytes) ->
    Types = [],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandpong, Decoded);
decode(commandconsumerstats, Bytes)
    when is_binary(Bytes) ->
    Types = [{4, consumer_id, uint64, []},
	     {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandconsumerstats, Decoded);
decode(commandconsumerstatsresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{15, msgbacklog, uint64, []},
	     {14, msgrateexpired, double, []},
	     {13, type, string, []},
	     {12, connectedsince, string, []},
	     {11, address, string, []},
	     {10, blockedconsumeronunackedmsgs, bool, []},
	     {9, unackedmessages, uint64, []},
	     {8, availablepermits, uint64, []},
	     {7, consumername, string, []},
	     {6, msgrateredeliver, double, []},
	     {5, msgthroughputout, double, []},
	     {4, msgrateout, double, []},
	     {3, error_message, string, []},
	     {2, error_code, servererror, []},
	     {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandconsumerstatsresponse, Decoded);
decode(commandgetlastmessageid, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, request_id, uint64, []},
	     {1, consumer_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgetlastmessageid, Decoded);
decode(commandgetlastmessageidresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, request_id, uint64, []},
	     {1, last_message_id, messageiddata, [is_record]}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgetlastmessageidresponse, Decoded);
decode(commandgettopicsofnamespace, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, mode, commandgettopicsofnamespace_mode,
	      []},
	     {2, namespace, string, []},
	     {1, request_id, uint64, []}],
    Defaults = [{3, mode, 'PERSISTENT'}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgettopicsofnamespace, Decoded);
decode(commandgettopicsofnamespaceresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, topics, string, [repeated]},
	     {1, request_id, uint64, []}],
    Defaults = [{2, topics, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgettopicsofnamespaceresponse, Decoded);
decode(commandgetschema, Bytes) when is_binary(Bytes) ->
    Types = [{3, schema_version, bytes, []},
	     {2, topic, string, []}, {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgetschema, Decoded);
decode(commandgetschemaresponse, Bytes)
    when is_binary(Bytes) ->
    Types = [{5, schema_version, bytes, []},
	     {4, schema, schema, [is_record]},
	     {3, error_message, string, []},
	     {2, error_code, servererror, []},
	     {1, request_id, uint64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(commandgetschemaresponse, Decoded);
decode(basecommand, Bytes) when is_binary(Bytes) ->
    Types = [{37, authresponse, commandauthresponse,
	      [is_record]},
	     {36, authchallenge, commandauthchallenge, [is_record]},
	     {35, getschemaresponse, commandgetschemaresponse,
	      [is_record]},
	     {34, getschema, commandgetschema, [is_record]},
	     {33, gettopicsofnamespaceresponse,
	      commandgettopicsofnamespaceresponse, [is_record]},
	     {32, gettopicsofnamespace, commandgettopicsofnamespace,
	      [is_record]},
	     {31, active_consumer_change,
	      commandactiveconsumerchange, [is_record]},
	     {30, getlastmessageidresponse,
	      commandgetlastmessageidresponse, [is_record]},
	     {29, getlastmessageid, commandgetlastmessageid,
	      [is_record]},
	     {28, seek, commandseek, [is_record]},
	     {27, reachedendoftopic, commandreachedendoftopic,
	      [is_record]},
	     {26, consumerstatsresponse,
	      commandconsumerstatsresponse, [is_record]},
	     {25, consumerstats, commandconsumerstats, [is_record]},
	     {24, lookuptopicresponse, commandlookuptopicresponse,
	      [is_record]},
	     {23, lookuptopic, commandlookuptopic, [is_record]},
	     {22, partitionmetadataresponse,
	      commandpartitionedtopicmetadataresponse, [is_record]},
	     {21, partitionmetadata, commandpartitionedtopicmetadata,
	      [is_record]},
	     {20, redeliverunacknowledgedmessages,
	      commandredeliverunacknowledgedmessages, [is_record]},
	     {19, pong, commandpong, [is_record]},
	     {18, ping, commandping, [is_record]},
	     {17, producer_success, commandproducersuccess,
	      [is_record]},
	     {16, close_consumer, commandcloseconsumer, [is_record]},
	     {15, close_producer, commandcloseproducer, [is_record]},
	     {14, error, commanderror, [is_record]},
	     {13, success, commandsuccess, [is_record]},
	     {12, unsubscribe, commandunsubscribe, [is_record]},
	     {11, flow, commandflow, [is_record]},
	     {10, ack, commandack, [is_record]},
	     {9, message, commandmessage, [is_record]},
	     {8, send_error, commandsenderror, [is_record]},
	     {7, send_receipt, commandsendreceipt, [is_record]},
	     {6, send, commandsend, [is_record]},
	     {5, producer, commandproducer, [is_record]},
	     {4, subscribe, commandsubscribe, [is_record]},
	     {3, connected, commandconnected, [is_record]},
	     {2, connect, commandconnect, [is_record]},
	     {1, type, basecommand_type, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(basecommand, Decoded).

decode(<<>>, Types, Acc) ->
    reverse_repeated_fields(Acc, Types);
decode(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
      {FNum, Name, Type, Opts} ->
	  {Value1, Rest1} = case lists:member(is_record, Opts) of
			      true ->
				  {{FNum, V}, R} = protobuffs:decode(Bytes,
								     bytes),
				  RecVal = decode(Type, V),
				  {RecVal, R};
			      false ->
				  case lists:member(repeated_packed, Opts) of
				    true ->
					{{FNum, V}, R} =
					    protobuffs:decode_packed(Bytes,
								     Type),
					{V, R};
				    false ->
					{{FNum, V}, R} =
					    protobuffs:decode(Bytes, Type),
					{unpack_value(V, Type), R}
				  end
			    end,
	  case lists:member(repeated, Opts) of
	    true ->
		case lists:keytake(FNum, 1, Acc) of
		  {value, {FNum, Name, List}, Acc1} ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1) | List]}
			      | Acc1]);
		  false ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1)]} | Acc])
		end;
	    false ->
		decode(Rest1, Types,
		       [{FNum, Name, int_to_enum(Type, Value1)} | Acc])
	  end;
      false ->
	  case lists:keyfind('$extensions', 2, Acc) of
	    {_, _, Dict} ->
		{{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
		Diff = size(Bytes) - size(R),
		<<V:Diff/binary, _/binary>> = Bytes,
		NewDict = dict:store(FNum, V, Dict),
		NewAcc = lists:keyreplace('$extensions', 2, Acc,
					  {false, '$extensions', NewDict}),
		decode(R, Types, NewAcc);
	    _ ->
		{ok, Skipped} = protobuffs:skip_next_field(Bytes),
		decode(Skipped, Types, Acc)
	  end
    end.

reverse_repeated_fields(FieldList, Types) ->
    [begin
       case lists:keyfind(FNum, 1, Types) of
	 {FNum, Name, _Type, Opts} ->
	     case lists:member(repeated, Opts) of
	       true -> {FNum, Name, lists:reverse(Value)};
	       _ -> Field
	     end;
	 _ -> Field
       end
     end
     || {FNum, Name, Value} = Field <- FieldList].

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(schema, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields, schema),
						   Record, Name, Val)
			  end,
			  #schema{}, DecodedTuples),
    Record1;
to_record(messageiddata, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       messageiddata),
						   Record, Name, Val)
			  end,
			  #messageiddata{}, DecodedTuples),
    Record1;
to_record(keyvalue, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       keyvalue),
						   Record, Name, Val)
			  end,
			  #keyvalue{}, DecodedTuples),
    Record1;
to_record(keylongvalue, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       keylongvalue),
						   Record, Name, Val)
			  end,
			  #keylongvalue{}, DecodedTuples),
    Record1;
to_record(encryptionkeys, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       encryptionkeys),
						   Record, Name, Val)
			  end,
			  #encryptionkeys{}, DecodedTuples),
    Record1;
to_record(messagemetadata, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       messagemetadata),
						   Record, Name, Val)
			  end,
			  #messagemetadata{}, DecodedTuples),
    Record1;
to_record(singlemessagemetadata, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       singlemessagemetadata),
						   Record, Name, Val)
			  end,
			  #singlemessagemetadata{}, DecodedTuples),
    Record1;
to_record(commandconnect, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandconnect),
						   Record, Name, Val)
			  end,
			  #commandconnect{}, DecodedTuples),
    Record1;
to_record(commandconnected, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandconnected),
						   Record, Name, Val)
			  end,
			  #commandconnected{}, DecodedTuples),
    Record1;
to_record(commandauthresponse, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandauthresponse),
						   Record, Name, Val)
			  end,
			  #commandauthresponse{}, DecodedTuples),
    Record1;
to_record(commandauthchallenge, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandauthchallenge),
						   Record, Name, Val)
			  end,
			  #commandauthchallenge{}, DecodedTuples),
    Record1;
to_record(authdata, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       authdata),
						   Record, Name, Val)
			  end,
			  #authdata{}, DecodedTuples),
    Record1;
to_record(commandsubscribe, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandsubscribe),
						   Record, Name, Val)
			  end,
			  #commandsubscribe{}, DecodedTuples),
    Record1;
to_record(commandpartitionedtopicmetadata,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandpartitionedtopicmetadata),
						   Record, Name, Val)
			  end,
			  #commandpartitionedtopicmetadata{}, DecodedTuples),
    Record1;
to_record(commandpartitionedtopicmetadataresponse,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandpartitionedtopicmetadataresponse),
						   Record, Name, Val)
			  end,
			  #commandpartitionedtopicmetadataresponse{},
			  DecodedTuples),
    Record1;
to_record(commandlookuptopic, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandlookuptopic),
						   Record, Name, Val)
			  end,
			  #commandlookuptopic{}, DecodedTuples),
    Record1;
to_record(commandlookuptopicresponse, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandlookuptopicresponse),
						   Record, Name, Val)
			  end,
			  #commandlookuptopicresponse{}, DecodedTuples),
    Record1;
to_record(commandproducer, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandproducer),
						   Record, Name, Val)
			  end,
			  #commandproducer{}, DecodedTuples),
    Record1;
to_record(commandsend, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandsend),
						   Record, Name, Val)
			  end,
			  #commandsend{}, DecodedTuples),
    Record1;
to_record(commandsendreceipt, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandsendreceipt),
						   Record, Name, Val)
			  end,
			  #commandsendreceipt{}, DecodedTuples),
    Record1;
to_record(commandsenderror, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandsenderror),
						   Record, Name, Val)
			  end,
			  #commandsenderror{}, DecodedTuples),
    Record1;
to_record(commandmessage, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandmessage),
						   Record, Name, Val)
			  end,
			  #commandmessage{}, DecodedTuples),
    Record1;
to_record(commandack, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandack),
						   Record, Name, Val)
			  end,
			  #commandack{}, DecodedTuples),
    Record1;
to_record(commandactiveconsumerchange, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandactiveconsumerchange),
						   Record, Name, Val)
			  end,
			  #commandactiveconsumerchange{}, DecodedTuples),
    Record1;
to_record(commandflow, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandflow),
						   Record, Name, Val)
			  end,
			  #commandflow{}, DecodedTuples),
    Record1;
to_record(commandunsubscribe, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandunsubscribe),
						   Record, Name, Val)
			  end,
			  #commandunsubscribe{}, DecodedTuples),
    Record1;
to_record(commandseek, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandseek),
						   Record, Name, Val)
			  end,
			  #commandseek{}, DecodedTuples),
    Record1;
to_record(commandreachedendoftopic, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandreachedendoftopic),
						   Record, Name, Val)
			  end,
			  #commandreachedendoftopic{}, DecodedTuples),
    Record1;
to_record(commandcloseproducer, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandcloseproducer),
						   Record, Name, Val)
			  end,
			  #commandcloseproducer{}, DecodedTuples),
    Record1;
to_record(commandcloseconsumer, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandcloseconsumer),
						   Record, Name, Val)
			  end,
			  #commandcloseconsumer{}, DecodedTuples),
    Record1;
to_record(commandredeliverunacknowledgedmessages,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandredeliverunacknowledgedmessages),
						   Record, Name, Val)
			  end,
			  #commandredeliverunacknowledgedmessages{},
			  DecodedTuples),
    Record1;
to_record(commandsuccess, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandsuccess),
						   Record, Name, Val)
			  end,
			  #commandsuccess{}, DecodedTuples),
    Record1;
to_record(commandproducersuccess, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandproducersuccess),
						   Record, Name, Val)
			  end,
			  #commandproducersuccess{}, DecodedTuples),
    Record1;
to_record(commanderror, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commanderror),
						   Record, Name, Val)
			  end,
			  #commanderror{}, DecodedTuples),
    Record1;
to_record(commandping, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandping),
						   Record, Name, Val)
			  end,
			  #commandping{}, DecodedTuples),
    Record1;
to_record(commandpong, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandpong),
						   Record, Name, Val)
			  end,
			  #commandpong{}, DecodedTuples),
    Record1;
to_record(commandconsumerstats, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandconsumerstats),
						   Record, Name, Val)
			  end,
			  #commandconsumerstats{}, DecodedTuples),
    Record1;
to_record(commandconsumerstatsresponse,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandconsumerstatsresponse),
						   Record, Name, Val)
			  end,
			  #commandconsumerstatsresponse{}, DecodedTuples),
    Record1;
to_record(commandgetlastmessageid, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgetlastmessageid),
						   Record, Name, Val)
			  end,
			  #commandgetlastmessageid{}, DecodedTuples),
    Record1;
to_record(commandgetlastmessageidresponse,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgetlastmessageidresponse),
						   Record, Name, Val)
			  end,
			  #commandgetlastmessageidresponse{}, DecodedTuples),
    Record1;
to_record(commandgettopicsofnamespace, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgettopicsofnamespace),
						   Record, Name, Val)
			  end,
			  #commandgettopicsofnamespace{}, DecodedTuples),
    Record1;
to_record(commandgettopicsofnamespaceresponse,
	  DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgettopicsofnamespaceresponse),
						   Record, Name, Val)
			  end,
			  #commandgettopicsofnamespaceresponse{},
			  DecodedTuples),
    Record1;
to_record(commandgetschema, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgetschema),
						   Record, Name, Val)
			  end,
			  #commandgetschema{}, DecodedTuples),
    Record1;
to_record(commandgetschemaresponse, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       commandgetschemaresponse),
						   Record, Name, Val)
			  end,
			  #commandgetschemaresponse{}, DecodedTuples),
    Record1;
to_record(basecommand, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       basecommand),
						   Record, Name, Val)
			  end,
			  #basecommand{}, DecodedTuples),
    Record1.

decode_extensions(Record) -> Record.

decode_extensions(_Types, [], Acc) ->
    dict:from_list(Acc);
decode_extensions(Types, [{FNum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(FNum, 1, Types) of
	       {FNum, Name, Type, Opts} ->
		   {Value1, Rest1} = case lists:member(is_record, Opts) of
				       true ->
					   {{FNum, V}, R} =
					       protobuffs:decode(Bytes, bytes),
					   RecVal = decode(Type, V),
					   {RecVal, R};
				       false ->
					   case lists:member(repeated_packed,
							     Opts)
					       of
					     true ->
						 {{FNum, V}, R} =
						     protobuffs:decode_packed(Bytes,
									      Type),
						 {V, R};
					     false ->
						 {{FNum, V}, R} =
						     protobuffs:decode(Bytes,
								       Type),
						 {unpack_value(V, Type), R}
					   end
				     end,
		   case lists:member(repeated, Opts) of
		     true ->
			 case lists:keytake(FNum, 1, Acc) of
			   {value, {FNum, Name, List}, Acc1} ->
			       decode(Rest1, Types,
				      [{FNum, Name,
					lists:reverse([int_to_enum(Type, Value1)
						       | lists:reverse(List)])}
				       | Acc1]);
			   false ->
			       decode(Rest1, Types,
				      [{FNum, Name, [int_to_enum(Type, Value1)]}
				       | Acc])
			 end;
		     false ->
			 [{FNum,
			   {optional, int_to_enum(Type, Value1), Type, Opts}}
			  | Acc]
		   end;
	       false -> [{FNum, Bytes} | Acc]
	     end,
    decode_extensions(Types, Tail, NewAcc).

set_record_field(Fields, Record, '$extensions',
		 Value) ->
    Decodable = [],
    NewValue = decode_extensions(element(1, Record),
				 Decodable, dict:to_list(Value)),
    Index = list_index('$extensions', Fields),
    erlang:setelement(Index + 1, Record, NewValue);
set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index + 1, Record, Value).

list_index(Target, List) -> list_index(Target, List, 1).

list_index(Target, [Target | _], Index) -> Index;
list_index(Target, [_ | Tail], Index) ->
    list_index(Target, Tail, Index + 1);
list_index(_, [], _) -> -1.

extension_size(_) -> 0.

has_extension(_Record, _FieldName) -> false.

get_extension(_Record, _FieldName) -> undefined.

set_extension(Record, _, _) -> {error, Record}.

