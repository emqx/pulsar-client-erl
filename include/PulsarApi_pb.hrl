-ifndef(SCHEMA_PB_H).
-define(SCHEMA_PB_H, true).
-record(schema, {
    name = erlang:error({required, name}),
    schema_data = erlang:error({required, schema_data}),
    type = erlang:error({required, type}),
    properties = []
}).
-endif.

-ifndef(MESSAGEIDDATA_PB_H).
-define(MESSAGEIDDATA_PB_H, true).
-record(messageiddata, {
    ledgerid = erlang:error({required, ledgerid}),
    entryid = erlang:error({required, entryid}),
    partition = -1,
    batch_index = -1
}).
-endif.

-ifndef(KEYVALUE_PB_H).
-define(KEYVALUE_PB_H, true).
-record(keyvalue, {
    key = erlang:error({required, key}),
    value = erlang:error({required, value})
}).
-endif.

-ifndef(KEYLONGVALUE_PB_H).
-define(KEYLONGVALUE_PB_H, true).
-record(keylongvalue, {
    key = erlang:error({required, key}),
    value = erlang:error({required, value})
}).
-endif.

-ifndef(ENCRYPTIONKEYS_PB_H).
-define(ENCRYPTIONKEYS_PB_H, true).
-record(encryptionkeys, {
    key = erlang:error({required, key}),
    value = erlang:error({required, value}),
    metadata = []
}).
-endif.

-ifndef(MESSAGEMETADATA_PB_H).
-define(MESSAGEMETADATA_PB_H, true).
-record(messagemetadata, {
    producer_name = erlang:error({required, producer_name}),
    sequence_id = erlang:error({required, sequence_id}),
    publish_time = erlang:error({required, publish_time}),
    properties = [],
    replicated_from,
    partition_key,
    replicate_to = [],
    compression = 'NONE',
    uncompressed_size = 0,
    num_messages_in_batch = 1,
    event_time = 0,
    encryption_keys = [],
    encryption_algo,
    encryption_param,
    schema_version,
    partition_key_b64_encoded = false,
    ordering_key
}).
-endif.

-ifndef(SINGLEMESSAGEMETADATA_PB_H).
-define(SINGLEMESSAGEMETADATA_PB_H, true).
-record(singlemessagemetadata, {
    properties = [],
    partition_key,
    payload_size = erlang:error({required, payload_size}),
    compacted_out = false,
    event_time = 0,
    partition_key_b64_encoded = false,
    ordering_key
}).
-endif.

-ifndef(COMMANDCONNECT_PB_H).
-define(COMMANDCONNECT_PB_H, true).
-record(commandconnect, {
    client_version = erlang:error({required, client_version}),
    auth_method,
    auth_data,
    protocol_version = 0,
    auth_method_name,
    proxy_to_broker_url,
    original_principal,
    original_auth_data,
    original_auth_method
}).
-endif.

-ifndef(COMMANDCONNECTED_PB_H).
-define(COMMANDCONNECTED_PB_H, true).
-record(commandconnected, {
    server_version = erlang:error({required, server_version}),
    protocol_version = 0
}).
-endif.

-ifndef(COMMANDAUTHRESPONSE_PB_H).
-define(COMMANDAUTHRESPONSE_PB_H, true).
-record(commandauthresponse, {
    client_version,
    response,
    protocol_version = 0
}).
-endif.

-ifndef(COMMANDAUTHCHALLENGE_PB_H).
-define(COMMANDAUTHCHALLENGE_PB_H, true).
-record(commandauthchallenge, {
    server_version,
    challenge,
    protocol_version = 0
}).
-endif.

-ifndef(AUTHDATA_PB_H).
-define(AUTHDATA_PB_H, true).
-record(authdata, {
    auth_method_name,
    auth_data
}).
-endif.

-ifndef(COMMANDSUBSCRIBE_PB_H).
-define(COMMANDSUBSCRIBE_PB_H, true).
-record(commandsubscribe, {
    topic = erlang:error({required, topic}),
    subscription = erlang:error({required, subscription}),
    subtype = erlang:error({required, subtype}),
    consumer_id = erlang:error({required, consumer_id}),
    request_id = erlang:error({required, request_id}),
    consumer_name,
    priority_level,
    durable = true,
    start_message_id,
    metadata = [],
    read_compacted,
    schema,
    initialposition = 'Latest'
}).
-endif.

-ifndef(COMMANDPARTITIONEDTOPICMETADATA_PB_H).
-define(COMMANDPARTITIONEDTOPICMETADATA_PB_H, true).
-record(commandpartitionedtopicmetadata, {
    topic = erlang:error({required, topic}),
    request_id = erlang:error({required, request_id}),
    original_principal,
    original_auth_data,
    original_auth_method
}).
-endif.

-ifndef(COMMANDPARTITIONEDTOPICMETADATARESPONSE_PB_H).
-define(COMMANDPARTITIONEDTOPICMETADATARESPONSE_PB_H, true).
-record(commandpartitionedtopicmetadataresponse, {
    partitions,
    request_id = erlang:error({required, request_id}),
    response,
    error,
    message
}).
-endif.

-ifndef(COMMANDLOOKUPTOPIC_PB_H).
-define(COMMANDLOOKUPTOPIC_PB_H, true).
-record(commandlookuptopic, {
    topic = erlang:error({required, topic}),
    request_id = erlang:error({required, request_id}),
    authoritative = false,
    original_principal,
    original_auth_data,
    original_auth_method
}).
-endif.

-ifndef(COMMANDLOOKUPTOPICRESPONSE_PB_H).
-define(COMMANDLOOKUPTOPICRESPONSE_PB_H, true).
-record(commandlookuptopicresponse, {
    brokerserviceurl,
    brokerserviceurltls,
    response,
    request_id = erlang:error({required, request_id}),
    authoritative = false,
    error,
    message,
    proxy_through_service_url = false
}).
-endif.

-ifndef(COMMANDPRODUCER_PB_H).
-define(COMMANDPRODUCER_PB_H, true).
-record(commandproducer, {
    topic = erlang:error({required, topic}),
    producer_id = erlang:error({required, producer_id}),
    request_id = erlang:error({required, request_id}),
    producer_name,
    encrypted = false,
    metadata = [],
    schema
}).
-endif.

-ifndef(COMMANDSEND_PB_H).
-define(COMMANDSEND_PB_H, true).
-record(commandsend, {
    producer_id = erlang:error({required, producer_id}),
    sequence_id = erlang:error({required, sequence_id}),
    num_messages = 1
}).
-endif.

-ifndef(COMMANDSENDRECEIPT_PB_H).
-define(COMMANDSENDRECEIPT_PB_H, true).
-record(commandsendreceipt, {
    producer_id = erlang:error({required, producer_id}),
    sequence_id = erlang:error({required, sequence_id}),
    message_id
}).
-endif.

-ifndef(COMMANDSENDERROR_PB_H).
-define(COMMANDSENDERROR_PB_H, true).
-record(commandsenderror, {
    producer_id = erlang:error({required, producer_id}),
    sequence_id = erlang:error({required, sequence_id}),
    error = erlang:error({required, error}),
    message = erlang:error({required, message})
}).
-endif.

-ifndef(COMMANDMESSAGE_PB_H).
-define(COMMANDMESSAGE_PB_H, true).
-record(commandmessage, {
    consumer_id = erlang:error({required, consumer_id}),
    message_id = erlang:error({required, message_id}),
    redelivery_count = 0
}).
-endif.

-ifndef(COMMANDACK_PB_H).
-define(COMMANDACK_PB_H, true).
-record(commandack, {
    consumer_id = erlang:error({required, consumer_id}),
    ack_type = erlang:error({required, ack_type}),
    message_id = [],
    validation_error,
    properties = []
}).
-endif.

-ifndef(COMMANDACTIVECONSUMERCHANGE_PB_H).
-define(COMMANDACTIVECONSUMERCHANGE_PB_H, true).
-record(commandactiveconsumerchange, {
    consumer_id = erlang:error({required, consumer_id}),
    is_active = false
}).
-endif.

-ifndef(COMMANDFLOW_PB_H).
-define(COMMANDFLOW_PB_H, true).
-record(commandflow, {
    consumer_id = erlang:error({required, consumer_id}),
    messagepermits = erlang:error({required, messagepermits})
}).
-endif.

-ifndef(COMMANDUNSUBSCRIBE_PB_H).
-define(COMMANDUNSUBSCRIBE_PB_H, true).
-record(commandunsubscribe, {
    consumer_id = erlang:error({required, consumer_id}),
    request_id = erlang:error({required, request_id})
}).
-endif.

-ifndef(COMMANDSEEK_PB_H).
-define(COMMANDSEEK_PB_H, true).
-record(commandseek, {
    consumer_id = erlang:error({required, consumer_id}),
    request_id = erlang:error({required, request_id}),
    message_id,
    message_publish_time
}).
-endif.

-ifndef(COMMANDREACHEDENDOFTOPIC_PB_H).
-define(COMMANDREACHEDENDOFTOPIC_PB_H, true).
-record(commandreachedendoftopic, {
    consumer_id = erlang:error({required, consumer_id})
}).
-endif.

-ifndef(COMMANDCLOSEPRODUCER_PB_H).
-define(COMMANDCLOSEPRODUCER_PB_H, true).
-record(commandcloseproducer, {
    producer_id = erlang:error({required, producer_id}),
    request_id = erlang:error({required, request_id})
}).
-endif.

-ifndef(COMMANDCLOSECONSUMER_PB_H).
-define(COMMANDCLOSECONSUMER_PB_H, true).
-record(commandcloseconsumer, {
    consumer_id = erlang:error({required, consumer_id}),
    request_id = erlang:error({required, request_id})
}).
-endif.

-ifndef(COMMANDREDELIVERUNACKNOWLEDGEDMESSAGES_PB_H).
-define(COMMANDREDELIVERUNACKNOWLEDGEDMESSAGES_PB_H, true).
-record(commandredeliverunacknowledgedmessages, {
    consumer_id = erlang:error({required, consumer_id}),
    message_ids = []
}).
-endif.

-ifndef(COMMANDSUCCESS_PB_H).
-define(COMMANDSUCCESS_PB_H, true).
-record(commandsuccess, {
    request_id = erlang:error({required, request_id}),
    schema
}).
-endif.

-ifndef(COMMANDPRODUCERSUCCESS_PB_H).
-define(COMMANDPRODUCERSUCCESS_PB_H, true).
-record(commandproducersuccess, {
    request_id = erlang:error({required, request_id}),
    producer_name = erlang:error({required, producer_name}),
    last_sequence_id = -1,
    schema_version
}).
-endif.

-ifndef(COMMANDERROR_PB_H).
-define(COMMANDERROR_PB_H, true).
-record(commanderror, {
    request_id = erlang:error({required, request_id}),
    error = erlang:error({required, error}),
    message = erlang:error({required, message})
}).
-endif.

-ifndef(COMMANDPING_PB_H).
-define(COMMANDPING_PB_H, true).
-record(commandping, {
    
}).
-endif.

-ifndef(COMMANDPONG_PB_H).
-define(COMMANDPONG_PB_H, true).
-record(commandpong, {
    
}).
-endif.

-ifndef(COMMANDCONSUMERSTATS_PB_H).
-define(COMMANDCONSUMERSTATS_PB_H, true).
-record(commandconsumerstats, {
    request_id = erlang:error({required, request_id}),
    consumer_id = erlang:error({required, consumer_id})
}).
-endif.

-ifndef(COMMANDCONSUMERSTATSRESPONSE_PB_H).
-define(COMMANDCONSUMERSTATSRESPONSE_PB_H, true).
-record(commandconsumerstatsresponse, {
    request_id = erlang:error({required, request_id}),
    error_code,
    error_message,
    msgrateout,
    msgthroughputout,
    msgrateredeliver,
    consumername,
    availablepermits,
    unackedmessages,
    blockedconsumeronunackedmsgs,
    address,
    connectedsince,
    type,
    msgrateexpired,
    msgbacklog
}).
-endif.

-ifndef(COMMANDGETLASTMESSAGEID_PB_H).
-define(COMMANDGETLASTMESSAGEID_PB_H, true).
-record(commandgetlastmessageid, {
    consumer_id = erlang:error({required, consumer_id}),
    request_id = erlang:error({required, request_id})
}).
-endif.

-ifndef(COMMANDGETLASTMESSAGEIDRESPONSE_PB_H).
-define(COMMANDGETLASTMESSAGEIDRESPONSE_PB_H, true).
-record(commandgetlastmessageidresponse, {
    last_message_id = erlang:error({required, last_message_id}),
    request_id = erlang:error({required, request_id})
}).
-endif.

-ifndef(COMMANDGETTOPICSOFNAMESPACE_PB_H).
-define(COMMANDGETTOPICSOFNAMESPACE_PB_H, true).
-record(commandgettopicsofnamespace, {
    request_id = erlang:error({required, request_id}),
    namespace = erlang:error({required, namespace}),
    mode = 'PERSISTENT'
}).
-endif.

-ifndef(COMMANDGETTOPICSOFNAMESPACERESPONSE_PB_H).
-define(COMMANDGETTOPICSOFNAMESPACERESPONSE_PB_H, true).
-record(commandgettopicsofnamespaceresponse, {
    request_id = erlang:error({required, request_id}),
    topics = []
}).
-endif.

-ifndef(COMMANDGETSCHEMA_PB_H).
-define(COMMANDGETSCHEMA_PB_H, true).
-record(commandgetschema, {
    request_id = erlang:error({required, request_id}),
    topic = erlang:error({required, topic}),
    schema_version
}).
-endif.

-ifndef(COMMANDGETSCHEMARESPONSE_PB_H).
-define(COMMANDGETSCHEMARESPONSE_PB_H, true).
-record(commandgetschemaresponse, {
    request_id = erlang:error({required, request_id}),
    error_code,
    error_message,
    schema,
    schema_version
}).
-endif.

-ifndef(BASECOMMAND_PB_H).
-define(BASECOMMAND_PB_H, true).
-record(basecommand, {
    type = erlang:error({required, type}),
    connect,
    connected,
    subscribe,
    producer,
    send,
    send_receipt,
    send_error,
    message,
    ack,
    flow,
    unsubscribe,
    success,
    error,
    close_producer,
    close_consumer,
    producer_success,
    ping,
    pong,
    redeliverunacknowledgedmessages,
    partitionmetadata,
    partitionmetadataresponse,
    lookuptopic,
    lookuptopicresponse,
    consumerstats,
    consumerstatsresponse,
    reachedendoftopic,
    seek,
    getlastmessageid,
    getlastmessageidresponse,
    active_consumer_change,
    gettopicsofnamespace,
    gettopicsofnamespaceresponse,
    getschema,
    getschemaresponse,
    authchallenge,
    authresponse
}).
-endif.

