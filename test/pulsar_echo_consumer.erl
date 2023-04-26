-module(pulsar_echo_consumer).

%% pulsar consumer API
-export([init/2, handle_message/3]).

init(Topic, Args) ->
    SendTo = maps:get(send_to, Args),
    SendTo ! {consumer_started, #{topic => Topic}},
    {ok, #{topic => Topic, send_to => SendTo}}.

handle_message(Message, Payloads, State) ->
    #{send_to := SendTo, topic := Topic} = State,
    SendTo ! {pulsar_message, Topic, Message, Payloads},
    {ok, 'Individual', State}.
