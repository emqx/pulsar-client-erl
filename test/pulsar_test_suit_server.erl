%%%-------------------------------------------------------------------
%%% @author DDDHuang
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 3月 2021 3:56 下午
%%%-------------------------------------------------------------------
-module(pulsar_test_suit_server).
-author("DDDHuang").

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).
-export([consumer_new_msg/3, consumer_state/1]).

-define(SERVER, ?MODULE).

-record(p_state, {
    last_message,
    counter
}).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    {ok, #p_state{counter = 0}}.

consumer_new_msg(Pid, Msg, Payloads) ->
    gen_server:call(Pid, {consumer_new_msg, {Msg, Payloads}}).
consumer_state(Pid) ->
    gen_server:call(Pid, consumer_state).

handle_call({consumer_new_msg, {Msg, Payloads}}, _From,
    State = #p_state{counter = Counter}) ->
    PayloadSize = erlang:length(Payloads),
    {reply, ok, State#p_state{last_message = {Msg, Payloads},
        counter = Counter + PayloadSize}};
handle_call(consumer_state, _From, State = #p_state{counter = Counter, last_message = Message}) ->
    {reply, {Counter, Message}, State}.

handle_cast(_Request, State = #p_state{}) ->
    {noreply, State}.

handle_info(_Info, State = #p_state{}) ->
    {noreply, State}.


terminate(_Reason, _State = #p_state{}) ->
    ok.

code_change(_OldVsn, State = #p_state{}, _Extra) ->
    {ok, State}.

