-module(pulsar_utils_tests).

-include_lib("eunit/include/eunit.hrl").

-define(l2m(L), maps:from_list(L)).

merge_opts_test_() ->
    [ ?_assertError(_, pulsar_utils:merge_opts([[a,b]]))
    , ?_assertEqual([a,b], pulsar_utils:merge_opts([[a], [b]]))
    , ?_assertEqual([a,b], pulsar_utils:merge_opts([[a,b], [b]]))
    , ?_assertEqual([a,b], pulsar_utils:merge_opts([[a,b], []]))
    , ?_assertEqual([a,b,c], pulsar_utils:merge_opts([[a,b], [c]]))
    , ?_assertEqual([a,b,c], pulsar_utils:merge_opts([[a], [b], [c]]))
    , ?_assertMatch(#{op1 := 1, op2 := 2},
        ?l2m(pulsar_utils:merge_opts([
            [{op1, 1}],
            [{op1, 1}, {op2, 2}]])))
    , ?_assertMatch(#{op1 := 1, op2 := 2},
        ?l2m(pulsar_utils:merge_opts(
            [[{op1, 1}, {op2, 0}],
             [{op1, 1}, {op2, 2}]])))
    , ?_assertMatch(#{op1 := 1, op2 := 2, op3 := 3},
        ?l2m(pulsar_utils:merge_opts(
            [[{op1, 1}, {op2, a}],
             [{op1, a}, {op2, 2}],
             [{op1, 1}, {op3, 3}]])))
    , ?_assertMatch(#{op1 := 1, op2 := 2, op3 := 3, op4 := 4},
        ?l2m(pulsar_utils:merge_opts(
            [[{op1, 1}, {op2, a}],
             [{op1, a}, {op2, 2}],
             [{op3, 3}],
             [{op1, 1}, {op4, 4}]])))
    ].
