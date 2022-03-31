-module(pulsar_utils_tests).

-include_lib("eunit/include/eunit.hrl").

merge_opts_test_() ->
    [ ?_assertError(_, pulsar_utils:merge_opts([[1,2]]))
    , ?_assertEqual([1,2], pulsar_utils:merge_opts([[1], [2]]))
    , ?_assertEqual([1,2], pulsar_utils:merge_opts([[1,2], [2]]))
    , ?_assertEqual([1,2], pulsar_utils:merge_opts([[1,2], []]))
    , ?_assertEqual([{op1, 1}, {op2, 3}],
        pulsar_utils:merge_opts([[{op1, 1}],
                                 [{op1, 1}, {op2, 3}]]))
    , ?_assertEqual([{op1, 1}, {op2, 3}],
        pulsar_utils:merge_opts([[{op1, 1}, {op2, 2}],
                                 [{op1, 1}, {op2, 3}]]))
    , ?_assertEqual([{op1, 1}, {op2, 2}, {op3, 3}],
        pulsar_utils:merge_opts([[{op1, 1}, {op2, a}],
                                 [{op1, a}, {op2, 2}],
                                 [{op1, 1}, {op3, 3}]
                                ]))
    , ?_assertEqual([{op1, 1}, {op2, 2}, {op3, 3}, {op4, 4}],
        pulsar_utils:merge_opts([[{op1, 1}, {op2, a}],
                                 [{op1, a}, {op2, 2}],
                                 [{op3, 3}],
                                 [{op1, 1}, {op4, 4}]
                                ]))
    ].
