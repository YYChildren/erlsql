%% Description: Test code for ErlSQL
%% Author: Yariv Sadan (yarivvv@gmail.com)
%%
%% For license see LICENSE.txt


-module(test_erlsql).
-author("Yariv Sadan (yarivvv@gmail.com)").

-include_lib("eunit/include/eunit.hrl").

assert(Q, Str) ->
    assert(Q, Str, true).
assert(Q, Str, true) ->
    io:format("~p ->~n", [Q]),
    Res = binary_to_list(erlsql:sql(Q, true)),
    io:format("  ~p~n  ~p~n~n", [Str, Res]),
    ?assert(Res =:= Str);
assert(Q, Str, false) ->
    io:format("~p ->~n", [Q]),
    Res = binary_to_list(erlsql:unsafe_sql(Q,true)),
    io:format("  ~s~n  ~s~n~n", [Str, Res]),
    ?assert(Res =:= Str),
    
    case catch erlsql:sql(Q) of
	    {error, {unsafe_expression, _}} ->
	        ok;
	    Other ->
	        throw({"statement passed illegally!!!", {Q, Other}})
    end.

all_test() ->
    Pairs =
    [
     [{insert, project, [{foo, 5}, {baz, "bob"}]},
      "INSERT INTO project(foo,baz) VALUES (5,\'bob\')"],

     [{insert, project, [foo, bar, baz], [[a,b,c], [d,e,f]]},
      "INSERT INTO project(foo,bar,baz) VALUES "
      "('a','b','c'),('d','e','f')"],

     [{insert, project, [foo, bar, baz], [{a,b,c}, {d,e,f}]},
      "INSERT INTO project(foo,bar,baz) VALUES "
      "('a','b','c'),('d','e','f')"],

     [{insert_duplicate, project, [foo, bar, baz], [{a,b,c}, {d,e,f}]},
       "INSERT INTO project(foo,bar,baz) VALUES "
       "('a','b','c'),('d','e','f') "
       "ON DUPLICATE KEY UPDATE foo=VALUES(foo),bar=VALUES(bar),baz=VALUES(baz)"],

     [{insert_ignore, project, [foo, bar, baz], [{a,b,c}, {d,e,f}]},
       "INSERT IGNORE INTO project(foo,bar,baz) VALUES "
       "('a','b','c'),('d','e','f')"],
     
     [{update, project, [{foo,5},{bar,6},{baz,"hello"}]},
      "UPDATE project SET foo = 5,bar = 6,baz = 'hello'"],

     [{update, project, [{foo, "quoted"}, {baz, blub}],
       {where, {'not', {a,'=',5}}}},
      "UPDATE project SET foo = 'quoted',baz = blub WHERE NOT (a = 5)"],
     
     [{update, project, [{started_on, "2000213"}],
       {name,like,"blob"}},
      "UPDATE project SET started_on = '2000213' WHERE (name LIKE 'blob')"],
     
     [{delete, project},
      "DELETE FROM project"],
     
     [{delete, project, {a,'=',5}},
      "DELETE FROM project WHERE (a = 5)"],

     [{delete, {from, project}, {where, {a,'=',5}}},
      "DELETE FROM project WHERE (a = 5)"],
     
     [{delete, developer,
       {'not',
        {{name,like,"%Paul%"},'or',{name,like,"%Gerber%"}}}},
      "DELETE FROM developer WHERE "
      "NOT ((name LIKE '%Paul%') OR (name LIKE '%Gerber%'))"],

     [{select, ["foo"]},
      "SELECT 'foo'"],

     [{select, ["foo", "bar"]},
      "SELECT 'foo','bar'"],

     [{select, {1,'+',1}},
      "SELECT (1 + 1)"],

     [{select, {foo, as, bar}, {from, {baz, as, blub}}},
      "SELECT foo AS bar FROM baz AS blub"],

     [{select, name, {from, developer},
       {where, {country, '=', "quoted \' \" string"}}},
      "SELECT name FROM developer "
      "WHERE (country = 'quoted \\\' \\\" string')"],

     [{select, [{{p, name}, as, name}, {{p, age}, as, age},
            {project, '*'}],
       {from, [{person, as, p}, project]}},
      "SELECT p.name AS name,p.age AS age,project.* "
      "FROM person AS p,project"],
     
     [{select, {call, count, name}, {from, developer}},
      "SELECT count(name) FROM developer"],

     [{select, {call, last_insert_id, []}},
      "SELECT last_insert_id()"],
     
     [{{select, name, {from, person}}, union,
       {select, name, {from, project}}},
      "(SELECT name FROM person) UNION (SELECT name FROM project)"],

     [{select, distinct, name, {from, person}, {limit, 5}},
      "SELECT DISTINCT name FROM person LIMIT 5"],
     
     [{select, [name, age], {from, person},
       {order_by, [{name, desc}, age]}},
      "SELECT name,age FROM person ORDER BY name DESC,age"],

     [{select, [{call, count, name}, age], {from, developer},
       {group_by, age}},
      "SELECT count(name),age FROM developer GROUP BY age"],

     [{select, [{call, count, name}, age, country],
       {from, developer},
       {group_by, [age, country], having, {age, '>', 20}}},
      "SELECT count(name),age,country FROM developer "
      "GROUP BY age,country HAVING (age > 20)"],
     
     [{select, '*', {from, developer},
       {where, {name, in, ["Paul", "Frank"]}}},
      "SELECT * FROM developer WHERE name IN (\'Paul\',\'Frank\')"],
     
     [{select, name, {from, developer},
       {where, {name, in, {select, distinct, name, {from, gymnist}}}}},
      "SELECT name FROM developer "
      "WHERE name IN (SELECT DISTINCT name FROM gymnist)"],

     [{select, name, {from, developer},
       {where, {name, in,
            {{select, distinct, name, {from, gymnist}},
             union,
             {select, name, {from, dancer},
              {where, {{name, like, "Mikhail%"}, 'or',
                   {country,'=',"Russia"}}}},
             {where, {name, like, "M%"}},
             [{order_by, {name, desc}}, {limit, 5, 10}]
            }}}},
      "SELECT name FROM developer "
      "WHERE name IN "
      "((SELECT DISTINCT name FROM gymnist) "
      "UNION (SELECT name FROM dancer"
      " WHERE ((name LIKE 'Mikhail%') OR (country = 'Russia'))) "
      "WHERE (name LIKE 'M%') ORDER BY name DESC LIMIT 5,10)"],
     
     [{select, '*', {from, developer}, {where, {name,'=','?'}}},
      "SELECT * FROM developer WHERE (name = ?)"],

     [{select, '*', {from, foo}, {where, {a,'=',{'+', [1,2,3]}}}},
      "SELECT * FROM foo WHERE (a = (1 + 2 + 3))"],

     [{select, '*', {from, foo},
       {where, {'=', [{'+',[a,b,c]}, {'+', [d,e,f]}]}}},
      "SELECT * FROM foo WHERE ((a + b + c) = (d + e + f))"],
     
     [{select, '*', {from, foo},
       {where, {'and', [{a,'=',b}, {c,'=',d}, {e,'=',f}]}}},
      "SELECT * FROM foo WHERE ((a = b) AND (c = d) AND (e = f))"],
     
     [{select, '*', {from, blah}, undefined, undefined},
      "SELECT * FROM blah"]
    ],
         
    lists:foreach(
      fun([Q, Str]) -> assert(Q, Str) end,
      Pairs),

    %% WARNING: the following queries use verbatim WHERE and
    %% other trailing clauses, which is VERY DANGEROUS because
    %% it exposes you to SQL injection attacks.
    %%
    %% TRY TO AVOID WRITING SUCH QUERIES WHENEVER POSSIBLE

    UnsafeQueries =
    [
     [{select, '*', {from, foo}, "WHERE a=b"},
      "SELECT * FROM foo WHERE a=b"],

     [{select, '*', {from, foo}, "WHERE a='foo'"},
      "SELECT * FROM foo WHERE a='foo'"],

     [{select, '*', {from, foo}, <<"WHERE a='i'm an evil query'">>},
      "SELECT * FROM foo WHERE a='i'm an evil query'"],

     [{select, '*', {from, foo}, <<"WHERE a=b">>},
      "SELECT * FROM foo WHERE a=b"],

     [{select, '*', {from, foo}, {where, "a=b"}},
      "SELECT * FROM foo WHERE a=b"],

     [{select, '*', {from, foo}, {where, <<"a=b">>}},
      "SELECT * FROM foo WHERE a=b"],

     [{select, '*', {from, foo},
       {where, {a, in,
            {select, '*', {from, bar}, {where, "a=b"}}}}},
      "SELECT * FROM foo WHERE a IN (SELECT * FROM bar WHERE a=b)"],

     [{select, '*', {from, foo},
       {where, {a, in,
            {select, '*', {from, bar}, {where, <<"a=b">>}}}}},
      "SELECT * FROM foo WHERE a IN (SELECT * FROM bar WHERE a=b)"],

     [{select, '*', {from, foo},
       {where, {a, in,
            {select, '*', {from, bar}, <<"WHERE a=b">>}}}},
      "SELECT * FROM foo WHERE a IN (SELECT * FROM bar WHERE a=b)"],
     
     [{select, '*', {from, foo}, {where, <<"a=b">>}, <<"LIMIT 5">>},
      "SELECT * FROM foo WHERE a=b LIMIT 5"],

     [{{select, '*', {from, foo}, {where, <<"a=b">>}},
       union,
       {select, '*', {from, bar}}},
      "(SELECT * FROM foo WHERE a=b) UNION (SELECT * FROM bar)"],

     [{{select, '*', {from, foo}},
       union,
       {select, '*', {from, bar}, {where, <<"a=b">>}}},
      "(SELECT * FROM foo) UNION (SELECT * FROM bar WHERE a=b)"],

     [{{select, '*', {from, foo}},
        union,
        {select, '*', {from, bar}},
        {where, "a=b"}},
       "(SELECT * FROM foo) UNION (SELECT * FROM bar) WHERE a=b"],
     
     [{select, {a, 'or', "foo"}},
      "SELECT (a OR (foo))"],

     [{select, {"bar", 'or', b}},
      "SELECT ((bar) OR b)"],

     [{select, {"foo", 'and', <<"bar">>}},
      "SELECT ((foo) AND (bar))"],

     [{select, {'not', "foo=bar"}},
      "SELECT NOT (foo=bar)"],

     [{select, {'!', "foo=bar"}},
      "SELECT NOT (foo=bar)"]
     ],

    lists:foreach(
      fun([Q, Str]) -> assert(Q, Str, false) end,
      UnsafeQueries).
