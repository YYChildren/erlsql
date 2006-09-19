%% Description: Test code for ErlSQL
%% Author: Yariv Sadan (yarivvv@gmail.com)
%%
%% For license see LICENSE.txt


-module(test_erlsql).
-author("Yariv Sadan (yarivvv@gmail.com)").

-export([assert/2, test/0]).

assert(Q, Str) ->
    io:format("~p ->~n", [Q]),
    Res = binary_to_list(iolist_to_binary(erlsql:sql(Q))),
    io:format("  ~p~n  ~p~n~n", [Str, Res]),
    Res = Str.

test() ->
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
	 
	 [{update, project, [{foo,5},{bar,6},{baz,"hello"}]},
	  "UPDATE project SET foo=5,bar=6,baz=\'hello'"],

	 [{update, project, [{foo, "quo\'ted"}, {baz, blub}],
	   {where, {'not', {a,'=',5}}}},
	  "UPDATE project SET foo='quo\\\'ted',baz='blub' "
	   "WHERE NOT (a = 5)"],
	 
	 [{update, project, [{started_on, {2000,21,3}}],
	   {name,like,"blob"}},
	  "UPDATE project SET started_on='2000213' "
	  "WHERE (name LIKE \'blob\')"],
	 
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
	  "SELECT * FROM foo WHERE (a = 1 + 2 + 3)"],

	 [{select, '*', {from, foo},
	   {where, {'=', [{'+',[a,b,c]}, {'+', [d,e,f]}]}}},
	  "SELECT * FROM foo WHERE a + b + c = d + e + f"],
	 
	 [{select, '*', {from, foo},
	   {where, {'and', [{a,'=',b}, {c,'=',d}, {e,'=',f}]}}},
	  "SELECT * FROM foo WHERE (a = b) AND (c = d) AND (e = f)"]
	],
	     
    lists:foreach(
      fun([Q, Str]) -> assert(Q, Str) end,
      Pairs).
