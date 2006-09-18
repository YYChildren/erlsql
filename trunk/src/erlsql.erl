%% Author: Yariv Sadan (yarivvv@gmail.com) (http://yarivsblog.com)
%% Date: 9/16/06
%%
%% Description
%% -----------
%% ErlSQL (ESQL) is a domain specific embedded language for
%% expressing SQL statements in Erlang combined with a library
%% for generating the literal equivalents of esql expressions. 
%%
%% ErlSQL lets you describe SQL queries using a combination of Erlang
%% lists, tuples, atoms and values in a way that resembles the
%% structure of SQL statements. You can pass this structure to
%% the sql/1 or sql/2 functions, which parse it and return an
%% iolist (a tree of strings and/or binaries) or a single binary,
%% either of which can be sent to database engine through a socket
%% (usually via a database-specific driver).
%%
%% ErlSQL supports a large subset of the SQL language implemented by
%% some popular RDBMS's, including most common INSERT, UPDATE, DELETE
%% and SELECT statements. ErlSQL can generate complex queries including
%% those with unions, nested statements and aggregate functions but
%% it does not currently attempt to cover every feature and extension
%% of the SQL language.
%% 
%% ErlSQL's benefits are:
%% - Easy dynamic generation of SQL queries from Erlang for application
%%   developers.
%% - Prevention of most, if not all, SQL injection attacks by
%%   assuring that all string values are properly escaped.
%% - Integration with higher level libraries such as ErlyDB
%%   (http://code.google.com/p/erlydb).
%%
%% For usage examples, look at the file test_erlsql.erl under the test/
%% directory.
%%   
%% For more information, visit ErlSQL's home page at
%% http://code.google.com/p/erlsql.
%% 
%% For license information see LICENSE.TXT

-module(erlsql).
-author("Yariv Sadan (yarivvv@gmail.com) (http://yarivsblog.com)").
-export([sql/1, sql/2]).

%% @doc Generate an iolist (a tree of strings and/or binaries)
%%  for a literal SQL statement that corresponds to the ESQL
%%  structure. If the structure is invalid, this function would
%%  crash.
%%
%% @spec sql(ESQL::term()) -> iolist()
sql({select, Tables})->
    select(Tables);
sql({select, Fields, {from, Tables}}) ->
    select(Fields, Tables);
sql({select, Fields, {from, Tables}, {where, WhereExpr}}) ->
    select(undefined, Fields, Tables, WhereExpr, undefined);
sql({select, Fields, {from, Tables}, Extras}) ->
    select(undefined, Fields, Tables, undefined, Extras);
sql({select, Tables, {where, WhereExpr}}) ->
    select(undefined, undefined, Tables, WhereExpr);
sql({select, Modifier, Fields, {from, Tables}}) ->
    select(Modifier, Fields, Tables);
sql({select, Modifier, Fields, {from, Tables}, {where, WhereExpr}}) ->
    select(Modifier, Fields, Tables, WhereExpr);
sql({select, Modifier, Fields, {from, Tables}, Extras}) ->
    select(Modifier, Fields, Tables, undefined, Extras);
sql({select, Modifier, Fields, {from, Tables}, {where, WhereExpr}, Extras}) ->
    select(Modifier, Fields, Tables, WhereExpr, Extras);
sql({Select1, union, Select2}) ->
    [$(, sql(Select1), <<") UNION (">>, sql(Select2), $)];
sql({Select1, union, Select2, {where, WhereExpr}}) ->
    [sql({Select1, union, Select2}), where(WhereExpr)];
sql({Select1, union, Select2, Extras}) ->
    [sql({Select1, union, Select2}), extra_clause(Extras)];
sql({Select1, union, Select2, {where, _} = Where, Extras}) ->
    [sql({Select1, union, Select2, Where}), extra_clause(Extras)];
sql({insert, Table, Params}) ->
    insert(Table, Params);
sql({insert, Table, Fields, Values}) ->
    insert(Table, Fields, Values);
sql({update, Table, Params}) ->
    update(Table, Params);
sql({update, Table, Params, {where, Where}}) ->
    update(Table, Params, Where);
sql({update, Table, Params, Where}) ->
    update(Table, Params, Where);
sql({delete, {from, Table}}) ->
    delete(Table);
sql({delete, Table}) ->
    delete(Table);
sql({delete, {from, Table}, {where, Where}}) ->
    delete(Table, Where);
sql({delete, Table, {where, Where}}) ->
    delete(Table, Where);
sql({delete, Table, Where}) ->
    delete(Table, Where).

%% @doc Similar to sql/1, but accepts a boolean parameter
%%   indicating if the return value should be a single binary
%%   rather than an iolist.
%% 
%% @spec sql(Esql::term(), true) -> binary()
%% @spec sql(Esql::term(), false) -> iolist()
sql(Esql, true) ->
    iolist_to_binary(sql(Esql, false));
sql(Esql, false) ->
    sql(Esql).


%% Internal functions

select(Fields) ->
    select(undefined, Fields, undefined, undefined, undefined).

select(Fields, Tables) ->
    select(undefined, Fields, Tables, undefined, undefined).

select(Modifier, Fields, Tables) ->
    select(Modifier, Fields, Tables, undefined, undefined).

select(Modifier, Fields, Tables, WhereExpr) ->
    select(Modifier, Fields, Tables, WhereExpr, undefined).

select(Modifier, Fields, Tables, WhereExpr, Extras) ->
    S1 = <<"SELECT ">>,
    S2 = case Modifier of
	     undefined ->
		 S1;
	     Modifier ->
		 Modifier1 = case Modifier of
				 distinct -> 'DISTINCT';
				 'all' -> 'ALL';
				 Other -> Other
			     end,
		 [S1, convert(Modifier1), 32]
	 end,

    S3 = [S2, make_list(Fields, fun expr2/1)],
    S4 = case Tables of
	     undefined ->
		 S3;
	     _Other ->
		 [S3, <<" FROM ">>, make_list(Tables, fun expr2/1)]
	 end,

    S5 =
	case where(WhereExpr) of
	    undefined ->
		S4;
	    WhereClause ->
		[S4, WhereClause]
	end,
    
    case extra_clause(Extras) of  
	undefined -> S5;
	Expr -> [S5, Expr]
    end.
					    

extra_clause(undefined) -> undefined;
extra_clause(Exprs) when is_list(Exprs) ->
    Res = lists:foldl(
	    fun(Expr, Acc) ->
		    [extra_clause(Expr) | Acc]
	    end, [], Exprs),
    [lists:reverse(Res)];
extra_clause({limit, Num}) ->
    [<<" LIMIT ">>, encode(Num)];
extra_clause({limit, Offset, Num}) ->
    [<<" LIMIT ">>, encode(Offset), $, , encode(Num)];
extra_clause({group_by, ColNames}) ->
    [<<" GROUP BY ">>, make_list(ColNames, fun convert/1)];
extra_clause({group_by, ColNames, having, Expr}) ->
    [extra_clause({group_by, ColNames}), <<" HAVING ">>, expr(Expr)];
extra_clause({order_by, ColNames}) ->
    [<<" ORDER BY ">>,
     make_list(ColNames,
		      fun({Name, Modifier}) when
			 Modifier == 'asc' ->
			      [convert(Name), 32, convert('ASC')];
			 ({Name, Modifier}) when
			 Modifier == 'desc' ->
			      [convert(Name), 32, convert('DESC')];
			 (Name) ->
			      convert(Name)
		      end)].
				 
    
    
insert(Table, Params) ->
    Names = make_list(Params, fun({Name, _Value}) ->
					     convert(Name)
				     end),
    Values = [$(, make_list(
	       Params,
	       fun({_Name, Value}) ->
		       encode(Value)
	       end),
	      $)],
    make_insert_query(Table, Names, Values).

insert(Table, Fields, Records) ->
    Names = make_list(Fields, fun convert/1),
    Values =
	make_list(
	  Records,
	  fun(Record) ->
		  Record1 = if is_tuple(Record) ->
				    tuple_to_list(Record);
			       true -> Record
			    end,
		  [$(, make_list(Record1, fun encode/1), $)]
	  end),    
    make_insert_query(Table, Names, Values).

make_insert_query(Table, Names, Values) ->
    [<<"INSERT INTO ">>, convert(Table),
     $(, Names, <<") VALUES ">>, Values].

update(Table, Params) ->
    update(Table, Params, undefined).

update(Table, Params, WhereExpr) ->
    S1 = [<<"UPDATE ">>, convert(Table), <<" SET ">>],
    S2 = make_list(Params,
			  fun({Field, Val}) ->
				  [convert(Field), $=, encode(Val)]
			  end),
    

    S3 = case where(WhereExpr) of
	     undefined ->
		 [S1, S2];
	     WhereClause ->
		 [S1, S2, WhereClause]
	 end,
    S3.

delete(Table) ->
    delete(Table, undefined).

delete(Table, WhereExpr) ->
    S1 = [<<"DELETE FROM ">>, convert(Table)],
    case where(WhereExpr) of
	undefined ->
	    S1;
	WhereClause ->
	    [S1, WhereClause]
    end.
    

convert(Val) when is_atom(Val)->
    {_Stuff, Bin} = split_binary(term_to_binary(Val), 4),
    Bin.

make_list(Vals, ConvertFun) when is_list(Vals) ->
    {Res, _} =
        lists:foldl(
          fun(Val, {Acc, false}) ->
                  {[ConvertFun(Val) | Acc], true};
	     (Val, {Acc, true}) ->
                  {[ConvertFun(Val) , $, | Acc], true}
          end, {[], false}, Vals),
    lists:reverse(Res);
make_list(Val, ConvertFun) ->
    ConvertFun(Val).

where(undefined) -> undefined;
where(WhereExpr) ->
    S1 = make_list(WhereExpr, fun expr/1),
    [<<" WHERE ">>, S1].


expr({Not, Expr}) when Not == 'not'; Not == '!' ->
    [<<"NOT ">>, expr(Expr)];
expr({Table, Field}) when is_atom(Table), is_atom(Field) ->
    [convert(Table), $., convert(Field)];
expr({Expr1, as, Alias}) when is_atom(Alias) ->
    [expr2(Expr1), <<" AS ">>, convert(Alias)];
expr({call, FuncName, []}) ->
    [convert(FuncName), <<"()">>];
expr({call, FuncName, Param}) ->
    [convert(FuncName), $(, expr2(Param), $)];
expr({Val, Op, {select, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {select, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {select, _, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {select, _, _, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {select, _, _, _, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {select, _, _, _, _, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {_, union, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {_, union, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, {_, union, _, _, _} = Subquery}) ->
    subquery(Val, Op, Subquery);
expr({Val, Op, Values}) when (Op == in orelse
			      Op == any orelse
			      Op == some) andalso
			     is_list(Values) ->
    [expr2(Val), subquery_op(Op), make_list(Values, fun encode/1), $)];
expr({Expr1, Op, Expr2}) ->
    Op1 = case Op of
	      'and' -> 'AND';
	      'or' -> 'OR';
	      like -> 'LIKE';
	      Other -> Other
	  end,
    [$(, expr2(Expr1), 32, convert(Op1), 32, expr(Expr2), $)];
expr({list, Vals}) ->
    [$(, make_list(Vals, fun encode/1), $)];
expr('?') -> $?;
expr(Val) -> encode(Val).

subquery(Val, Op, Subquery) ->
    [expr2(Val), subquery_op(Op), sql(Subquery), $)].

subquery_op(in) -> <<" IN (">>;
subquery_op(any) -> <<" ANY (">>;
subquery_op(some) -> <<" SOME (">>.

expr2(Expr) when is_atom(Expr) -> convert(Expr);
expr2(Expr) -> expr(Expr).
    

encode(Val) ->
    encode(Val, true).
encode(Val, false) when Val == undefined; Val == null ->
    "null";
encode(Val, true) when Val == undefined; Val == null ->
    <<"null">>;
encode(Val, false) when is_binary(Val) ->
    binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
    quote(Val);
encode(Val, true) ->
    list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
    quote(atom_to_list(Val));
encode(Val, false) when is_list(Val) ->
    quote(Val);
encode(Val, false) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode({datetime, Val}, AsBinary) ->
    encode(Val, AsBinary);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
    Res = io_lib:format("'~B~B~B~B~B~B'", [Year, Month, Day, Hour,
					  Minute, Second]),
    lists:flatten(Res);
encode({TimeType, Val}, AsBinary)
  when TimeType == 'date';
       TimeType == 'time' ->
    encode(Val, AsBinary);
encode({Time1, Time2, Time3}, false) ->
    Res = io_lib:format("'~B~B~B'", [Time1, Time2, Time3]),
    lists:flatten(Res);
encode(Val, _AsBinary) ->
    {error, {unrecognized_value, {Val}}}.

quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).
