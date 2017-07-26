A = LOAD 'data' AS (a1:int,a2:int,a3:int);

B = FOREACH A GENERATE (chararray)f1, ('b:',(chararray)f2), ('c:',(chararray)f3);


A = LOAD '/user/xxx/flightdelays/' USING PigStorage(',');

B = FOREACH A GENERATE (int) $0 as Year,(int) $1 as Month,(int) $2 as Dayofmonth,(int) $3 as Dayofweek,(chararray) $16 as Origin,(chararray) $17 as Destination

C = GROUP B BY Dayofweek


D = FOREACH C GENERATE group as Week,MAX(B.Dayofmonth);


D = FOREACH B {user_details_sorted_by_no = ORDER A BY num DESC;
    top_record = LIMIT user_details_sorted_by_no 1;
    GENERATE FLATTEN(top_record);
}


==============================================================================================================================

Cust = LOAD '/users/xxx/Customers.txt' USING PigStorage(',') as (id:int, name:chararray, age:int, address:chararray, salary:int);

Ord = LOAD '/users/xxx/Orders.txt' USING PigStorage(',') as (oid:int, date:chararray, customer_id:int, amount:int);

JOINT = JOIN Cust BY id, Ord BY customer_id;

LEFT_OUTER = JOIN Cust BY id LEFT OUTER,Ord BY id ;

===============================================================================================================================

A = LOAD 'XXX.PROJECT_INFO' USING org.apache.hive.hcatalog.pig.HCatLoader();

B = GROUP A BY PROJECT_TYPE_ID

C = FOREACH B GENERATE group as id:int,MAX(A.ve_project_id) as maxnumber:int;

C = FOREACH B GENERATE (int) group as id,(int) MAX(A.ve_project_id) as maxnumber;

DUMP C;

C = FOREACH B GENERATE group ,MAX(A.ve_project_id);

-- D = FOREACH C GENERATE $0 as id:double,$1 as maxnumber:double;

E = FOREACH C GENERATE $0 as (double) id:double,$1 as maxnumber:double;

STORE C INTO 'XXX.raghava_pig_output' USING org.apache.hive.hcatalog.pig.HCatStorer();

create table XXX.raghava_pig_output (id double,maxnumber double)
row format delimited fields terminated by ',';



