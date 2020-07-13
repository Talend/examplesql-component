create database testing;
commit;
use testing;
create table table1(id int, col1 varchar(255));
insert into table1 values(1,'Value1');
insert into table1 values(2,'Value2');
commit;