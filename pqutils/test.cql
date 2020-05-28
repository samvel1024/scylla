drop keyspace if exists mk;

create keyspace mk with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };


-- create table mk.example (pk1 text primary key, or1 int);
-- insert into mk.example (pk1, or1) values ('1', null);
-- insert into mk.example (pk1, or1) values ('2', 2);
--

-- create table mk.composite_pk
-- (
--     pk1 text,
--     pk2 int,
--     ck1 int,
--     ck2 text,
--     ck3 text,
--     ck4 int,
--     ck5 int,
--     or1 int,
--     or2 int,
--     or3 text,
--     primary key ((pk1, pk2), ck1, ck2, ck3, ck4, ck5)
-- );
--
--
-- insert into mk.composite_pk (pk1, pk2, ck1, ck2, ck3, ck4, ck5, or1, or2, or3)
-- values ('one', 1, 2, 'a', 'b', 3, 4, 5, 6, 'c');
-- insert into mk.composite_pk (pk1, pk2, ck1, ck2, ck3, ck4, ck5, or1, or2, or3)
-- values ('one', 11, 7, 'd', 'e', 8, 9, null, 6, 'A');
-- insert into mk.composite_pk (pk1, pk2, ck1, ck2, ck3, ck4, ck5, or1, or2, or3)
-- values ('two', 2, 2,
--         'LOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOONG',
--         'VERY LOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOONG',
--         2, 4, null, 6, null);
-- insert into mk.composite_pk (pk1, pk2, ck1, ck2, ck3, ck4, ck5, or1, or2, or3)
-- values ('two', 22, 2, 'a', 'b', 3, 4, 5, 6, 'c');
--
--
--
-- create table mk.test
-- (
--     pk1 text,
--     pk2 text,
--     c1  text,
--     c2  text,
--     s1  text static,
--     s2  text static,
--     r1  text,
--     r2  text,
--     primary key ((pk1, pk2), c1, c2)
-- );
--
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster1', '1', 'static1-1', 'static2-1', null, 'regular2-1');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster1', '1', 'static1-2', 'static2-2', null, 'regular2-2');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster1', '2', 'static1-3', 'static2-3', null, 'regular2-3');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster1', '3', 'static1-4', 'static2-4', null, 'regular2-4');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster2', '1', 'static1-5', 'static2-5', null, 'regular2-5');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '1', 'cluster2', '1', 'static1-6', 'static2-6', null, 'regular2-6');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part1', '2', 'cluster1', '1', 'static1-6', 'static2-6', null, 'regular2-6');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part2', '1', 'cluster1', '1', 'static1-6', 'static2-6', null, 'regular2-6');
-- insert into mk.test (pk1, pk2, c1, c2, s1, s2, r1, r2)
-- VALUES ('part2', '1', 'cluster1', '2', 'static1-6', 'static2-6', null, 'regular2-6');
--
--


CREATE TABLE mk.t1
(
    k  text PRIMARY KEY,
    v1 text,
    v2 text
);

insert into mk.t1 (k, v1, v2) values ('7', '1', '1');
insert into mk.t1 (k, v1, v2) values ('6', '6', '2');



