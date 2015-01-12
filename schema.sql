
CREATE TABLE e(
    id SERIAL PRIMARY KEY
);

CREATE TABLE d(
    id SERIAL PRIMARY KEY,
    e_id_nn INT REFERENCES e

);

CREATE TABLE c(
    id SERIAL PRIMARY KEY

);

CREATE TABLE b(
    id SERIAL PRIMARY KEY,
    c_id__nn INT REFERENCES c NOT NULL,
    d_id__nn INT REFERENCES d NOT NULL
);


CREATE TABLE a(
    id SERIAL PRIMARY KEY,
    b_id__nn INT REFERENCES b
);

insert into e values(1);
insert into e values(2);
insert into e values(3);
insert into d values(1, 1);
insert into d values(2, 1);
insert into d values(3, 2);
insert into c values(1);
insert into c values(2);
insert into b values(1, 1, 1);
insert into b values(2, 1, 2);
insert into b values(3, 2, 1);
insert into a values(1, 2);
insert into a values(2, 2);

SELECT
T.table_name, TC.constraint_name, TC.constraint_type
FROM
information_schema.tables T
LEFT JOIN information_schema.table_constraints TC ON T.table_name = TC.table_name
WHERE
T.table_type = 'BASE TABLE'
AND
T.table_schema NOT IN ('pg_catalog', 'information_schema');


SELECT
T.table_name
FROM information_schema.tables T
WHERE
T.table_type = 'BASE TABLE';

SELECT a.*, b.*
FROM a
LEFT JOIN b ON a.b_id__nn = b.id;
