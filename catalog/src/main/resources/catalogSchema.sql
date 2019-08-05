-- The MIT License (MIT)
--
-- Copyright (c) 2017 Databases and Information Systems Research Group, University of Basel, Switzerland
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in all
-- copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.


CREATE TABLE "user" (
    "id"       INTEGER IDENTITY NOT NULL,
    "username" VARCHAR(100)     NOT NULL,
    "password" VARCHAR(100)     NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "database" (
    "id"               BIGINT IDENTITY NOT NULL,
    "name"             VARCHAR(100)    NOT NULL,
    "owner"            INTEGER         NOT NULL REFERENCES "user" ("id"),
    "encoding"         INTEGER         NOT NULL,
    "collation"        INTEGER         NOT NULL,
    "connection_limit" INTEGER         NOT NULL,
    "default_schema"   BIGINT          NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "schema" (
    "id"        BIGINT IDENTITY NOT NULL,
    "database"  BIGINT          NOT NULL REFERENCES "database" ("id"),
    "name"      VARCHAR(100)    NOT NULL,
    "owner"     INTEGER         NOT NULL REFERENCES "user" ("id"),
    "encoding"  INTEGER         NULL,
    "collation" INTEGER         NULL,
    "type"      INTEGER         NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "table" (
    "id"          BIGINT IDENTITY            NOT NULL,
    "schema"      BIGINT                     NOT NULL REFERENCES "schema" ("id"),
    "name"        VARCHAR(100)               NOT NULL,
    "owner"       INTEGER                    NOT NULL REFERENCES "user" ("id"),
    "encoding"    INTEGER       DEFAULT 0    NULL,
    "collation"   INTEGER       DEFAULT 0    NULL,
    "type"        INTEGER                    NOT NULL,
    "definition"  VARCHAR(1000) DEFAULT NULL NULL,
    "primary_key" BIGINT        DEFAULT NULL NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "column" (
    "id"            BIGINT IDENTITY NOT NULL,
    "table"         BIGINT          NOT NULL REFERENCES "table" ("id"),
    "name"          VARCHAR(100)    NOT NULL,
    "position"      INTEGER         NOT NULL,
    "type"          INTEGER         NOT NULL,
    "length"        INTEGER         NULL,
    "precision"     INTEGER         NULL,
    "nullable"      BOOLEAN         NOT NULL,
    "encoding"      INTEGER         NOT NULL,
    "collation"     INTEGER         NOT NULL,
    "force_default" BOOLEAN         NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "default_value" (
    "column"                    BIGINT       NOT NULL REFERENCES "column" ("id"),
    "type"                      INTEGER      NOT NULL,
    "value"                     BLOB         NULL,
    "function_name"             VARCHAR(100) NULL,
    "autoincrement_start_value" BIGINT       NULL,
    "autoincrement_next_value"  BIGINT       NULL,
    PRIMARY KEY ("column")
);


CREATE TABLE "column_privilege" (
    "column"    BIGINT  NOT NULL REFERENCES "column" ("id"),
    "user"      INTEGER NOT NULL REFERENCES "user" ("id"),
    "privilege" INTEGER NOT NULL,
    "grantable" BOOLEAN NOT NULL,
    PRIMARY KEY ("column", "user", "privilege")
);


CREATE TABLE "table_privilege" (
    "table"     BIGINT  NOT NULL REFERENCES "table" ("id"),
    "user"      INTEGER NOT NULL REFERENCES "user" ("id"),
    "privilege" INTEGER NOT NULL,
    "grantable" BOOLEAN NOT NULL,
    PRIMARY KEY ("table", "user", "privilege")
);


CREATE TABLE "schema_privilege" (
    "schema"    BIGINT  NOT NULL REFERENCES "schema" ("id"),
    "user"      INTEGER NOT NULL REFERENCES "user" ("id"),
    "privilege" INTEGER NOT NULL,
    "grantable" BOOLEAN NOT NULL,
    PRIMARY KEY ("schema", "user", "privilege")
);


CREATE TABLE "database_privilege" (
    "database"  BIGINT  NOT NULL REFERENCES "database" ("id"),
    "user"      INTEGER NOT NULL REFERENCES "user" ("id"),
    "privilege" INTEGER NOT NULL,
    "grantable" BOOLEAN NOT NULL,
    PRIMARY KEY ("database", "user", "privilege")
);


CREATE TABLE "global_privilege" (
    "user"      INTEGER NOT NULL REFERENCES "user" ("id"),
    "privilege" INTEGER NOT NULL,
    "grantable" BOOLEAN NOT NULL,
    PRIMARY KEY ("user", "privilege")
);


CREATE TABLE "store" (
    "id"          INTEGER IDENTITY NOT NULL,
    "unique_name" VARCHAR(100)     NOT NULL,
    "adapter"     VARCHAR(100)     NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "data_placement" (
    "store" INTEGER NOT NULL REFERENCES "store" ("id"),
    "table" BIGINT  NOT NULL REFERENCES "table" ("id"),
    PRIMARY KEY ("store", "table")
);


CREATE TABLE "key" (
    "id"     BIGINT IDENTITY NOT NULL,
    "table"  BIGINT          NOT NULL REFERENCES "table" ("id"),
    "unique" BOOLEAN         NOT NULL,
    "name"   VARCHAR(100)    NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "key_column" (
    "key"    BIGINT NOT NULL REFERENCES "key" ("id"),
    "column" BIGINT NOT NULL REFERENCES "column" ("id"),
    PRIMARY KEY ("key", "column")
);


CREATE TABLE "foreign_key" (
    "key"           BIGINT               NOT NULL REFERENCES "key" ("id"),
    "references"    BIGINT               NOT NULL REFERENCES "key" ("id"),
    "on_update"     INTEGER DEFAULT NULL NULL,
    "on_delete"     INTEGER DEFAULT NULL NULL,
    "deferrability" INTEGER              NULL,
    PRIMARY KEY ("key")
);


CREATE TABLE "index" (
    "id"       BIGINT IDENTITY NOT NULL,
    "key"      BIGINT          NOT NULL REFERENCES "key" ("id"),
    "type"     INTEGER         NOT NULL,
    "location" INTEGER         NULL REFERENCES "store" ("id"),
    "name"     VARCHAR(100)    NOT NULL,
    PRIMARY KEY ("id")
);


-- -------------------------------------------------------------------


ALTER TABLE "database"
    ADD FOREIGN KEY ("default_schema") REFERENCES "schema" ("id");

ALTER TABLE "table"
    ADD FOREIGN KEY ("primary_key") REFERENCES "key" ("id");


-- -------------------------------------------------------------------


CREATE INDEX "table_name"
    ON "table" ("name");
CREATE UNIQUE INDEX "table_schema_name"
    ON "table" ("schema", "name");

CREATE INDEX "table_privilege_user"
    ON "table_privilege" ("user");
CREATE INDEX "table_privilege_table"
    ON "table_privilege" ("table");
CREATE INDEX "table_privilege_privilege"
    ON "table_privilege" ("privilege");
CREATE INDEX "table_privilege_user_table"
    ON "table_privilege" ("user", "table");

CREATE INDEX "column_name"
    ON "column" ("name");
CREATE UNIQUE INDEX "column_table_name"
    ON "column" ("table", "name");
CREATE UNIQUE INDEX "column_position_table"
    ON "column" ("table", "position");

CREATE INDEX "column_privilege_column"
    ON "column_privilege" ("column");
CREATE INDEX "column_privilege_user"
    ON "column_privilege" ("user");
CREATE INDEX "column_privilege_privilege"
    ON "column_privilege" ("privilege");
CREATE INDEX "column_privilege_user_column"
    ON "column_privilege" ("user", "column");

CREATE UNIQUE INDEX "database_name"
    ON "database" ("name");

CREATE INDEX "database_privilege_user"
    ON "database_privilege" ("user");
CREATE INDEX "database_privilege_database"
    ON "database_privilege" ("database");
CREATE INDEX "database_privilege_privilege"
    ON "database_privilege" ("privilege");
CREATE INDEX "database_privilege_user_database"
    ON "database_privilege" ("user", "database");

CREATE INDEX "global_privilege_privilege"
    ON "global_privilege" ("privilege");
CREATE INDEX "global_privilege_user"
    ON "global_privilege" ("user");

CREATE UNIQUE INDEX "schema_database_name"
    ON "schema" ("database", "name");
CREATE INDEX "schema_name"
    ON "schema" ("name");
CREATE INDEX "schema_database"
    ON "schema" ("database");

CREATE INDEX "schema_privilege_user"
    ON "schema_privilege" ("user");
CREATE INDEX "schema_privilege_schema"
    ON "schema_privilege" ("schema");
CREATE INDEX "schema_privilege_privilege"
    ON "schema_privilege" ("privilege");
CREATE INDEX "schema_privilege_user_schema"
    ON "schema_privilege" ("user", "schema");

CREATE INDEX "key_table"
    ON "key" ("table");
CREATE UNIQUE INDEX "key_name"
    ON "key" ("name");

CREATE INDEX "key_column_key"
    ON "key_column" ("key");

CREATE INDEX "foreign_key_references"
    ON "foreign_key" ("references");

CREATE UNIQUE INDEX "index_key"
    ON "index" ("key");
CREATE UNIQUE INDEX "index_name"
    ON "index" ("name");
CREATE UNIQUE INDEX "index_key_type_location"
    ON "index" ("key", "type", "location");

CREATE UNIQUE INDEX "user_username"
    ON "user" ("username");

CREATE UNIQUE INDEX "store_unique_name"
    ON "store" ("unique_name");
CREATE INDEX "store_adapter"
    ON "store" ("adapter");

CREATE INDEX "data_placement_store"
    ON "data_placement" ("store");
CREATE INDEX "data_placement_table"
    ON "data_placement" ("table");


-- ---------------------------------------------------------------

-- user
INSERT INTO "user" ( "id", "username", "password" )
VALUES ( 0, 'system', '' ),
       ( 1, 'pa', '' );

ALTER TABLE "user"
    ALTER COLUMN "id"
        RESTART WITH 2;

--
-- database
--
INSERT INTO "database" ( "id", "name", "owner", "encoding", "collation", "connection_limit", "default_schema" )
VALUES ( 0, 'APP', 0, 1, 2, 0, NULL );

ALTER TABLE "database"
    ALTER COLUMN "id"
        RESTART WITH 1;


--
-- schema
--
INSERT INTO "schema" ( "id", "database", "name", "owner", "encoding", "collation", "type" )
VALUES ( 0, 0, 'public', 0, 1, 2, 1 );

ALTER TABLE "schema"
    ALTER COLUMN "id"
        RESTART WITH 1;

UPDATE "database"
SET "default_schema" = 0
WHERE "id" = 0;


--
-- table
--
INSERT INTO "table" ( "id", "schema", "name", "owner", "encoding", "collation", "type", "definition" )
VALUES ( 0, 0, 'depts', 0, 1, 2, 1, NULL ),
       ( 1, 0, 'emps', 0, 1, 2, 1, NULL ),
       ( 2, 0, 'test', 0, 1, 2, 1, NULL );

ALTER TABLE "table"
    ALTER COLUMN "id"
        RESTART WITH 3;


--
-- column
--
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default" )
VALUES ( 0, 0, 'deptno', 1, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 1, 0, 'name', 2, 9, 20, 0, FALSE, 1, 2, FALSE ),
       ( 2, 1, 'empid', 1, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 3, 1, 'deptno', 2, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 4, 1, 'name', 3, 9, 20, 0, FALSE, 1, 2, FALSE ),
       ( 5, 1, 'salary', 4, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 6, 1, 'commission', 5, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 8, 2, 'id', 1, 3, 11, 0, FALSE, 1, 2, FALSE ),
       ( 9, 2, 'name', 2, 9, 20, 0, FALSE, 1, 2, FALSE );

ALTER TABLE "column"
    ALTER COLUMN "id"
        RESTART WITH 10;


--
-- store
--
INSERT INTO "store" ( "id", "unique_name", "adapter" )
VALUES ( 0, 'hsqldb', 'ch.unibas.dmi.dbis.polyphenydb.adapter.hsqldb.HsqldbStore' ),
       ( 1, 'csv', 'ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvStore' );

ALTER TABLE "store"
    ALTER COLUMN "id"
        RESTART WITH 2;


--
-- data placement
--
INSERT INTO "data_placement" ( "store", "table" )
VALUES ( 0, 2 ),
       ( 1, 0 ),
       ( 1, 1 );


--
-- keys
--
INSERT INTO "key" ( "id", "table", "unique", "name" )
VALUES ( 0, 0, TRUE, 'key_0' ),
       ( 1, 1, TRUE, 'key_1' ),
       ( 2, 1, FALSE, 'key_2' ),
       ( 3, 1, TRUE, 'key_3' ),
       ( 4, 2, TRUE, 'key_4' );

ALTER TABLE "key"
    ALTER COLUMN "id"
        RESTART WITH 5;

UPDATE "table"
SET "primary_key" = 0
WHERE "id" = 0;


--
-- key columns
--
INSERT INTO "key_column" ( "key", "column" )
VALUES ( 0, 0 ),
       ( 1, 1 ),
       ( 2, 3 ),
       ( 3, 3 ),
       ( 3, 4 ),
       ( 4, 8 );


--
-- foreign_key
--
INSERT INTO "foreign_key" ( "key", "references", "on_update", "on_delete", "deferrability" )
VALUES ( 2, 0, NULL, NULL, 0 );


--
-- index
--
INSERT INTO "index" ( "id", "key", "type", "location", "name" )
VALUES ( 0, 0, 0, NULL, 'i_0_0' ),
       ( 1, 2, 0, NULL, 'i_2_0' ),
       ( 2, 3, 0, NULL, 'i_3_0' );

ALTER TABLE "key"
    ALTER COLUMN "id"
        RESTART WITH 2;
