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


CREATE TABLE "column" (
  "internal_name"             VARCHAR(10)  NOT NULL,
  "internal_name_part"        INTEGER      NOT NULL,
  "table"                     VARCHAR(10)  NOT NULL,
  "name"                      VARCHAR(100) NOT NULL,
  "position"                  INTEGER      NOT NULL,
  "type"                      INTEGER      NOT NULL,
  "length"                    INTEGER      NULL,
  "precision"                 INTEGER      NULL,
  "nullable"                  BOOLEAN      NOT NULL,
  "encoding"                  INTEGER      NOT NULL,
  "collation"                 INTEGER      NOT NULL,
  "autoincrement_start_value" BIGINT       NULL,
  "autoincrement_next_value"  BIGINT       NULL,
  "default_value"             BLOB         NULL,
  "force_default"             BOOLEAN      NOT NULL,
  PRIMARY KEY ("internal_name")
);


CREATE TABLE "column_privilege" (
  "user"      INTEGER     NOT NULL,
  "column"    VARCHAR(10) NOT NULL,
  "privilege" INTEGER     NOT NULL,
  "grantable" BOOLEAN     NOT NULL,
  PRIMARY KEY ("user", "column", "privilege")
);


CREATE TABLE "database" (
  "internal_name"      VARCHAR(10)  NOT NULL,
  "internal_name_part" INTEGER      NOT NULL,
  "name"               VARCHAR(100) NOT NULL,
  "owner"              INTEGER      NOT NULL,
  "encoding"           INTEGER      NOT NULL,
  "collation"          INTEGER      NOT NULL,
  "connection_limit"   INTEGER      NOT NULL,
  PRIMARY KEY ("internal_name")
);


CREATE TABLE "database_privilege" (
  "user"      INTEGER     NOT NULL,
  "database"  VARCHAR(10) NOT NULL,
  "privilege" INTEGER     NOT NULL,
  "grantable" BOOLEAN     NOT NULL,
  PRIMARY KEY ("user", "database", "privilege")
);


CREATE TABLE "global_privilege" (
  "user"      INTEGER NOT NULL,
  "privilege" INTEGER NOT NULL,
  "grantable" BOOLEAN NOT NULL,
  PRIMARY KEY ("user", "privilege")
);


CREATE TABLE "schema" (
  "internal_name"      VARCHAR(10)  NOT NULL,
  "internal_name_part" INTEGER      NOT NULL,
  "database"           VARCHAR(10)  NOT NULL,
  "name"               VARCHAR(100) NOT NULL,
  "owner"              INTEGER      NOT NULL,
  "encoding"           INTEGER      NULL,
  "collation"          INTEGER      NULL,
  PRIMARY KEY ("internal_name")
);


CREATE TABLE "schema_privilege" (
  "user"      INTEGER     NOT NULL,
  "schema"    VARCHAR(10) NOT NULL,
  "privilege" INTEGER     NOT NULL,
  "grantable" BOOLEAN     NOT NULL,
  PRIMARY KEY ("user", "schema", "privilege")
);


CREATE TABLE "table" (
  "internal_name"      VARCHAR(10)   NOT NULL,
  "internal_name_part" INTEGER       NOT NULL,
  "schema"             VARCHAR(10)   NOT NULL,
  "name"               VARCHAR(100)  NOT NULL,
  "owner"              INTEGER       NOT NULL,
  "encoding"           INTEGER       NULL,
  "collation"          INTEGER       NULL,
  "type"               INTEGER       NOT NULL,
  "definition"         VARCHAR(1000) NULL,
  "chunk_column"       VARCHAR(10)   NULL,
  "chunk_size"         INTEGER       NULL,
  PRIMARY KEY ("internal_name")
);


CREATE TABLE "table_privilege" (
  "user"      INTEGER     NOT NULL,
  "table"     VARCHAR(10) NOT NULL,
  "privilege" INTEGER     NOT NULL,
  "grantable" BOOLEAN     NOT NULL,
  PRIMARY KEY ("user", "table", "privilege")
);


CREATE TABLE "user" (
  "id"       INTEGER IDENTITY NOT NULL,
  "username" VARCHAR(100)     NOT NULL,
  "password" VARCHAR(100)     NOT NULL,
  PRIMARY KEY ("id")
);

-- -------------------------------------------------------------------


CREATE INDEX "table_name"
  ON "table" ("name");
CREATE UNIQUE INDEX "table_internal_name_part_schema"
  ON "table" ("internal_name_part", "schema");
CREATE UNIQUE INDEX "table_schema_name"
  ON "table" ("schema", "name");

CREATE INDEX "column_name"
  ON "column" ("name");
CREATE UNIQUE INDEX "column_table_name"
  ON "column" ("table", "name");
CREATE UNIQUE INDEX "column_internal_name_part_table"
  ON "column" ("internal_name_part", "table");

CREATE INDEX "column_privilege_user"
  ON "column_privilege" ("user");
CREATE INDEX "column_privilege_column"
  ON "column_privilege" ("column");
CREATE INDEX "column_privilege_privilege"
  ON "column_privilege" ("privilege");
CREATE INDEX "column_privilege_user_column"
  ON "column_privilege" ("user", "column");

CREATE UNIQUE INDEX "database_name"
  ON "database" ("name");
CREATE UNIQUE INDEX "database_internal_name_part"
  ON "database" ("internal_name_part");

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
CREATE UNIQUE INDEX "schema_internal_name_part_database"
  ON "schema" ("internal_name_part", "database");

CREATE INDEX "schema_privilege_user"
  ON "schema_privilege" ("user");
CREATE INDEX "schema_privilege_schema"
  ON "schema_privilege" ("schema");
CREATE INDEX "schema_privilege_privilege"
  ON "schema_privilege" ("privilege");
CREATE INDEX "schema_privilege_user_schema"
  ON "schema_privilege" ("user", "schema");

CREATE INDEX "table_privilege_user"
  ON "table_privilege" ("user");
CREATE INDEX "table_privilege_table"
  ON "table_privilege" ("table");
CREATE INDEX "table_privilege_privilege"
  ON "table_privilege" ("privilege");
CREATE INDEX "table_privilege_user_table"
  ON "table_privilege" ("user", "table");

-- ---------------------------------------------------------------

INSERT INTO "user" ("id", "username", "password") VALUES (0, 'system', '');
INSERT INTO "user" ("id", "username", "password") VALUES (1, 'pa', '');

ALTER TABLE "user"
  ALTER COLUMN "id"
  RESTART WITH 2;

-- ---------------------------------------------------------------
-- Internal stuff
--INSERT INTO "database" ("internal_name", "internal_name_part", "name", "owner", "encoding", "collation", "connection_limit") VALUES ('aa', 1, 'system', 0, 1, 1, 0);
--INSERT INTO "schema" ("internal_name", "internal_name_part", "database", "name", "owner", "encoding", "collation") VALUES ('aaaa', 1, 'aa', 'internal', 0, 1, 1);
--INSERT INTO "table" ("internal_name", "internal_name_part", "schema", "name", "owner", "encoding", "collation", "type", "definition") VALUES ('aaaaaaa', 1, 'aaaa', 'dbstates', 0, 1, 1, 1, NULL);
--INSERT INTO "column" ("internal_name", "internal_name_part", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "autoincrement_start_value", "autoincrement_next_value", "default_value", "force_default") VALUES
--  ('aaaaaaaaaa', 1, 'aaaaaaa', 'key', 1, 13, NULL, NULL, FALSE, 1, 1, NULL, NULL, NULL, FALSE),
--  ('aaaaaaaaab', 2, 'aaaaaaa', 'value', 2, 13, NULL, NULL, FALSE, 1, 1, NULL, NULL, NULL, FALSE);

-- Standard user, database and schema
INSERT INTO "user" ("username", "password") VALUES ('pa', '');
INSERT INTO "database" ("internal_name", "internal_name_part", "name", "owner", "encoding", "collation", "connection_limit") VALUES ('aa', 2, 'app', 1, 1, 1, 0);
INSERT INTO "schema" ("internal_name", "internal_name_part", "database", "name", "owner", "encoding", "collation") VALUES ('aaaa', 1, 'aa', 'public', 1, 1, 1);

-- ----------------------------------------------------------------
-- For testing

