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
  "id"                         BIGINT IDENTITY   NOT NULL,
  "table"                      BIGINT            NOT NULL,
  "name"                       VARCHAR(100)      NOT NULL,
  "position"                   INTEGER           NOT NULL,
  "type"                       INTEGER           NOT NULL,
  "length"                     INTEGER           NULL,
  "precision"                  INTEGER           NULL,
  "nullable"                   BOOLEAN           NOT NULL,
  "encoding"                   INTEGER           NOT NULL,
  "collation"                  INTEGER           NOT NULL,
  "force_default"              BOOLEAN        NOT NULL,
  PRIMARY KEY ("id")
);


CREATE TABLE "column_privilege" (
  "column"      BIGINT      NOT NULL,
  "user"        INTEGER     NOT NULL,
  "privilege"   INTEGER     NOT NULL,
  "grantable"   BOOLEAN     NOT NULL,
  PRIMARY KEY ("column", "user", "privilege")
);


CREATE TABLE "default_value" (
  "column"                      BIGINT         NOT NULL,
  "type"                        INTEGER        NOT NULL,
  "value"                       BLOB           NULL,
  "function_name"               VARCHAR(100)   NULL,
  "autoincrement_start_value"   BIGINT         NULL,
  "autoincrement_next_value"    BIGINT         NULL,
  PRIMARY KEY ("column")
);


CREATE TABLE "table" (
  "id"                 BIGINT IDENTITY   NOT NULL,
  "schema"             BIGINT            NOT NULL,
  "name"               VARCHAR(100)      NOT NULL,
  "owner"              INTEGER           NOT NULL,
  "encoding"           INTEGER           NULL,
  "collation"          INTEGER           NULL,
  "type"               INTEGER           NOT NULL,
  "definition"         VARCHAR(1000)     NULL,
  PRIMARY KEY ("id")
);


CREATE TABLE "table_privilege" (
  "table"      BIGINT    NOT NULL,
  "user"       INTEGER   NOT NULL,
  "privilege"  INTEGER   NOT NULL,
  "grantable"  BOOLEAN   NOT NULL,
  PRIMARY KEY ("table", "user", "privilege")
);


CREATE TABLE "schema" (
  "id"            BIGINT IDENTITY   NOT NULL,
  "database"      BIGINT            NOT NULL,
  "name"          VARCHAR(100)      NOT NULL,
  "owner"         INTEGER           NOT NULL,
  "encoding"      INTEGER           NULL,
  "collation"     INTEGER           NULL,
  "type"          INTEGER           NULL,
  PRIMARY KEY ("id")
);


CREATE TABLE "schema_privilege" (
  "schema"     BIGINT    NOT NULL,
  "user"       INTEGER   NOT NULL,
  "privilege"  INTEGER   NOT NULL,
  "grantable"  BOOLEAN   NOT NULL,
  PRIMARY KEY ("schema", "user", "privilege")
);


CREATE TABLE "database" (
  "id"                 BIGINT IDENTITY   NOT NULL,
  "name"               VARCHAR(100)      NOT NULL,
  "owner"              INTEGER           NOT NULL,
  "encoding"           INTEGER           NOT NULL,
  "collation"          INTEGER           NOT NULL,
  "connection_limit"   INTEGER           NOT NULL,
  PRIMARY KEY ("id")
);


CREATE TABLE "database_privilege" (
  "database"   BIGINT    NOT NULL,
  "user"       INTEGER   NOT NULL,
  "privilege"  INTEGER   NOT NULL,
  "grantable"  BOOLEAN   NOT NULL,
  PRIMARY KEY ("database", "user", "privilege")
);


CREATE TABLE "global_privilege" (
  "user"       INTEGER  NOT NULL,
  "privilege"  INTEGER  NOT NULL,
  "grantable"  BOOLEAN  NOT NULL,
  PRIMARY KEY ("user", "privilege")
);


CREATE TABLE "foreign_key" (
  "column"      BIGINT         NOT NULL,
  "references"  BIGINT         NOT NULL,
  "on_update"   INTEGER        NOT NULL,
  "on_delete"   INTEGER        NOT NULL,
  "database"    VARCHAR(100)   NOT NULL,
  "name"        VARCHAR(100)   NOT NULL,
  PRIMARY KEY ("column", "references")
);


CREATE TABLE "index" (
  "id"        BIGINT IDENTITY   NOT NULL,
  "type"      INTEGER           NOT NULL,
  "database"  VARCHAR(100)      NOT NULL,
  "name"      VARCHAR(100)      NOT NULL,
  PRIMARY KEY ("id")
);


CREATE TABLE "index_columns" (
  "index"    BIGINT    NOT NULL,
  "column"   BIGINT    NOT NULL,
  PRIMARY KEY ("index", "column")
);


CREATE TABLE "user" (
  "id"        INTEGER IDENTITY  NOT NULL,
  "username"  VARCHAR(100)      NOT NULL,
  "password"  VARCHAR(100)      NOT NULL,
  PRIMARY KEY ("id")
);


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

CREATE INDEX "foreign_key_column"
  ON "foreign_key" ("column");
CREATE INDEX "foreign_key_references"
  ON "foreign_key" ("references");
CREATE INDEX "foreign_key_database"
  ON "foreign_key" ("database");
CREATE INDEX "foreign_key_name"
  ON "foreign_key" ("name");
CREATE UNIQUE INDEX "foreign_key_database_name"
  ON "foreign_key" ("database", "name");

CREATE INDEX "index_database"
  ON "index" ("database");
CREATE INDEX "index_name"
  ON "index" ("name");
CREATE UNIQUE INDEX "index_database_name"
  ON "index" ("database", "name");

CREATE INDEX "index_columns_index"
  ON "index_columns" ("index");
CREATE INDEX "index_columns_column"
  ON "index_columns" ("column");

CREATE UNIQUE INDEX "user_username"
  ON "user" ("username");


-- ---------------------------------------------------------------

INSERT INTO "user" ("id", "username", "password") VALUES (0, 'system', '');
INSERT INTO "user" ("id", "username", "password") VALUES (1, 'pa', '');

ALTER TABLE "user"
  ALTER COLUMN "id"
  RESTART WITH 2;


INSERT INTO "database" ("id", "name", "owner", "encoding", "collation", "connection_limit") VALUES ( 0, 'APP', 0, 1, 2, 0 );

ALTER TABLE "database"
  ALTER COLUMN "id"
  RESTART WITH 1;


INSERT INTO "schema" ( "id", "database", "name", "owner", "encoding", "collation", "type") VALUES ( 0, 0, 'public', 0, 1, 2, 1 );
INSERT INTO "schema" ( "id", "database", "name", "owner", "encoding", "collation", "type") VALUES ( 1, 0, 'csv', 0, 1, 2, 1 );
INSERT INTO "schema" ( "id", "database", "name", "owner", "encoding", "collation", "type") VALUES ( 2, 0, 'hsqldb', 0, 1, 2, 1 );

ALTER TABLE "schema"
  ALTER COLUMN "id"
  RESTART WITH 3;


INSERT INTO "table" ( "id", "schema", "name", "owner", "encoding", "collation", "type", "definition" ) VALUES ( 0, 1, 'depts', 0, 1, 2, 1, '' );
INSERT INTO "table" ( "id", "schema", "name", "owner", "encoding", "collation", "type", "definition" ) VALUES ( 1, 1, 'emps', 0, 1, 2, 1, '' );
INSERT INTO "table" ( "id", "schema", "name", "owner", "encoding", "collation", "type", "definition" ) VALUES ( 2, 2, 'test', 0, 1, 2, 1, '' );

ALTER TABLE "schema"
  ALTER COLUMN "id"
  RESTART WITH 3;


INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 0, 0, 'deptno', 1, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 1, 0, 'name', 2, 9, 20, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 2, 1, 'empid', 1, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 3, 1, 'deptno', 2, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 4, 1, 'name', 3, 9, 20, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 5, 1, 'salary', 4, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 6, 1, 'commission', 5, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 8, 2, 'id', 1, 3, 11, 0, false, 1, 2, false);
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "precision", "nullable", "encoding", "collation", "force_default") VALUES ( 9, 2, 'name', 2, 9, 20, 0, false, 1, 2, false);

ALTER TABLE "column"
  ALTER COLUMN "id"
  RESTART WITH 10;