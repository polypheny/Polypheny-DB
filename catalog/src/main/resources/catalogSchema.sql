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
    "default_schema"   BIGINT          NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "schema" (
    "id"        BIGINT IDENTITY NOT NULL,
    "database"  BIGINT          NOT NULL REFERENCES "database" ("id"),
    "name"      VARCHAR(100)    NOT NULL,
    "owner"     INTEGER         NOT NULL REFERENCES "user" ("id"),
    "type"      INTEGER         NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "table" (
    "id"          BIGINT IDENTITY            NOT NULL,
    "schema"      BIGINT                     NOT NULL REFERENCES "schema" ("id"),
    "name"        VARCHAR(100)               NOT NULL,
    "owner"       INTEGER                    NOT NULL REFERENCES "user" ("id"),
    "type"        INTEGER                    NOT NULL,
    "definition"  VARCHAR(1000) DEFAULT NULL NULL,
    "primary_key" BIGINT        DEFAULT NULL NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "column" (
    "id"        BIGINT IDENTITY NOT NULL,
    "table"     BIGINT          NOT NULL REFERENCES "table" ("id"),
    "name"      VARCHAR(100)    NOT NULL,
    "position"  INTEGER         NOT NULL,
    "type"      VARCHAR(50)     NOT NULL,
    "length"    INTEGER         NULL,
    "scale"     INTEGER         NULL,
    "nullable"  BOOLEAN         NOT NULL,
    "collation" INTEGER         NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "default_value" (
    "column"        BIGINT        NOT NULL REFERENCES "column" ("id"),
    "type"          VARCHAR(50)   NOT NULL,
    "value"         VARCHAR(1000) NULL,
    "function_name" VARCHAR(100)  NULL,
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
    "settings"    VARCHAR(5000)    NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "column_placement" (
    "store" INTEGER NOT NULL REFERENCES "store" ("id"),
    "column" BIGINT  NOT NULL REFERENCES "column" ("id"),
    "table" BIGINT  NOT NULL REFERENCES "table" ("id"),
    "type" INTEGER NOT NULL,
    "physical_schema"  VARCHAR(100)     NULL,
    "physical_table"  VARCHAR(100)     NULL,
    "physical_column"  VARCHAR(100)    NULL,
    PRIMARY KEY ("store", "column")
);


CREATE TABLE "key" (
    "id"     BIGINT IDENTITY NOT NULL,
    "table"  BIGINT          NOT NULL REFERENCES "table" ("id"),
    PRIMARY KEY ("id")
);


CREATE TABLE "key_column" (
    "key"    BIGINT  NOT NULL REFERENCES "key" ("id"),
    "column" BIGINT  NOT NULL REFERENCES "column" ("id"),
    "seq"    INTEGER NOT NULL,
    PRIMARY KEY ("key", "column")
);


CREATE TABLE "constraint" (
    "id"   BIGINT IDENTITY NOT NULL,
    "key"  BIGINT          NOT NULL REFERENCES "key" ("id"),
    "type" INTEGER         NOT NULL,
    "name" VARCHAR(100)    NOT NULL,
    PRIMARY KEY ("id")
);


CREATE TABLE "foreign_key" (
    "key"        BIGINT               NOT NULL REFERENCES "key" ("id"),
    "references" BIGINT               NOT NULL REFERENCES "key" ("id"),
    "on_update"  INTEGER DEFAULT NULL NULL,
    "on_delete"  INTEGER DEFAULT NULL NULL,
    "name"       VARCHAR(100)         NOT NULL,
    PRIMARY KEY ("key")
);


CREATE TABLE "index" (
    "id"       BIGINT IDENTITY NOT NULL,
    "key"      BIGINT          NOT NULL REFERENCES "key" ("id"),
    "type"     INTEGER         NOT NULL,
    "unique"   BOOLEAN         NOT NULL,
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

CREATE INDEX "key_column_key"
    ON "key_column" ("key");
CREATE UNIQUE INDEX "key_column_key_seq"
    ON "key_column" ("key", "seq");

CREATE INDEX "constraint_key"
    ON "constraint" ("key");
CREATE UNIQUE INDEX "constraint_name"
    ON "constraint" ("name");

CREATE INDEX "foreign_key_references"
    ON "foreign_key" ("references");
CREATE UNIQUE INDEX "foreign_key_name"
    ON "foreign_key" ("name");

CREATE INDEX "index_key"
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

CREATE INDEX "column_placement_store"
    ON "column_placement" ("store");
CREATE INDEX "column_placement_table"
    ON "column_placement" ("table");
CREATE INDEX "column_placement_column"
    ON "column_placement" ("column");

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
INSERT INTO "database" ( "id", "name", "owner", "default_schema" )
VALUES ( 0, 'APP', 0, NULL );

ALTER TABLE "database"
    ALTER COLUMN "id"
        RESTART WITH 1;


--
-- schema
--
INSERT INTO "schema" ( "id", "database", "name", "owner", "type" )
VALUES ( 0, 0, 'public', 0, 1 );

ALTER TABLE "schema"
    ALTER COLUMN "id"
        RESTART WITH 1;

UPDATE "database"
SET "default_schema" = 0
WHERE "id" = 0;


--
-- table
--
INSERT INTO "table" ( "id", "schema", "name", "owner", "type", "definition" )
VALUES ( 0, 0, 'depts', 0, 1, NULL ),
       ( 1, 0, 'emps', 0, 1, NULL ),
       ( 2, 0, 'test', 0, 1, NULL ),
       ( 3, 0, 'explore', 0, 1, NULL );

ALTER TABLE "table"
    ALTER COLUMN "id"
        RESTART WITH 4;


--
-- column
--
INSERT INTO "column" ( "id", "table", "name", "position", "type", "length", "scale", "nullable", "collation" )
VALUES ( 0, 0, 'deptno', 1, 'INTEGER', NULL, NULL, FALSE, NULL ),
       ( 1, 0, 'name', 2, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 2, 1, 'empid', 1, 'INTEGER', NULL, NULL, FALSE, NULL ),
       ( 3, 1, 'deptno', 2, 'INTEGER', NULL, NULL, FALSE, NULL ),
       ( 4, 1, 'name', 3, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 5, 1, 'salary', 4, 'INTEGER', NULL, NULL, FALSE, NULL ),
       ( 6, 1, 'commission', 5, 'INTEGER', NULL, NULL, FALSE, NULL ),
       ( 7, 2, 'country', 1, 'VARCHAR', 20, NULL , FALSE, 2 ),
       ( 8, 2, 'ttt', 2, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 9, 2, 'category', 3, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 10, 2, 'working', 4, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 11, 3, 'Age', 1, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 12, 3, 'Attrition', 2, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 13, 3, 'BusinessTravel', 3, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 14, 3, 'DailyRate', 4, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 15, 3, 'Department', 5, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 16, 3, 'DistanceFromHome', 6, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 17, 3, 'Education', 7, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 18, 3, 'EducationField', 8, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 19, 3, 'EmployeeCount', 9, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 20, 3, 'EmployeeNumber', 10, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 21, 3, 'EnvironmentSatisfaction', 11, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 22, 3, 'Gender', 12, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 23, 3, 'HourlyRate', 13, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 24, 3, 'JobInvolvement', 14, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 25, 3, 'JobLevel', 15, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 26, 3, 'JobRole', 16, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 27, 3, 'JobSatisfaction', 17, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 28, 3, 'MaritalStatus', 18, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 29, 3, 'MonthlyIncome', 19, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 30, 3, 'MonthlyRate', 20, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 31, 3, 'NumCompaniesWorked', 21, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 32, 3, 'Over18', 22, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 33, 3, 'OverTime', 23, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 34, 3, 'PercentSalaryHike', 24, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 35, 3, 'PerformanceRating', 25, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 36, 3, 'RelationshipSatisfaction', 26, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 37, 3, 'StandardHours', 27, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 38, 3, 'StockOptionLevel', 28, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 39, 3, 'TotalWorkingYears', 29, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 40, 3, 'TrainingTimesLastYear', 30, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 41, 3, 'WorkLifeBalance', 31, 'VARCHAR', 20, NULL, FALSE, 2 ),
       ( 42, 3, 'YearsAtCompany', 32, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 43, 3, 'YearsInCurrentRole', 33, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 44, 3, 'YearsSinceLastPromotion', 34, 'INTEGER', NULL, NULL, FALSE, NULL),
       ( 45, 3, 'YearsWithCurrManager', 35, 'INTEGER', NULL, NULL, FALSE, NULL);

ALTER TABLE "column"
    ALTER COLUMN "id"
        RESTART WITH 46;


--
-- store
--
INSERT INTO "store" ( "id", "unique_name", "adapter", "settings" )
VALUES ( 0, 'hsqldb', 'org.polypheny.db.adapter.jdbc.stores.HsqldbStore', '{"type": "Memory","path": "","maxConnections": "25","trxControlMode": "mvcc","trxIsolationLevel": "read_committed"}' ),
       ( 1, 'csv', 'org.polypheny.db.adapter.csv.CsvStore', '{"directory": "classpath://hr"}' );

ALTER TABLE "store"
    ALTER COLUMN "id"
        RESTART WITH 2;


--
-- column placement
--
INSERT INTO "column_placement" ( "store", "column", "table", "type", "physical_schema", "physical_table", "physical_column" )
VALUES ( 1, 0, 0, 2, null, 'depts', 'deptno' ),
    ( 1, 1, 0, 2, null, 'depts', 'name' ),
    ( 1, 2, 1, 2, null, 'emps', 'empid' ),
    ( 1, 3, 1, 2, null, 'emps', 'deptno' ),
    ( 1, 4, 1, 2, null, 'emps', 'name' ),
    ( 1, 5, 1, 2, null, 'emps', 'salary' ),
    ( 1, 6, 1, 2, null, 'emps', 'commission' ),
    ( 1, 7, 2, 2, null, 'test', 'country' ),
    ( 1, 8, 2, 2, null, 'test', 'ttt' ),
    ( 1, 9, 2, 2, null, 'test', 'category' ),
    ( 1, 10, 2, 2, null, 'test', 'working' ),
    ( 1, 11, 3, 2, null, 'explore', 'Age'),
    ( 1, 12, 3, 2, null, 'explore', 'Attrition' ),
    ( 1, 13, 3, 2, null, 'explore', 'BusinessTravel'),
    ( 1, 14, 3, 2, null, 'explore', 'DailyRate'),
    ( 1, 15, 3, 2, null, 'explore', 'Department' ),
    ( 1, 16, 3, 2, null, 'explore', 'DistanceFromHome'),
    ( 1, 17, 3, 2, null, 'explore', 'Education'),
    ( 1, 18, 3, 2, null, 'explore', 'EducationField'),
    ( 1, 19, 3, 2, null, 'explore', 'EmployeeCount'),
    ( 1, 20, 3, 2, null, 'explore', 'EmployeeNumber'),
    ( 1, 21, 3, 2, null, 'explore', 'EnvironmentSatisfaction'),
    ( 1, 22, 3, 2, null, 'explore', 'Gender'),
    ( 1, 23, 3, 2, null, 'explore', 'HourlyRate'),
    ( 1, 24, 3, 2, null, 'explore', 'JobInvolvement'),
    ( 1, 25, 3, 2, null, 'explore', 'JobLevel'),
    ( 1, 26, 3, 2, null, 'explore', 'JobRole'),
    ( 1, 27, 3, 2, null, 'explore', 'JobSatisfaction'),
    ( 1, 28, 3, 2, null, 'explore', 'MaritalStatus'),
    ( 1, 29, 3, 2, null, 'explore', 'MonthlyIncome'),
    ( 1, 30, 3, 2, null, 'explore', 'MonthlyRate'),
    ( 1, 31, 3, 2, null, 'explore', 'NumCompaniesWorked'),
    ( 1, 32, 3, 2, null, 'explore', 'Over18' ),
    ( 1, 33, 3, 2, null, 'explore', 'OverTime' ),
    ( 1, 34, 3, 2, null, 'explore', 'PercentSalaryHike'),
    ( 1, 35, 3, 2, null, 'explore', 'PerformanceRating'),
    ( 1, 36, 3, 2, null, 'explore', 'RelationshipSatisfaction' ),
    ( 1, 37, 3, 2, null, 'explore', 'StandardHours'),
    ( 1, 38, 3, 2, null, 'explore', 'StockOptionLevel'),
    ( 1, 39, 3, 2, null, 'explore', 'TotalWorkingYears'),
    ( 1, 40, 3, 2, null, 'explore', 'TrainingTimesLastYear'),
    ( 1, 41, 3, 2, null, 'explore', 'WorkLifeBalance'),
    ( 1, 42, 3, 2, null, 'explore', 'YearsAtCompany'),
    ( 1, 43, 3, 2, null, 'explore', 'YearsInCurrentRole'),
    ( 1, 44, 3, 2, null, 'explore', 'YearsSinceLastPromotion'),
    ( 1, 45, 3, 2, null, 'explore', 'YearsWithCurrManager');
