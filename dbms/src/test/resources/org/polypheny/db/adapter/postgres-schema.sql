CREATE TABLE "auction"
(
    "id"          SERIAL        NOT NULL,
    "title"       varchar(100)  NOT NULL,
    "description" varchar(2500) NOT NULL,
    "start_date"  timestamp(0)  NOT NULL,
    "end_date"    timestamp(0)  NOT NULL,
    "category"    int           NOT NULL,
    "user"        int           NOT NULL,
    PRIMARY KEY (id)
)
CREATE TABLE "bid"
(
    "id"        SERIAL       NOT NULL,
    "amount"    int          NOT NULL,
    "timestamp" timestamp(0) NOT NULL,
    "user"      int          NOT NULL,
    "auction"   int          NOT NULL,
    PRIMARY KEY (id)
)
CREATE TABLE "category"
(
    "id"   SERIAL       NOT NULL,
    "name" varchar(100) NOT NULL,
    PRIMARY KEY (id)
)
CREATE TABLE "picture"
(
    "filename" varchar(50) NOT NULL,
    "type"     varchar(20) NOT NULL,
    "size"     int         NOT NULL,
    "auction"  int         NOT NULL,
    PRIMARY KEY (filename)
)
CREATE TABLE "user"
(
    "id"         SERIAL       NOT NULL,
    "email"      varchar(100) NOT NULL,
    "password"   varchar(100) NOT NULL,
    "last_name"  varchar(50)  NOT NULL,
    "first_name" varchar(50)  NOT NULL,
    "gender"     varchar(1)   NOT NULL,
    "birthday"   date         NOT NULL,
    "city"       varchar(50)  NOT NULL,
    "zip_code"   varchar(20)  NOT NULL,
    "country"    varchar(50)  NOT NULL,
    PRIMARY KEY ("id")
) INSERT INTO "auction" VALUES ( 1, 'Atari 2600', 'The Atari 2600 is a home video game console developed and produced by Atari, Inc.', '2021-02-02 12:11:02', '2021-03-02 11:11:02', 1, 1);