-- @formatter:off
CREATE TABLE `auction`
(
    `id`          int                            NOT NULL AUTO_INCREMENT,
    `title`       varchar(100) COLLATE utf8_bin  NOT NULL,
    `description` varchar(2500) COLLATE utf8_bin NOT NULL,
    `start_date`  datetime                       NOT NULL,
    `end_date`    datetime                       NOT NULL,
    `category`    int                            NOT NULL,
    `user`        int                            NOT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE `bid`
(
    `id`        int      NOT NULL AUTO_INCREMENT,
    `amount`    int      NOT NULL,
    `timestamp` datetime NOT NULL,
    `user`      int      NOT NULL,
    `auction`   int      NOT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE `category`
(
    `id`   int                           NOT NULL AUTO_INCREMENT,
    `name` varchar(100) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE `picture`
(
    `filename` varchar(50) COLLATE utf8_bin NOT NULL,
    `type`     varchar(20) COLLATE utf8_bin NOT NULL,
    `size`     int                          NOT NULL,
    `auction`  int                          NOT NULL,
    PRIMARY KEY (`filename`)
);

CREATE TABLE `user`
(
    `id`         int                           NOT NULL AUTO_INCREMENT,
    `email`      varchar(100) COLLATE utf8_bin NOT NULL,
    `password`   varchar(100) COLLATE utf8_bin NOT NULL,
    `last_name`  varchar(50) COLLATE utf8_bin  NOT NULL,
    `first_name` varchar(50) COLLATE utf8_bin  NOT NULL,
    `gender`     varchar(1) COLLATE utf8_bin   NOT NULL,
    `birthday`   date                          NOT NULL,
    `city`       varchar(50) COLLATE utf8_bin  NOT NULL,
    `zip_code`   varchar(20) COLLATE utf8_bin  NOT NULL,
    `country`    varchar(50) COLLATE utf8_bin  NOT NULL,
    PRIMARY KEY (`id`)
);

INSERT INTO `auction` VALUES (1, 'Atari 2600', 'The Atari 2600 is a home video game console developed and produced by Atari, Inc.', '2021-02-02 12:11:02', '2021-03-02 11:11:02', 1, 1);