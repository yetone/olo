CREATE TABLE `dummy` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `age` int(10) NOT NULL,
  `password` varchar(512) DEFAULT NULL,
  `flag` smallint(5) NOT NULL DEFAULT '0',
  `tags` text NOT NULL,
  `payload` text NOT NULL,
  `foo` int(10) DEFAULT NULL,
  `dynasty` varchar(4) DEFAULT NULL,
  `dynasty1` varchar(4) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  `created_date` date NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_name_age` (`name`, `age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `foo` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`name` varchar(255) NOT NULL,
`age` int(10) NOT NULL,
`age_str` int(10) NOT NULL,
`key` varchar(255) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `key` (`key`),
UNIQUE KEY `name-age` (`name`, `age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `__dummy` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`name` int(10) NOT NULL,
`age` int(10) NOT NULL,
`password` varchar(512) DEFAULT NULL,
`flag` smallint(5) NOT NULL DEFAULT '0',
`tags` text NOT NULL,
`payload` text NOT NULL,
`foo` int(10) DEFAULT NULL,
`dynasty` varchar(4) DEFAULT NULL,
`dynasty1` varchar(4) DEFAULT NULL,
`created_at` datetime NOT NULL,
`updated_at` datetime NOT NULL,
`created_date` date NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `bar` (
`name` varchar(255) NOT NULL,
`age` int(10) NOT NULL,
`key` varchar(255) NOT NULL,
`word` varchar(255),
PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `ttt` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`created_at` datetime NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `cool` (
`name` varchar(255) NOT NULL,
`age` int(10) NOT NULL,
`key` varchar(255) NOT NULL,
PRIMARY KEY (`name`, `age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `lala` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`name` varchar(255) NOT NULL,
`age` int(10) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `uk_name_age` (`name`, `age`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
