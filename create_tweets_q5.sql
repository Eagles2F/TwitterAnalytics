create table tweets_q5_1 (
  uid BIGINT NOT NULL, 
  counts VARCHAR(16) NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q5_1 add index (uid);

create table tweets_q5_2 (
  uid BIGINT NOT NULL, 
  counts VARCHAR(16) NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q5_2 add index (uid);

create table tweets_q5_3 (
  uid BIGINT NOT NULL, 
  counts VARCHAR(16) NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q5_3 add index (uid);

create table tweets_q5_4 (
  uid BIGINT NOT NULL, 
  counts VARCHAR(16) NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q5_4 add index (uid);
