create table tweets_q3 (
  tid BIGINT NOT NULL, 
  uid BIGINT NOT NULL, 
  time BIGINT NOT NULL, 
  score INT NOT NULL, 
  text VARCHAR(300) NOT NULL DEFAULT ''
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q3 add index (uid, time, score);

