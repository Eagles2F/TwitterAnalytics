create table tweets(
  tid BIGINT NOT NULL, 
  uid BIGINT NOT NULL, 
  time BIGINT NOT NULL, 
  text VARCHAR(300) NOT NULL DEFAULT '', 
  score INT NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets add index (uid, time);

