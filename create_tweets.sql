create table tweets(
  tweetId CHAR(20) NOT NULL, 
  idtime CHAR(90) NOT NULL, 
  text CHAR(145) NOT NULL DEFAULT '', 
  score int NOT NULL) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets add index (idtime);

