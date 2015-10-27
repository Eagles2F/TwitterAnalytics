create table tweets(
  tweetId CHAR(20) NOT NULL, 
  userId CHAR(70) NOT NULL, 
  time CHAR(20) NOT NULL, 
  text CHAR(145) NOT NULL DEFAULT '', 
  score int NOT NULL) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets add index (userId), add index (time);

