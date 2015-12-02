create table tweets_q4_1 (
  tag VARCHAR(256) NOT NULL, 
  content TEXT NOT NULL
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q4_1 add index (tag);

create table tweets_q4_2 (
  tag VARCHAR(256) NOT NULL, 
  content TEXT NOT NULL 
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q4_2 add index (tag);

create table tweets_q4_3 (
  tag VARCHAR(256) NOT NULL, 
  content TEXT NOT NULL 
) ENGINE=MyISAM DEFAULT CHARACTER SET=utf8mb4;

alter table tweets_q4_3 add index (tag);
