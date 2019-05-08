CREATE TABLE IF NOT EXISTS ogre_importlog (
  `filename` VARCHAR(255) NOT NULL,
  `tablename` VARCHAR(255) NOT NULL,
  `timestamp` timestamp NOT NULL,
  PRIMARY KEY (filename)
);

CREATE TABLE IF NOT EXISTS ogre_ddllog (
  `filename` VARCHAR(255) NOT NULL,
  `sql` TEXT NOT NULL,
  PRIMARY KEY (filename)
);

CREATE TABLE IF NOT EXISTS ogre_columnmapping (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `tablename` VARCHAR(255) NOT NULL,
  `jsonpath` VARCHAR(255) NOT NULL,

  PRIMARY KEY (id)
);
