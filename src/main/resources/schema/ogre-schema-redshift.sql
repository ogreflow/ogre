CREATE TABLE IF NOT EXISTS ogre_importlog (
  filename varchar not null encode lzo,
  tablename varchar not null encode bytedict,
  timestamp timestamp not null encode lzo,
  PRIMARY KEY (filename))
  SORTKEY (timestamp);

CREATE TABLE IF NOT EXISTS ogre_ddllog (
  filename varchar not null encode lzo,
  sql varchar(60000) not null encode lzo,
  PRIMARY KEY (filename));

CREATE TABLE IF NOT EXISTS ogre_columnmapping (
  id int IDENTITY(0,1) encode delta,
  tablename varchar not null encode bytedict,
  jsonpath varchar not null encode lzo,

  PRIMARY KEY (id))
  SORTKEY (tablename,id);
