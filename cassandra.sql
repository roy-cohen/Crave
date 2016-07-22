
CREATE KEYSPACE dishes_db WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
};

USE dishes_db;

DROP TABLE IF EXISTS businesses;
CREATE TABLE businesses (
  businessId TEXT,
  name TEXT,
  full_address TEXT,
  citystate TEXT,
  city TEXT,
  state TEXT,
  stars DOUBLE,
  reviewcount INT,
  PRIMARY KEY (businessid, citystate)
);


DROP TABLE IF EXISTS dishes;
CREATE TABLE dishes (
  citystate TEXT,
  dish TEXT,
  avgrating DOUBLE,
  businessId TEXT,
  numreviews INT,
  totalscore INT,
  promotext TEXT,
  promoscore INT,
  PRIMARY KEY (citystate, dish, businessid)
);
CREATE INDEX ON dishes (businessid);


/*
Use cases:

1. get best dishes in a city 
select * from dishes where citystate = 'MunhallPA';

2. get best dishes by dish
select * from dishes where citystate = 'MunhallPA' and dish = 'burger';

3. get dishes by restaurant 
select * from dishes where citystate = 'MunhallPA' and businessid = 'asdfg';

4. update/insert restaurant-dish combo with new review
update dishes set totalscore = 100 where businessid = 'asdfg' and dish = 'burger' if exists;
*/