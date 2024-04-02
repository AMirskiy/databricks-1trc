-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Catalog and Schemas

-- COMMAND ----------

create catalog if not exists amirskiy;
use catalog amirskiy;

-- COMMAND ----------

create schema if not exists 1brc;
create schema if not exists 1trc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Generate Stations table

-- COMMAND ----------

-- DBTITLE 1,Stations Data Creation Using values()
/* Create stations data using values() */
create or replace table `1trc`.`stations`
using delta
as
select id, station, mean_temp
from values (1,'Abha',18.0), (2,'Abidjan',26.0), (3,'Abéché',29.4), (4,'Accra',26.4), (5,'Addis Ababa',16.0), (6,'Adelaide',17.3), (7,'Aden',29.1), (8,'Ahvaz',25.4), (9,'Albuquerque',14.0), (10,'Alexandra',11.0), (11,'Alexandria',20.0), (12,'Algiers',18.2), (13,'Alice Springs',21.0), (14,'Almaty',10.0), (15,'Amsterdam',10.2), (16,'Anadyr	',6.9), (17,'Anchorage',2.8), (18,'Andorra la Vella',9.8), (19,'Ankara',12.0), (20,'Antananarivo',17.9), (21,'Antsiranana',25.2), (22,'Arkhangelsk',1.3), (23,'Ashgabat',17.1), (24,'Asmara',15.6), (25,'Assab',30.5), (26,'Astana',3.5), (27,'Athens',19.2), (28,'Atlanta',17.0), (29,'Auckland',15.2), (30,'Austin',20.7), (31,'Baghdad',22.77), (32,'Baguio',19.5), (33,'Baku',15.1), (34,'Baltimore',13.1), (35,'Bamako',27.8), (36,'Bangkok',28.6), (37,'Bangui',26.0), (38,'Banjul',26.0), (39,'Barcelona',18.2), (40,'Bata',25.1), (41,'Batumi',14.0), (42,'Beijing',12.9), (43,'Beirut',20.9), (44,'Belgrade',12.5), (45,'Belize City',26.7), (46,'Benghazi',19.9), (47,'Bergen',7.7), (48,'Berlin',10.3), (49,'Bilbao',14.7), (50,'Birao',26.5), (51,'Bishkek',11.3), (52,'Bissau',27.0), (53,'Blantyre',22.2), (54,'Bloemfontein',15.6), (55,'Boise',11.4), (56,'Bordeaux',14.2), (57,'Bosaso',30.0), (58,'Boston',10.9), (59,'Bouaké',26.0), (60,'Bratislava',10.5), (61,'Brazzaville',25.0), (62,'Bridgetown',27.0), (63,'Brisbane',21.4), (64,'Brussels',10.5), (65,'Bucharest',10.8), (66,'Budapest',11.3), (67,'Bujumbura',23.8), (68,'Bulawayo',18.9), (69,'Burnie',13.1), (70,'Busan',15.0), (71,'Cabo San Lucas',23.9), (72,'Cairns',25.0), (73,'Cairo',21.4), (74,'Calgary',4.4), (75,'Canberra',13.1), (76,'Cape Town',16.2), (77,'Changsha',17.4), (78,'Charlotte',16.1), (79,'Chiang Mai',25.8), (80,'Chicago',9.8), (81,'Chihuahua',18.6), (82,'Chișinău',10.2), (83,'Chittagong',25.9), (84,'Chongqing',18.6), (85,'Christchurch',12.2), (86,'City of San Marino',11.8), (87,'Colombo',27.4), (88,'Columbus',11.7), (89,'Conakry',26.4), (90,'Copenhagen',9.1), (91,'Cotonou',27.2), (92,'Cracow',9.3), (93,'Da Lat',17.9), (94,'Da Nang',25.8), (95,'Dakar',24.0), (96,'Dallas',19.0), (97,'Damascus',17.0), (98,'Dampier',26.4), (99,'Dar es Salaam',25.8), (100,'Darwin',27.6), (101,'Denpasar',23.7), (102,'Denver',10.4), (103,'Detroit',10.0), (104,'Dhaka',25.9), (105,'Dikson	',11.1), (106,'Dili',26.6), (107,'Djibouti',29.9), (108,'Dodoma',22.7), (109,'Dolisie',24.0), (110,'Douala',26.7), (111,'Dubai',26.9), (112,'Dublin',9.8), (113,'Dunedin',11.1), (114,'Durban',20.6), (115,'Dushanbe',14.7), (116,'Edinburgh',9.3), (117,'Edmonton',4.2), (118,'El Paso',18.1), (119,'Entebbe',21.0), (120,'Erbil',19.5), (121,'Erzurum',5.1), (122,'Fairbanks	',2.3), (123,'Fianarantsoa',17.9), (124,'Flores,  Petén',26.4), (125,'Frankfurt',10.6), (126,'Fresno',17.9), (127,'Fukuoka',17.0), (128,'Gabès',19.5), (129,'Gaborone',21.0), (130,'Gagnoa',26.0), (131,'Gangtok',15.2), (132,'Garissa',29.3), (133,'Garoua',28.3), (134,'George Town',27.9), (135,'Ghanzi',21.4), (136,'Gjoa Haven	',14.4), (137,'Guadalajara',20.9), (138,'Guangzhou',22.4), (139,'Guatemala City',20.4), (140,'Halifax',7.5), (141,'Hamburg',9.7), (142,'Hamilton',13.8), (143,'Hanga Roa',20.5), (144,'Hanoi',23.6), (145,'Harare',18.4), (146,'Harbin',5.0), (147,'Hargeisa',21.7), (148,'Hat Yai',27.0), (149,'Havana',25.2), (150,'Helsinki',5.9), (151,'Heraklion',18.9), (152,'Hiroshima',16.3), (153,'Ho Chi Minh City',27.4), (154,'Hobart',12.7), (155,'Hong Kong',23.3), (156,'Honiara',26.5), (157,'Honolulu',25.4), (158,'Houston',20.8), (159,'Ifrane',11.4), (160,'Indianapolis',11.8), (161,'Iqaluit	',9.3), (162,'Irkutsk',1.0), (163,'Istanbul',13.9), (164,'İzmir',17.9), (165,'Jacksonville',20.3), (166,'Jakarta',26.7), (167,'Jayapura',27.0), (168,'Jerusalem',18.3), (169,'Johannesburg',15.5), (170,'Jos',22.8), (171,'Juba',27.8), (172,'Kabul',12.1), (173,'Kampala',20.0), (174,'Kandi',27.7), (175,'Kankan',26.5), (176,'Kano',26.4), (177,'Kansas City',12.5), (178,'Karachi',26.0), (179,'Karonga',24.4), (180,'Kathmandu',18.3), (181,'Khartoum',29.9), (182,'Kingston',27.4), (183,'Kinshasa',25.3), (184,'Kolkata',26.7), (185,'Kuala Lumpur',27.3), (186,'Kumasi',26.0), (187,'Kunming',15.7), (188,'Kuopio',3.4), (189,'Kuwait City',25.7), (190,'Kyiv',8.4), (191,'Kyoto',15.8), (192,'La Ceiba',26.2), (193,'La Paz',23.7), (194,'Lagos',26.8), (195,'Lahore',24.3), (196,'Lake Havasu City',23.7), (197,'Lake Tekapo',8.7), (198,'Las Palmas de Gran Canaria',21.2), (199,'Las Vegas',20.3), (200,'Launceston',13.1), (201,'Lhasa',7.6), (202,'Libreville',25.9), (203,'Lisbon',17.5), (204,'Livingstone',21.8), (205,'Ljubljana',10.9), (206,'Lodwar',29.3), (207,'Lomé',26.9), (208,'London',11.3), (209,'Los Angeles',18.6), (210,'Louisville',13.9), (211,'Luanda',25.8), (212,'Lubumbashi',20.8), (213,'Lusaka',19.9), (214,'Luxembourg City',9.3), (215,'Lviv',7.8), (216,'Lyon',12.5), (217,'Madrid',15.0), (218,'Mahajanga',26.3), (219,'Makassar',26.7), (220,'Makurdi',26.0), (221,'Malabo',26.3), (222,'Malé',28.0), (223,'Managua',27.3), (224,'Manama',26.5), (225,'Mandalay',28.0), (226,'Mango',28.1), (227,'Manila',28.4), (228,'Maputo',22.8), (229,'Marrakesh',19.6), (230,'Marseille',15.8), (231,'Maun',22.4), (232,'Medan',26.5), (233,'Mek`ele',22.7), (234,'Melbourne',15.1), (235,'Memphis',17.2), (236,'Mexicali',23.1), (237,'Mexico City',17.5), (238,'Miami',24.9), (239,'Milan',13.0), (240,'Milwaukee',8.9), (241,'Minneapolis',7.8), (242,'Minsk',6.7), (243,'Mogadishu',27.1), (244,'Mombasa',26.3), (245,'Monaco',16.4), (246,'Moncton',6.1), (247,'Monterrey',22.3), (248,'Montreal',6.8), (249,'Moscow',5.8), (250,'Mumbai',27.1), (251,'Murmansk',0.6), (252,'Muscat',28.0), (253,'Mzuzu',17.7), (254,'N`Djamena',28.3), (255,'Naha',23.1), (256,'Nairobi',17.8), (257,'Nakhon Ratchasima',27.3), (258,'Napier',14.6), (259,'Napoli',15.9), (260,'Nashville',15.4), (261,'Nassau',24.6), (262,'Ndola',20.3), (263,'New Delhi',25.0), (264,'New Orleans',20.7), (265,'New York City',12.9), (266,'Ngaoundéré',22.0), (267,'Niamey',29.3), (268,'Nicosia',19.7), (269,'Niigata',13.9), (270,'Nouadhibou',21.3), (271,'Nouakchott',25.7), (272,'Novosibirsk',1.7), (273,'Nuuk	',1.4), (274,'Odesa',10.7), (275,'Odienné',26.0), (276,'Oklahoma City',15.9), (277,'Omaha',10.6), (278,'Oranjestad',28.1), (279,'Oslo',5.7), (280,'Ottawa',6.6), (281,'Ouagadougou',28.3), (282,'Ouahigouya',28.6), (283,'Ouarzazate',18.9), (284,'Oulu',2.7), (285,'Palembang',27.3), (286,'Palermo',18.5), (287,'Palm Springs',24.5), (288,'Palmerston North',13.2), (289,'Panama City',28.0), (290,'Parakou',26.8), (291,'Paris',12.3), (292,'Perth',18.7), (293,'Petropavlovsk-Kamchatsky',1.9), (294,'Philadelphia',13.2), (295,'Phnom Penh',28.3), (296,'Phoenix',23.9), (297,'Pittsburgh',10.8), (298,'Podgorica',15.3), (299,'Pointe-Noire',26.1), (300,'Pontianak',27.7), (301,'Port Moresby',26.9), (302,'Port Sudan',28.4), (303,'Port Vila',24.3), (304,'Port-Gentil',26.0), (305,'Portland (OR)',12.4), (306,'Porto',15.7), (307,'Prague',8.4), (308,'Praia',24.4), (309,'Pretoria',18.2), (310,'Pyongyang',10.8), (311,'Rabat',17.2), (312,'Rangpur',24.4), (313,'Reggane',28.3), (314,'Reykjavík',4.3), (315,'Riga',6.2), (316,'Riyadh',26.0), (317,'Rome',15.2), (318,'Roseau',26.2), (319,'Rostov-on-Don',9.9), (320,'Sacramento',16.3), (321,'Saint Petersburg',5.8), (322,'Saint-Pierre',5.7), (323,'Salt Lake City',11.6), (324,'San Antonio',20.8), (325,'San Diego',17.8), (326,'San Francisco',14.6), (327,'San Jose',16.4), (328,'San José',22.6), (329,'San Juan',27.2), (330,'San Salvador',23.1), (331,'Sana`a',20.0), (332,'Santo Domingo',25.9), (333,'Sapporo',8.9), (334,'Sarajevo',10.1), (335,'Saskatoon',3.3), (336,'Seattle',11.3), (337,'Ségou',28.0), (338,'Seoul',12.5), (339,'Seville',19.2), (340,'Shanghai',16.7), (341,'Singapore',27.0), (342,'Skopje',12.4), (343,'Sochi',14.2), (344,'Sofia',10.6), (345,'Sokoto',28.0), (346,'Split',16.1), (347,'St. John`s',5.0), (348,'St. Louis',13.9), (349,'Stockholm',6.6), (350,'Surabaya',27.1), (351,'Suva',25.6), (352,'Suwałki',7.2), (353,'Sydney',17.7), (354,'Tabora',23.0), (355,'Tabriz',12.6), (356,'Taipei',23.0), (357,'Tallinn',6.4), (358,'Tamale',27.9), (359,'Tamanrasset',21.7), (360,'Tampa',22.9), (361,'Tashkent',14.8), (362,'Tauranga',14.8), (363,'Tbilisi',12.9), (364,'Tegucigalpa',21.7), (365,'Tehran',17.0), (366,'Tel Aviv',20.0), (367,'Thessaloniki',16.0), (368,'Thiès',24.0), (369,'Tijuana',17.8), (370,'Timbuktu',28.0), (371,'Tirana',15.2), (372,'Toamasina',23.4), (373,'Tokyo',15.4), (374,'Toliara',24.1), (375,'Toluca',12.4), (376,'Toronto',9.4), (377,'Tripoli',20.0), (378,'Tromsø',2.9), (379,'Tucson',20.9), (380,'Tunis',18.4), (381,'Ulaanbaatar	',0.4), (382,'Upington',20.4), (383,'Ürümqi',7.4), (384,'Vaduz',10.1), (385,'Valencia',18.3), (386,'Valletta',18.8), (387,'Vancouver',10.4), (388,'Veracruz',25.4), (389,'Vienna',10.4), (390,'Vientiane',25.9), (391,'Villahermosa',27.1), (392,'Vilnius',6.0), (393,'Virginia Beach',15.8), (394,'Vladivostok',4.9), (395,'Warsaw',8.5), (396,'Washington, D.C.',14.6), (397,'Wau',27.8), (398,'Wellington',12.9), (399,'Whitehorse	',0.1), (400,'Wichita',13.9), (401,'Willemstad',28.0), (402,'Winnipeg',3.0), (403,'Wrocław',9.6), (404,'Xi`an',14.1), (405,'Yakutsk	',8.8), (406,'Yangon',27.5), (407,'Yaoundé',23.8), (408,'Yellowknife	',4.3), (409,'Yerevan',12.4), (410,'Yinchuan',9.0), (411,'Zagreb',10.7), (412,'Zanzibar City',26.0), (413,'Zürich',9.3)
 AS data(id,station,mean_temp);

-- COMMAND ----------

-- DBTITLE 1,Stations Data Creation Using CTAS
create or replace table `1brc`.`stations`
using delta
as
select * from `1trc`.`stations`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Generate 1 Billion Row Challenge table (1BRC)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1BRC - non-partitioned table

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Billion Normal Distribution Samples
/* Generate new samples with a normal distribution using Phil Factors's classic solution:
   https://www.red-gate.com/simple-talk/blogs/getting-normally-distributed-random-numbers-in-tsql/  */
create or replace table `1brc`.`measurements_delta`
as
select a.station,
      round(((rand() * 2 - 1) + (rand() * 2 - 1) + (rand() * 2 - 1)) /* Box-Muller transform */
       * 5 /* Standard Deviation */
       + a.mean_temp, 1) as measure
from `1trc`.`stations` as a
join (range(1000000000)) b on a.id = (mod(b.id, 412)+1)
;

-- COMMAND ----------

-- DBTITLE 1,Describe Table History
describe history `1brc`.`measurements_delta`;
--{"numFiles": "256", "numOutputRows": "1000000000", "numOutputBytes": "1379976408"}

-- COMMAND ----------

-- DBTITLE 1,Vacuum Delta Table - Retain No History
set spark.databricks.delta.retentionDurationCheck.enabled = false;
vacuum `1brc`.`measurements_delta` RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1BRC - partitioned table
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Billion Normal Distribution Samples - CTAS
create or replace table `1brc`.`measurements_delta_partitioned`
partitioned by (station)
as
select station, measure
from `1brc`.`measurements_delta`
;

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Billion Normal Distribution Samples
/* Generate new samples with a normal distribution using Phil Factors's classic solution:
   https://www.red-gate.com/simple-talk/blogs/getting-normally-distributed-random-numbers-in-tsql/  */
/*
create or replace table `1brc`.`measurements_delta_partitioned`
partitioned by (station)
as
select a.station,
      round(((rand() * 2 - 1) + (rand() * 2 - 1) + (rand() * 2 - 1)) /* Box-Muller transform */
       * 5 /* Standard Deviation */
       + a.mean_temp, 1) as measure
from `1trc`.`stations` as a
join (range(1000000000)) b on a.id = (mod(b.id, 412)+1)
;
*/

-- COMMAND ----------

-- DBTITLE 1,Describe Table History
describe history `1brc`.`measurements_delta_partitioned`;
--{"numFiles": "105472", "numOutputRows": "1000000000", "numOutputBytes": "1300546415"}

-- COMMAND ----------

-- DBTITLE 1,Vacuum Delta Table - Retain No History
set spark.databricks.delta.retentionDurationCheck.enabled = false;
vacuum `1brc`.`measurements_delta_partitioned` RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Non-partitioned Table

-- COMMAND ----------

-- DBTITLE 1,Run 1BRC Query - cold run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- DBTITLE 1,Run 1BRC Query - warm run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Partitioned Table

-- COMMAND ----------

-- DBTITLE 1,Run 1BRC Query - cold run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- DBTITLE 1,Run 1BRC Query - warm run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1brc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Generate 1 Trillion Row Challenge table (1TRC)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1TRC - non-partitioned table

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Trillion Normal Distribution Samples
/* Generate new samples with a normal distribution using Phil Factors's classic solution:
   https://www.red-gate.com/simple-talk/blogs/getting-normally-distributed-random-numbers-in-tsql/  */
create or replace table `1trc`.`measurements_delta`
as
select a.station,
      round(((rand() * 2 - 1) + (rand() * 2 - 1) + (rand() * 2 - 1)) /* Box-Muller transform */
       * 5 /* Standard Deviation */
       + a.mean_temp, 1) as measure
from `1trc`.`stations` as a
join (range(1000000000000)) b on a.id = (mod(b.id, 412)+1)
;

-- COMMAND ----------

-- DBTITLE 1,Describe Table History
describe history `1trc`.`measurements_delta`;
-- {"numFiles": "256", "numOutputRows": "1000000000000", "numOutputBytes": "1378906028507"}

-- COMMAND ----------

-- DBTITLE 1,Vacuum Delta Table - Retain No History
set spark.databricks.delta.retentionDurationCheck.enabled = false;
vacuum `1trc`.`measurements_delta` RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##1TRC - partitioned table
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Trillion Normal Distribution Samples as CTAS
create or replace table `1trc`.`measurements_delta_partitioned`
partitioned by (station)
as
select station, measure
from `1trc`.`measurements_delta`
;

-- COMMAND ----------

-- DBTITLE 1,Generate 1 Trillion Normal Distribution Samples
/* Generate new samples with a normal distribution using Phil Factors's classic solution:
   https://www.red-gate.com/simple-talk/blogs/getting-normally-distributed-random-numbers-in-tsql/  */
/*
create or replace table `1trc`.`measurements_delta_partitioned`
partitioned by (station)
as
select a.station,
      round(((rand() * 2 - 1) + (rand() * 2 - 1) + (rand() * 2 - 1)) /* Box-Muller transform */
       * 5 /* Standard Deviation */
       + a.mean_temp, 1) as measure
from `1trc`.`stations` as a
join (range(1000000000000)) b on a.id = (mod(b.id, 412)+1)
;
*/

-- COMMAND ----------

-- DBTITLE 1,Describe Table History
describe history `1trc`.`measurements_delta_partitioned`;
-- { /* Original */
--   "numFiles": "207648",
--   "numOutputRows": "1000000000000",
--   "numOutputBytes": "1127626491323"
-- }

-- COMMAND ----------

-- DBTITLE 1,Vacuum Delta Table - Retain No History
set spark.databricks.delta.retentionDurationCheck.enabled = false;
vacuum `1trc`.`measurements_delta_partitined` RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Non-partitioned table

-- COMMAND ----------

-- DBTITLE 1,Run 1TRC Query - cold run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- DBTITLE 1,Run 1TRC Query - warm run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Partitioned table

-- COMMAND ----------

-- DBTITLE 1,Run 1TRC Query - cold run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- DBTITLE 1,Run 1TRC Query - warm run
SET use_cached_result=false;
SELECT station, min(measure), max(measure), mean(measure)
FROM `1trc`.`measurements_delta_partitioned`
GROUP BY station
ORDER BY station;

-- COMMAND ----------

-- Cold | 64w | 412 rows | 1.20 minutes runtime
-- Warm | 64w | 412 rows | 54.74 seconds runtime
-- Warm | 64w | 412 rows | 53.96 seconds runtime
