/* Create trips table */
CREATE TABLE IF NOT EXISTS trips (
    tripID INTEGER PRIMARY KEY AUTOINCREMENT,
    regionID INTEGER, 
    groupID INTEGER, 
    datasourceID INTEGER, 
    datetime TEXT,
    originLongitud TEXT, 
    originLatitud TEXT, 
    destinationLongitud TEXT, 
    destinationLatitud TEXT
   );



/* Create groups table */
CREATE TABLE IF NOT EXISTS groups (
   groupID INTEGER PRIMARY KEY AUTOINCREMENT,
   groupSequence text NOT NULL UNIQUE
);


/* Create regions table */
CREATE TABLE IF NOT EXISTS regions (
   regionID INTEGER PRIMARY KEY AUTOINCREMENT,
   region text NOT NULL UNIQUE
   );



/* Create datasources table */
CREATE TABLE IF NOT EXISTS datasources (
   datasourceID INTEGER PRIMARY KEY AUTOINCREMENT,
   datasource text NOT NULL UNIQUE
   );