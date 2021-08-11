/* Facilities schema */
DROP TABLE IF EXISTS facilities;

CREATE TABLE facilities (
  id INTEGER NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  street TEXT,
  city TEXT,
  state TEXT,
  zip INTEGER,
  region TEXT
);
 
/* DRGs schema */
DROP TABLE IF EXISTS drgs;

CREATE TABLE drgs (
  id INTEGER NOT NULL PRIMARY KEY,
  description TEXT NOT NULL
);

/* Treatments schema */
DROP TABLE IF EXISTS treatments;

CREATE TABLE treatments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  facility_id INTEGER NOT NULL,
  drg_id INTEGER NOT NULL,
  discharge_count INTEGER,
  mean_covered_charges DECIMAL(10, 3),
  mean_medicare_payments DECIMAL(10, 3),
  mean_medicare_reimburse DECIMAL(10, 3)
);