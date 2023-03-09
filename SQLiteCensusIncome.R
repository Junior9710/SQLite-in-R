#Importing packages
library("RSQLite")

#Loading in dataframe (change to location dataset is saved)
censusincome <- read.csv("census-income.data", header=FALSE)
#Listing column names of dataframe
colnames(censusincome)
#Changing column names
colnames(censusincome) <- c("AAGE","ACLSWKR","ADTIND","ADTOCC","AHGA","AHRSPAY","AHSCOL","AMARITAL","AMJIND","AMJOCC","ARACE","AREORGN","ASEX","AUNMEM","AUNTYPE","AWKSTAT","CAPGAIN","CAPLOSS","DIVVAL","FILESTAT","GRINREG","GRINST","HDFMX","HHDREL","MARSUPWT","MIGMTR1","MIGMTR3","MIGMTR4","MIGSAME","MIGSUN","NOEMP","PARENT","PEFNTVTY","PEMNTVTY","PENATVTY","PRCITSHP","SEOTR","VETQVA","VETYN","WKSWORK","YEAR","TRGT")
#Creating connection to database
db <- dbConnect(SQLite(), dbname="census_income")
#Removing any tables with the same name
dbRemoveTable(db, "Income")
#Creating table
dbSendQuery(db,
            "CREATE TABLE Income (
            SS_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            AAGE INT,
            ACLSWKR TEXT,
            ADTIND TEXT,
            ADTOCC TEXT,
            AHGA TEXT,
            AHRSPAY NUM,
            AHSCOL TEXT,
            AMARITAL TEXT,
            AMJIND TEXT,
            AMJOCC TEXT,
            ARACE TEXT,
            AREORGN TEXT,
            ASEX TEXT,
            AUNMEM TEXT,
            AUNTYPE TEXT,
            AWKSTAT TEXT,
            CAPGAIN NUM,
            CAPLOSS NUM,
            DIVVAL NUM,
            FILESTAT TEXT,
            GRINREG TEXT,
            GRINST TEXT,
            HDFMX TEXT,
            HHDREL TEXT,
            MARSUPWT NUM,
            MIGMTR1 TEXT,
            MIGMTR3 TEXT,
            MIGMTR4 TEXT,
            MIGSAME TEXT,
            MIGSUN TEXT,
            NOEMP NUM,
            PARENT TEXT,
            PEFNTVTY TEXT,
            PEMNTVTY TEXT,
            PENATVTY TEXT,
            PRCITSHP TEXT,
            SEOTR TEXT,
            VETQVA TEXT,
            VETYN TEXT,
            WKSWORK NUM,
            YEAR TEXT,
            TRGT TEXT
            )")
# Connecting dataframe to table named 'censustable' and inserting into 'Income' table 
dbWriteTable(conn = db, name = "censustable", value = censusincome)
dbSendQuery(conn = db, "INSERT INTO Income (AAGE,ACLSWKR,ADTIND,ADTOCC,AHGA,AHRSPAY,AHSCOL,AMARITAL,AMJIND,AMJOCC,ARACE,AREORGN,ASEX,AUNMEM,AUNTYPE,AWKSTAT,CAPGAIN,CAPLOSS,DIVVAL,FILESTAT,GRINREG,GRINST,HDFMX,HHDREL,MARSUPWT,MIGMTR1,MIGMTR3,MIGMTR4,MIGSAME,MIGSUN,NOEMP,PARENT,PEFNTVTY,PEMNTVTY,PENATVTY,PRCITSHP,SEOTR,VETQVA,VETYN,WKSWORK,YEAR,TRGT) SELECT * FROM censustable")
#Removing 'census'table
dbRemoveTable(db, "censustable")

#displaying income table column and fields
dbListTables(db)
dbReadTable(db,"Income")
dbListFields(db, "Income")

#Query counting total male and females in each race
AraceCount <- dbSendQuery(db, "SELECT ARACE,
    count(*) AS total,
    sum(case when ASEX = ' Male' then 1 else 0 end) AS Male,
    sum(case when ASEX = ' Female' then 1 else 0 end) AS Female
FROM Income
GROUP BY ARACE")
dbFetch(AraceCount)

#Query Calculating average annual income grouped by race
dbGetQuery(db, "SELECT ARACE AS Race, AVG( (AHRSPAY*40)*WKSWORK)
                AS Average_Income FROM Income
                WHERE AHRSPAY != 0
                GROUP BY ARACE ORDER BY AVG( (AHRSPAY*40)*WKSWORK ) DESC")

#Removing any tables with same name 
dbRemoveTable(db, "Person")
#Creating 'Person Table'
dbSendQuery(db,
            "CREATE TABLE Person(
            SS_ID PRIMARY KEY,
            AAGE INT,
            AHGA TEXT,
            ASEX TEXT,
            PRCITSHP TEXT,
            PARENT TEXT,
            GRINST TEXT,
            GRINREG TEXT,
            AREORGN TEXT,
            AWKSTAT TEXT
            )")
#Inserting Columns from Income table into Person table
dbSendQuery(db, "INSERT INTO Person (SS_ID, AAGE, AHGA, ASEX, PRCITSHP, PARENT, GRINST, GRINREG, AREORGN, AWKSTAT) 
            SELECT SS_ID, AAGE, AHGA, ASEX, PRCITSHP, PARENT, GRINST, GRINREG, AREORGN, AWKSTAT 
            FROM Income")
#Displaying columns and fields in Person table
dbListFields(db, "Person")
dbReadTable(db,"Person")

#Removing any tables with same name
dbRemoveTable(db, "Job")
#Creating Job table
dbSendQuery(db,
            "CREATE TABLE Job(
            SS_ID PRIMARY KEY,
            ADTIND TEXT,
            ADTOCC TEXT,
            AMJOCC TEXT,
            AMJIND TEXT
            )")
#Inserting Columns from Income table into Job table
dbSendQuery(db, "INSERT INTO Job (SS_ID, ADTIND, ADTOCC, AMJOCC, AMJIND) 
            SELECT SS_ID, ADTIND, ADTOCC, AMJOCC, AMJIND 
            FROM Income")

#Displaying columns and fields in Job table
dbListFields(db, "Job")
dbReadTable(db,"Job")

#Removing any tables with same name
dbRemoveTable(db, "Pay")
#Creating Pay table
dbSendQuery(db,
            "CREATE TABLE Pay(
            SS_ID PRIMARY KEY,
            AHRSPAY NUM,
            WKSWORK NUM
            )")
#Inserting Columns from Income table into Pay table
dbSendQuery(db, "INSERT INTO Pay (SS_ID, AHRSPAY, WKSWORK)
            SELECT SS_ID, AHRSPAY, WKSWORK
            FROM Income")
#Displaying columns and fields in Pay table
dbListFields(db, "Pay")
dbReadTable(db,"Pay")

#Selecting highest wage
dbGetQuery(db, "SELECT MAX(AHRSPAY) AS Max_wage FROM Pay")

# highest earning job query
dbGetQuery(db, "SELECT Person.GRINST AS State, COUNT(*) AS Total
                FROM Person
                INNER JOIN Job ON Job.SS_ID = Person.SS_ID
                WHERE AMJOCC = ' Professional specialty'
                GROUP BY GRINST ORDER BY COUNT(*) DESC")
# Highest earner table
dbGetQuery(db, "SELECT
           Pay.AHRSPAY,
           Person.GRINST,
           Job.AMJOCC,
           Job.AMJIND
           FROM
           Person
           INNER JOIN Job ON Job.SS_ID = Person.SS_ID
           INNER JOIN Pay ON Pay.SS_ID = Person.SS_ID
           WHERE
           AHRSPAY = 9999;
           ")
#highest earner state location
dbGetQuery(db, "SELECT GRINST AS State, COUNT(*) AS Total
                FROM Person WHERE GRINST = ' Not in universe'")
# total job type number 
dbGetQuery(db, "SELECT AMJIND AS Job, COUNT(*) AS Total
                FROM Job WHERE AMJIND = ' Other professional services'")
# highest earner to major industry
dbGetQuery(db, "SELECT AMJOCC AS Industry, COUNT(*) AS Total FROM
                Job WHERE AMJOCC = ' Professional specialty'")

# Create hispanic table
dbSendQuery(conn = db, "CREATE TABLE Hispanic
            (
            SS_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            AHGA TEXT,
            AHRSPAY NUM,
            AMJIND TEXT,
            AREORGN TEXT,
            WKSWORK NUM
            )")

# Insert data into Hispanic table
dbSendQuery(db, "INSERT INTO Hispanic
           (
           AREORGN,
           AHGA,
           AMJIND,
           AHRSPAY,
           WKSWORK
           )
           
           SELECT

           Person.AREORGN AS Hispanic,
           Person.AHGA AS Education,
           Job.AMJIND AS Industry,
           Pay.AHRSPAY AS Wage,
           Pay.WKSWORK AS Weeks
           FROM
           Person
           INNER JOIN Job ON Job.SS_ID = Person.SS_ID
           INNER JOIN Pay ON Pay.SS_ID = Person.SS_ID
           WHERE
           (
           AHGA = ' Bachelors degree(BA AB BS)'
           OR
           AHGA = ' Masters degree(MA MS MEng MEd MSW MBA)'
           OR
           AHGA = ' Doctorate degree(PhD EdD)'
           )
           AND
           (
           AREORGN NOT IN (' All other', ' Do not know', ' NA')
           );")

# List of industries
dbGetQuery(db, "SELECT DISTINCT AMJIND AS 'List of industries' FROM Hispanic")

# Average Wage
dbGetQuery(db, "SELECT AMJIND AS Industry, AVG(AHRSPAY)
                AS 'Average hourly wage' FROM Hispanic
                GROUP BY AMJIND ORDER BY AVG(AHRSPAY) DESC")

# Average Weeks
dbGetQuery(db, "SELECT AMJIND AS Industry, AVG(WKSWORK)
                AS 'Average weeks worked' FROM Hispanic
                GROUP BY AMJIND ORDER BY AVG(WKSWORK) DESC")
