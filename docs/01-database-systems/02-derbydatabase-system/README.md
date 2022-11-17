# [Derby](https://db.apache.org/derby/)

1. Download from https://db.apache.org/derby/releases/release-10_16_1_1.cgi
1. Set DERBY_HOME
    ```
    DERBY_HOME=~/Downloads/db-derby-10.16.1.1-bin
    ```
1. Copy sql
    ```
    cp ~/Downloads/db-derby-10.16.1.1-bin/demo/programs/toursdb/*.sql .
    ```
1. Run `ij`.
    ```
    java -jar $DERBY_HOME/lib/derbyrun.jar ij
    ```
1. Connect to the database (embeded)
    ```
    ij> CONNECT 'jdbc:derby:firstdb;create=true';
    ```

    Check `derby.log` and `firstdb` dir:

    ```
    firstdb
    ├── README_DO_NOT_TOUCH_FILES.txt
    ├── log
    ├── seg0
    └── service.properties
    ```
1. Run SQL

    ```sql
    CREATE TABLE FIRSTTABLE
        (ID INT PRIMARY KEY,
        NAME VARCHAR(12));
    INSERT INTO FIRSTTABLE VALUES
        (10,'TEN'),(20,'TWENTY'),(30,'THIRTY');
    SELECT * FROM FIRSTTABLE;
    SELECT * FROM FIRSTTABLE WHERE ID=20;
    ```
1. Disconnect
    ```sql
    disconnect;
    exit;
    ```
