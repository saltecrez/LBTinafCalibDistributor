# LBTinafCalibDistributor

- **Goal**: distribute files towards different sites based on the queries specified in the conf.json file. Scalable to N destinations. It supports two file transfer modes: scp and sftp. Based on MySQL database.


- **Targets**: Used for the LBT archive, Tucson machine. 

- **Description**: 

- **Configuration parameters**:

    - "dates_file": file containing one column with the dates in the form year-month-dayThour:min:sec.f (eg 2019-09-26T12:00:00.000) 
    - "db_host": ip address of the host containing the local database
    - "db_pwd": password of the local database for the user specified in db_user
    - "db_user": user of the database who can access the schema specified in db_schema
    - "db_schema": name of the schema to be queried
    - "db_schema_check": schema name of the reference database
    - "db_table_check": table name contained in the reference database; table example in sql folder
    - "priv_sshkey": path to the local private key
    - "destination": information characterizing a destination; in particular:
        - "query": query performed on db_schema to select the data to be sent 
        - "table": table in db_schema on which the query is performed
        - "dest": destination path on the remote machine
        - "host": hostname of the remote machine where data is transferred
        - "user": user accessing the remote machine in order to transfer data
        - "label": destination label (eg MI, RM, AZ)
        - "mode": choose between scp (in case of no ssh access to the machine) or sftp. 

- **Usage**: 
    - prepare a file containing the observation days;
    - prepare the configuration file conf.json by specifying all the configuration
      parameters specified in the configuration section;
    - python2.7 calibDistributor.py 
