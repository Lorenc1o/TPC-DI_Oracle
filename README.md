# TPC-DI_Oracle
Repository for development of the second project of the course DATA WAREHOUSES (INFO-H-419) from the program BDMA, Fall 2022.

Development of a TPC-DI Benchmark in Oracle with several scale factors.

Group members:
- [Ivanović, Nikola](https://github.com/ivanovicnikola)
- [Lorencio Abril, Jose Antonio](https://github.com/Lorenc1o)
- [Yusupov, Sayyor](https://github.com/SYusupov)
- [Živković, Bogdana](https://github.com/zivkovicbogdana)

Professor: Zimányi, Esteban

## Structure of the repo

## Base steps
1. Download TPC-DI Tools (https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
2. Install java-8 if not already installed and set java-8 as the default version of java
3. Install python-oracledb library: (More information: https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html)
4. Rename folder PDGF to pdgf
5. Generate the data to a folder "staging/**sf**" where **sf** is the chosen scale factor
  - e.g. for scale factor 3:
    $ java -jar DIGen.jar -sf **3** -o ../staging/3

6. If implementing on Oracle Linux, convert the generated files to Linux format: `find staging/3/ -type f -exec dos2unix {} \; > results.out`
7. Create a database on Oracle `CREATE DATABASE <db-name>`
8. Set the information for connecting to your Oracle Database, in particular `host`(e.g. `localhost`), `port` (e.g.: `1521`),`user`, `password`, `db-name` that is set in step 7.
9. Do the Historical Load with

    $ python main.py -sf **sf**
    
    where:
    - **sf** is Scale factor to be used in benchmark, the same as the one used to generate the data
