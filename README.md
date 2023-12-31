## CSI3130-Project Symmetric Hash Join

Group Members:
- Giselle 300056566
- Maddie 300227635
- Deniz 300229393

## Results

<img width="392" alt="res" src="https://github.com/Deniz-Jasa/3130-A1/assets/46465622/44f12ce6-d9cd-43c2-aeb3-f24413ced338">


## Problems Encountered

In our group, we faced several challenges when trying to run older versions of PostgreSQL on newer Mac hardware. The main issue was the incompatibility with Apple's shift to ARM architecture in their M1 and M2 chips, as our PostgreSQL versions were designed for Intel's x86 architecture. 
Updates in the macOS also made it difficult for these older PostgreSQL versions to function properly, especially with changes in system libraries and security protocols. Additionally, our older 32-bit PostgreSQL versions became obsolete with macOS's move to support only 64-bit applications. 
We also couldn't leverage the full potential of the newer Macs' advanced hardware, like faster SSDs and more RAM, leading to performance issues. Networking changes in macOS further complicated connectivity for our PostgreSQL setup. To resolve these issues, we had to use an older macos version.

` test=# select e1.ename as "manager", e2.ename as "employee"
test-# from emp e1, emp e2, dept d, manages m
test-# where e1.eno = m.eno
test-# and m.dno = d.dno
test-# and d.dno = e2.dno
test-# order by manager;
server closed the connection unexpectedly
	This probably means the server terminated abnormally
	before or while processing the request.
The connection to the server was lost. Attempting reset: Failed.
!> `


When our group encountered the "server closed the connection unexpectedly" error during our SQL query execution, we first restarted the database server to re-establish a stable connection and reviewed the server logs. There, we found indications of intermittent network disruptions and a few instances of server overload, which likely caused the abrupt termination.

### Tech Stack

![C](https://img.shields.io/badge/c-%23ED8B00.svg?style=for-the-badge&logo=c&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-039BE5?style=for-the-badge&logo=PostgreSQL&logoColor=white)


<!-- GETTING STARTED -->
## Getting Started

1. Enter Postgres Folder: Navigate to the folder with cd postgresql-15.4.

2. Install PostgreSQL: Run the following commands in order:
./configure
make
sudo su (enter your password)
make install
Create PostgreSQL User: Make a 'postgres' user in your system settings and set a password.

3. Create Data Folder: Run mkdir -p /usr/local/pgsql/data and change ownership with chown postgres /usr/local/pgsql/data.

4. Switch to PostgreSQL User: Use su - postgres.

5. Test Installation:

Initialize database: /usr/local/pgsql/bin/initdb -D /usr/local/pgsql/data
Start server: /usr/local/pgsql/bin/pg_ctl -D /usr/local/pgsql/data -l logfile start
Create a test database: /usr/local/pgsql/bin/createdb testdb

6. Run python3 swapFiles.py

7. Test SQL Command: In the terminal, run psql.
   
### Prerequisites

* C
* PostgreSQL 8.1.7


