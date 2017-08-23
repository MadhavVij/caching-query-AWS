Name: Madhav Vij
Project: Comparing Memcache and RDS
Programming Lang: Python 3.6
*********

-----INSTRUCTIONS-----
## Create AWS profile
## Add authetication variables in memcache_rds.py
## Run the program


*********



*********
CODE_STRUCTURE

File: memcache_rds.py
#	connectDB()
		Connect database with RDS credentials
#	cleanDB()
		Clean incorrect database values
#	createDB()
		Create Database in RDS using csv file
#	generateQuery(r1,r2,choice)
		Generate random queries between given range and run through memcache or RDS depending on choice
#	fromDB(sql)
		run sql using database (RDS)
#	fromMemcache(sql)
		run sql using cached query results (memcache)
				
*********