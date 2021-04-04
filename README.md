### usage
`go run main.go -table ks.tablename -file dump-filename.txt`

save all contents from {ks.tablename} table into {dump-filename.txt} file


`go run main.go -table ks.tablename -file dump-filename.txt -upload`

insert all records from {dump-filename.txt} file into table {ks.tablename}

***
### args

`-dbhost string` cassandra host (default "127.0.0.1")

`-dbpass string` cassandra password (default "cassandra")

`-dbport int` cassandra port (default 9042)

`-dbuser string` cassandra username (default "cassandra")

`-file string` filename

`-table string` table name

`-upload` need upload?
