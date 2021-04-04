package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	upload := flag.Bool("upload", false, "do upload")
	tname := flag.String("table", "", "table name")
	fname := flag.String("file", "", "filename")
	dbhost := flag.String("dbhost", "127.0.0.1", "cassandra host")
	dbport := flag.Int("dbport", 9042, "cassandra port")
	dbuser := flag.String("dbuser", "cassandra", "cassandra username")
	dbpass := flag.String("dbpass", "cassandra", "cassandra password")
	flag.Parse()

	dbsess, err := connect(*dbhost, *dbport, *dbuser, *dbpass)
	if err != nil {
		panic(err)
	}

	if !*upload {
		file, err := os.Create(*fname)
		if err != nil {
			log.Fatal(err)
		}
		fetchTable(dbsess, file, *tname)

	} else {
		file, err := os.Open(*fname)
		if err != nil {
			log.Fatal(err)
		}
		uploadTable(dbsess, file, *tname)
	}
}

func connect(host string, port int, user, pass string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: user,
		Password: pass,
	}
	if port != 0 {
		cluster.Port = port
	}
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 2}
	cluster.DisableInitialHostLookup = true
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return sess, nil
}

func fetchTable(sess *gocql.Session, file io.Writer, tableName string) {
	isHeader := true
	var keys []string
	iter := sess.Query(`select * from ` + tableName).Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		if isHeader {
			var tbl *Table
			tbl, keys = TableFromRow(row)
			file.Write([]byte(tbl.Serialize() + "\n"))
			isHeader = false
		}
		file.Write([]byte(serializeRow(row, keys) + "\n"))
	}
}

type Table []Column

type Column struct {
	Name string
	Type string
}

func (t *Table) Serialize() string {
	arr := make([]string, 0, len(*t))
	for _, v := range *t {
		arr = append(arr, v.Name+":"+v.Type)
	}
	return strings.Join(arr, ",")
}

func TableFromRow(row map[string]interface{}) (*Table, []string) {
	table := &Table{}
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		c := Column{Name: k}
		switch row[k].(type) {
		case bool:
			c.Type = "bool"
		case int:
			c.Type = "int"
		case string:
			c.Type = "string"
		case float64:
			c.Type = "float"
		case time.Time:
			c.Type = "time"
		case gocql.UUID:
			c.Type = "uuid"
		default:
			panic("unsupported export type: " + reflect.TypeOf(row[k]).String())
		}
		*table = append(*table, c)
	}
	return table, keys
}

func TableFromHeader(line string) (*Table, []string) {
	table := &Table{}
	keys := make([]string, 0)
	for _, v := range strings.Split(line, ",") {
		x := strings.Split(v, ":")
		if len(x) != 2 {
			panic(v)
		}
		n, t := x[0], x[1]
		*table = append(*table, Column{Name: n, Type: t})
		keys = append(keys, n)

	}
	return table, keys
}

func serializeRow(row map[string]interface{}, keys []string) string {
	vals := make([]interface{}, 0, len(row))
	for _, k := range keys {
		vals = append(vals, row[k])
	}
	data, err := json.Marshal(vals)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func uploadTable(sess *gocql.Session, file io.Reader, tableName string) {
	isHeader := true
	var tbl *Table
	var query string
	s := bufio.NewScanner(file)
	batch := sess.NewBatch(gocql.LoggedBatch)
	for s.Scan() {
		if isHeader {
			var keys []string
			tbl, keys = TableFromHeader(s.Text())
			query = `insert into ` + tableName + `(` + strings.Join(keys, ", ") + `) ` +
				`values(` + strings.Repeat(`?, `, len(keys)-1) + `?)`
			isHeader = false
		} else {
			vals := deserialize(tbl, s.Bytes())
			batch.Query(query, vals...)
			if batch.Size() >= 100 {
				if err := sess.ExecuteBatch(batch); err != nil {
					panic(err)
				}
				batch = sess.NewBatch(gocql.LoggedBatch)
			}
		}
	}
	if batch.Size() > 0 {
		if err := sess.ExecuteBatch(batch); err != nil {
			panic(err)
		}
	}
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
}

func deserialize(tbl *Table, line []byte) []interface{} {
	var arr []json.RawMessage
	if err := json.Unmarshal(line, &arr); err != nil {
		panic(err)
	}
	vals := make([]interface{}, 0, len(arr))
	for i, v := range arr {
		tp := (*tbl)[i].Type
		switch tp {
		case "bool":
			var x bool
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		case "int":
			var x int
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		case "string":
			var x string
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		case "float":
			var x float64
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		case "time":
			var x time.Time
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		case "uuid":
			var x gocql.UUID
			if err := json.Unmarshal(v, &x); err != nil {
				panic(err)
			}
			vals = append(vals, &x)
		default:
			panic("unsupported import type: " + tp)
		}
	}
	return vals
}
