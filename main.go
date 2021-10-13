package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/jszwec/csvutil"
	_ "github.com/lib/pq"
)

//Credentials
var (
	user   string = "postgres"
	pword  string = "postgre"
	host   string = "localhost"
	port   int    = 5432
	dbname string = "benchDB"
)

var (
	db      *sql.DB
	wg      sync.WaitGroup
	GenData []model
)

//Our model struct for storing the fields required
type model struct {
	ID            int    `gorm:"column:id" db:"id"`
	Name          string `gorm:"column:name" db:"name"`
	CarMaker      string `gorm:"column:car_maker" db:"car_maker"`
	Gender        string `gorm:"column:gender" db:"gender"`
	SSN           string `gorm:"column:ssn" db:"ssn"`
	Email         string `gorm:"column:email" db:"email"`
	Address       string `gorm:"column:address" db:"address"`
	Phone         string `gorm:"column:phone" db:"phone"`
	Phone2        string `gorm:"column:phone2" db:"phone2"`
	CreditCardNum string `gorm:"column:credit_card" db:"credit_card"`
	JobTitle      string `gorm:"column:job_title" db:"job_title"`
	Level         string `gorm:"column:level" db:"level"`
	Company       string `gorm:"column:company" db:"company"`
	FatherName    string `gorm:"column:father_n" db:"father_n"`
	MotherName    string `gorm:"column:mother_n" db:"mother_n"`
	Street        string `gorm:"column:street" db:"street"`
	StreetName    string `gorm:"column:street_n" db:"street_n"`
	City          string `gorm:"column:city" db:"city"`
	State         string `gorm:"column:state" db:"state"`
	Country       string `gorm:"column:country" db:"country"`
	Zip           string `gorm:"column:zip" db:"zip"`
}

func init() {
	db, _ = sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, pword, dbname))
	//db.SetMaxOpenConns(1)
	err := db.Ping()
	if err != nil {
		panic(err)
	}

	f, err := os.Open("Sample-data.csv")

	if err != nil {
		log.Fatal("error readng file", err)
	}
	tempF, _ := ioutil.ReadAll(f)

	if err := csvutil.Unmarshal(tempF, &GenData); err != nil {
		log.Fatal("Error unmarshalling")
	}

	createtable()
}

func createtable() {
	//Create Table
	createQ := `
		CREATE TABLE IF NOT EXISTS test(
		  id serial,
		  name varchar(50) NOT NULL,
		  car_maker varchar(50) NOT NULL,
		  gender varchar(10) NOT NULL,
		  ssn varchar(20) NOT NULL,
				email varchar(150) NOT NULL,
		  address varchar(200) NOT NULL,
		  phone varchar(20) NOT NULL,
		  phone2 varchar(20) NOT NULL,
		  credit_card varchar(60) NOT NULL,
		  job_title varchar(20) NOT NULL,
		  level varchar(30) NOT NULL,
		  company varchar(100) NOT NULL,
		  father_n varchar(50) NOT NULL,
		  mother_n varchar(50) NOT NULL,
		  street varchar(60) NOT NULL,
		  street_n varchar(100) NOT NULL,
		  city varchar(100) NOT NULL,
		  state varchar(100) NOT NULL,
		  country varchar(100) NOT NULL,
		  zip varchar(10) NOT NULL				
		);`

	_, err := db.Exec(createQ)

	if err != nil {
		panic(err)
	}
}

func (model) TableName() string {
	return "test"
}

//Cleaning up the database after the operations
func Cleanup() {
	_, err := db.Exec(`DROP TABLE test`)
	if err != nil {
		log.Println("Connection not closed :- ", err)
	}
}

// Functions for Native SQL Library

// Insertion/ Write
func InsertionNative(dat model) {
	defer wg.Done()
	_, err := db.Exec(fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip))
	if err != nil {
		log.Fatalln("error inserting data", err)
	}

}

// Fetch/ Read
func FetchByIDNative(id int) model {
	dat := model{}
	defer wg.Done()
	err := db.QueryRow("SELECT * FROM test WHERE id = $1", id).Scan(&dat.ID, &dat.Name, &dat.CarMaker, &dat.Gender, &dat.SSN, &dat.Email, &dat.Address, &dat.Phone, &dat.Phone2, &dat.CreditCardNum, &dat.JobTitle, &dat.Level, &dat.Company, &dat.FatherName, &dat.MotherName, &dat.Street, &dat.StreetName, &dat.City, &dat.State, &dat.Country, &dat.Zip)
	if err != nil {
		log.Fatal("Failed to execute query: ", err)
	}
	return dat
}

// Functions for GORM library
func InsertionGORM(db *gorm.DB, dat model) {
	//Inserting Data
	db.Create(&dat)
	defer wg.Done()
}

func FetchGORM(db *gorm.DB, id int) model {
	defer wg.Done()
	dat := model{}
	db.First(&dat, id)
	return dat
}

// Fucntions for Pgx Library
func InsertionPgx(conn *pgx.Conn, dat model) {
	defer wg.Done()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip)); err != nil {
		fmt.Println("Unable to insert Data", err)
		return
	}

}

func FetchPgx(conn *pgx.Conn, id int) model {
	tempdat := model{}
	defer wg.Done()
	conn.QueryRow(context.Background(), "SELECT * FROM test WHERE id = $1;", id).Scan(&tempdat)
	return tempdat
}

// Fucntions for PgxPool Library
func InsertionPgxPool(conn *pgxpool.Pool, dat model) {
	defer wg.Done()
	if _, err := conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO test values('%d', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.ID, dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip)); err != nil {
		fmt.Println("Unable to insert Data", err)
		return
	}

}

func FetchPgxPool(conn *pgxpool.Pool, id int) model {
	defer wg.Done()
	tempdat := model{}

	conn.QueryRow(context.Background(), "SELECT * FROM test WHERE id=$1;", id).Scan(&tempdat)
	return tempdat
}

func main() {
	//defer Cleanup()
	fmt.Print("Starting the function")
	db.SetMaxOpenConns(180)
	t1 := time.Now()
	num := 10000
	//g, _ := gorm.Open("postgres", db)
	//defer g.Close()

	conn, _ := pgx.Connect(context.Background(), "postgres://postgres:postgre@localhost:5432/benchDB")
	defer conn.Close(context.Background())

	/* 	conn, _ := pgxpool.Connect(context.Background(), "postgres://postgres:postgre@localhost:5432/benchDB")
	   	defer conn.Close() */

	for i := 1; i <= num; {
		for k := 0; k < 9000 && i <= num; k++ {
			i++
			wg.Add(1)
			//go InsertionNative(GenData[i])
			//go FetchByIDNative(8)
			//go InsertionGORM(g, GenData[i])
			//go FetchGORM(g, i)
			//go InsertionPgx(conn, GenData[i])
			go FetchPgx(conn, i+1)
			//go InsertionPgxPool(conn, GenData[i])
			//go FetchPgxPool(conn, GenData[i])

		}
		wg.Wait()

	}
	//InsertionNative(GenData[1])

	//Inserting dummy values
	/* 	for j := 1; j <= 5; j++ {
	   		fmt.Println("Started inserting ....")
	   		sampleTableGen()
	   		fmt.Println("Finished ", j, " lakh entries ...")
	   	}
	   	fmt.Println("Finshed the insertions.")
	*/

	fmt.Println("Time took :- ", time.Since(t1))
	fmt.Println("Finished the operation data\n")
	defer db.Close()
}

func sampleTableGen() {
	for i := 1; i < 98000; {
		for k := 0; k < 2800; k++ {
			i++
			wg.Add(1)
			go sampTabInsertion(GenData[i])

		}
		wg.Wait()
	}

}

// Dummy Fuction for inserting sample values
func sampTabInsertion(dat model) {
	defer wg.Done()
	_, err := db.Exec(fmt.Sprintf("INSERT INTO test(name, car_maker, gender, ssn, email, address, phone, phone2, credit_card, job_title,level, company, father_n, mother_n, street, street_n, city,state, country, zip) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ON CONFLICT DO NOTHING;", dat.Name, dat.CarMaker, dat.Gender, dat.SSN, dat.Email, dat.Address, dat.Phone, dat.Phone2, dat.CreditCardNum, dat.JobTitle, dat.Level, dat.Company, dat.FatherName, dat.MotherName, dat.Street, dat.StreetName, dat.City, dat.State, dat.Country, dat.Zip))
	if err != nil {
		log.Fatalln("error inserting data", err)
	}

}
