package main

import (
  "fmt"
  "log"
  "os"
  "time"
  "net/http"
  "encoding/json"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
 )

type Score struct {
  Address string `json:"address"`
  Rounds int `json:"rounds"`
  Profit float64 `json:"profit_usd"`
  ProfitEth float64 `json:"profit_eth"`
  ProfitBnb float64 `json:"profit_bnb"`
}

type Rate struct {
  Ticker string `json:"ticker"`
  Price float64 `json:"price"`
}

var leaders = []Score{}
var rates = []Rate{}
var db *sql.DB

func main() {
  initDbCon()
  fmt.Println("Connected to MySQL!")

  go dataUpdater()

  // handle routes
  http.HandleFunc("/", GetStatus)
  http.HandleFunc("/leaders", GetLeaders)
  http.HandleFunc("/rates", GetRates)

  log.Fatal(http.ListenAndServe(":8082", nil))
}

func initDbCon() {
  var dbUser = os.Getenv("MYSQL_USER")
  var dbPwd = os.Getenv("MYSQL_PASS")

  dbURI := fmt.Sprintf("%s:%s@unix(/var/lib/mysql/mysql.sock)/banzai?parseTime=true",
    dbUser, dbPwd)

  var err error
  // dbPool is the pool of database connections.
  db, err = sql.Open("mysql", dbURI)
  if err != nil {
    panic(err.Error())
  }
}

func dataUpdater() {
  // get initial values
  fetchRates()
  fetchWinners()

  // check and update cache every 1 min
  for range time.Tick(time.Second * 60) {
    fmt.Println("Update cache from MySQL...")
    fetchRates()
    fetchWinners()
  }
}

func fetchRates() {
  rates = nil
  rows, err := db.Query("SELECT ticker, price FROM rates")
  if err != nil {
    panic(err.Error())
  }

  defer rows.Close()
  for rows.Next() {
    var r Rate
    if err := rows.Scan(&r.Ticker, &r.Price); err != nil {
      fmt.Println("Failed to parse rates")
      panic(err.Error())
    }
    rates = append(rates, r)
  }

  recalculateProfits()
}

func recalculateProfits() {
  _, err := db.Query("UPDATE winners SET profit_usd=profit_eth*(SELECT price FROM rates WHERE ticker='eth') WHERE profit_eth>0")
  if err != nil {
    panic(err.Error())
  }

        _, err2 := db.Query("UPDATE winners SET profit_usd=profit_bnb*(SELECT price FROM rates WHERE ticker='bnb') WHERE profit_bnb>0")
  if err2 != nil {
    panic(err.Error())
  }
}

func fetchWinners() {
  leaders = nil
  rows, err := db.Query("SELECT address, count(rounds) as c, sum(profit_usd) as p, "+
    "ROUND(sum(profit_eth), 2), ROUND(sum(profit_bnb),2) "+
    "FROM winners GROUP BY address ORDER BY c DESC, p DESC;")
  if err != nil {
    panic(err.Error())
  }

  defer rows.Close()
  for rows.Next() {
    var w Score
    if err := rows.Scan(&w.Address, &w.Rounds, &w.Profit, &w.ProfitEth, &w.ProfitBnb); err != nil {
      fmt.Println("Failed to parse winners")
      panic(err.Error())
    }

    leaders = append(leaders, w)
  }
}

func GetStatus(w http.ResponseWriter, r *http.Request) {
  fmt.Fprintf(w, "Status: live")
}

func GetRates(w http.ResponseWriter, r *http.Request) {
  w.WriteHeader(http.StatusOK)
  json.NewEncoder(w).Encode(rates)
}

func GetLeaders(w http.ResponseWriter, r *http.Request) {
  w.WriteHeader(http.StatusOK)
  json.NewEncoder(w).Encode(leaders)
}
