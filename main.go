package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/ilovepitsa/Account/api/handlers"
	"github.com/ilovepitsa/Account/api/rabbit"
	"github.com/ilovepitsa/Account/api/repo"
	_ "github.com/lib/pq"
)

func main() {
	l := log.New(os.Stdout, "Account ", log.LstdFlags)
	l.SetFlags(log.LstdFlags | log.Lshortfile)

	connStr := "user=postgres password=123 dbname=TransactionSystem sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		l.Print(err)
		return
	}
	defer db.Close()

	accountRepo := repo.NewAccountRepository(db, l)
	// fillTest(*transRepo, l)

	rabbitHandler := rabbit.NewRabbitHandler(l, accountRepo)
	err = rabbitHandler.Init(rabbit.RabbitParameters{
		Login:    "account",
		Password: "account",
		Ip:       "localhost",
		Port:     "5672"})

	if err != nil {
		l.Println("Cant create rabbitHandler", err)
	}
	defer rabbitHandler.Close()

	go rabbitHandler.Consume()

	accountHandler := handlers.NewAccountHandler(l, accountRepo, rabbitHandler)

	sm := mux.NewRouter()
	getRouter := sm.Methods(http.MethodGet).Subrouter()
	getRouter.HandleFunc("/active", accountHandler.GetActive)
	getRouter.HandleFunc("/frozen", accountHandler.GetFrozen)

	// postRouter := sm.Methods(http.MethodPost).Subrouter()
	getRouter.HandleFunc("/add", accountHandler.AddAccount)

	l.Printf("Starting  on port %v.... \n", 8082)
	srv := http.Server{
		Addr:         ":8082",
		Handler:      sm,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
	srv.ListenAndServe()
}
