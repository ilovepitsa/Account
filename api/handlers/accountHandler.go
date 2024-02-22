package handlers

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/ilovepitsa/Account/api/rabbit"
	"github.com/ilovepitsa/Account/api/repo"
	pb "github.com/ilovepitsa/protobufForTestCase"
)

type AccountHandler struct {
	l             *log.Logger
	repository    *repo.AccountRepository
	rabbitHandler *rabbit.RabbitHandler
}

func NewAccountHandler(l *log.Logger, repos *repo.AccountRepository, rabbitHandler *rabbit.RabbitHandler) *AccountHandler {
	return &AccountHandler{l: l, repository: repos, rabbitHandler: rabbitHandler}
}

func (x *AccountHandler) GetActive(w http.ResponseWriter, r *http.Request) {

	params := r.URL.Query()

	if !params.Has("id") {
		fmt.Fprintln(w, "add customer id")
	}

	idStr := params.Get("id")
	// x.l.Println(idStr)
	id, err := strconv.Atoi(idStr)
	if err != nil {
		x.l.Println(err)
		return
	}

	result := x.repository.GetActive(id)
	resultString := ""
	for _, r := range result {
		resultString += r
	}
	fmt.Fprintln(w, resultString)
}

func (x *AccountHandler) GetFrozen(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()

	if !params.Has("id") {
		fmt.Fprintln(w, "add customer id")
	}

	idStr := params.Get("id")
	// x.l.Println(idStr)
	id, err := strconv.Atoi(idStr)
	if err != nil {
		x.l.Println(err)
		return
	}

	result := x.repository.GetFrozen(id)
	resultString := ""
	for _, r := range result {
		resultString += r
	}
	fmt.Fprintln(w, resultString)

}

func (x *AccountHandler) AddAccount(w http.ResponseWriter, r *http.Request) {

	params := r.URL.Query()

	usdt, eur, rub, usd, btc := 0.0, 0.0, 0.0, 0.0, 0.0
	if !params.Has("num_invoice") {
		fmt.Fprintln(w, "add numinvoice")
		return
	}

	if !params.Has("customerid") {
		fmt.Fprintln(w, "add customerid")
		return
	}
	var err error

	num_invoice := params.Get("num_invoice")
	customerId, err := strconv.Atoi(params.Get("customerid"))

	if err != nil {
		x.l.Println(err)
		return
	}

	if params.Has("usdt") {
		usdt, err = strconv.ParseFloat(params.Get("usdt"), 64)
		if err != nil {
			usdt = 0
			x.l.Println(err)
		}
	}

	if params.Has("eur") {
		eur, err = strconv.ParseFloat(params.Get("eur"), 64)
		if err != nil {
			eur = 0
			x.l.Println(err)
		}
	}

	if params.Has("rub") {
		rub, err = strconv.ParseFloat(params.Get("rub"), 64)
		if err != nil {
			rub = 0
			x.l.Println(err)
		}
	}

	if params.Has("usd") {
		usd, err = strconv.ParseFloat(params.Get("usd"), 64)
		if err != nil {
			usd = 0
			x.l.Println(err)
		}
	}

	if params.Has("btc") {
		btc, err = strconv.ParseFloat(params.Get("btc"), 64)
		if err != nil {
			btc = 0
			x.l.Println(err)
		}
	}

	newAccount := pb.Invoice{
		Number:      num_invoice,
		UsdtBalance: usdt,
		UsdBalance:  usd,
		RubBalance:  rub,
		EurBalance:  eur,
		BtcBalance:  btc,
		Status:      true,
		CustomerId:  int32(customerId),
	}
	x.l.Println(newAccount.String())
	err = x.repository.Add(&newAccount)
	if err != nil {
		x.l.Println(err)
	}

}
