package repo

import (
	"database/sql"
	"fmt"
	"log"

	pb "github.com/ilovepitsa/protobufForTestCase"
	_ "github.com/lib/pq"
)

type AccountRepository struct {
	l  *log.Logger
	db *sql.DB
}

func NewAccountRepository(db *sql.DB, l *log.Logger) *AccountRepository {
	return &AccountRepository{db: db, l: l}
}

func (x *AccountRepository) Add(newAccount *pb.Invoice) error {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return err
	}

	stmt, err := trans.Exec(fmt.Sprintf("insert into accounts (num_invoice, customerid, usdt_invoice, rub_invoice, eur_invoice, usd_invoice, btc_invoice, status) values('%s', %v, %v, %v, %v, %v, %v, %v)",
		newAccount.Number,
		newAccount.CustomerId,
		newAccount.UsdtBalance,
		newAccount.RubBalance,
		newAccount.EurBalance,
		newAccount.UsdBalance,
		newAccount.BtcBalance,
		newAccount.Status,
	))
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return err
	}
	id, err := stmt.RowsAffected()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return err
	}
	x.l.Printf("Row affected: %v", id)

	trans.Commit()
	return nil
}

func (x *AccountRepository) UpdateInvoice(num_invoice string, newAmount float64, currency string) error {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return err
	}
	x.l.Printf("update accounts set status = true, %s_invoice = %v where num_invoice = '%s';\n", currency, newAmount, num_invoice)
	_, err = trans.Exec(fmt.Sprintf("update accounts set status = true, %s_invoice = %v where num_invoice = '%s';", currency, newAmount, num_invoice))
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return err
	}
	trans.Commit()
	return nil
}

func (x *AccountRepository) GetValue(num_invoice, currency string) float64 {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return -1
	}
	rows, err := trans.Query(fmt.Sprintf("select %s_invoice from accounts where num_invoice = '%s';", currency, num_invoice))

	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return -1
	}

	amount := 0.0
	for rows.Next() {
		err = rows.Scan(&amount)
		if err != nil {
			x.l.Println(err)
			trans.Rollback()
			return -1
		}
	}

	trans.Commit()
	return amount
}

func (x *AccountRepository) CheckInvoice(num_invoice string) bool {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return false
	}
	rows, err := trans.Query(fmt.Sprintf("select num_invoice from accounts where num_invoice = '%s'", num_invoice))

	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return false
	}

	scanned := ""
	for rows.Next() {
		err = rows.Scan(&scanned)
		if err != nil {
			x.l.Println(err)
			continue
		}

	}
	if scanned != num_invoice {
		x.l.Println("cant find invoice")
		trans.Rollback()
		return false
	}
	trans.Commit()
	return true
}

func (x *AccountRepository) GetActive(id int) []string {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return nil
	}

	rows, err := trans.Query(fmt.Sprintf("select num_invoice from accounts where customerid = %v and status=true;", id))
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return nil
	}
	var result []string
	var num string
	for rows.Next() {
		err = rows.Scan(&num)
		if err != nil {
			x.l.Println(err)
			continue
		}
		result = append(result, num)
	}

	return result
}

func (x *AccountRepository) GetFrozen(id int) []string {
	trans, err := x.db.Begin()
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return nil
	}

	rows, err := trans.Query(fmt.Sprintf("select num_invoice from accounts where customerid = %v and status=false;", id))
	if err != nil {
		x.l.Println(err)
		trans.Rollback()
		return nil
	}
	var result []string
	var num string
	for rows.Next() {
		err = rows.Scan(&num)
		if err != nil {
			x.l.Println(err)
			continue
		}
		result = append(result, num)
	}

	return result
}
