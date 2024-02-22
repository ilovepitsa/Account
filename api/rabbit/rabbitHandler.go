package rabbit

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ilovepitsa/Account/api/repo"
	pb "github.com/ilovepitsa/protobufForTestCase"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

type RabbitParameters struct {
	Login    string
	Password string
	Ip       string
	Port     string
}

type RabbitHandler struct {
	l                    *log.Logger
	repository           *repo.AccountRepository
	connection           *amqp.Connection
	channel              *amqp.Channel
	requestQueue         amqp.Queue
	responceQueueToTrans amqp.Queue
	responceQueueToCust  amqp.Queue
}

func NewRabbitHandler(l *log.Logger, cr *repo.AccountRepository) *RabbitHandler {
	return &RabbitHandler{l: l, repository: cr}
}

func (rb *RabbitHandler) Init(param RabbitParameters) error {
	var err error
	rb.connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/", param.Login, param.Password, param.Ip, param.Port))
	if err != nil {
		// rb.l.Println(err)
		return err
	}

	rb.channel, err = rb.connection.Channel()
	if err != nil {
		// rb.l.Println(err)
		return err
	}

	err = rb.channel.ExchangeDeclare("account", "topic", false, false, false, false, amqp.Table{})
	if err != nil {
		// rb.l.Println(err)
		return err
	}
	err = rb.channel.ExchangeDeclare("account", "topic", false, false, false, false, amqp.Table{})
	if err != nil {
		// rb.l.Println(err)
		return err
	}

	rb.requestQueue, err = rb.channel.QueueDeclare("accountRequest", false, false, false, false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}
	err = rb.channel.QueueBind(rb.requestQueue.Name, "request", "account", false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	rb.responceQueueToTrans, err = rb.channel.QueueDeclare("accountResponceT", false, false, false, false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	err = rb.channel.QueueBind(rb.responceQueueToTrans.Name, "responceTrans", "account", false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	rb.responceQueueToCust, err = rb.channel.QueueDeclare("accountResponceC", false, false, false, false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	err = rb.channel.QueueBind(rb.responceQueueToCust.Name, "responceCustomer", "account", false, amqp.Table{})

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	// err = rb.channel.QueueBind(rb.responceQueueToTrans.Name, "responce", "account", false, amqp.Table{})

	// if err != nil {
	// 	// rb.l.Println(err)
	// 	return err
	// }
	err = rb.channel.Qos(
		1,
		0,
		false,
	)

	if err != nil {
		// rb.l.Println(err)
		return err
	}

	return nil
}

func (rb *RabbitHandler) Close() {
	rb.channel.Close()
	rb.connection.Close()
}

func (rb *RabbitHandler) Consume() {
	consumeRequestChan, err := rb.channel.Consume(rb.requestQueue.Name, "", true, false, false, false, amqp.Table{})

	if err != nil {
		rb.l.Println(err)
		return
	}

	var forever chan struct{}

	request := &pb.RequestAccount{}
	go func() {
		for d := range consumeRequestChan {

			err = proto.Unmarshal(d.Body, request)
			rb.l.Println("Recieve new request: ", request.String())
			switch request.Req.(type) {
			case *pb.RequestAccount_ReqAct:
				rb.parseRequestActive(request.GetReqAct())
			case *pb.RequestAccount_ReqFroz:
				rb.parseRequestFrozen(request.GetReqFroz())
			case *pb.RequestAccount_ReqTrans:
				rb.parseRequestTransaction(request.GetReqTrans())
			}

			// rb.l.Println("I dont have any methods for this request")
		}
	}()

	rb.l.Println("Waiting commands")
	<-forever
}

func (rb *RabbitHandler) parseRequestActive(req *pb.RequestActiveBalance) {
	numbers := rb.repository.GetActive(int(req.CustomerId))
	var result []*pb.Invoice

	for _, number := range numbers {
		result = append(result, &pb.Invoice{CustomerId: req.CustomerId, Number: number})
	}

	var answer pb.ResponceAccount = pb.ResponceAccount{Resp: &pb.ResponceAccount_RespActive{RespActive: &pb.ResponceActiveBalance{RequestId: req.RequestId, Invoices: result}}}

	rb.publish(&answer, "responceCustomer")
}

func (rb *RabbitHandler) parseRequestFrozen(req *pb.RequestFrozenBalance) {

	numbers := rb.repository.GetFrozen(int(req.CustomerId))
	var result []*pb.Invoice

	for _, number := range numbers {
		result = append(result, &pb.Invoice{CustomerId: req.CustomerId, Number: number})
	}

	var answer pb.ResponceAccount = pb.ResponceAccount{Resp: &pb.ResponceAccount_RespFrozen{RespFrozen: &pb.ResponceFrozenBalance{RequestId: req.RequestId, Invoices: result}}}

	rb.publish(&answer, "responceCustomer")
}

func getCurrencyString(currency pb.CurrencyType) string {
	switch currency {
	case pb.CurrencyType_CURRENCY_BTC:
		return "btc"
	case pb.CurrencyType_CURRENCY_EUR:
		return "eur"
	case pb.CurrencyType_CURRENCY_RUB:
		return "rub"
	case pb.CurrencyType_CURRENCY_USD:
		return "usd"
	case pb.CurrencyType_CURRENCY_USDT:
		return "usdt"
	}
	return ""
}

func (rb *RabbitHandler) actionAdd(trans *pb.Transaction) {
	currency := getCurrencyString(trans.Currency)
	currentAmount := rb.repository.GetValue(trans.Number_Invoice, currency)
	if currentAmount < 0.0 {
		rb.responceAdd(trans.Id, false, "error currentAmount")
		return
	}
	newAmount := currentAmount + trans.Amount
	if newAmount < 0.0 {
		rb.responceAdd(trans.Id, false, "error newAmount less zero")
		return
	}

	if !rb.repository.CheckInvoice(trans.Number_Invoice) {
		rb.responceAdd(trans.Id, false, "number invoice doesnt exitst")
		return
	}
	err := rb.repository.UpdateInvoice(trans.Number_Invoice, newAmount, currency)

	if err != nil {
		rb.responceAdd(trans.Id, false, err.Error())
		return
	}
	rb.responceAdd(trans.Id, true, "")
}

func (rb *RabbitHandler) publish(responce *pb.ResponceAccount, routinKey string) error {

	resp, err := proto.Marshal(responce)

	if err != nil {
		return err
	}
	ctx, cancle := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancle()

	err = rb.channel.PublishWithContext(
		ctx,
		"account",
		routinKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        resp,
		},
	)

	return err
}

func (rb *RabbitHandler) parseRequestTransaction(req *pb.RequestTransaction) {
	transaction := req.Transaction
	switch transaction.Action {
	case pb.ActionType_ACTION_ADD:
		rb.actionAdd(transaction)
	case pb.ActionType_ACTION_SUB:
		rb.actionWithdraw(transaction)
	}
}

func (rb *RabbitHandler) responceAdd(id int32, success bool, err string) {

	message := pb.ResponceInvoice{TransId: id, Success: success, ErrorMessage: err}
	messageToSend := pb.ResponceAccount{Resp: &pb.ResponceAccount_RespInv{RespInv: &message}}
	rb.publish(&messageToSend, "responceTrans")
}

func (rb *RabbitHandler) actionWithdraw(transaction *pb.Transaction) {

	currency := getCurrencyString(transaction.Currency)
	currentAmountFrom := rb.repository.GetValue(transaction.Number_Invoice, currency)
	if currentAmountFrom < 0.0 {
		rb.responceRetransfer(transaction.Id, false, "error currentAmountFrom")
		return
	}

	if len(transaction.Number_InvoiceTo) < 1 {
		rb.responceRetransfer(transaction.Id, false, "error emtpy Number_InvoiceTo")
		return
	}

	currentAmountTo := rb.repository.GetValue(transaction.Number_InvoiceTo, currency)
	if currentAmountTo < 0.0 {
		rb.responceRetransfer(transaction.Id, false, "error currentAmountTo")
		return
	}

	newAmountFrom := currentAmountFrom - transaction.Amount
	if newAmountFrom < 0.0 {
		rb.responceRetransfer(transaction.Id, false, "not enought money")
		return
	}
	newAmountTo := currentAmountTo + transaction.Amount
	if newAmountTo < 0.0 {
		rb.responceRetransfer(transaction.Id, false, "error newAmountTo")
		return
	}

	if !rb.repository.CheckInvoice(transaction.Number_Invoice) {
		rb.responceRetransfer(transaction.Id, false, "number invoice from doesnt exitst")
		return
	}
	if !rb.repository.CheckInvoice(transaction.Number_InvoiceTo) {
		rb.responceRetransfer(transaction.Id, false, "number invoice to doesnt exitst")
		return
	}

	err := rb.repository.UpdateInvoice(transaction.Number_Invoice, newAmountFrom, currency)
	if err != nil {
		rb.responceRetransfer(transaction.Id, false, "error update invoice from")
		return
	}

	err = rb.repository.UpdateInvoice(transaction.Number_InvoiceTo, newAmountTo, currency)
	if err != nil {
		rb.responceRetransfer(transaction.Id, false, "error update invoice to")
		return
	}

	rb.responceRetransfer(transaction.Id, true, "")

}

func (rb *RabbitHandler) responceRetransfer(id int32, success bool, err string) {
	message := pb.ResponceWithdraw{TransId: id, Success: success, ErrorMessage: err}
	messageToSend := pb.ResponceAccount{Resp: &pb.ResponceAccount_RespWithdraw{RespWithdraw: &message}}
	rb.publish(&messageToSend, "responceTrans")
}
