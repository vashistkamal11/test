package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
)

const (
	TRANSACTION_NOT_FOUND_ERROR = "no sunch transaction"
)

type transaction struct {
	Amount float64 	`json:"amount"`
	Type string 	`json:"type"`
	ParentId int64		`json:"parent_id"`
	TransactionId int64 	`json:"transactionId"`
}


type transactionHandler struct {
	TransactionToTypeMap map[int64]string
	TransactionsMappedToType map[string]map[int64]transaction
	ParentMap map[int64]map[int64]bool
}


func (this *transactionHandler) InsertTransaction (transactionToBeInserted transaction) error {
	if this.TransactionsMappedToType[transactionToBeInserted.Type] == nil {
		this.TransactionsMappedToType[transactionToBeInserted.Type] = map[int64]transaction{}
	}
	this.TransactionsMappedToType[transactionToBeInserted.Type][transactionToBeInserted.TransactionId] = transactionToBeInserted
	this.TransactionToTypeMap[transactionToBeInserted.TransactionId] = transactionToBeInserted.Type
	if transactionToBeInserted.ParentId != -1 && transactionToBeInserted.ParentId != transactionToBeInserted.TransactionId {
		if this.ParentMap[transactionToBeInserted.ParentId] == nil {
			this.ParentMap[transactionToBeInserted.ParentId] = map[int64]bool{}
		}
		this.ParentMap[transactionToBeInserted.ParentId][transactionToBeInserted.TransactionId] = true
	}
	return nil
}

func (this *transactionHandler) GetTransaction (transactionId int64) (transaction, error) {
	transactionType , ok := this.TransactionToTypeMap[transactionId];
	if !ok {
		return transaction{}, errors.New(TRANSACTION_NOT_FOUND_ERROR)
	}
	return this.TransactionsMappedToType[transactionType][transactionId], nil
}

func (this *transactionHandler) GetTransactionsForType(transactionType string) ([]int64){
	transactionArray := []int64{};
	if value , ok := this.TransactionsMappedToType[transactionType]; ok {
			for _, valueId := range value {
				transactionArray = append(transactionArray, valueId.TransactionId)
			}
	}
	return transactionArray
}

func (this *transactionHandler) GetSumForTransaction(transactionId int64) float64 {
	currentTransaction, err := this.GetTransaction(transactionId)
	if err != nil {
		return 0;
	}
	resultSum := currentTransaction.Amount
	if value ,ok := this.ParentMap[transactionId]; ok {
		for childId, _ := range value {
			resultSum += this.GetSumForTransaction(childId)
		}
	}
	return resultSum
}

func (this *transactionHandler) init() error {
	this.ParentMap = map[int64]map[int64]bool{}
	this.TransactionToTypeMap  = map[int64]string{}
	this.TransactionsMappedToType = map[string]map[int64]transaction{}
	return nil
}

var handlerInstance  = transactionHandler{}

func GetTransactionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	transactionIdString := vars["transaction_id"]
	transactionId, err := strconv.ParseInt(transactionIdString, 10,  64)
	if err  != nil {

	}
	transactionToReturn, err := handlerInstance.GetTransaction(transactionId)
	if err != nil {

	}
	err = json.NewEncoder(w).Encode(transactionToReturn)
	if err != nil {

	}
}


func GetTransactionByTypeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	transactionType := vars["type"]
	transactionArray := handlerInstance.GetTransactionsForType(transactionType)
	err := json.NewEncoder(w).Encode(transactionArray)
	if err != nil {

	}
}


func GetTransactionSumHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	transactionIdString := vars["transaction_id"]
	transactionId, err := strconv.ParseInt(transactionIdString, 10,  64)
	if err  != nil {

	}
	transactionsum := handlerInstance.GetSumForTransaction(transactionId)
	resp := map[string]float64{
		"sum": transactionsum,
	}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {

	}

}

func InsertTransactionHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	transactionIdString := vars["transaction_id"]
	transactionId, err := strconv.ParseInt(transactionIdString, 10,  64)
	if err  != nil {

	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {

	}
	dataMap := map[string]interface{}{}
	t := transaction{}
	err = json.Unmarshal(data, &dataMap)
	err = json.Unmarshal(data, &t)
	if err != nil {

	}
	t.TransactionId = int64(transactionId)
	if _, ok := dataMap["parent_id"]; !ok {
			t.ParentId = -1
	}
	err = handlerInstance.InsertTransaction(t)
	if err != nil {

	}
	resp := map[string]string{"status":"ok"}
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {

	}
}

func main() {
	err := handlerInstance.init()
	if err != nil{
		fmt.Println("fatal error")
		return
	}
	r := mux.NewRouter()
	r.HandleFunc("/transactionservice/transaction/{transaction_id}", InsertTransactionHandler)
	r.HandleFunc("/transactionservice/retrieveTransaction/{transaction_id}", GetTransactionHandler)
	r.HandleFunc("/transactionservice/types/{type}", GetTransactionByTypeHandler)
	r.HandleFunc("/transactionservice/sum/{transaction_id}", GetTransactionSumHandler)
	srv := &http.Server{
		Handler:      r,
		Addr:         "127.0.0.1:8000",
	}

	log.Fatal(srv.ListenAndServe())
}


//test code
//var i int64
//for i=0;i<10;i++{
//t := transaction{float64(i*100) + 0.87, strconv.Itoa(int(i)), int64(9), i}
//handlerInstance.InsertTransaction(t)
//}
//t := transaction{500 + 0.87, "test", int64(1), 87}
//handlerInstance.InsertTransaction(t)
//t = transaction{500 + 0.87, "test", int64(2), 45}
//handlerInstance.InsertTransaction(t)
//t = transaction{500 + 0.87, "test", int64(9), 54}
//handlerInstance.InsertTransaction(t)
//t = transaction{500 + 0.87, "test", int64(2), 67}
//handlerInstance.InsertTransaction(t)
//t , err = handlerInstance.GetTransaction(8)
//fmt.Println(t)
//t, err = handlerInstance.GetTransaction(15)
//fmt.Println(t)
//arr := handlerInstance.GetTransactionsForType("7")
//fmt.Println(arr)
//arr = handlerInstance.GetTransactionsForType("test")
//fmt.Println(arr)
//totalSum := handlerInstance.GetSumForTransaction(2)
//fmt.Println(totalSum)

