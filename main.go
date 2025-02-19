package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/jlabath/netpod/server/pod"
	"google.golang.org/api/iterator"
)

// type to store generic one value from BQ
type Col struct {
	Schema *bigquery.FieldSchema
	Val    bigquery.Value
}

func (c *Col) MarshalJSON() ([]byte, error) {
	switch c.Schema.Type {
	case bigquery.IntegerFieldType, bigquery.StringFieldType, bigquery.BooleanFieldType, bigquery.FloatFieldType:
		return json.Marshal(c.Val)
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType:
		if c.Val == nil {
			//nil pointer
			return json.Marshal(c.Val)
		}
		if num, ok := c.Val.(*big.Rat); ok {
			str := num.String()
			return json.Marshal(str)
		} else {
			return nil, fmt.Errorf("Failed to convert field %s to json", c.Schema.Name)
		}
	case bigquery.TimestampFieldType:
		return json.Marshal(c.Val)
	case bigquery.TimeFieldType:
		if v, ok := c.Val.(civil.Time); ok {
			str := v.String()
			return json.Marshal(str)
		} else {
			return nil, fmt.Errorf("Failed to convert field %s to json", c.Schema.Name)
		}
	case bigquery.DateFieldType:
		if v, ok := c.Val.(civil.Date); ok {
			str := v.String()
			return json.Marshal(str)
		} else {
			return nil, fmt.Errorf("Failed to convert field %s to json", c.Schema.Name)
		}
	case bigquery.DateTimeFieldType:
		if v, ok := c.Val.(civil.DateTime); ok {
			str := v.String()
			return json.Marshal(str)
		} else {
			return nil, fmt.Errorf("Failed to convert field %s to json", c.Schema.Name)
		}
	default:
		return nil, fmt.Errorf("Unsure how to convert field %s and type %s", c.Schema.Name, c.Schema.Type)
	}
}

// type to store generic biquery row
type Row struct {
	columns []Col
}

func (r *Row) Load(v []bigquery.Value, s bigquery.Schema) error {
	r.columns = make([]Col, 0, len(s))
	for idx, val := range v {
		var col Col
		col.Val = val
		col.Schema = s[idx]
		r.columns = append(r.columns, col)
	}
	return nil
}

func (r *Row) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.columns)
}

type QueryTokenResp struct {
	Rows  []Row  `json:"rows"`
	Token string `json:"token"`
}

func query(client *bigquery.Client) pod.Handler {

	return func(ctx context.Context, args []json.RawMessage) (json.RawMessage, error) {
		var (
			sql     string
			results []Row
		)

		if err := pod.DecodeArgs(args, &sql); err != nil {
			return nil, err
		}

		// Run the query
		q := client.Query(sql)
		it, err := q.Read(ctx)
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}

		// Iterate through the results
		for {
			var row Row
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("Error iterating through results: %v", err)
			}

			results = append(results, row)
		}

		return json.Marshal(results)
	}
}

func queryToken(client *bigquery.Client) pod.Handler {

	return func(ctx context.Context, args []json.RawMessage) (json.RawMessage, error) {
		var (
			sql      string
			token    string
			pageSize int
			results  []Row
		)

		if err := pod.DecodeArgs(args, &sql, &token, &pageSize); err != nil {
			return nil, err
		}

		// Run the query
		q := client.Query(sql)
		it, err := q.Read(ctx)
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}
		if token != "" {
			it.PageInfo().Token = token
		}
		it.PageInfo().MaxSize = pageSize

		// Iterate through the results
		for i := 0; i < pageSize; i = i + 1 {
			var row Row
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("Error iterating through results: %v", err)
			}

			results = append(results, row)
		}
		resp := QueryTokenResp{
			Token: it.PageInfo().Token,
			Rows:  results,
		}
		return json.Marshal(resp)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Missing a filepath argument for socket to listen on\n")
	}

	// socket file path is first argument given to program
	socketPath := os.Args[1]

	//ctx for mongo client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if os.Getenv("GOOGLE_CLOUD_PROJECT") == "" {
		log.Fatal("GOOGLE_CLOUD_PROJECT variable is missing")
	}

	client, err := bigquery.NewClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ds := pod.DescribeResponse{

		Format: "json",
		Namespaces: []pod.Namespace{pod.Namespace{
			Name: "netpod.jlabath.bigquery",
			Vars: []pod.Var{
				pod.Var{
					Name:    "query",
					Handler: query(client)},
				pod.Var{
					Name:    "query-token",
					Handler: queryToken(client)},
			}},
		}}

	os.Remove(socketPath)

	//run the server
	pod.Listen(socketPath, ds)
}
