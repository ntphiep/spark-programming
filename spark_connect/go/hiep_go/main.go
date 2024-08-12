package main

import (
	"context"
	"flag"
	"log"

	"github.com/apache/spark-connect-go/v35/client/sql"
)

var (
	remote = flag.String("remote", "sc://localhost:15002",
		"the remote address of Spark Connect server to connect to")
)

func main() {
    flag.Parse()
	ctx := context.Background()

	spark, err := sql.NewSessionBuilder().Remote(*remote).Build(ctx)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
	
	defer spark.Stop()

	df, err := spark.Sql(ctx, "select 'apple' as word, 123 as count union all select 'orange' as word, 456 as count")
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
}
