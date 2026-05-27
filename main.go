package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"

	"github.com/abligh/gonbdserver/nbd"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ebs"
)

var region string
var socket string

func main() {
	defaultRegion, ok := os.LookupEnv("AWS_REGION")
	if !ok {
		defaultRegion = "us-east-1"
	}

	defaultSocketDir, ok := os.LookupEnv("XDG_RUNTIME_DIR")
	if !ok {
		defaultSocketDir = "/tmp"
	}
	defaultSocket := path.Join(defaultSocketDir, "nbd.sock")

	flag.StringVar(&region, "region", defaultRegion, "AWS region of snapshot")
	flag.StringVar(&socket, "socket", defaultSocket, "path to listen on")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(flag.CommandLine.Output(), "  %s [flags] <required_unnamed_arg>\n\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Flags:")
		flag.PrintDefaults() // Prints defined flags like -model automatically
	}

	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		return
	}

	args := flag.Args()
	snapshot := args[0]

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	client := ebs.New(sess)

	nbd.RegisterBackend("ebs", func(ctx context.Context, e *nbd.ExportConfig) (nbd.Backend, error) {
		return NewSnapshotBackend(ctx, client, snapshot)
	})

	ctx, cancelFunc := context.WithCancel(context.Background())
	var sessionWaitGroup sync.WaitGroup
	defer func() {
		cancelFunc()
		sessionWaitGroup.Wait()
	}()

	go func() {
		nbd.StartServer(ctx, ctx, &sessionWaitGroup, log.New(os.Stderr, "", log.LstdFlags), nbd.ServerConfig{
			Protocol:      "unix",
			Address:       socket,
			DefaultExport: "ebs",
			Exports: []nbd.ExportConfig{
				{
					Name:     "ebs",
					Driver:   "ebs",
					ReadOnly: true,
				},
			},
		})
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
		case <-ctx.Done():
		case <-sigChan:
	}

	fmt.Printf("\nStarting graceful shutdown...\n")
	if err := os.RemoveAll(socket); err != nil {
		fmt.Printf("Error socket file: %v\n", err)
		return
	}
}
