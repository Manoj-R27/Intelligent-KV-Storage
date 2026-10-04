package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	address := flag.String("address", "127.0.0.1:7000", "KV server address")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: client [-address host:port] PUT key value | GET key | DELETE key")
		os.Exit(2)
	}
	connection, err := net.Dial("tcp", *address)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer connection.Close()
	request := strings.Join(flag.Args(), " ")
	if _, err := fmt.Fprintln(connection, request); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	response, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print(strings.TrimSuffix(response, "\n"))
}
