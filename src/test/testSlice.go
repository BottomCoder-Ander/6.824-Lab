package main

import "fmt"

type A struct {
	a []string
}

func main() {

	a := A{}

	a.a = append(a.a, "1")

	a.a = append(a.a, "3")

	fmt.Println(a.a)

}
