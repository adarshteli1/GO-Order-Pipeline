package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Order struct {
	ID     int
	Status string
}

func main() {

	var wg sync.WaitGroup

	orderchan := make(chan *Order)
	updateorderchan := make(chan *Order)
	deliveryorderchan := make(chan *Order)

	numworkers := 5
	wg.Add(numworkers)

	for i := 1; i <= numworkers; i++ {
		go processorders(i, orderchan, updateorderchan, &wg)
	}

	wg.Add(2)

	// go processorders(orderchan, updateorderchan, &wg)
	go updateorders(updateorderchan, deliveryorderchan, &wg)
	go deliveryorders(deliveryorderchan, &wg)

	generateorders(20, orderchan)

	wg.Wait()
}

func generateorders(count int, orderchan chan<- *Order) {
	for i := 0; i < count; i++ {
		order := &Order{
			ID:     i + 1,
			Status: "Pending",
		}
		orderchan <- order
	}
	close(orderchan)

}

func processorders(id int, orderchan <-chan *Order, updateorderchan chan<- *Order, wg *sync.WaitGroup) {
	defer wg.Done()

	for order := range orderchan {
		fmt.Printf("[Worker %d] Processing Order %d\n", id, order.ID)
		order.Status = "Processing"
		updateorderchan <- order
	}
}

func updateorders(updateorderchan <-chan *Order, deliveryorderchan chan<- *Order, wg *sync.WaitGroup) {
	defer wg.Done()

	for order := range updateorderchan {
		order.Status = "Shipping"
		deliveryorderchan <- order
	}
	close(deliveryorderchan)

}

func deliveryorders(deliverorderchan <-chan *Order, wg *sync.WaitGroup) {
	defer wg.Done()

	for order := range deliverorderchan {
		time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

		order.Status = "Delivered"
		fmt.Printf("Delivered order %d\n", order.ID)
	}

}
