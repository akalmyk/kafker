kp, err := NewKafkaProducer("localhost:9092", "myGroup", "test_topic")
if err != nil {
	log.Fatal(err)
}
defer kp.Close()

kc, err := NewKafkaConsumer("localhost:9092", "myGroup", "test_topic")
if err != nil {
	log.Fatal(err)
}
defer kc.Close()

msgChan := make(chan []byte)
defer close(msgChan)

go kc.Consume(msgChan)

go func() {
	for {
		//fmt.Println("read channel")
		select {
		case data := <-msgChan:
			log.Println(string(data))
		default:
			time.Sleep(time.Second)
		}
	}
}()

for i := 0; i < 10; i++ {
	i := i
	go func() {
		time.Sleep(time.Second * time.Duration(rand.Intn(5)+1))
		if err := kp.Produce(fmt.Sprintf("message %d at %s", i, time.Now().String())); err != nil {
			log.Println(err)
		} else {
			log.Println("msg sent")
		}
	}()
}

signalChan := make(chan os.Signal, 1)
signal.Notify(signalChan, os.Interrupt)
<-signalChan