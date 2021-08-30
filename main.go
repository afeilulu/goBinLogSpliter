package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"cn.xbt/binlog/logger"
	"cn.xbt/binlog/model"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	if len(os.Args) < 5 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <sourceTopic> <targetTopics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	sourceTopic := os.Args[3]
	targetTopics := os.Args[4:]

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// str := `{"ts":{"$numberLong":"6971461266984403022"},"v":{"$numberInt":"2"},"op":"i","ns":"linked_db.dailyChargeDetail","o":[{"name":"_id","value":"2451448"},{"name":"actualPrice","value":{"$numberDouble":"150.0"}},{"name":"actualPrice1","value":{"$numberDouble":"150.0"}},{"name":"actualPrice2","value":{"$numberDouble":"0.0"}},{"name":"actualPrice3","value":{"$numberDouble":"0.0"}},{"name":"actualPrice4","value":{"$numberDouble":"0.0"}},{"name":"actualPrice5","value":{"$numberDouble":"0.0"}},{"name":"actualPrice6","value":{"$numberDouble":"0.0"}},{"name":"appointmentType","value":{"$numberInt":"1"}},{"name":"billNo","value":"GXL21060800031"},{"name":"billTime","value":{"$date":{"$numberLong":"1623153112000"}}},{"name":"billingPerformance","value":{"$numberDouble":"150.0"}},{"name":"cardVolumeDiscount","value":{"$numberDouble":"0.0"}},{"name":"chargeItemNames","value":"X线计算机体层(CT)成像"},{"name":"checkInTime","value":"2021-06-08T19:29:55"},{"name":"checkInType","value":"初诊"},{"name":"comments","value":"享团购"},{"name":"consultantName","value":"张萌-咨询"},{"name":"createOfficeName","value":"高新路分院"},{"name":"department","value":"综合科"},{"name":"detailDiscountPrice","value":{"$numberDouble":"0.0"}},{"name":"discount","value":"50"},{"name":"discountPrice","value":{"$numberDouble":"150.0"}},{"name":"doctorName","value":"吴耀辉-综合"},{"name":"feeType","value":{"$numberInt":"0"}},{"name":"firstVisitTime","value":"2021-06-08T00:00:00"},{"name":"giftAmount","value":{"$numberDouble":"0.0"}},{"name":"isFirstVisit","value":true},{"name":"itemSubCategory","value":"其他服务"},{"name":"manualSingleDiscount","value":{"$numberDouble":"150.0"}},{"name":"manualSingleDiscounts","value":{"$numberDouble":"0.0"}},{"name":"membercardDiscount","value":{"$numberDouble":"0.0"}},{"name":"mobile","value":"13991836331"},{"name":"office","value":"小白兔口腔高新路分院"},{"name":"officeId","value":{"$numberInt":"88"}},{"name":"officeName","value":"高新路分院"},{"name":"orderId","value":{"$numberInt":"2451448"}},{"name":"orderType","value":"detail"},{"name":"originalPrice","value":{"$numberDouble":"300.0"}},{"name":"overdue","value":{"$numberDouble":"0.0"}},{"name":"patientAge","value":{"$numberInt":"48"}},{"name":"patientCreatedTime","value":"2021-06-08T19:29:38"},{"name":"patientCreatedUser","value":"雷倩"},{"name":"patientId","value":{"$numberInt":"1681280"}},{"name":"patientName","value":"吕洪涛"},{"name":"patientType","value":"普通"},{"name":"payTime","value":{"$date":{"$numberLong":"1623153175000"}}},{"name":"payType","value":"微信支付"},{"name":"payee","value":"王玥"},{"name":"performance","value":{"$numberDouble":"150.0"}},{"name":"planPrice","value":{"$numberDouble":"150.0"}},{"name":"principalAmount","value":{"$numberDouble":"0.0"}},{"name":"privateId","value":"GXLP21060800020"},{"name":"reason","value":""},{"name":"reduceAndPayCard","value":{"$numberDouble":"0.0"}},{"name":"refereeId","value":{"$numberInt":"0"}},{"name":"refereeName","value":"高新路分院"},{"name":"revenue","value":{"$numberDouble":"150.0"}},{"name":"sex","value":{"$numberInt":"1"}},{"name":"shouldPayPrice","value":{"$numberDouble":"150.0"}},{"name":"sourceType","value":"门店招牌"},{"name":"sourceTypeL1","value":"自然到院"},{"name":"sourceTypeL2","value":"门店招牌"},{"name":"sourceTypeL3","value":"高新路分院"},{"name":"sourceTypeLevel1","value":"自然到院"},{"name":"status","value":"已收费"},{"name":"totalCount","value":{"$numberInt":"0"}},{"name":"totalDisCountPrice","value":{"$numberDouble":"150.0"}},{"name":"totalDiscount","value":{"$numberDouble":"150.0"}},{"name":"totalPrice","value":{"$numberDouble":"300.0"}},{"name":"yibaoContent","value":""},{"name":"monthTag","value":"2021-06"},{"name":"_class","value":"cn.xbt.platform.eky.models.DailyChargeDetail"}],"o2":null,"lsid":{"id":{"kind":{"$numberInt":"4"},"data":{"$binary":{"base64":"J9ZZeiJhTemTkmi/Q6zXnQ==","subType":"00"}}},"uid":{"$binary":{"base64":"qr5CpYOBENpuDGwwNLW9g54iqv06eaeHWoUCu5PKfIo=","subType":"00"}}},"txnNumber":{"$numberLong":"791"}}`
	// var json_data interface{}
	// var error = json.Unmarshal([]byte(str), &json_data)
	// if error != nil {
	// 	logger.Error.Println(error)
	// }
	// // $.store.book[?(@.author =~ /(?i).*REES/)].author
	// abc, error := jsonpath.JsonPathLookup(json_data, "$.o[?(@.name =~ /(?i).*actualPrice$/)].value.$numberDouble")
	// if error != nil {
	// 	logger.Error.Println(error)
	// }
	// logger.Info.Println(abc.([]interface{})[0])
	// logger.Info.Println(abc)
	// logger.Info.Println(abc)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// "security.protocol":               "SASL_PLAINTEXT",
		// "sasl.mechanism":                  "PLAIN",
		// "sasl.username":                   "username",
		// "sasl.password":                   "password",
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": false,
		"auto.offset.reset":    "earliest"})

	if err != nil {
		logger.Error.Printf("Failed to create consumer: %s\n", err)
		// fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.Subscribe(sourceTopic, nil)
	if err != nil {
		fmt.Printf("Failed to subscribe : %s\n", err)
		os.Exit(1)
	}

	// Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	offset := ""
	run := true

	for run {
		select {
		case sig := <-sigchan:
			logger.Info.Printf("offset end: %s \n", offset)
			logger.Info.Printf("Caught signal %v: terminating\n", sig)
			// fmt.Printf("offset end: %s \n", offset)
			// fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				logger.Info.Printf("%% %v\n", e)
				// fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				logger.Info.Printf("%% %v\n", e)
				// fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				// fmt.Printf("%% Message on %s:\n%s\n",
				// 	*e.TopicPartition.Topic, string(e.Value))

				// 指定源分区
				if *e.TopicPartition.Topic == sourceTopic {
					// fmt.Printf("%% Message on %s: offset %s\n",*e.TopicPartition.Topic, e.TopicPartition.Offset)
					if len(offset) < 1 {
						logger.Info.Printf("offset start: %s\n", e.TopicPartition.Offset)
						// fmt.Printf("offset start: %s\n", e.TopicPartition.Offset)
					}
					offset = e.TopicPartition.Offset.String()

					var msgObj model.BinLog
					var err = json.Unmarshal(e.Value, &msgObj)
					if err != nil {
						logger.Error.Printf("json parse error: %s", string(e.Value))
						// fmt.Printf("json parse error: %s",string(e.Value))
					} else {

						newTopic := msgObj.TableName
						if contains(targetTopics, newTopic) {
							// send to new topic
							p.Produce(&kafka.Message{
								TopicPartition: kafka.TopicPartition{Topic: &newTopic, Partition: kafka.PartitionAny},
								Value:          e.Value,
							}, deliveryChan)

							e := <-deliveryChan
							m := e.(*kafka.Message)

							if m.TopicPartition.Error != nil {
								logger.Info.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
								// fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
							} else {
								logger.Info.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
								// fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
								// 	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
							}
						}
					}

				}
			case kafka.PartitionEOF:
				logger.Info.Printf("%% Reached %v\n", e)
				// fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				logger.Error.Printf("%% Error: %v\n", e)
				// fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}

	close(deliveryChan)

	fmt.Printf("Closing consumer\n")
	c.Close()
}

func If(condition bool, trueVal, falseVal interface{}) interface{} {
	if condition {
		return trueVal
	}
	return falseVal
}

func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}
