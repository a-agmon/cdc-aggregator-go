package main

import "testing"

func TestRecordProcessor_ProcessAggregatedMessages(t *testing.T) {

	m1 := `{"CUSTOM_FIELD": "", "STANDARD_FIELD": "TOTAL", "VALUE_NEW": "4960.02", "VALUE_OLD": "", "OPERATION": "Set", "DATE_CREATED": "2022-09-02 04:09:21", "TRANSACTION_ID": "1008554"}`
	//m2 := `{"CUSTOM_FIELD": "", "STANDARD_FIELD": "TRANID", "VALUE_NEW": "SOISR54440", "VALUE_OLD": "", "OPERATION": "Set", "DATE_CREATED": "2022-09-02 04:09:22", "TRANSACTION_ID": "1008554"}`
	m3 := `{"CUSTOM_FIELD": "", "STANDARD_FIELD": "TOTAL", "VALUE_NEW": "6084.00", "VALUE_OLD": "", "OPERATION": "Set", "DATE_CREATED": "2022-09-02 08:00:40", "TRANSACTION_ID": "1009352"}`
	m4 := `{"CUSTOM_FIELD": "", "STANDARD_FIELD": "TRANID", "VALUE_NEW": "PAYISR67466", "VALUE_OLD": "", "OPERATION": "Set", "DATE_CREATED": "2022-09-02 08:00:47", "TRANSACTION_ID": "1009352"}`

	var agg1 [][]byte = [][]byte{[]byte(m1)}
	var agg2 [][]byte = [][]byte{[]byte(m3), []byte(m4)}

	recordProcessor := RecordProcessor{}

	jsonBytes, err := recordProcessor.ExtractJsonMessage("Number1", agg1)
	if err != nil {
		t.Errorf("Error extracting message: %v", err)
	}
	println(string(jsonBytes))
	jsonBytes, err = recordProcessor.ExtractJsonMessage("Number2", agg2)
	println(string(jsonBytes))

}
