package broker

import "banka1.com/types"

func GetCustomerById(id int64) (*CustomerResponse, error) {
	var m CustomerResponse
	err := sendAndRecieve("get-customer", id, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func SendOTCTransactionInit(dto *types.OTCTransactionInitiationDTO) error {
	return sendReliable("init-otc", dto)
}

func SendOTCTransactionFailure(uid string, message string) error {
	dto := &types.OTCTransactionACKDTO{
		Uid:     uid,
		Failure: true,
		Message: message,
	}
	return sendReliable("otc-ack-banking", dto)
}

func SendOTCTransactionSuccess(uid string) error {
	dto := &types.OTCTransactionACKDTO{
		Uid:     uid,
		Failure: false,
		Message: "",
	}
	return sendReliable("otc-ack-banking", dto)
}
