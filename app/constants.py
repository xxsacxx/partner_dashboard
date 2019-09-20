Payment_Choices = ["repay", "subscribe"]


class Payment:

    def __init__(self, number: str, month: str, vendor: str, value: str, status: bool, pay_type: int, tnx_id: str):
        self.Payment_Choices = Payment_Choices
        self.Payment_dict = dict(receipt_number=str(), month=str(), vendor=str(), value=str(), status=bool(), tnx=str(),
                                 type=str())
        self.Payment_dict["receipt_number"] = number
        self.Payment_dict["month"] = month
        self.Payment_dict["vendor"] = vendor
        self.Payment_dict["value"] = value
        self.Payment_dict["status"] = status
        self.Payment_dict["tnx"] = tnx_id
        self.Payment_dict["type"] = self.Payment_Choices[pay_type]
