from crdh_reader_factory import get_data_source

class CRDH_Extractor:

# class CRDH_Data_Extractor(CRDH_Extractor):
    def customer_extractor(self):
        return get_data_source("table", "customers")
    
    def accounts_extractor(self):
        return get_data_source("table", "accounts")
    
    def creditcard_transactions_extractor(self):
        return get_data_source("csv", "K:\\Manish\\Data Engineering\\Data\\Financial\\creditcard_transactions.txt")
    
    def demanddraft_extractor(self):
        return get_data_source("csv", "K:\\Manish\\Data Engineering\\Data\\Financial\\demanddraft_data.txt")
    
    def loans_extractor(self):
        return get_data_source("csv", "K:\\Manish\\Data Engineering\\Data\\Financial\\loans_transactions_data.txt")
    