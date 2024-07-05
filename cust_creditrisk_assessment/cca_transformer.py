from crdh_extractor import CRDH_Extractor
import pandas as pd
from pyspark.sql.functions import current_date, add_months, sum as _sum

class CCA_Transformer:
    def __init__(self) -> None:
        pass

    def data_aggregation(self):
        customer_df = CRDH_Extractor().customer_extractor()
        accounts_df = CRDH_Extractor().accounts_extractor()
        cc_df = CRDH_Extractor().creditcard_transactions_extractor()
        dda_df = CRDH_Extractor().demanddraft_extractor()
        loans_df = CRDH_Extractor().loans_extractor()

        ##
        # Aggregate total credit card spending by each customer over the past year.
        ##
        # cc_df = cc_df.filter(cc_df['TransactionDate'] >= add_months(current_date(), -12))

        # cc_total_spend = cc_df.groupBy("customerID").agg(_sum("TransactionAmount").alias("total_cc_spend_l12m")).orderBy("customerID")
        # cc_total_spend = cc_total_spend.join(customer_df, "customerID", "inner")
        # cc_total_spend = cc_total_spend.select("customerID", "customerName","total_cc_spend_l12m")
        # cc_total_spend.show()

        ##
        # Calculate the total outstanding loan amount for each customer.
        ##

        # Filter Active Loans
        # loans_active_df = loans_df.filter(loans_df["LoanStatus"]=="Active")
        # loans_total_outstanding_df = loans_active_df.groupBy("customerID").agg(_sum("LoanAmount").alias("total_outstanding_loan"))
        # loans_total_outstanding_df.show()


        accounts_df.show()

CCA_Transformer().data_aggregation()