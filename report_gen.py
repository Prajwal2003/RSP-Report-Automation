from pymongo import MongoClient
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def center_inv():
    # MongoDB connection
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)

# Specify the database and collection
    db = client['test']
    purchase_order_collection = db['tblPurchaseOrder']

    locations = [
        "Waranga phata",
        "Narsiphata",
        "Hiremyagiri",
        "Ghungrala",
        "Naregal",
        "Sovenahalli",
        "Kadampur",
        "Borgaon",
        "Bhokar phata",
        "Khanapur",
        "Sasavehalli",
        "Ittagi",
        "Mehkar",
        "Arsikere",
        "Farthabhad",
        "Siddeshwar"
    ]

    # Aggregation pipeline
    pipeline = [
        {
            "$match": {
                "HonorDistrict": {"$in": locations}
            }
        },
        {
            "$unwind": "$PurchaseOrderDetail"  # Unwind the PurchaseOrderDetail array
        },
        {
            "$match": {
                "PurchaseOrderDetail.GradeA": {"$gt": 0},
                "UserType": "Godown"
            }
        },
        {
            "$group": {
                "_id": "$HonorDistrict",  # Group by HonorDistrict
                "totalGradeA": {"$sum": "$PurchaseOrderDetail.GradeA"},
                "totalProduct": {
                    "$sum": {
                        "$multiply": ["$PurchaseOrderDetail.GradeA", "$PurchaseOrderDetail.GradeAAvgPrice"]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,  # Exclude the _id field from the result
                "HonorDistrict": "$_id",
                "TotalGradeA": "$totalGradeA",
                "TotalProduct": "$totalProduct"
            }
        }
    ]

    # Execute the aggregation pipeline
    results = purchase_order_collection.aggregate(pipeline)

    grand_total_gradeA = 0
    grand_total_value = 0

    # Google Sheets credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client_gspread = gspread.authorize(creds)

    sheet = client_gspread.open("RSP weekly report").worksheet("CENTER_INV")


    # Process and update individual rows
    row_index = 2  # Start from the third row (B3)
    for doc in results:
        value_in_L = round(doc['TotalProduct'] / 100000, 2)
        row = [
            [doc['HonorDistrict'], round(doc['TotalGradeA'], 2), value_in_L]
        ]
        range_name = f'A{row_index}:C{row_index}'
        sheet.update(range_name, row)
        row_index += 1
        
        # Update grand totals
        grand_total_gradeA += round(doc['TotalGradeA'], 2)
        grand_total_value += value_in_L

    grand_total_text = "Grand Total"
    row_total = [
        [grand_total_text, grand_total_gradeA, grand_total_value]
    ]
    sheet.update(f'A{row_index}:C{row_index}', row_total)

    client.close()

def receviables():
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)

    # Specify the database and collection
    db = client['test']
    consumer_transaction_collection = db['tblConsumerTransaction']

    # Fetch the current date
    current_date = datetime.now()

    # Define the list of names to filter by
    names_to_filter = [
        "NARMADA SOLVEX PVT LTD",
        "PANDIT HEERALAL VYAS FARMERS PRODUCER",
        "RADIANT DISTRIBUTION ENTERPRISE",
        "RAHUL AGRICOM COMPANY",
        "RISHABH JAIN",
        "SACHIN INTERNATIONAL PROTEINS PRIVATE LIMITED",
        "SAMUNNATI AGRO SOLUTIONS PRIVATE LIMITED",
        "SUPERIOR MALT PVT LTD "
    ]

    # Aggregation pipeline
    pipeline = [
        {
            "$match": {
                "Status": "FinancePending"
            }
        },
        {
            "$lookup": {
                "from": "users",
                "localField": "BuyerId",
                "foreignField": "_id",
                "as": "user_info"
            }
        },
        {
            "$unwind": "$user_info"
        },
        {
            "$addFields": {
                "days_due": {
                    "$dateDiff": {
                        "startDate": "$PaymentDate",
                        "endDate": current_date,
                        "unit": "day"
                    }
                }
            }
        },
        {
            "$match": {
                "days_due": {"$gt": 0},
                "user_info.profile.CompanyName": {"$in": names_to_filter}
            }
        },
        {
            "$bucket": {
                "groupBy": "$days_due",
                "boundaries": [0, 30, 60, 90, float('inf')],
                "default": "> 90 days",
                "output": {
                    "TotalPOAmount": {"$sum": {"$divide": ["$POAmount", 100000]}},
                    "CompanyName": {"$first": "$user_info.profile.CompanyName"},
                    "POAmounts": {"$push": {"DaysDue": "$days_due", "POAmount": {"$divide": ["$POAmount", 100000]}}}
                }
            }
        },
        {
            "$group": {
                "_id": "$CompanyName",
                "1_30_days": {
                    "$sum": {"$cond": [{"$eq": ["$_id", 30]}, "$TotalPOAmount", 0]}
                },
                "31_60_days": {
                    "$sum": {"$cond": [{"$eq": ["$_id", 60]}, "$TotalPOAmount", 0]}
                },
                "61_90_days": {
                    "$sum": {"$cond": [{"$eq": ["$_id", 90]}, "$TotalPOAmount", 0]}
                },
                ">90_days": {
                    "$sum": {"$cond": [{"$eq": ["$_id", "> 90 days"]}, "$TotalPOAmount", 0]}
                },
                "GrandTotal": {"$sum": "$TotalPOAmount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "CompanyName": "$_id",
                "1_30_days": 1,
                "31_60_days": 1,
                "61_90_days": 1,
                ">90_days": 1,
                "GrandTotal": 1
            }
        }
    ]

    # Execute the aggregation pipeline
    result = consumer_transaction_collection.aggregate(pipeline)

    # Google Sheets credentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client_gspread = gspread.authorize(creds)

    sheet = client_gspread.open("RSP weekly report").worksheet("RSP_4 RECEIVEABLE")

    sheet.update('A2:F2', [["Company Name", "1-30 Days", "31-60 Days", "61-90 Days", ">90 Days", "Grand Total"]])

    grand_total_1_30 = 0
    grand_total_31_60 = 0
    grand_total_61_90 = 0
    grand_total_90_plus = 0
    grand_total_all = 0

    # Process and update individual rows
    row_index = 3  # Start from the third row
    for doc in result:
        grand_total_1_30 += doc['1_30_days']
        grand_total_31_60 += doc['31_60_days']
        grand_total_61_90 += doc['61_90_days']
        grand_total_90_plus += doc['>90_days']
        grand_total_all += doc['GrandTotal']

        row = [
            [doc['CompanyName'], doc['1_30_days'], doc['31_60_days'], doc['61_90_days'], doc['>90_days'], doc['GrandTotal']]
        ]
        range_name = f'A{row_index}:F{row_index}'
        sheet.update(range_name, row)
        row_index += 1

    # Add the grand total row at the end
    row_total = [
        ["Grand Total", grand_total_1_30, grand_total_31_60, grand_total_61_90, grand_total_90_plus, grand_total_all]
    ]
    sheet.update(f'A{row_index}:F{row_index}', row_total)

    client.close()

def crop_inv():
    mongo_uri = "mongodb://localhost:27017/"
    client = MongoClient(mongo_uri)

    db = client['test']
    purchase_order_collection = db['tblPurchaseOrder']

    locations = [
        "Waranga phata",
        "Narsiphata",
        "Hiremyagiri",
        "Ghungrala",
        "Naregal",
        "Sovenahalli",
        "Kadampur",
        "Borgaon",
        "Bhokar phata",
        "Khanapur",
        "Sasavehalli",
        "Ittagi",
        "Mehkar",
        "Arsikere",
        "Farthabhad",
        "Siddeshwar"
    ]

    pipeline = [
        {
            "$match": {
                "HonorDistrict": {"$in": locations}
            }
        },
        {
            "$unwind": "$PurchaseOrderDetail"
        },
        {
            "$match": {
                "PurchaseOrderDetail.GradeA": {"$gt": 0},
                "UserType": "Godown"
            }
        },
        {
            "$group": {
                "_id": "$PurchaseOrderDetail.CropName",
                "totalGradeA": {"$sum": "$PurchaseOrderDetail.GradeA"},
                "totalProduct": {
                    "$sum": {
                        "$multiply": ["$PurchaseOrderDetail.GradeA", "$PurchaseOrderDetail.GradeAAvgPrice"]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "CropName": "$_id",
                "TotalGradeA": "$totalGradeA",
                "TotalProduct": "$totalProduct"
            }
        }
    ]

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('key.json', scope)
    client_gspread = gspread.authorize(creds)

    sheet = client_gspread.open("RSP weekly report").worksheet("CROP_INV")

    results = purchase_order_collection.aggregate(pipeline)

    grand_total_gradeA = 0
    grand_total_value = 0
    row_index = 2
    for doc in results:
        print(doc)
        value_in_L = round(doc['TotalProduct'] / 100000, 2)
        row = [
            [doc['CropName'], round(doc['TotalGradeA'], 2), value_in_L]
        ]
        range_name = f'A{row_index}:C{row_index}'
        sheet.update(range_name, row)
        row_index += 1
        
        # Update grand totals
        grand_total_gradeA += round(doc['TotalGradeA'], 2)
        grand_total_value += value_in_L

    grand_total_text = "Grand Total"
    row_total = [
        [grand_total_text, grand_total_gradeA, grand_total_value]
    ]
    sheet.update(f'A{row_index}:C{row_index}', row_total)
    print("\nGrand Total:")
    print(f"Total GradeA: {round(grand_total_gradeA,2)}, Total Product (in lakhs): {round(grand_total_value,2)}")

    client.close()

crop_inv()
center_inv()
receviables()