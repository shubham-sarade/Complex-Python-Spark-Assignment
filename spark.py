from pyspark.sql import SparkSession
from pyspark.sql.functions import expr 

# Initialize a Spark session
spark = SparkSession.builder.appName("FlattenedDataTransformation").getOrCreate()

# Define the paths to the CSV files for each table
customer_path = "file:///home/system/Desktop/AZURE_Assignment/tables/customer_data.csv"
inventory_path = "file:///home/system/Desktop/AZURE_Assignment/tables/inventory_data.csv"
vehicle_sales_path = "file:///home/system/Desktop/AZURE_Assignment/tables/vehicle_sales_data.csv"
repairs_path = "file:///home/system/Desktop/AZURE_Assignment/tables/repairs_data.csv"
maintenance_path = "file:///home/system/Desktop/AZURE_Assignment/tables/maintenance_data.csv"
campaigns_path = "file:///home/system/Desktop/AZURE_Assignment/tables/campaigns_data.csv"
leads_path = "file:///home/system/Desktop/AZURE_Assignment/tables/leads_data.csv"
loans_path = "file:///home/system/Desktop/AZURE_Assignment/tables/loans_data.csv"
parts_path = "file:///home/system/Desktop/AZURE_Assignment/tables/parts_data.csv"
employees_path = "file:///home/system/Desktop/AZURE_Assignment/tables/employees_data.csv"
departments_path = "file:///home/system/Desktop/AZURE_Assignment/tables/departments_data.csv"
vehicle_path = "file:///home/system/Desktop/AZURE_Assignment/tables/vehicle_data.csv"

print("level1")

# Read CSV data into DataFrames
customers_df = spark.read.csv(customer_path, header=True)
inventory_df = spark.read.csv(inventory_path, header=True)
vehicle_sales_df = spark.read.csv(vehicle_sales_path, header=True)
repairs_df = spark.read.csv(repairs_path, header=True)
maintenance_df = spark.read.csv(maintenance_path, header=True)
campaigns_df = spark.read.csv(campaigns_path, header=True)
leads_df = spark.read.csv(leads_path, header=True)
loans_df = spark.read.csv(loans_path, header=True)
parts_df = spark.read.csv(parts_path, header=True)
employees_df = spark.read.csv(employees_path, header=True)
departments_df = spark.read.csv(departments_path, header=True)
vehicle_df = spark.read.csv(vehicle_path, header=True)




# Add a new column 
parts_df = parts_df.withColumn("VehicleId", expr("cast(rand() * 5 + 1 as int)"))
leads_df = leads_df.withColumn("CustomerID", expr("cast(rand() * 5 + 1 as int )"))
employees_df = employees_df.withColumn("SaleID", expr("cast(rand() * 5 + 1 as int )"))
departments_df = departments_df.withColumn("EmployeeID", expr("cast(rand() * 5 + 1 as int)"))



departments_df.show()


print("level2")

# Perform necessary joins to create the flattened table
flattened_data = vehicle_sales_df \
    .join(customers_df, "CustomerID") \
    .join(repairs_df.selectExpr("VehicleID","RepairID","RepairDate","RepairDescription", "Cost as Repaired_Cost"), "VehicleID") \
    .join(vehicle_df, "VehicleID")\
    .join(loans_df, "CustomerID") \
    .join(maintenance_df.selectExpr("VehicleID","MaintenanceID","MaintenanceDate","MaintenanceType", "Cost as maintenance_Cost"), "VehicleID") \
    .join(parts_df, "VehicleID") \
    .join(inventory_df, "PartID") \
    .join(leads_df, "CustomerID")\
    .join(campaigns_df, "CampaignID")\
    .join(employees_df.selectExpr("saleID","EmployeeID","FirstName as emp_FirstName","LastName as emp_LastName","Position"), "SaleID") \
    .join(departments_df, "EmployeeID") \

    

flattened_data.show()


print("level3")
# Save the flattened data as a CSV file
flattened_data.write.csv("file:///home/system/Desktop/AZURE_Assignment/flattened_data.csv", header=True, mode="overwrite")


print("level4")
# Stop the Spark session
spark.stop()
