import random
import string
from datetime import datetime, timedelta
import mysql.connector
import csv
from collections import namedtuple
import os


# Define namedtuples for your data structures
Customer = namedtuple('Customer', ['CustomerID', 'FirstName', 'LastName', 'Email', 'Phone'])
Inventory = namedtuple('Inventory', ['InventoryID', 'PartID', 'QuantityInStock'])
Vehicle = namedtuple('Vehicle', ['VehicleID', 'Make', 'Model', 'Year', 'Price'])
Sale = namedtuple('Sale', ['SaleID', 'CustomerID', 'VehicleID', 'SaleDate', 'SalePrice'])
Repair = namedtuple('Repair', ['RepairID', 'VehicleID', 'RepairDate', 'RepairDescription', 'Cost'])
Maintenance = namedtuple('Maintenance', ['MaintenanceID', 'VehicleID', 'MaintenanceDate', 'MaintenanceType', 'Cost'])
Campaign = namedtuple('Campaign', ['CampaignID', 'CampaignName', 'StartDate', 'EndDate'])
Lead = namedtuple('Lead', ['LeadID', 'CampaignID', 'LeadSource', 'LeadDate'])
Loan = namedtuple('Loan', ['LoanID', 'CustomerID', 'LoanAmount', 'InterestRate', 'LoanTerm'])
Part = namedtuple('Part', ['PartID', 'PartName', 'PartDescription', 'PartPrice'])
Employee = namedtuple('Employee', ['EmployeeID', 'FirstName', 'LastName', 'Position'])
Department = namedtuple('Department', ['DepartmentID', 'DepartmentName'])


class CreateTableQueries:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def create_connection(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.connection.cursor()

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def commit(self):
        self.connection.commit()

    def create_tables(self):
        try:
            cursor = self.cursor

            # Create Vehicles table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Vehicles (
                VehicleID INT PRIMARY KEY,
                Make VARCHAR(50),
                Model VARCHAR(50),
                Year INT,
                Price DECIMAL(10, 2)
            )
            """)

            # Create Sales table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Sales (
                SaleID INT PRIMARY KEY,
                CustomerID INT,
                VehicleID INT,
                SaleDate DATE,
                SalePrice DECIMAL(10, 2)
            )
            """)

            # Create Customers table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Customers (
                CustomerID INT PRIMARY KEY,
                FirstName VARCHAR(50),
                LastName VARCHAR(50),
                Email VARCHAR(100),
                Phone VARCHAR(15)
            )
            """)

            # Create Repairs table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Repairs (
                RepairID INT PRIMARY KEY,
                VehicleID INT,
                RepairDate DATE,
                RepairDescription TEXT,
                Cost DECIMAL(10, 2)
            )
            """)

            # Create Maintenance table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Maintenance (
                MaintenanceID INT PRIMARY KEY,
                VehicleID INT,
                MaintenanceDate DATE,
                MaintenanceType VARCHAR(50),
                Cost DECIMAL(10, 2)
            )
            """)

            # Create Campaigns table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Campaigns (
                CampaignID INT PRIMARY KEY,
                CampaignName VARCHAR(100),
                StartDate DATE,
                EndDate DATE
            )
            """)

            # Create Leads table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Leads (
                LeadID INT PRIMARY KEY,
                CampaignID INT,
                LeadSource VARCHAR(50),
                LeadDate DATE
            )
            """)

            # Create Loans table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Loans (
                LoanID INT PRIMARY KEY,
                CustomerID INT,
                LoanAmount DECIMAL(10, 2),
                InterestRate DECIMAL(5, 2),
                LoanTerm INT
            )
            """)

            # Create Parts table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Parts (
                PartID INT PRIMARY KEY,
                PartName VARCHAR(100),
                PartDescription TEXT,
                PartPrice DECIMAL(10, 2)
            )
            """)

            # Create Inventory table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Inventory (
                InventoryID INT PRIMARY KEY,
                PartID INT,
                QuantityInStock INT
            )
            """)

            # Create Employees table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Employees (
                EmployeeID INT PRIMARY KEY,
                FirstName VARCHAR(50),
                LastName VARCHAR(50),
                Position VARCHAR(100)
            )
            """)

            # Create Departments table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS Departments (
                DepartmentID INT PRIMARY KEY,
                DepartmentName VARCHAR(100)
            )
            """)

            self.connection.commit()
            print("\n### Tables created successfully ###")

        except mysql.connector.Error as err:
            print(f"Error: {err}")

        finally:
            pass
            # self.cursor.close() 

#########################################################################################################

# Sample data generation functions 
def random_date(start_date, end_date):
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

# Generate customer data
def generate_customer_data(num_customer):
    customer = []
    for i in range(num_customer):
        customerID = i + 1
        first_name = random_string(6)
        last_name = random_string(6)
        email = random_string(6) + "@gmail.com"
        phone = ''.join(str(random.randint(0, 9)) for _ in range(10))
        customer.append((customerID, first_name, last_name, email, phone))
    return customer

# Generate inventory data
def generate_inventory_data(num_inventory):
    inventory = []
    for i in range(num_inventory):
        inventory_id = i + 1
        part_id = random.randint(1, 10)
        quantityinstock = random.randint(1, 5)
        inventory.append((inventory_id, part_id, quantityinstock))
    return inventory

# Generate vehicle data
def generate_vehicle_data(num_vehicle):
    vehicle = []
    for i in range(num_vehicle):
        vehicle_id = i + 1
        make = random_string(5)
        model = random_string(5)
        year = random.randint(2000, 2023)
        price = round(random.uniform(10000, 50000), 2)
        vehicle.append((vehicle_id, make, model, year, price))
    return vehicle

# Generate vehicle sales data
def generate_vehicle_sales_data(num_sales):
    sales = []
    for sale_id in range(1, num_sales + 1):
        customer_id = random.randint(1, 5)
        vehicle_id = random.randint(1, 10)
        sale_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        sale_price = round(random.uniform(10000, 50000), 2)
        sales.append((sale_id, customer_id, vehicle_id, sale_date, sale_price))
    return sales

# Generate repairs and maintenance data
def generate_after_sales_data(num_repairs, num_maintenance):
    repairs = []
    maintenance = []
    repair_id = 1
    maintenance_id = 1

    for _ in range(num_repairs):
        vehicle_id = random.randint(1, 10)
        repair_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        repair_description = random_string(50)
        cost = round(random.uniform(50, 500), 2)
        repairs.append((repair_id, vehicle_id, repair_date, repair_description, cost))
        repair_id += 1

    for _ in range(num_maintenance):
        vehicle_id = random.randint(1, 10)
        maintenance_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        maintenance_type = random.choice(["Oil Change", "Tire Rotation", "Brake Service"])
        cost = round(random.uniform(50, 500), 2)
        maintenance.append((maintenance_id, vehicle_id, maintenance_date, maintenance_type, cost))
        maintenance_id += 1
    return repairs, maintenance

# Generate campaigns and leads data
def generate_marketing_data(num_campaigns, num_leads):
    campaigns = []
    leads = []
    campaign_id = 1
    lead_id = 1

    for _ in range(num_campaigns):
        campaign_name = random_string(20)
        start_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        end_date = random_date(start_date, datetime(2023, 12, 31))
        campaigns.append((campaign_id, campaign_name, start_date, end_date))
        campaign_id += 1

    for _ in range(num_leads):
        campaign_id = random.randint(1, num_campaigns)
        lead_source = random.choice(["Website", "Social Media", "Email"])
        lead_date = random_date(datetime(2020, 1, 1), datetime(2023, 12, 31))
        leads.append((lead_id, campaign_id, lead_source, lead_date))
        lead_id += 1
    return campaigns, leads

# Generate loans data
def generate_financing_data(num_loans):
    loans = []

    for loan_id in range(1, num_loans + 1):
        customer_id = random.randint(1, 5)
        loan_amount = round(random.uniform(5000, 50000), 2)
        interest_rate = round(random.uniform(2.5, 10.0), 2)
        loan_term = random.randint(12, 60)  # Months
        loans.append((loan_id, customer_id, loan_amount, interest_rate, loan_term))
    return loans

# Generate parts data
def generate_parts_data(num_parts):
    parts = []

    for part_id in range(1, num_parts + 1):
        part_name = random_string(20)
        part_description = random_string(50)
        part_price = round(random.uniform(10, 1000), 2)
        parts.append((part_id, part_name, part_description, part_price))
    return parts

# Generate employees and departments data
def generate_management_data(num_employees, num_departments):
    employees = []
    departments = []
    employee_id = 1
    department_id = 1

    for _ in range(num_employees):
        first_name = random_string(6)
        last_name = random_string(6)
        position = random_string(15)
        employees.append((employee_id, first_name, last_name, position))
        employee_id += 1

    for _ in range(num_departments):
        department_name = random_string(20)
        departments.append((department_id, department_name))
        department_id += 1
    return employees, departments

# Function to insert data into the database
def insert_data(connection, cursor, table_name, data):
    placeholders = ",".join(["%s" for _ in data[0]])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(insert_query, data)
    connection.commit()


def save_data_as_csv(file_path, data, headers):
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write the header
        writer.writerow(headers)

        # Write the data
        for item in data:
            writer.writerow(item)


# Main function to generate and insert sample data
def main():
    num_customer = 20
    num_inventory = 10
    num_vehicle = 20
    num_vehicle_sales = 20
    num_repairs = 15
    num_maintenance = 15
    num_campaigns = 5
    num_leads = 50
    num_loans = 10
    num_parts = 30
    num_employees = 15
    num_departments = 5

    # Create table into the database
    create_tables = CreateTableQueries("localhost", "root", "cdac123", "azure1")
    create_tables.create_connection()


    customer_data = [Customer(*item) for item in generate_customer_data(num_customer)]
    inventory_data = [Inventory(*item) for item in generate_inventory_data(num_inventory)]
    vehicle_data = [Vehicle(*item) for item in generate_vehicle_data(num_vehicle)]
    vehicle_sales_data = [Sale(*item) for item in generate_vehicle_sales_data(num_vehicle_sales)]
    repairs_data, maintenance_data = generate_after_sales_data(num_repairs, num_maintenance)
    campaigns_data, leads_data = generate_marketing_data(num_campaigns, num_leads)
    loans_data = generate_financing_data(num_loans)
    parts_data = [Part(*item) for item in generate_parts_data(num_parts)]
    employees_data, departments_data = generate_management_data(num_employees, num_departments)


    customer_data = generate_customer_data(num_customer)
    inventory_data = generate_inventory_data(num_inventory)
    vehicle_data = generate_vehicle_data(num_vehicle)
    vehicle_sales_data = generate_vehicle_sales_data(num_vehicle_sales)
    repairs_data, maintenance_data = generate_after_sales_data(num_repairs, num_maintenance)
    campaigns_data, leads_data = generate_marketing_data(num_campaigns, num_leads)
    loans_data = generate_financing_data(num_loans)
    parts_data = generate_parts_data(num_parts)
    employees_data, departments_data = generate_management_data(num_employees, num_departments)

    try:
        # Create tables
        create_tables.create_tables()

        
        # Insert data into databases.
        insert_data(create_tables.connection, create_tables.cursor, "Customers", customer_data)
        insert_data(create_tables.connection, create_tables.cursor, "Inventory", inventory_data)
        insert_data(create_tables.connection, create_tables.cursor, "Vehicles", vehicle_data)
        insert_data(create_tables.connection, create_tables.cursor, "Sales", vehicle_sales_data)
        insert_data(create_tables.connection, create_tables.cursor, "Repairs", repairs_data)
        insert_data(create_tables.connection, create_tables.cursor, "Maintenance", maintenance_data)
        insert_data(create_tables.connection, create_tables.cursor, "Campaigns", campaigns_data)
        insert_data(create_tables.connection, create_tables.cursor, "Leads", leads_data)
        insert_data(create_tables.connection, create_tables.cursor, "Loans", loans_data)
        insert_data(create_tables.connection, create_tables.cursor, "Parts", parts_data)
        insert_data(create_tables.connection, create_tables.cursor, "Employees", employees_data)
        insert_data(create_tables.connection, create_tables.cursor, "Departments", departments_data)

        # Commit the changes
        create_tables.commit()

        # Specifing the folder path for saving CSV files
        folder_path = "/home/system/Desktop/AZURE_Assignment/tables/"
        os.makedirs(folder_path, exist_ok=True)


        #save data as a CSV file.
        save_data_as_csv(folder_path + "customer_data.csv", customer_data, ['CustomerID', 'FirstName', 'LastName', 'Email', 'Phone'])
        save_data_as_csv(folder_path + "inventory_data.csv", inventory_data, ['InventoryID', 'PartID', 'QuantityInStock'])
        save_data_as_csv(folder_path + "vehicle_sales_data.csv", vehicle_sales_data, ['SaleID', 'CustomerID', 'VehicleID', 'SaleDate', 'SalePrice'])
        save_data_as_csv(folder_path + "repairs_data.csv", repairs_data, ['RepairID', 'VehicleID', 'RepairDate', 'RepairDescription', 'Cost'])
        save_data_as_csv(folder_path + "maintenance_data.csv", maintenance_data, ['MaintenanceID', 'VehicleID', 'MaintenanceDate', 'MaintenanceType', 'Cost'])
        save_data_as_csv(folder_path + "campaigns_data.csv", campaigns_data, ['CampaignID', 'CampaignName', 'StartDate', 'EndDate'])
        save_data_as_csv(folder_path + "leads_data.csv", leads_data, ['LeadID', 'CampaignID', 'LeadSource', 'LeadDate'])
        save_data_as_csv(folder_path + "loans_data.csv", loans_data, ['LoanID', 'CustomerID', 'LoanAmount', 'InterestRate', 'LoanTerm'])
        save_data_as_csv(folder_path + "parts_data.csv", parts_data, ['PartID', 'PartName', 'PartDescription', 'PartPrice'])
        save_data_as_csv(folder_path + "employees_data.csv", employees_data, ['EmployeeID', 'FirstName', 'LastName', 'Position'])
        save_data_as_csv(folder_path + "departments_data.csv", departments_data, ['DepartmentID', 'DepartmentName'])
        save_data_as_csv(folder_path + "vehicle_data.csv", vehicle_data, ['VehicleID', 'Make', 'Model', 'Year', 'Price'])


        # Print or save the generated data as needed
        print("\n**** Data successfully generated and passed to MySQL ****\n")
        print("\nGenerated data below:-")

        print("\nGenerate customer data:")
        for customer in customer_data:
            print(customer)

        print("\nGenerated Inventory Data:")
        for inventory in inventory_data:
            print(inventory)

        print("Generated Vehicle Sales Data:")
        for sale in vehicle_sales_data:
            print(sale)

        print("\nGenerated Repairs Data:")
        for repair in repairs_data:
            print(repair)

        print("\nGenerated Maintenance Data:")
        for maintenance in maintenance_data:
            print(maintenance)

        print("\nGenerated Campaigns Data:")
        for campaign in campaigns_data:
            print(campaign)

        print("\nGenerated Leads Data:")
        for lead in leads_data:
            print(lead)

        print("\nGenerated Loans Data:")
        for loan in loans_data:
            print(loan)

        print("\nGenerated Parts Data:")
        for part in parts_data:
            print(part)

        print("\nGenerated Employees Data:")
        for employee in employees_data:
            print(employee)

        print("\nGenerated Departments Data:")
        for department in departments_data:
            print(department)

        print("\nGenerated Vehicle Data:")
        for vehicle in vehicle_data:
            print(vehicle)

    finally:
        # Close the cursor and database connection
        create_tables.close_connection()

if __name__ == "__main__":
    main()
