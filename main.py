import mysql.connector
import pandas as pd
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, database_name=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=database_name,
        )
        print("Підключення до БД MySQL пройшло успішно")
    except Error as e:
        print(f"Виникла помилка: {e}")
    return connection


def create_database(connection):
    try:
        cursor = connection.cursor()
        database_name = "Store_lab6"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База даних '{database_name}' створена успішно")
    except Error as e:
        print(f"Виникла помилка: {e}")


def create_tables(connection):
    try:
        database_name = "Store_lab6"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        # Створення таблиці "Customers"
        cursor.execute('''
CREATE TABLE Customers (
    Customer_ID INT AUTO_INCREMENT PRIMARY KEY,
    Company_Name TEXT,
    Legal_or_Natural_Person TEXT CHECK (Legal_or_Natural_Person IN ('юридична', 'фізична')),
    Address TEXT,
    Phone TEXT,
    Contact_Person TEXT,
    Bank_Account TEXT
);
''')

        # Створення таблиці "Products"
        cursor.execute('''
CREATE TABLE Products (
    Product_ID INT AUTO_INCREMENT PRIMARY KEY,
    Product_Name TEXT,
    Price REAL,
    Quantity_In_Store INTEGER
);
''')

        # Створення таблиці "Sales"
        cursor.execute('''
CREATE TABLE Sales (
    Sale_ID INTEGER AUTO_INCREMENT PRIMARY KEY,
    Sale_Date DATE,
    Customer_ID INTEGER,
    Product_ID INTEGER,
    Sold_Quantity INTEGER,
    Discount REAL CHECK (Discount >= 3 AND Discount <= 20),
    Payment_Method TEXT CHECK (Payment_Method IN ('готівковий', 'безготівковий')),
    Need_Delivery BOOLEAN,
    Delivery_Cost REAL,
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID),
    FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
);
        ''')

        connection.commit()
        print("Таблиці створені успішно")
    except Error as e:
        print(f"Виникла помилка: {e}")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запит виконаний успішно.")
    except Error as e:
        print(f"Виникла помилка при виконанні запиту: {e}")


def execute_query_print(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        # Перетворення результату в об'єкт DataFrame
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])

        # Виведення DataFrame
        print(df)

        print("Запит виконано успішно.")
    except Error as e:
        print(f"Виникла помилка при виконанні запиту: {e}")
    finally:
        cursor.close()


def display(conn):
    connect = conn
    while True:
        print("Список запитів:")
        print("1. Відобразити всі продажі, які були оплачені готівкою та відсортувати їх по назві клієнта за алфавітом")
        print("2. Відобразити всі продажі, по яких потрібна була доставка")
        print("3. Порахувати суму та суму з урахуванням скидки, яку треба сплатити кожному клієнту")
        print("4. Відобразити всі покупки вказаного клієнта (запит з параметром)")
        print("5. Порахувати кількість покупок, які скоїв кожен клієнт (підсумковий запит)")
        print("6. Порахувати суму, яку сплатив кожен клієнт (перехресний запит)")
        print("0. Вийти")

        query_number = input("Введіть номер запиту, який ви хочете виконати (або 0 для виходу): ")
        if query_number == "0":
            break

        elif query_number == "1":
            # Відобразити всі продажі, які були оплачені готівкою та відсортувати їх по назві клієнта за алфавітом:
            query = ("""
            SELECT C.Company_Name, S.Sale_Date, S.Payment_Method
            FROM Customers AS C
            JOIN Sales AS S ON C.Customer_ID = S.Customer_ID
            WHERE S.Payment_Method = 'готівковий'
            ORDER BY C.Company_Name;
        
            """)
            execute_query_print(connect, query)

        elif query_number == "2":
            # Відобразити всі продажі, по яких потрібна була доставка
            query = ("""
            SELECT C.Company_Name, S.Sale_Date, S.Need_Delivery
            FROM Customers AS C
            JOIN Sales AS S ON C.Customer_ID = S.Customer_ID
            WHERE S.Need_Delivery = 1;
        
                """)

            execute_query_print(connect, query)

        elif query_number == "3":
            # Порахувати суму та суму з урахуванням скидки, яку треба сплатити кожному клієнту
            query = (f"""
            SELECT C.Customer_ID, C.Company_Name, COALESCE(SUM(P.Price * S.Sold_Quantity), 0) AS Total_Amount, 
            COALESCE(SUM(P.Price * S.Sold_Quantity * (1 - S.Discount / 100)), 0) AS Total_Amount_With_Discount
            FROM Customers AS C
            LEFT JOIN Sales AS S ON C.Customer_ID = S.Customer_ID
            LEFT JOIN Products AS P ON S.Product_ID = P.Product_ID
            GROUP BY C.Customer_ID, C.Company_Name;
            """)

            execute_query_print(connect, query)

        elif query_number == "4":
            # Відобразити всі покупки вказаного клієнта:
            print("Відобразити всі покупки вказаного клієнта (запит з параметром):")
            customer_id = int(input())  # Замініть на бажану марку автомобіля
            query = (f"""
            SELECT S.Customer_ID, P.Product_Name, S.Sale_Date, S.Sold_Quantity
            FROM Sales AS S
            JOIN Products AS P ON S.Product_ID = P.Product_ID
            WHERE S.Customer_ID = '{customer_id}';
        
                """)

            execute_query_print(connect, query)

        elif query_number == "5":
            # Порахувати кількість покупок, які скоїв кожен клієнт (підсумковий запит):

            query = ("""
                SELECT C.Customer_ID, C.Company_Name, COUNT(S.Sale_ID) AS Purchase_Count
                FROM Customers AS C
                LEFT JOIN Sales AS S ON C.Customer_ID = S.Customer_ID
                GROUP BY C.Customer_ID, C.Company_Name;
                """)
            execute_query_print(connect, query)

        elif query_number == "6":
            # Порахувати суму, яку сплатив кожен клієнт (перехресний запит):

            query = ("""
            SELECT C.Customer_ID, C.Company_Name, SUM(P.Price * S.Sold_Quantity) AS Total_Payment
            FROM Customers AS C
            LEFT JOIN Sales AS S ON C.Customer_ID = S.Customer_ID
            LEFT JOIN Products AS P ON S.Product_ID = P.Product_ID
            GROUP BY C.Customer_ID, C.Company_Name;
            """)
            execute_query_print(connect, query)


def insert_tables(conn):
    conn = conn
    query = (f"""
-- Додавання клієнтів
INSERT INTO Customers (Company_Name, Legal_or_Natural_Person, Address, Phone, Contact_Person, Bank_Account)
VALUES
    ('Клієнт1', 'юридична', 'Адреса1', '+1234567890', 'Контактна особа1', 'Рахунок1'),
    ('Клієнт2', 'фізична', 'Адреса2', '+9876543210', 'Контактна особа2', 'Рахунок2'),
    ('Клієнт3', 'юридична', 'Адреса3', '+1112223333', 'Контактна особа3', 'Рахунок3'),
    ('Клієнт4', 'фізична', 'Адреса4', '+4445556666', 'Контактна особа4', 'Рахунок4');
""")
    execute_query(conn, query)

    query = (f"""
     -- Додавання товарів
INSERT INTO Products (Product_Name, Price, Quantity_In_Store)
VALUES
    ('Товар1', 10.99, 100),
    ('Товар2', 19.99, 50),
    ('Товар3', 5.99, 200),
    ('Товар4', 25.99, 75),
    ('Товар5', 7.99, 150),
    ('Товар6', 14.99, 90),
    ('Товар7', 8.49, 120),
    ('Товар8', 12.49, 80),
    ('Товар9', 17.99, 60),
    ('Товар10', 9.99, 110);
     """)
    execute_query(conn, query)

    query = ("""
-- Додавання продажів
INSERT INTO Sales (Sale_Date, Customer_ID, Product_ID, Sold_Quantity, Discount, Payment_Method, Need_Delivery, Delivery_Cost)
VALUES
    ('2023-01-01', 1, 2, 5, 10.0, 'готівковий', 0, 0.0),
    ('2023-01-02', 2, 4, 3, 5.0, 'безготівковий', 1, 15.0),
    ('2023-01-03', 1, 1, 2, 7.5, 'готівковий', 0, 0.0),
    ('2023-01-04', 3, 6, 7, 8.0, 'безготівковий', 0, 0.0),
    ('2023-01-05', 4, 3, 4, 12.0, 'готівковий', 1, 25.0),
    ('2023-01-06', 2, 9, 1, 15.0, 'готівковий', 1, 18.0),
    ('2023-01-07', 3, 10, 8, 6.0, 'безготівковий', 0, 0.0),
    ('2023-01-08', 1, 5, 6, 9.0, 'готівковий', 0, 0.0),
    ('2023-01-09', 4, 2, 3, 20.0, 'безготівковий', 1, 22.0),
    ('2023-01-10', 3, 7, 5, 11.0, 'готівковий', 0, 0.0),
    ('2023-01-11', 1, 8, 4, 16.0, 'готівковий', 1, 12.0),
    ('2023-01-12', 2, 1, 3, 14.0, 'безготівковий', 0, 0.0),
    ('2023-01-13', 4, 4, 2, 10.0, 'готівковий', 0, 0.0),
    ('2023-01-14', 2, 6, 5, 7.0, 'безготівковий', 0, 0.0),
    ('2023-01-15', 1, 3, 4, 18.0, 'готівковий', 1, 15.0),
    ('2023-01-16', 3, 9, 2, 10.0, 'готівковий', 0, 0.0),
    ('2023-01-17', 4, 2, 1, 5.0, 'безготівковий', 1, 20.0),
    ('2023-01-18', 2, 5, 3, 12.0, 'готівковий', 0, 0.0),
    ('2023-01-19', 1, 8, 7, 15.0, 'готівковий', 0, 0.0);
""")
    execute_query(conn, query)
    conn.close()


if __name__ == "__main__":
    config = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
    }
    config1 = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
        'database_name': 'Store_lab6',
    }
    # Підключення до сервера MySQL
    conn = create_connection(**config)

    # Створення БД
    create_database(conn)

    # Створення таблиць
    create_tables(conn)
    conn.close()
    conn = create_connection(**config1)

    # Вставка таблиць
    insert_tables(conn)

    #Виведення
    conn = create_connection(**config1)
    display(conn)

    conn.close()

    print("База даних та таблиці створені успішно.")
