#pip3 install mysql-connector-python
import mysql.connector
import csv
from configparser import ConfigParser


def read_db_config(filename='config.ini', section='mysql'):
    """ Read database configuration file and return a dictionary object
    
    Args:
       filename: name of the configuration file
       section: section of database configuration
    
    Return: 
        a dictionary of database parameters

    Reference:
        https://www.mysqltutorial.org/python-connecting-mysql-databases/
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to mysql
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db



def get_db_connection():
    """ Connect to MYSQL database """

    connection = None
    config = read_db_config()

    try:
        connection = mysql.connector.connect(**config)

    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    
    return connection
        
    
def create_third_party_sales(connection):
    """Create thrid_party_sales table
    
    Args:
            connection (): database connection
    """

    cur = connection.cursor()

    #Drop table if exists, and create it new
    stmt_drop = "Drop Table IF EXISTS sales"
    cur.execute(stmt_drop)

    stmt_create = (
        "CREATE TABLE sales ("
        "ticket_id INT NOT NULL, "
        "trans_date DATE, "
        "event_id INT, "
        "event_name VARCHAR(50), "
        "event_date DATE, "
        "event_type VARCHAR(10), "
        "event_city VARCHAR(20), "
        #"event_addr VARCHAR(100), "
        "customer_id INT, "
        "price DECIMAL, "
        "num_tickets INT, "
        "PRIMARY KEY (ticket_id))"
    )

    cur.execute(stmt_create)
    connection.commit()
    cur.close()
    return



def load_third_party(connection, file_path_csv):
    """Load csv "thrid_party_sales" into sales table
    
    Args:
            connection (): database connection
            file_path_csv(str): CSV file path
    """
    
    
    cursor = connection.cursor()
    # [Iterate through the CSV file and execute insert statement]

    with open(file_path_csv, newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    insertstmt = "INSERT INTO sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    #print(data)

    for line in data:
        #print(line)
        cursor.execute(insertstmt, line)
        connection.commit()

    cursor.close()
    return


def query_popular_tickets(connection):
    """Get the most popular ticket in the past month
    
    Args:
            connection (): database connection
    """
    
    sql_statement = """SELECT event_name FROM (
                        SELECT event_name, SUM(num_tickets) AS Total FROM ticket.sales 
                        WHERE trans_date >= DATE_ADD('2020-08-31', INTERVAL -1 MONTH)
                        GROUP BY ticket_id
                    ) f ORDER BY Total DESC LIMIT 3"""
    
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()

    print("Here are the most popular tickets in the past month:")
    for record in list(records):
        print('- ',record[0])

    cursor.close()
    return records



connection = get_db_connection()
create_third_party_sales(connection)
load_third_party(connection,"third_party_sales_1.csv" )
query_popular_tickets(connection)


