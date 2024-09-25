import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    #select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):

    #print("TODO: load_users")
    with open(file_path,'r') as file:
        record=csv.DictReader(file)
        
        for r in record:
            if r['firstName'].isalpha() and r['lastName'].isalpha():
                f=r['firstName'].strip()
                l=r['lastName'].strip()
                cursor.execute('''insert into users(firstName,lastName) values(?,?)''',(f,l))

# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):

    #print("TODO: load_call_logs")
    '''with open(file_path,'r') as file:
        record=csv.DictReader(file)
        for r in record:
            phone = r.get('phoneNumber', '').strip()
            start = r.get('startTime', '').strip()
            end = r.get('endTime', '').strip()
            direction = r.get('direction', '').strip()
            user = r.get('userId', '').strip()'''
            
            #if phone and start and end and (direction in ['inbound', 'outbound']) and user.isdigit():
                #cursor.execute('''INSERT INTO callLogs(phoneNumber, startTime, endTime, direction, userId)
                                     # VALUES(?, ?, ?, ?, ?)''',(phone, start, end, direction, user))
    
    with open(file_path, 'r', newline='') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Skip header row
        for r in reader:
            # Ensure the row has exactly the right number of columns
            if len(r) != 5:
                continue

            phone_number = r[0].strip()
            try:
                start_time = int(r[1].strip())
                end_time = int(r[2].strip())
            except ValueError:
                print(f"Skipping row with non-numeric startTime or endTime: {r}")
                continue

            direction = r[3].strip()
            try:
                user_id = int(r[4].strip())
            except ValueError:
                print(f"Skipping row with non-numeric userId: {r}")
                continue

            # Validate that direction is either "inbound" or "outbound"
            if direction not in {"inbound", "outbound"}:
                continue

            # Ensure start time is before end time
            if start_time >= end_time:
                print(f"Skipping row where startTime is not before endTime: {r}")
                continue

            try:
                cursor.execute('''INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId) 
                                  VALUES (?, ?, ?, ?, ?)''', 
                               (phone_number, start_time, end_time, direction, user_id))
            except Exception as e:
                print(f"Error inserting row {r}: {e}")
                
# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):

    #print("TODO: write_user_analytics")
    cursor.execute("SELECT userId,AVG(endTime - startTime) AS avgDuration, COUNT(callId) AS numCalls FROM callLogs GROUP BY userId")
    
    with open(csv_file_path, "w", newline='') as file:
        w = csv.writer(file)
        w.writerow(['userId', 'avgDuration', 'numCalls'])
        for r in cursor.fetchall():
            w.writerow(r)


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):

    #print("TODO: write_ordered_calls")
    cursor.execute("SELECT * FROM callLogs ORDER BY userId, startTime")
    
    with open(csv_file_path, "w", newline='') as file:
        w = csv.writer(file)
        w.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
        for r in cursor.fetchall():
            w.writerow(r)


# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
