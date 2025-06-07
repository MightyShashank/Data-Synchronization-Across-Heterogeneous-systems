import pandas as pd

# Load the CSV
df = pd.read_csv('student_course_grades.csv')

# Specify the column you want to check
columns_to_check = ['student-ID','course-id']

# Check for duplicates
duplicates_exist = df.duplicated(subset=columns_to_check).any()

if duplicates_exist:
    print("Duplicates found in the column.")
else:
    print("No duplicates in the column.")
