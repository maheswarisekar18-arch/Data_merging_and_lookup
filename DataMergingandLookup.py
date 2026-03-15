import  pandas as pd

students_data = {
    'student_id': [101, 102, 103, 104, 105, 106, 107],
    'name': ['Alice', 'Bob', None, 'David', 'Emma', 'Frank', 'Grace'],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com', None, 'emma@email.com', 'frank@email.com', 'grace@email.com'],
    'city': ['Mumbai', 'Delhi', 'Bangalore', 'Mumbai', None, 'Chennai', 'Delhi']
}
enrollments_data = {
    'student_id': [101, 102, 103, 105, 108, 109],
    'course_name': ['Python', 'Data Science', 'Python', 'Machine Learning', 'AI', 'Python'],
    'enrollment_date': ['2024-01-15', '2024-01-20', '2024-02-01', '2024-02-10', '2024-02-15', '2024-03-01']
}
scores_data = {
    'student_id': [101, 102, 104, 105, 106],
    'exam_score': [85, 92, 78, 88, 95]
}
print("===================== TASK 1: Data Preparation =====================")
student_df= pd.DataFrame(students_data)
enrollments_df = pd.DataFrame(enrollments_data)
scores_df = pd.DataFrame(scores_data)
print("Original Students DataFrame:",student_df)
null_percentage = student_df.isnull().sum() / len(student_df) * 100
print("Percentage of null values in each column:", null_percentage)
fill_student_df = student_df.fillna({"city": "Unknown"})
print("\nDataFrame after filling null values in 'city' column:")
print(fill_student_df)
drop_student_df = fill_student_df.dropna(subset=['name'])
print("\nDataFrame after dropping rows with null values in 'name' column:")
print("Student DataFrame after cleaning:")
print(drop_student_df.reset_index(drop=True))
print("\n===================== TASK 2: Join Operations =====================")

inner_join_df = pd.merge(student_df, enrollments_df, on='student_id', how='inner')
print("Inner Join Result:", inner_join_df)

left_join_df = pd.merge(student_df, enrollments_df, on='student_id', how='left')
print("\nLeft Join Result:", left_join_df)

right_join_df = pd.merge(student_df, enrollments_df, on='student_id', how='right')
print("\nRight Join Result:", right_join_df)

outer_join_df = pd.merge(student_df, enrollments_df, on='student_id', how='outer',indicator=True)
print("\nOuter Join Result:", outer_join_df)

print("\n===================== TASK 3: Lookup Operations =====================")
lookup_df = pd.merge(student_df, scores_df, on='student_id', how='left')
print("Lookup Result (Student DataFrame with Exam Scores):", lookup_df)

score_dict = dict(zip(scores_df['student_id'], scores_df['exam_score']))
student_df['exam_score'] = student_df['student_id'].map(score_dict)
print("\nStudents with their Exam Scores (using .map()):")
print(student_df)

score_merge_df = pd.merge(student_df, scores_df, on='student_id', how='left')
print("\nStudents with their Exam Scores (using pd.merge()):")
print(score_merge_df)

def auto_merge(df1, df2, join_type, key_column):
    if join_type not in ['inner', 'left', 'right', 'outer']:
        raise ValueError("Invalid join type. Choose from 'inner', 'left', 'right', 'outer'.")
    merged_df = pd.merge(df1, df2, on=key_column, how=join_type)
    return {
        'result_df': merged_df,
        'row_count': len(merged_df),
        'join_type': join_type
    }
# Example usage of auto_merge function
merged_result = auto_merge(student_df, enrollments_df, 'inner', 'student_id')
print("\nAuto-Merged Result:")
print(merged_result)

