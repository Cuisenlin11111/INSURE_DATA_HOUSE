import os
import re


def search_keywords_in_sql_files(directories, keywords):
    # Iterate over each directory in the list of directories
    for directory in directories:
        # Iterate over all files in the current directory
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)

            # Check if it's a file and ends with .sql
            if os.path.isfile(filepath) and filename.endswith('.sql'):
            #if os.path.isfile(filepath) and filename.endswith('.job'):
                with open(filepath, 'r', encoding='utf-8') as file:
                    content = file.read()

                    # Check if all keywords are found in the file content
                    if all(re.search(keyword, content, re.IGNORECASE) for keyword in keywords):
                        print(f"All keywords {keywords} found in file {filename}.")


if __name__ == '__main__':
    directories = [
        r'E:\项目代码\项目管理\运营脚本\业务报表\history',
        r'E:\项目代码\项目管理\运营脚本\日常报表',
        r'E:\因朔桔科技数仓架构文档\report',
        r'E:\项目代码\项目管理\运营脚本\每月报表',
        r'E:\报表脚本'
        #  r'C:\Users\hjkj-028\Desktop\杨招玉'
    ]
    # keywords = ['case_status','insurance_company_review_record']
    keywords = ['门慢类型']
    search_keywords_in_sql_files(directories, keywords)


