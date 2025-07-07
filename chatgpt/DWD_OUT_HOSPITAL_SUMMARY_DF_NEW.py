import pymysql
from datetime import datetime, date, timedelta
import requests
import base64
import concurrent.futures



# 阿里云rds for MySQL的相关参数
host = 'rr-uf636kzrob17osr7j.mysql.rds.aliyuncs.com'
port = 3306  # 或者使用实际提供的端口号
user = 'prd_readonly'
password = '7MmY^nEJ3fQhysj=B'
db_name = 'claim_prd'

# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')  # 可根据实际情况指定字符集

now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")


# 获取今天的日期
today = date.today()

# 计算7天前的日期
seven_days_ago = today   - timedelta(days=10)

# 计算昨天的日期
yesterday = today  - timedelta(days=5)

# 格式化日期为字符串，符合SQL语句中日期格式的要求
seven_days_ago_str = seven_days_ago.strftime('%Y-%m-%d')
yesterday_str = yesterday.strftime('%Y-%m-%d')
try:
    # 创建游标对象
    with connection.cursor() as cursor:

        # 将结果数据写入结果表
        sql_13 = f"""
        select C_APP_NO,
       concat(replace(IMAGE_UPLOAD_PATH,'/mnt/insuresmart/prd/work','https://nas-tmp.insuresmart.tech/nas/claim/prd'),IMAGE_NAME)  as url,
       replace(substr(CREATE_TIME,1,10),'-','')  data_dt
from claim_prd.clm_case_image_track  where  IMAGE_TYPE='17'  and  C_APP_NO is not null
 and  substr(CREATE_TIME,1,10)  >= '{seven_days_ago_str}'  and substr(CREATE_TIME,1,10) <= '{yesterday_str}'  ;

        """
        cursor.execute(sql_13)


        result = [dict(zip(('C_APP_NO', 'url','data_dt'), row)) for row in cursor.fetchall()]



finally:
    # 关闭数据库连接
    connection.close()



def img_url_to_base64(img_url):
    # 发送GET请求获取图片
    response = requests.get(img_url)
    response.raise_for_status()  # 确保请求成功

    # 读取图片内容为字节流
    img_bytes = response.content

    # 将字节流转换为Base64编码
    b64 = base64.b64encode(img_bytes).decode('utf-8')

    return b64


def send_to_ocr(base64_img, ocr_url):
    # 发送POST请求到OCR API
    response = requests.post(url=ocr_url, data={'img_b64': base64_img})
    response.raise_for_status()  # 确保请求成功

    # 解析返回的JSON数据
    result = response.json()

    # 假设返回的JSON结构中有'data'和'raw_out'键
    raw_out_content = result.get('data', {}).get('raw_out', [])

    text_concatenated = ";".join([item[1] for item in raw_out_content])
    return text_concatenated


##ocr服务api地址
ocr_url = 'http://172.19.14.141:8080/api/ocr'
result_total = []


def async_tupian_jiexi(result, max_workers=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(img_url_to_base64_and_send_to_ocr, aa, ocr_url): aa for aa in result}
        result_total = []

        for future in concurrent.futures.as_completed(futures):
            aa = futures[future]
            try:
                text = future.result()
                aa['text'] = text
                result_total.append(aa)
            except Exception as exc:
                print(f"Exception occurred while processing image for C_APP_NO: {aa['C_APP_NO']}, Error: {exc}")

    return result_total


def img_url_to_base64_and_send_to_ocr(aa, ocr_url):
    img_url = aa['url']
    img_b64 = img_url_to_base64(img_url)
    return send_to_ocr(img_b64, ocr_url)


result_tt = async_tupian_jiexi(result, max_workers=10)

host = 'am-uf61afo16ust6f600167320.ads.aliyuncs.com'
port = 3306  # 或者使用实际提供的端口号
user = 'claim_all'
password = 'S#5DH1ar%*1n'
db_name = 'claim_ods'

# 创建连接
connection = pymysql.connect(host=host,
                             port=port,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8mb4')

now = datetime.now()
start_time = now.strftime("%Y-%m-%d %H:%M:%S")
print("数据写入开始时间：", start_time)

try:
    # 创建游标对象
    with connection.cursor() as cursor:

        sql = "INSERT INTO CLAIM_DWD.DWD_OUT_HOSPITAL_SUMMARY_DF_NEW (`C_APP_NO`, `url`, `text`,`dt`) VALUES (%s, %s, %s,%s)"
        batch = []
        for data in result_tt:
            batch.append((data['C_APP_NO'], data['url'], data['text'],data['data_dt']))
            if len(batch) >= 10000:
                # 执行批量插入
                cursor.executemany(sql, batch)
                connection.commit()
                batch = []  # 清空当前批次

        if batch:
            # 处理剩余的数据
            cursor.executemany(sql, batch)
            connection.commit()

        connection.commit()
        # 获取结果（如果适用）
        print("success")


finally:
    # 关闭数据库连接
    connection.close()
    end = datetime.now()
    end_time = end.strftime("%Y-%m-%d %H:%M:%S")
    # 输出当前时间
    print("数据写入结束时间：", end_time)








