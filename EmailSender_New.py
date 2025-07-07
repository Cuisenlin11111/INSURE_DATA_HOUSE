from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, date, timedelta


class EmailSender_New:
    def __init__(self, sender, password, recipients, smtp_server, smtp_port):
        self.sender = sender
        self.password = password
        self.recipients = recipients
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email_with_attachment(self, subject, message, excel_filename):
        # 构建邮件
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = ', '.join(self.recipients)

        # 添加文本内容
        text = message
        msg.attach(MIMEText(text, 'plain'))

        # 添加附件
        file_name = excel_filename.split("/")[-1]  # 获取文件名部分，假设路径分隔符是 "/"
        with open(excel_filename, "rb") as file:
            attachment = MIMEApplication(file.read(), _subtype="xlsx")
            attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
            msg.attach(attachment)

        # 发送邮件
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.recipients, msg.as_string())
            print("邮件已发送！")
        except Exception as e:
            print(f"发送邮件时发生错误: {e}")

    def send_email(self, subject, message):
        # 构建邮件
        msg = MIMEText(message, 'plain')
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = ', '.join(self.recipients)

        # 发送邮件
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.recipients, msg.as_string())
            print("邮件已发送！")
        except Exception as e:
            print(f"发送邮件时发生错误: {e}")

    def send_email_with_attachments(self, subject, message, excel_filenames):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.sender
        msg['To'] = ', '.join(self.recipients)

        text = message
        msg.attach(MIMEText(text, 'plain'))

        for filename in excel_filenames:
            file_name = filename.split("/")[-1]  # 获取文件名部分，假设路径分隔符是 "/"
            with open(filename, "rb") as file:
                attachment = MIMEApplication(file.read(), _subtype="xlsx")
                attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
                msg.attach(attachment)

        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.recipients, msg.as_string())
            print("邮件已发送！")
        except Exception as e:
            print(f"发送邮件时发生错误: {e}")