import os
import ssl
import smtplib
from email.message import EmailMessage

class email_alert():
    def __init__(self):
        self.email_sender = os.getenv('snowflake_user')
        self.email_password = os.getenv("gmail_pass")
        #self.email_receiver = os.getenv('email_receiver')
        #self.email_receiver = 'pmi-discrepancies@integralads.com'
        #self.email_receiver = 'dfuentes@integralads.com,jcescobar@integralads.com'
        #self.email_receiver = 'techacctmanagement@integralads.com'
        self.email_receiver = os.getenv('email_receiver')
        self.context = ssl.create_default_context()

    def send_email(self,alert, subject):
        em = EmailMessage()
        em['From'] = self.email_sender
        em['To'] = self.email_receiver
        em['Subject'] = subject

        body = f"""{alert}
        """
        em.set_content(body)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=self.context) as smtp:
            smtp.login(self.email_sender, self.email_password)
            smtp.sendmail(self.email_sender, self.email_receiver, em.as_string())
        
        return