import smtplib
from email.message import EmailMessage
import logging
from typing import List, Optional, Union
import os
from pathlib import Path
from schd.config import EmailConfig

logger = logging.getLogger(__name__)


class EmailService:
    def __init__(self, smtp_server: str, smtp_user: str, smtp_password: str,
                 from_addr: str, smtp_port: int = 25, smtp_starttls: bool = False):
        self.smtp_server = smtp_server
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_addr = from_addr
        self.smtp_port = smtp_port
        self.smtp_starttls = smtp_starttls

    def send_mail(self, title: str, content: str, to_emails: Union[str, List[str]],
                  attachments: Optional[List[str]] = None,
                  content_html: Optional[str] = None,
                  cc_emails: Optional[List[str]] = None,
                  bcc_emails: Optional[List[str]] = None):
        msg = EmailMessage()
        msg['Subject'] = title
        msg['From'] = self.from_addr
        if isinstance(to_emails, str):
            to_emails = [to_emails]
        msg['To'] = ', '.join(to_emails)
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        recipients = to_emails + (cc_emails or []) + (bcc_emails or [])

        # Add text and HTML
        if content_html:
            msg.set_content(content)
            msg.add_alternative(content_html, subtype='html')
        else:
            msg.set_content(content)

        # Attach files
        for filepath in attachments or []:
            file_path = Path(filepath)
            with open(file_path, 'rb') as f:
                file_data = f.read()
                msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_path.name)

        # Send email
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.smtp_starttls:
                server.starttls()
            if self.smtp_user and self.smtp_password:
                server.login(self.smtp_user, self.smtp_password)
            else:
                logger.info('no username/pass, skip logging in.')
            server.send_message(msg, from_addr=self.from_addr, to_addrs=recipients)

    @classmethod
    def from_config(cls, config: 'EmailConfig') -> 'EmailService':
        return cls(
            smtp_server=config.smtp_server,
            smtp_user=config.smtp_user,
            smtp_password=config.smtp_password,
            from_addr=config.from_addr,
            smtp_port=config.smtp_port,
            smtp_starttls=config.smtp_starttls
        )
