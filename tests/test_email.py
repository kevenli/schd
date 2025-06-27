import os
import unittest
from unittest.mock import patch
from schd.config import EmailConfig
from schd.email import EmailService


class EmailConfigTest(unittest.TestCase):
    def test_empty(self):
        target = EmailConfig()
        self.assertIsNone(target.smtp_server)
        self.assertIsNone(target.smtp_user)
        self.assertIsNone(target.smtp_password)
        self.assertIsNone(target.from_addr)
        self.assertIsNone(target.to_addr)
        self.assertEqual(target.smtp_port, 25)
        self.assertEqual(target.smtp_starttls, False)

    @patch.dict(os.environ, {"SCHD_SMTP_SERVER": "smtp.test.com"}, clear=True)
    def test_env_var_override(self):
        config = EmailConfig.from_dict(dict(
            smtp_server="default.server",
            smtp_user="user",
            smtp_password="pass",
            from_addr="from@example.com",
            to_addr="to@example.com"
        ))

        # value should have been overrided by environ variable
        self.assertEqual(config.smtp_server, "smtp.test.com")
        self.assertEqual(config.smtp_user, "user")  # not from env, uses instance value

    @patch.dict(os.environ, {"SCHD_SMTP_PORT": "589"}, clear=False)
    def test_env_port_type(self):
        # call `from_dict` to let env_var override effective
        config = EmailConfig.from_dict(dict(
            smtp_server="default.server",
            smtp_user="user",
            smtp_password="pass",
            from_addr="from@example.com",
            to_addr="to@example.com"
        ))

        self.assertEqual(config.smtp_port, 589)

    @patch.dict(os.environ, {"SCHD_SMTP_TLS": "true"}, clear=False)
    def test_env_tls_type(self):
        config = EmailConfig.from_dict(dict(
            smtp_server="default.server",
            smtp_user="user",
            smtp_password="pass",
            from_addr="from@example.com",
            to_addr="to@example.com"
        ))

        self.assertEqual(config.smtp_starttls, True)


class EmailServiceTest(unittest.TestCase):
    def test_send_email(self):
        config = EmailConfig.from_dict(dict())
        service = EmailService.from_config(config)
        try:
            recipient = os.environ['SCHD_SMTP_TO']
        except KeyError:
            raise unittest.SkipTest('SCHD_SMTP_TO env not specified, skip test')
        
        service.send_mail('test', 'test_content', recipient)
