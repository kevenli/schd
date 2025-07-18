"""
scsendmail - Send email via command line using EmailService

Usage:
    scsendmail --to someone@example.com --title "Report" --content "Text body"
    scsendmail --to a@example.com --content-html-file ./body.html -a report.pdf
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional
import sys

from schd.config import read_config, ConfigFileNotFound, EmailConfig
from schd.email import EmailService


def parse_recipients(values: Optional[List[str]]) -> List[str]:
    if not values:
        return []
    emails = []
    for val in values:
        emails.extend(email.strip() for email in val.split(',') if email.strip())
    return emails


def main():
    parser = argparse.ArgumentParser(description='scsendmail command')
    parser.add_argument('--title', default='report', help='Email subject')
    parser.add_argument('--content', default='no content', help='Plain text content')
    parser.add_argument('--content-html-file', help='Path to HTML file for HTML content')
    parser.add_argument('--to', dest='recipients', action='append', help='To recipients (comma-separated or multiple flags)')
    parser.add_argument('--cc', action='append', help='CC recipients (comma-separated or multiple flags)')
    parser.add_argument('--bcc', action='append', help='BCC recipients (comma-separated or multiple flags)')
    parser.add_argument('--add-attach', '-a', action='append', dest='attachments', help='Attachment file paths')
    parser.add_argument('--debug', action='store_true', default=False, help='Print instead of sending')
    parser.add_argument('--config')
    parser.add_argument('--loglevel', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel.upper(),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Load HTML content if provided
    html_content = None
    if args.content_html_file:
        try:
            html_content = Path(args.content_html_file).read_text(encoding='utf-8')
        except Exception as e:
            print(f"Failed to read HTML content file: {e}", file=sys.stderr)
            sys.exit(1)

    # Load config from environment or config file
    try:
        schd_config = read_config(args.config)
        email_config = schd_config.email
    except ConfigFileNotFound:
        if args.config:
            print(f"Config file not found: {args.config}", file=sys.stderr)
            return sys.exit(1)
        email_config = EmailConfig.from_dict({})
        
    logging.debug(email_config)
    service = EmailService.from_config(email_config)

    to_emails = parse_recipients(args.recipients) or [email_config.to_addr]
    cc_emails = parse_recipients(args.cc)
    bcc_emails = parse_recipients(args.bcc)
    attachments = args.attachments or []

    if args.debug:
        print("DEBUG MODE: Email will not be sent")
        print(f"Subject: {args.title}")
        print(f"To: {to_emails}")
        print(f"CC: {cc_emails}")
        print(f"BCC: {bcc_emails}")
        print(f"Attachments: {attachments}")
        print(f"Content: {args.content}")
        print(f"HTML Content File: {args.content_html_file}")
    else:
        service.send_mail(
            title=args.title,
            content=args.content,
            content_html=html_content,
            to_emails=to_emails,
            cc_emails=cc_emails,
            bcc_emails=bcc_emails,
            attachments=attachments
        )


if __name__ == '__main__':
    main()
