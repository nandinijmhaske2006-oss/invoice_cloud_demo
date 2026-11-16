
"""InvoiceProcessor Lambda function.

Trigger: S3 ObjectCreated events for invoice JSON files.
Reads the invoice, calculates GST, saves a summary and sends an SNS notification.
"""

import json
import os
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")

GST_RATE = 0.18
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")


def lambda_handler(event, context):
    results = []

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        print(f"New invoice detected: s3://{bucket}/{key}")

        invoice = json.loads(
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        )

        # Read the customer phone number
        phone = invoice.get("phone", "")

        # Calculate GST
        gst = round(invoice["amount"] * GST_RATE, 2)

        # Create processed summary
        summary = {
            "invoice_id": invoice["invoice_id"],
            "customer": invoice["customer"],
            "phone": phone,
            "item": invoice["item"],
            "amount": invoice["amount"],
            "gst_18_percent": gst,
            "grand_total": round(invoice["amount"] + gst, 2),
            "source_file": key,
            "processed_by": context.function_name if context else "local-test",
        }

        out_key = f"processed/{invoice['invoice_id']}_summary.json"

        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )

        print(f"Summary written: s3://{bucket}/{out_key}")

        # Send email notification through SNS
        if SNS_TOPIC_ARN:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"New invoice {invoice['invoice_id']}",
                Message=(
                    f"Invoice {invoice['invoice_id']} for {invoice['customer']}\n"
                    f"Phone: {phone}\n"
                    f"Item: {invoice['item']} x {invoice['quantity']}\n"
                    f"Amount: Rs {invoice['amount']:.2f}\n"
                    f"GST (18%): Rs {gst:.2f}\n"
                    f"Grand total: Rs {summary['grand_total']:.2f}\n"
                ),
            )

            print("Email notification sent through SNS")

        results.append(summary)

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
