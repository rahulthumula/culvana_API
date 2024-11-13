import logging
import json
import azure.functions as func
import tempfile
import os
import traceback
from shared.cosmos_operations import get_cosmos_manager
from shared.invoice_processor import process_invoice_with_gpt
from shared.models import Invoice

ALLOWED_EXTENSIONS = ['.pdf', '.csv', '.jpg', '.jpeg', '.png']

async def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Function to process invoices via POST request."""
    logging.info('Starting request processing')

    try:
        if req.method != "POST":
            return func.HttpResponse(
                json.dumps({"message": "Use POST method to process invoices."}),
                mimetype="application/json",
                status_code=405
            )

        # Validate user ID
        user_id = req.headers.get('x-user-id')
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": "Missing user ID in headers"}),
                mimetype="application/json",
                status_code=400
            )

        # Validate file upload
        invoice_file = req.files.get('invoice')
        if not invoice_file:
            return func.HttpResponse(
                json.dumps({"error": "No invoice file found in request"}),
                mimetype="application/json",
                status_code=400
            )

        # Check file extension
        filename = invoice_file.filename.lower()
        if not any(filename.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            return func.HttpResponse(
                json.dumps({"error": "Invalid file type. Accepted types are PDF, CSV, JPG, PNG"}),
                mimetype="application/json",
                status_code=400
            )

        # Save the uploded file temporarily
        suffix = os.path.splitext(filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            invoice_file.save(temp_file)
            temp_file_path = temp_file.name

        # Process the invoice and get parsed data
        parsed_invoice_data = await process_invoice_with_gpt(temp_file_path)

        # Check if parsed_invoice_data is a list
        if isinstance(parsed_invoice_data, list):
            # Convert each invoice data dictionary to an Invoice object
            invoices = [Invoice.from_dict(inv_data) for inv_data in parsed_invoice_data]
        else:
            # Single invoice case
            invoices = [Invoice.from_dict(parsed_invoice_data)]

        # Get Cosmos DB manager and store invoices under user_id
        cosmos_manager = await get_cosmos_manager()
        await cosmos_manager.store_invoices(user_id, invoices)

        return func.HttpResponse(
            json.dumps({
                "message": "Invoices processed and stored successfully",
                "user_id": user_id,
                "invoice_numbers": [invoice.Invoice_Number for invoice in invoices]
            }, default=str),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f'Error processing request: {str(e)}')
        logging.error(f'Error traceback: {traceback.format_exc()}')
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )

    finally:
        # Clean up the temporary file
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
