import os
import json
import time
import sys
import re
from openai import AsyncOpenAI
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from datetime import datetime
import csv
import asyncio 


FORM_RECOGNIZER_ENDPOINT = os.environ["AZURE_FORM_RECOGNIZER_ENDPOINT"]
FORM_RECOGNIZER_KEY = os.environ["AZURE_FORM_RECOGNIZER_KEY"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Azure Form Recognizer Setup
document_analysis_client = DocumentAnalysisClient(
    endpoint=FORM_RECOGNIZER_ENDPOINT,
    credential=AzureKeyCredential(FORM_RECOGNIZER_KEY)
)

# OpenAI Setup
client = AsyncOpenAI(api_key=OPENAI_API_KEY)

def remove_non_printable(text):
    """Remove non-printable characters from text"""
    return ''.join(char for char in text if char.isprintable() or char.isspace())

def Removingunwanted_from_Json(Jsonfile):
    """Extract JSON objects from text with improved error handling"""
    text =Jsonfile.strip()
    
    # Remove code block markers if present
    if text.startswith("```") and text.endswith("```"):
        text = text[3:-3].strip()
    if text.lower().startswith("json"):
        text = text[4:].strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        json_objects = []
        start = 0
        while True:
            try:
                start = text.index('{', start)
                brace_count = 1
                pos = start + 1
                
                while brace_count > 0 and pos < len(text):
                    if text[pos] == '{':
                        brace_count += 1
                    elif text[pos] == '}':
                        brace_count -= 1
                    pos += 1
                
                if brace_count == 0:
                    json_str = text[start:pos]
                    try:
                        json_obj = json.loads(json_str)
                        json_objects.append(json_obj)
                    except json.JSONDecodeError:
                        pass
                    
                    start = pos
                else:
                    break
                    
            except ValueError:
                break
        
        return json_objects if json_objects else None

async def send_to_gpt(page_data):
    """
    Enhanced GPT processing with improved prompt and error handling
    """
    json_template = {
        "Supplier Name": "",
        "Sold to Address": "",
        "Order Date": "",
        "Ship Date": "",
        "Invoice Number": "",
        "Shipping Address": "",
        "Total": 0,
        "List of Items": [
            {
                "Item Number": "",
                "Item Name": "",
                "Product Category": "",
                "Quantity Shipped": 1.0,
                "Extended Price": 1.0,
                "Quantity In a Case": 1.0,
                "Measurement Of Each Item": 1.0,
                "Measured In": "",
                "Total Units Ordered": 1.0,
                "Case Price": 0,
                "Catch Weight": "",
                "Priced By": "",
                "Splitable": "",
                "Split Price": "N/A",
                "Cost of a Unit": 1.0,
                "Currency": "",
                "Cost of Each Item":1.0
            }
        ]
    }
    system_message = """You are an expert invoice analysis AI specialized in wholesale produce invoices. Your task is to:
1. Extract structured information with 100% accuracy
2. Maintain data integrity across all fields
3. Apply standardized validation rules
4. Handle missing data according to specific rules
5. Ensure all calculations are precise and verified
6.Extract the all the items even it has duplicates and"""

    prompt = f"""
DETAILED INVOICE ANALYSIS INSTRUCTIONS:

1. HEADER INFORMATION
   Extract these specific fields:

   A. Basic Invoice Information
      • Supplier Name
        Headers to check:
        - "Vendor:", "Supplier:", "From:", "Sold By:"
        Rules:
        - Use FIRST supplier name found
        - Use EXACTLY same name throughout
        - Don't modify or formalize
      
      • Sold to Address
        Headers to check:
        - "Sold To:", "Bill To:", "Customer:"
        Format:
        - Complete address with all components
        - Include street, city, state, ZIP
      
      • Order Date
        Headers to check:
        - "Order Date:", "Date Ordered:", "PO Date:"
        Format: YYYY-MM-DD
      
      • Ship Date
        Headers to check:
        - "Ship Date:", "Delivery Date:", "Shipped:"
        Format: YYYY-MM-DD
      
      • Invoice Number
        Headers to check:
        - Search for "Invoice Numbers" in the text like "Invoice NO","Invoice No","Invoice Number","Invoice ID"
        - "Invoice #:", "Invoice Number:", "Invoice ID:"
        Rules:
        - Include all digits/characters
        - Keep leading zeros
      
      • Shipping Address
        Headers to check:
        - "Ship To:", "Deliver To:", "Destination:"
        Format:
        - Complete delivery address
        - All address components included
      
      • Total
        Headers to check:
        - "Total:", "Amount Due:", "Balance Due:"
        Rules:
        - Must match sum of line items
        - Include tax if listed
        - Round to 2 decimals

2. LINE ITEM DETAIL
    Extract the all the items even it has duplicates and
   Extract these fields for each item:

   A. Basic Item Information
      • Item Number
        Headers to check:
        -"Product Code:" -"Item Number:" -"SKU:" -"UPC:"
        Rules:
        - Keep full identifier
        - Include leading zeros
      
      • Item Name
        Headers to check:
        - "Description:", "Product:", "Item:"
        Rules:
        - Include full description with measeurement as well
        - Keep original format
      
      • Product Category
        Classify as:
        - PRODUCE: Fresh fruits/vegetables
        - DAIRY: Milk, cheese, yogurt
        - MEAT: Beef, pork, poultry
        - SEAFOOD: Fish, shellfish
        - Beverages: Sodas,juices,water
        - Dry Grocery: Chips, candy, nuts,Canned goods, spices, sauces
        - BAKERY: Bread, pastries, cakes
        - FROZEN: Ice cream, meals, desserts
        - paper goods and Disposables: Bags, napkins, plates, cups, utensils,packing materials
        - liquor: Beer, wine, spirits
        - Chemical: Soaps, detergents, supplies
        - OTHER: Anything not in above categories

   B. Quantity and Measurement Details
      • Quantity Shipped
        Headers to check:
        - "Qty:", "Quantity:", "Shipped:"
        Rules:
        - Must be positive number
        - Default to 1 if missing
      
      • Quantity In a Case
        Headers to check:
        - "Units/Case:", "Pack Size:", "Case Pack:"
        Patterns to check:
        -  24= "24 units"
        - "24/12oz" = 24 units
        - "2/12ct" = 24 units
        Default: 1 if not found
      
      • Measurement Of Each Item
        Headers to check:
        - "Size:", "Weight:", "Volume:"
        Extract from description:
        - "5 LB BAG" → 5
        - "32 OZ PKG" → 32
      

   B. Measurement Units:
      • Measured In - Standard Units:
        
        WEIGHT:
        - pounds: LB, LBS, #, POUND
        - ounces: OZ, OUNCE
        - kilos: KG, KILO
        - grams: G, GM, GRAM

        COUNT:
        - each: EA, PC, CT, COUNT, PIECE
        - case: CS, CASE, BX, BOX
        - dozen: DOZ, DZ
        - pack: PK, PACK, PKG
        - bundle: BDL, BUNDLE

        VOLUME:
        - gallons: GAL, GALLON
        - quarts: QT, QUART
        - pints: PT, PINT
        - fluid_ounces: FL OZ, FLOZ
        - liters: L, LT, LTR
        - milliliters: ML

        CONTAINERS:
        - cans: CN, CAN, #10 CAN
        - jars: JR, JAR
        - bottles: BTL, BOTTLE
        - containers: CTN, CONT
        - tubs: TB, TUB
        - bags: BG, BAG

        PRODUCE:
        - bunch: BN, BCH, BUNCH
        - head: HD, HEAD
        - basket: BSK, BASKET
        - crate: CRT, CRATE
        - carton: CRTN, CARTON
      
      • Total Units Ordered
        Calculate: Measurement of Each Item * Quantity In Case * Quantity Shipped
        Example: 5lb * 10 per case * 2 cases = 100 pounds

   C. Pricing Information
      • Extended Price
        Headers to check:
        - "Ext Price:", "Total:", "Amount:"
        Rules:
        - Must equal Case Price * Quantity Shipped
      
      • Case Price
        Headers to check:
        - "Unit Price:", 
        Rules:
        - Price for single Unit price 
      
      • Cost of a Unit
        Calculate: Extended Price ÷ Total Units Ordered
        Example: $100 ÷ 100 pounds = $1.00/lb
      
      • Currency
        Default: "USD" if not specified

      • Cost of Each Item
        Cost of Each Item is calculated by Cost of Each Item=Cost of a unit* Measurement of each item
        Verfiy by (Extended Price*Mesurement of each item)/Total Units Ordered
        Default: if not specified "N/A"
       

   D. Additional Attributes
      • Catch Weight:
        If the item number is same in the previous item and quantity shipped is different then set "YES" 
         else N/A

      
      • Priced By
       Look for the reference "Measured in" 
        Values:
        - "per pound"
        - "per case"
        - "per each"
        - "per dozen"
        - "per Ounce"
      
      • Splitable
        -Set "YES" if:
        -if you have "YES" reference to Splitable

        Set "NO" if:
        - if you have "NO" reference to Splitable

        Set "NO" if:
        - Bulk only
        - Single unit
      
      • Split Price
        If Splitable = "YES":
        - Calculate: Case Price ÷ Quantity In Case
        If Splitable = "NO":
        - Use "N/A"

3. VALIDATION RULES
   • Numeric Checks:
     - All quantities must be positive
     - All prices must be positive
     - Total must match sum of line items
   
   • Required Fields:
     - Supplier Name
     - Invoice Number
     - Total Amount
     - Item Name
     - Extended Price
   
   • Default Values:
     - Quantity: 1.0
     - Currency: "USD"
     - Split Price: "N/A"
     - Category: "OTHER"

OUTPUT FORMAT:
Return a JSON array containing each invoice as an object matching this template:
{json.dumps(json_template, indent=2)}INVOICE TEXT TO PROCESS:
{page_data}
"""


    try:
        # Attempt to process with GPT-4
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ],
            max_tokens=16000,
            temperature=0.1
        )
        
        content = response.choices[0].message.content
        content = remove_non_printable(content)
        try:
                # Remove markdown code blocks if present
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()
                
                parsed_data = json.loads(content)
                return parsed_data
        except json.JSONDecodeError as e2:
                
                # Third attempt: Try to extract structured data
                try:
                    # Extract everything between first { and last }
                    content = content[content.find('{'):content.rfind('}')+1]
                    parsed_data = json.loads(content)
                    return parsed_data
                except json.JSONDecodeError as e3:
                    return None
                
    except Exception as e:
        return None
        

def extract_text_and_tables_from_invoice(file_path):
    """Extract text and tables with improved table structure"""
    try:
        with open(file_path, "rb") as f:
            poller = document_analysis_client.begin_analyze_document(
                "prebuilt-layout", 
                f
            )
            result = poller.result()

        pages_content = {}
        
        # Process tables first to get their locations
        table_regions = {}
        for table in result.tables:
            page_num = table.bounding_regions[0].page_number
            if page_num not in table_regions:
                table_regions[page_num] = []
            table_regions[page_num].append(table.bounding_regions[0])
            
            if page_num not in pages_content:
                pages_content[page_num] = {'text': [], 'tables': []}
            
            # Process cells with better handling
            rows = {}
            for cell in table.cells:
                if cell.row_index not in rows:
                    rows[cell.row_index] = {}
                
                # Handle cell content and spans
                content = cell.content.strip()
                rows[cell.row_index][cell.column_index] = content
            
            # Convert to formatted string
            table_content = []
            for row_idx in sorted(rows.keys()):
                row = rows[row_idx]
                # Ensure all columns are present
                row_content = []
                for col_idx in range(table.column_count):
                    row_content.append(row.get(col_idx, ''))
                table_content.append('\t'.join(row_content))
            
            if table_content:
                pages_content[page_num]['tables'].append('\n'.join(table_content))

        # Then process text, excluding table regions
        for page in result.pages:
            page_num = page.page_number
            if page_num not in pages_content:
                pages_content[page_num] = {'text': [], 'tables': []}
            
            # Sort text by position
            lines_with_pos = []
            for line in page.lines:
                y_pos = min(p.y for p in line.polygon)
                x_pos = min(p.x for p in line.polygon)
                lines_with_pos.append((y_pos, x_pos, line.content))
            
            lines_with_pos.sort()
            pages_content[page_num]['text'] = [line[2] for line in lines_with_pos]


        return pages_content

    except Exception as e:

        raise

def format_page_content(page_data, page_num):
    """Format page content with improved structure"""
    content = []
    
    # Add page marker
    content.append(f"\n----- Page {page_num} Start -----\n")
    
    # Add text content with line numbers for better tracking
    content.append("TEXT CONTENT:")
    for line_num, line in enumerate(page_data['text'], 1):
        content.append(f"{line_num}:{line}")
    
    # Add tables with clear structure
    for table_idx, table in enumerate(page_data['tables']):
        content.append(f"\n----- Table {table_idx + 1} Start -----\n")
        
        # Split table into rows for better processing
        rows = table.split('\n')
        if rows:
            # Process header
            content.append(f"Header: {rows[0]}")
            
            # Process data rows with row numbers
            for row_idx, row in enumerate(rows[1:], 1):
                content.append(f"Row {row_idx}: {row}")
        
        content.append(f"----- Table {table_idx + 1} End -----\n")
    
    content.append(f"----- Page {page_num} End -----\n")
    
    return '\n'.join(content)
async def process_invoice_with_gpt(file_path):
    try:
        pages_content = extract_text_and_tables_from_invoice(file_path)
        all_invoices = []
        current_invoice = None
        
        # Process each page
        for page_num in sorted(pages_content.keys()):
            
            try:
                page_data = pages_content[page_num]
                
                
                # Format page content
                page_text = format_page_content(page_data, page_num)
                
                
                # Process in chunks if needed
                if len(page_text) > 16000:  # GPT-4o token limit safety
                   
                    # Handle large content
                    current_invoice = await handle_large_page(page_text, current_invoice, all_invoices)
                else:
                    # Send to GPT
                    page_result = await send_to_gpt(page_text)
                    
                    
                    # Process result
                    if page_result:
                        current_invoice = await process_page_result(page_result, current_invoice, all_invoices)
                
                await asyncio.sleep(1)  # Rate limiting
                
            except Exception as e:
                continue
        
        # Add last invoice
        if current_invoice:
            all_invoices.append(current_invoice)
        
        return all_invoices
        
    except Exception as e:
        return []

async def handle_large_page(page_text, current_invoice, all_invoices):
    """Handle pages that are too large for single processing"""
    # Split at table boundaries
    parts = re.split(r'(----- Table \d+ Start -----)', page_text)
    current_part = []
    
    for part in parts:
        if part.startswith('----- Table'):
            # Process accumulated content
            if current_part:
                result = await send_to_gpt('\n'.join(current_part))
                if result:
                    current_invoice =await process_page_result(result, current_invoice, all_invoices)
                current_part = []
        
        current_part.append(part)
    
    # Process last part
    if current_part:
        result = await send_to_gpt('\n'.join(current_part))
        if result:
            current_invoice = await process_page_result(result, current_invoice, all_invoices)
    
    return current_invoice

async def process_page_result(page_result, current_invoice, all_invoices):
    """Process a single page result"""
    if isinstance(page_result, list):
        for invoice in page_result:
            current_invoice = await merge_or_add_invoice(invoice, current_invoice, all_invoices)
    else:
        current_invoice = await merge_or_add_invoice(page_result, current_invoice, all_invoices)
    
    return current_invoice

async def merge_or_add_invoice(new_invoice, current_invoice, all_invoices):
    """Merge or add new invoice data - keeping all items including duplicates"""
    if not new_invoice:
        return current_invoice
        
    if current_invoice and new_invoice.get('Invoice Number') == current_invoice.get('Invoice Number'):
        # Simply append all items from new invoice
        current_invoice['List of Items'].extend(new_invoice.get('List of Items', []))
        
        # Update total if needed
        if new_invoice.get('Total', 0):
            current_invoice['Total'] = new_invoice.get('Total')
    else:
        if current_invoice:
            # Add current invoice to list before starting new one
            all_invoices.append(current_invoice)
        current_invoice = new_invoice
    
    return current_invoice
    
        
async def main(invoice_file_path):
    try:
        parsed_invoices = await process_invoice_with_gpt(invoice_file_path)
        
        if not parsed_invoices:
            print("No invoices were successfully parsed")
            return None
        print(f"Successfully processed {len(parsed_invoices)} invoices")
        
        return parsed_invoices
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        invoice_file_path = "C:/Users/rahul/Downloads/RESTAURANT DEPOT INVOICE 4.pdf"
        results = asyncio.run(main(invoice_file_path))
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)