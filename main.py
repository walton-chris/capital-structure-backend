import os
import base64
import uuid
import json
import io
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import openai
import openpyxl

# Initialize FastAPI app
app = FastAPI(
    title="Capital Structure API",
    version="2.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")

# In-memory file storage
file_storage = {}

# Pydantic models
class Security(BaseModel):
    name: str
    shares_outstanding: float
    original_investment_per_share: float
    liquidation_preference_multiple: float
    seniority: Optional[int] = None
    is_participating: bool
    participation_cap_multiple: float
    cumulative_dividend_rate: float
    years_since_issuance: float

class CapitalStructureInput(BaseModel):
    securities: List[Security]
    total_option_pool_shares: float

class FileUploadRequest(BaseModel):
    file_content: str
    file_name: str

class DocumentExtractRequest(BaseModel):
    file_id: str

class DocumentUploadResponse(BaseModel):
    file_id: str
    file_name: str
    message: str
    file_size_bytes: int

# Excel parsing functions
def parse_excel_cap_table(file_bytes: bytes) -> Dict[str, Any]:
    """Parse Excel file and extract structured cap table data"""
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
        
        # Try to find the main cap table sheet
        cap_table_sheet = None
        option_ledger_sheet = None
        
        for sheet_name in workbook.sheetnames:
            if 'cap table' in sheet_name.lower() or 'detailed' in sheet_name.lower():
                cap_table_sheet = workbook[sheet_name]
            elif 'option' in sheet_name.lower() or 'ledger' in sheet_name.lower():
                option_ledger_sheet = workbook[sheet_name]
        
        # If no specific sheet found, use first sheet
        if cap_table_sheet is None:
            cap_table_sheet = workbook.worksheets[0]
        
        # Extract cap table data
        cap_table_data = extract_cap_table_structure(cap_table_sheet)
        
        # Extract option ledger data if exists
        option_data = None
        if option_ledger_sheet:
            option_data = extract_option_ledger(option_ledger_sheet)
        
        return {
            "cap_table": cap_table_data,
            "option_ledger": option_data,
            "source": "excel"
        }
    
    except Exception as e:
        raise Exception(f"Failed to parse Excel file: {str(e)}")

def extract_cap_table_structure(sheet) -> Dict[str, Any]:
    """Extract structured data from cap table sheet"""
    data = {
        "headers": [],
        "security_classes": {},
        "totals": {},
        "prices": {}
    }
    
    # Find header row (usually contains "Name", "Common", "Series", etc.)
    header_row = None
    for row_idx, row in enumerate(sheet.iter_rows(min_row=1, max_row=10), start=1):
        row_values = [cell.value for cell in row if cell.value]
        if any(val and ('Common' in str(val) or 'Series' in str(val)) for val in row_values):
            header_row = row_idx
            data["headers"] = [cell.value for cell in row]
            break
    
    if not header_row:
        return data
    
    # Find the rows with totals and prices (usually at bottom)
    for row in sheet.iter_rows(min_row=header_row + 1):
        row_label = str(row[0].value or '').strip().lower()
        
        # Extract total shares outstanding
        if 'total shares outstanding' in row_label or 'fully diluted shares' in row_label:
            for idx, cell in enumerate(row[1:], start=1):
                if cell.value and isinstance(cell.value, (int, float)) and cell.value > 0:
                    col_name = data["headers"][idx] if idx < len(data["headers"]) else f"Column_{idx}"
                    if col_name:
                        data["totals"][str(col_name)] = float(cell.value)
        
        # Extract price per share
        elif 'price per share' in row_label:
            for idx, cell in enumerate(row[1:], start=1):
                if cell.value:
                    col_name = data["headers"][idx] if idx < len(data["headers"]) else f"Column_{idx}"
                    if col_name:
                        # Clean price string
                        price_str = str(cell.value).replace('$', '').replace(',', '').strip()
                        try:
                            data["prices"][str(col_name)] = float(price_str)
                        except:
                            pass
        
        # Extract option pool
        elif 'available for issuance' in row_label or 'option pool' in row_label:
            for cell in row[1:]:
                if cell.value and isinstance(cell.value, (int, float)) and cell.value > 0:
                    data["option_pool_available"] = float(cell.value)
                    break
        
        # Extract outstanding options
        elif "option" in row_label and "outstanding" in row_label:
            for cell in row[1:]:
                if cell.value and isinstance(cell.value, (int, float)) and cell.value > 0:
                    data["options_outstanding"] = float(cell.value)
                    break
    
    return data

def extract_option_ledger(sheet) -> List[Dict[str, Any]]:
    """Extract option grants from option ledger sheet"""
    options = []
    
    # Find header row
    header_row = None
    headers = {}
    
    for row_idx, row in enumerate(sheet.iter_rows(min_row=1, max_row=10), start=1):
        row_values = [str(cell.value or '').lower() for cell in row]
        
        # Look for key column headers
        if any('exercise' in val and 'price' in val for val in row_values):
            header_row = row_idx
            
            # Map column names to indices
            for idx, cell in enumerate(row):
                col_name = str(cell.value or '').lower().strip()
                if 'outstanding' in col_name and 'option' in col_name:
                    headers['options_outstanding'] = idx
                elif 'granted' in col_name and 'option' in col_name:
                    headers['options_granted'] = idx
                elif 'exercise' in col_name and 'price' in col_name:
                    headers['exercise_price'] = idx
                elif col_name == 'id':
                    headers['id'] = idx
                elif 'name' in col_name or 'optionholder' in col_name:
                    headers['name'] = idx
            break
    
    if not header_row or 'options_outstanding' not in headers or 'exercise_price' not in headers:
        return []
    
    # Extract option data
    for row in sheet.iter_rows(min_row=header_row + 1):
        try:
            outstanding = row[headers['options_outstanding']].value
            exercise_price = row[headers['exercise_price']].value
            
            # Skip if no outstanding options or invalid data
            if not outstanding or not exercise_price:
                continue
            
            if isinstance(outstanding, (int, float)) and outstanding > 0:
                # Clean exercise price
                price_str = str(exercise_price).replace('$', '').replace(',', '').strip()
                price_float = float(price_str)
                
                option_entry = {
                    'options_outstanding': float(outstanding),
                    'exercise_price': price_float
                }
                
                # Add optional fields
                if 'id' in headers:
                    option_entry['id'] = row[headers['id']].value
                if 'name' in headers:
                    option_entry['name'] = row[headers['name']].value
                if 'options_granted' in headers:
                    granted = row[headers['options_granted']].value
                    if granted:
                        option_entry['options_granted'] = float(granted)
                
                options.append(option_entry)
        
        except (ValueError, TypeError, AttributeError):
            continue
    
    return options

# System prompt for OpenAI (simplified since we now send structured data)
EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst specializing in venture capital cap tables.

You will receive PRE-PROCESSED, STRUCTURED data from a cap table in JSON format. Your job is to convert this into the required output format.

INSTRUCTIONS:

1. **Security Classes:**
   - Use the "totals" field to get shares_outstanding for each security class
   - Use the "prices" field to get original_investment_per_share
   - Common stock always has price = $0.00

2. **Options - CRITICAL:**
   - You will receive a pre-processed "option_ledger" with individual grants
   - Group options by "exercise_price"
   - For each unique exercise price, sum the "options_outstanding" values
   - Create separate security entries named: "Options at $X.XX Exercise Price"
   - The shares_outstanding = sum of options_outstanding for that price
   - The original_investment_per_share = the exercise_price
   - DO NOT use "options_granted" - only use "options_outstanding"

3. **Option Pool:**
   - Use "option_pool_available" for total_option_pool_shares
   - This is shares available for future grants

4. **Seniority:**
   - If all preferred classes have liquidation_preference_multiple = 1.0, they are pari passu
   - All pari passu preferred = seniority 1
   - Common stock = seniority null
   - Options = seniority null

5. **Default Values:**
   - Unless specified: liquidation_preference_multiple = 1.0
   - Unless specified: is_participating = false
   - Unless specified: participation_cap_multiple = 0.0
   - Unless specified: cumulative_dividend_rate = 0.0
   - Unless specified: years_since_issuance = 0.0

Return ONLY valid JSON with NO markdown code blocks:
{
  "securities": [
    {
      "name": "Common Stock",
      "shares_outstanding": 8054469,
      "original_investment_per_share": 0.0,
      "liquidation_preference_multiple": 0.0,
      "seniority": null,
      "is_participating": false,
      "participation_cap_multiple": 0.0,
      "cumulative_dividend_rate": 0.0,
      "years_since_issuance": 0.0
    }
  ],
  "total_option_pool_shares": 1167233
}"""

# API endpoints
@app.get("/")
async def root():
    return {"message": "Capital Structure API", "version": "2.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/api/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(request: FileUploadRequest):
    """Upload a document for processing"""
    try:
        # Decode base64 content
        file_bytes = base64.b64decode(request.file_content)
        
        # Generate unique file ID
        file_extension = request.file_name.split('.')[-1].lower()
        file_id = f"upload_{uuid.uuid4()}.{file_extension}"
        
        # Store in memory
        file_storage[file_id] = {
            "content": file_bytes,
            "original_name": request.file_name,
            "size": len(file_bytes),
            "extension": file_extension
        }
        
        return DocumentUploadResponse(
            file_id=file_id,
            file_name=request.file_name,
            message="File uploaded successfully",
            file_size_bytes=len(file_bytes)
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Upload failed: {str(e)}")

@app.post("/api/documents/extract", response_model=CapitalStructureInput)
async def extract_data(request: DocumentExtractRequest):
    """Extract capital structure data from uploaded document"""
    try:
        # Get file from storage
        if request.file_id not in file_storage:
            raise HTTPException(status_code=404, detail="File not found")
        
        file_data = file_storage[request.file_id]
        file_bytes = file_data["content"]
        file_extension = file_data.get("extension", "txt")
        
        # Process based on file type
        if file_extension in ['xlsx', 'xls']:
            # Parse Excel file
            structured_data = parse_excel_cap_table(file_bytes)
            document_text = json.dumps(structured_data, indent=2)
        else:
            # Text file - use as-is
            document_text = file_bytes.decode("utf-8")
            structured_data = {"source": "text"}
        
        # Call OpenAI for extraction
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": f"Extract capital structure data from this {'structured Excel data' if file_extension in ['xlsx', 'xls'] else 'document'}:\n\n{document_text}"}
            ],
            temperature=0.1,
            max_tokens=3000
        )
        
        # Parse response
        extracted_json = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present
        if extracted_json.startswith("```"):
            extracted_json = extracted_json.split("```")[1]
            if extracted_json.startswith("json"):
                extracted_json = extracted_json[4:]
            extracted_json = extracted_json.strip()
        
        # Validate and return
        result = CapitalStructureInput.model_validate_json(extracted_json)
        return result
        
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse AI response: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
