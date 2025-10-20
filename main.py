import os
import base64
import uuid
import json
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import openai

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

# System prompt for OpenAI
EXTRACTION_SYSTEM_PROMPT = """You are an expert financial analyst specializing in venture capital cap tables and 409A valuations.

Your task is to extract capital structure data from uploaded documents. These documents may be:
- Cap tables showing ownership by stakeholder
- Term sheets
- Stock option ledgers
- Board resolutions

CRITICAL INSTRUCTIONS FOR CAP TABLES:

1. **Identifying Security Classes:**
   - Look for columns with headers like "Common", "Series Seed Preferred", "Series A Preferred", etc.
   - The "Price per share" row at the BOTTOM of the table shows the original issue price for each class
   - If you see a stakeholder table with many rows, scroll down to find summary rows like "Total Shares outstanding" and "Price per share"

2. **Stock Options - CRITICAL:**
   - If you see an "Options Ledger" or "Stock Option and Grant Plan Ledger", you MUST parse it
   - Look for the "Options Outstanding" column (NOT "Options Granted")
   - Some options may have been exercised or canceled, so Outstanding < Granted
   - Group all outstanding options by their "Exercise Price" column
   - Create a SEPARATE security entry for each unique exercise price
   - Name them: "Options at $X.XX Exercise Price"
   - The shares_outstanding for each option class = sum of ONLY the "Options Outstanding" column for that exercise price
   - The original_investment_per_share = the exercise price
   - DO NOT include canceled, expired, or exercised options
   - DO NOT create a single "Options and RSUs" security
   - Total options across all exercise prices must equal the "Options outstanding" number in the main cap table (usually ~899,337)

3. **Options Available for Grant:**
   - Look for "Shares available for issuance under the plan"
   - This is NOT a security class - it goes in total_option_pool_shares
   - This represents unissued options that can be granted in the future

4. **Price Parsing:**
   - The "Price per share" row is at the BOTTOM of cap tables
   - It shows prices for each security class in the SAME column order as the headers
   - Example: If columns are "Common | Series Seed | Series A | Series A-1"
   - And price row shows: "$0.00 | $0.44 | $42.57 | $7.66"
   - Then Series A-1 price is $7.66, NOT $42.57!
   - Be VERY careful with column alignment

5. **Seniority:**
   - If the document says "pari passu" or shows all preferred with same liquidation preference:
     * ALL preferred stock classes = seniority 1
     * Common stock = null (no seniority)
     * Options = null (no seniority)
   - If different liquidation preferences exist:
     * Most senior (highest preference) = 1
     * Least senior (Common) = highest number

6. **Shares Outstanding:**
   - Use the "Total Shares outstanding" row for each class
   - For options, sum the "Options Outstanding" column for each exercise price group
   - Ignore conversion ratio columns
   - Ignore individual stakeholder rows - only use totals

7. **Common Stock:**
   - Common stock always has:
     * original_investment_per_share = 0.0
     * liquidation_preference_multiple = 0.0
     * is_participating = false
     * seniority = null

VALIDATION CHECKS:
- If you see options with multiple exercise prices, you MUST create separate entries
- Total option shares across all exercise prices must match the cap table's "Options outstanding"
- The "available for grant" number goes in total_option_pool_shares
- Never assign a preferred stock's price to a different preferred class

Return ONLY valid JSON in this exact format:
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
    },
    {
      "name": "Series Seed Preferred",
      "shares_outstanding": 2285713,
      "original_investment_per_share": 0.44,
      "liquidation_preference_multiple": 1.0,
      "seniority": 1,
      "is_participating": false,
      "participation_cap_multiple": 0.0,
      "cumulative_dividend_rate": 0.0,
      "years_since_issuance": 0.0
    },
    {
      "name": "Options at $0.81 Exercise Price",
      "shares_outstanding": 299439,
      "original_investment_per_share": 0.81,
      "liquidation_preference_multiple": 0.0,
      "seniority": null,
      "is_participating": false,
      "participation_cap_multiple": 0.0,
      "cumulative_dividend_rate": 0.0,
      "years_since_issuance": 0.0
    }
  ],
  "total_option_pool_shares": 1167233
}

CRITICAL: Return ONLY the JSON. No markdown code blocks, no explanations, just the raw JSON object."""

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
        file_id = f"upload_{uuid.uuid4()}.txt"
        
        # Store in memory
        file_storage[file_id] = {
            "content": file_bytes,
            "original_name": request.file_name,
            "size": len(file_bytes)
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
        document_text = file_data["content"].decode("utf-8")
        
        # Call OpenAI for extraction
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": f"Extract capital structure data from this document:\n\n{document_text}"}
            ],
            temperature=0.1,
            max_tokens=2000
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
