# hackathon-climat-2025

## API and Website

🚀 How to Use

Terminal 1 - Start the API:
```
cd hackathon-climat-2025
uv run uvicorn api.main:app --reload --port 8000
```
Terminal 2 - Start the Website:
```
cd hackathon-climat-2025/website
quarto preview --port 3000
```
Access Points:

- Website: http://localhost:3000
- API Docs: http://localhost:8000/docs
- API Health: http://localhost:8000/api/health