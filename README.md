# Australian Greyhound Race Panel

Fetches AU greyhound races for a selected date, displays them grouped by track, and evaluates Each Way value using TAB/TABTouch pricing from Racenet.

## Setup

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env   # Add your LADBROKES_API_KEY
python app.py
```

Then open http://localhost:5000 in your browser.

## Architecture

- **backend/app.py** — Flask app serving API + static HTML
- **backend/static/index.html** — Single-file frontend (no build step)
- **backend/services/ladbrokes_service.py** — Fetches races from Ladbrokes API
- **backend/services/racenet_service.py** — Scrapes TAB/TABTouch odds (swappable)
- **backend/services/calculations.py** — Implied probability + EW evaluation
