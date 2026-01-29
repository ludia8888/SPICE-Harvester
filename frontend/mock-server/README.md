# Frontend Mock Server

Dev-only Express server that serves predictable `/api/v1` responses for the SPICE-Harvester frontend.

- Reads CSV fixtures from `frontend/mock-data/` (and `frontend/mock-data/clean/`).
- Default port: `3001` (override with `PORT`).

## Run

```bash
cd frontend/mock-server
npm ci
npm run start
```

Then point the frontend to it:

```bash
cd frontend
echo "VITE_API_BASE_URL=http://localhost:3001" > .env.local
echo "VITE_MOCK_API=false" >> .env.local
```

## Data

All fixture data under `frontend/mock-data/` is synthetic and intended for local development only.
